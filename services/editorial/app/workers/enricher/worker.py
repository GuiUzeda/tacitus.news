import asyncio

from app.config import Settings
from app.utils.article_manager import ArticleManager
from app.workers.base_worker import BaseQueueWorker
from app.workers.enricher.domain import ContentEnricherDomain, EnrichmentStatus
from loguru import logger
from news_events_lib.audit import receive_after_flush  # noqa: F401

# Models
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    JobStatus,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import selectinload, sessionmaker, undefer


class NewsEnricherWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        self.domain = ContentEnricherDomain()

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.ENRICHER,
            batch_size=10,
            pending_status=JobStatus.PENDING,
        )

    def _fetch_jobs(self, session):
        stmt = (
            select(ArticlesQueueModel)
            .options(
                selectinload(ArticlesQueueModel.article).options(
                    undefer(ArticleModel.content)
                ),
                selectinload(ArticlesQueueModel.article).selectinload(
                    ArticleModel.newspaper
                ),
            )
            .where(
                ArticlesQueueModel.status == self.pending_status,
                ArticlesQueueModel.queue_name == self.queue_name,
            )
            .order_by(ArticlesQueueModel.created_at.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        jobs = session.execute(stmt).scalars().all()

        for job in jobs:
            job.status = self.processing_status

        session.flush()
        return list(jobs)

    async def process_item(self, session, job):
        article: ArticleModel = job.article
        was_untitled = article.title == "Unknown"

        # 1. Run Domain Logic
        result = await self.domain.enrich_article(session, article)

        # 3. Handle Result
        if result.status == EnrichmentStatus.SUCCESS:
            next_queue = ArticlesQueueName.ANALYZER
            if was_untitled:
                next_queue = ArticlesQueueName.FILTER
            next_job = ArticleManager.create_article_queue(
                session, article.id, next_queue, reason="Enrichment Complete"
            )
            session.add(next_job)

            job.status = JobStatus.COMPLETED
            job.msg = result.reason
            logger.debug(
                f"Success: {article.title[:10]} - {article.published_date} - {result.reason} - {'Back to filter' if was_untitled else 'To Analyzer'}"
            )

        elif result.status == EnrichmentStatus.ARCHIVED:
            job.status = JobStatus.COMPLETED
            job.msg = f"Archived: {result.reason}"
            logger.debug(
                f"Archived: {article.title[:10]} - {article.published_date} - {result.reason}"
            )

        elif result.status == EnrichmentStatus.FAILED:
            job.status = JobStatus.FAILED
            job.msg = result.reason
            logger.debug(
                f"Failed: {article.title[:10]} - {article.published_date} - {result.reason}"
            )


if __name__ == "__main__":
    worker = NewsEnricherWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")
