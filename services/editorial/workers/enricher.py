import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime, timezone
from typing import List, Tuple, Dict
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, selectinload

# Models
from news_events_lib.models import ArticleModel, JobStatus, ArticleContentModel
from core.models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from core.base_worker import BaseQueueWorker

# IMPORT DOMAIN LOGIC
from domain.enriching import ContentEnricherDomain


class NewsEnricherWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        # Instantiate Domain Logic (Manages ProcessPool)
        self.domain = ContentEnricherDomain(max_cpu_workers=2, http_concurrency=20)

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.ENRICH,
            batch_size=20, 
            pending_status=JobStatus.PENDING,
        )

    async def run(self):
        # We override run only to call warmup, then delegate to base
        logger.info(f"🚀 Miner Worker started.")
        self.domain.warmup()
        await super().run()

    def _fetch_jobs(self, session, limit=None):
        """
        Override standard fetch to prioritize 'Unknown' titles.
        """
        fetch_limit = limit or self.batch_size
        stmt = (
            select(ArticlesQueueModel, ArticleModel)
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .options(selectinload(ArticleModel.contents))
            .where(ArticlesQueueModel.status == self.pending_status)
            .where(ArticlesQueueModel.queue_name == self.queue_name)
            .order_by(ArticlesQueueModel.created_at.asc())
            .with_for_update(skip_locked=True)
        )
        
        # 1. Prioritize articles with missing titles
        rows = session.execute(
            stmt.where(
                ArticleModel.title.in_(["unknown", "no title", "Unknown", "No Title"])
            ).limit(fetch_limit)
        ).all()
        
        # 2. Fill remainder with standard articles
        if len(rows) < fetch_limit:
            rows.extend(
                session.execute(
                    stmt.where(
                        ArticleModel.title.not_in(
                            ["unknown", "no title", "Unknown", "No Title"]
                        )
                    ).limit(fetch_limit - len(rows))
                ).all()
            )

        jobs = []
        for queue_item, article in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            queue_item.article = article
            jobs.append(queue_item)

        session.commit()
        return jobs

    async def process_items(self, session, jobs):
        """
        Standard BaseWorker hook. Replaces the custom loop from the old Enricher.
        """
        if not jobs:
            return

        # 1. Run CPU Enrichment (Fetch, Parse, Vectorize)
        results = await self.domain.run_cpu_enrichment(jobs)

        # 2. Process transitions
        count_success = 0
        count_fail = 0
        count_drops = 0
        
        for job, res in results:
            # Re-attach to current session context
            job = session.merge(job)
            status = res["status"]

            if status == "success":
                self._apply_data(job, res)
                
                if res.get("status") == "boomerang":
                    # Title found for previously unknown article -> Re-filter
                    job.status = JobStatus.PENDING
                    job.queue_name = ArticlesQueueName.FILTER
                    job.msg = "Boomerang: Title Found"
                    logger.info(f"🪃 Boomerang: {job.article.title[:30]}")
                else:
                    # Success -> Hand off to Analyst (LLM)
                    job.status = JobStatus.PENDING
                    job.queue_name = ArticlesQueueName.ANALYZE
                    count_success += 1

            elif status == "archived":
                job.status = JobStatus.COMPLETED
                job.msg = res.get("stop_reason")
                count_drops += 1

            elif status == "failed":
                job.status = JobStatus.FAILED
                job.msg = res.get("stop_reason")
                count_fail += 1

        logger.info(f"⛏️  Batch Processed: {count_success} Mined, {count_fail} Failed, {count_drops} Archived")

    def _apply_data(self, job, res):
        """
        Maps extracted miner data to the article model. 
        Removed LLM output mapping (that belongs to Analyst).
        """
        art = job.article

        if not art.contents:
            art.contents = [ArticleContentModel(content=res["content"])]

        if res.get("title") and (
            not art.title or art.title.lower() in ["unknown", "no title"]
        ):
            art.title = res["title"]

        if res.get("subtitle"):
            art.subtitle = res["subtitle"]

        if res["published_date"]:
            art.published_date = res["published_date"]

        art.embedding = res["embedding"]

if __name__ == "__main__":
    from multiprocessing import freeze_support

    freeze_support()

    worker = NewsEnricherWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")
        _ = worker.domain.shutdown()