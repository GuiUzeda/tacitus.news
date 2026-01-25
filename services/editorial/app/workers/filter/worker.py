import asyncio
from datetime import datetime, timezone
from typing import List

from app.config import Settings
from app.utils.article_manager import ArticleManager
from app.workers.base_worker import BaseQueueWorker

# Domain & Utils
from app.workers.filter.domain import NewsFilterDomain
from loguru import logger
from news_events_lib.audit import receive_after_flush  # noqa: F401

# Models
from news_events_lib.models import ArticlesQueueModel, ArticlesQueueName, JobStatus
from sqlalchemy import create_engine, select
from sqlalchemy.orm import contains_eager, sessionmaker


class NewsFilterWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        self.domain = NewsFilterDomain()

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.FILTER,
            batch_size=50,  # Optimized for Batch LLM Call
            pending_status=JobStatus.PENDING,
        )

    async def process_batch(self) -> int:
        """
        Overrides BaseQueueWorker.process_batch because Filter requires
        BULK processing (sending all titles to LLM at once).
        """
        with self.SessionLocal() as session:
            # 1. Fetch
            jobs = self._fetch_jobs(session)
            if not jobs:
                return 0

            # 2. Pre-Flight Checks & Data Prep
            titles = []
            valid_jobs = []
            drop_count = 0
            fail_count = 0
            # We filter out old articles immediately without LLM cost
            cutoff_date = (
                datetime.now(timezone.utc) - self.settings.cutoff_period
            )  # 2 days is safer than 1

            for job in jobs:
                # Rule: Drop if too old
                if (
                    job.article.published_date
                    and job.article.published_date.replace(tzinfo=timezone.utc)
                    < cutoff_date
                ):
                    job.status = JobStatus.COMPLETED
                    job.msg = "Dropped: Article Too Old"
                    drop_count += 1
                    logger.info(
                        f"Dropped: {job.article.title[:10]} - {job.article.published_date}"
                    )

                # Rule: Must have title
                elif job.article and job.article.title:
                    titles.append(job.article.title)
                    valid_jobs.append(job)

                # Fallback: Data Error
                else:
                    job.status = JobStatus.FAILED
                    job.msg = "Data Error: Missing Article or Title"
                    fail_count += 1

            # If no valid jobs remain after pre-flight, just commit the drops and exit
            if not valid_jobs:
                session.commit()
                return len(jobs)

            # 3. Call Domain (Logic)
            try:
                # Returns [True, False, True...] corresponding to valid_jobs
                decisions = await self.domain.filter_batch(titles)
            except Exception as e:
                logger.error(f"Batch LLM Failed: {e}")
                session.rollback()
                return 0

            # 4. Process Results (Controller Logic)
            success_count = 0
            approve_count = 0
            rejection_count = 0
            analyzer_count = 0
            for job, is_approved in zip(valid_jobs, decisions):
                try:
                    # Use Nested Transaction per item for safety
                    with session.begin_nested():
                        now = datetime.now(timezone.utc)
                        job.updated_at = now
                        job.attempts += 1

                        if is_approved:
                            # Routing Logic
                            next_queue = ArticlesQueueName.ENRICHER

                            if job.article.summary_status == JobStatus.WAITING:
                                next_queue = ArticlesQueueName.ANALYZER
                                analyzer_count += 1

                            # Create Next Job
                            new_job = ArticleManager.create_article_queue(
                                session,
                                job.article.id,
                                next_queue,
                                "Approved by Filter",
                            )
                            session.add(new_job)

                            job.status = JobStatus.COMPLETED
                            job.msg = "Approved"
                            approve_count += 1
                        else:
                            job.status = JobStatus.COMPLETED
                            job.msg = "Rejected by Filter"
                            rejection_count += 1

                    success_count += 1

                except Exception as e:
                    logger.opt(exception=True).error(f"Error saving job {job.id}: {e}")
                    job.status = JobStatus.FAILED
                    job.msg = f"Save Error: {e}"
                    job.attempts += 1

            # 5. Commit All (Drops + Approved + Rejected)
            session.commit()

            logger.info(
                f"Filter Batch: ({success_count}S|{drop_count}D|{fail_count}F)/{len(jobs)} processed "
                f"({approve_count} Approved, {rejection_count} Rejected) - {analyzer_count} to Analyzer"
            )
            return len(jobs)

    def _fetch_jobs(self, session) -> List[ArticlesQueueModel]:
        stmt = (
            select(ArticlesQueueModel)
            .join(ArticlesQueueModel.article)
            .options(contains_eager(ArticlesQueueModel.article))
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
        # Not used because we override process_batch
        pass


if __name__ == "__main__":
    worker = NewsFilterWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")
