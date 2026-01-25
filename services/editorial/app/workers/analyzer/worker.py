import asyncio
from datetime import datetime, timezone

from app.config import Settings
from app.utils.article_manager import ArticleManager
from app.workers.analyzer.domain import AnalystStatus, ContentAnalystDomain
from app.workers.base_worker import BaseArticleQueueWorker
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


class NewsAnalystWorker(BaseArticleQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        self.domain = ContentAnalystDomain()

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.ANALYZER,
            batch_size=2,
            pending_status=JobStatus.PENDING,
        )

    async def process_batch(self) -> int:
        with self.SessionLocal() as session:
            # 1. Fetch
            jobs = self._fetch_jobs(session)
            if not jobs:
                return 0

            # 2. Prepare Data
            texts = []
            valid_jobs = []
            cutoff_date = datetime.now(timezone.utc) - self.settings.cutoff_period
            for job in jobs:
                content = job.article.content
                if not content or len(content) < 50:
                    job.status = JobStatus.FAILED
                    job.msg = "Content too short/missing"
                    # We fail the article analysis itself too
                    job.article.summary_status = JobStatus.FAILED
                    logger.info(
                        f"Failed: {job.article.title[:10]} - {job.article.published_date} - {job.msg}"
                    )
                    continue
                if job.article.summary_status == JobStatus.COMPLETED:
                    job.status = JobStatus.COMPLETED
                    job.msg = "Already Analyzed"
                    logger.info(
                        f"Ignored: {job.article.title[:10]} - {job.article.published_date} - {job.msg}"
                    )
                    continue
                if (
                    job.article.published_date is not None
                    and job.article.published_date < cutoff_date
                ):
                    job.status = JobStatus.COMPLETED
                    job.msg = "Article Too Old"
                    logger.info(
                        f"Dropped: {job.article.title[:10]} - {job.article.published_date} - {job.msg}"
                    )
                    continue
                if not job.article.published_date:
                    job.status = JobStatus.FAILED
                    job.msg = "Missing Published Date"
                    logger.info(
                        f"Dropped: {job.article.title[:10]} - {job.article.published_date} - {job.msg}"
                    )
                    continue

                clean_content = content[:15000]
                context_str = (
                    f"Title: {job.article.title}\n"
                    f"Date: {job.article.published_date}\n"
                    f"Content: {clean_content}"
                )
                texts.append(context_str)
                valid_jobs.append(job)

            if not valid_jobs:
                session.commit()
                return len(jobs)

            # 3. Call Domain
            try:
                results = await self.domain.analyze_batch(texts)
            except Exception as e:
                # Batch-level failure (e.g. API Down)
                # We fail all jobs in this batch so they retry later
                session.rollback()  # Undo the 'processing' status set in fetch
                logger.opt(exception=True).error(f"Batch Failed: {e}")
                return 0
            count_success = 0
            count_ignores = 0
            count_failed = 0

            # 4. Process Results
            for job, result in zip(valid_jobs, results):
                try:
                    with session.begin_nested():
                        now = datetime.now(timezone.utc)
                        job.updated_at = now

                        # Always mark that we attempted a summary
                        job.article.summary_date = now

                        # --- CASE 1: SUCCESS ---
                        if result.status == AnalystStatus.SUCCESS and result.data:
                            # 1. Map Data
                            self._apply_llm_data(job.article, result.data)

                            # 2. Update Article Status
                            job.article.summary_status = JobStatus.COMPLETED

                            # 3. Route to CLUSTER
                            next_job = ArticleManager.create_article_queue(
                                session,
                                job.article.id,
                                ArticlesQueueName.CLUSTER,
                                "Analyzed Successfully",
                            )
                            session.add(next_job)

                            # 4. Complete Job
                            job.status = JobStatus.COMPLETED
                            job.attempts += 1
                            job.msg = result.reason
                            count_success += 1

                        # --- CASE 2: IRRELEVANT (Archive) ---
                        elif result.status == AnalystStatus.IRRELEVANT:
                            # 1. Update Article Status (REJECTED = Archived/Ignored)
                            job.article.summary_status = JobStatus.REJECTED

                            # 2. Complete Job (We are done with this item)
                            job.status = JobStatus.COMPLETED
                            job.attempts += 1
                            job.msg = result.reason
                            count_ignores += 1

                        # --- CASE 3: ERROR (Retry) ---
                        else:  # AnalystStatus.ERROR
                            # 1. Fail Job (Will trigger retry logic if handled by base,
                            # but here we set explicit Failed state to be safe)
                            job.status = JobStatus.FAILED
                            job.attempts += 1
                            job.msg = result.reason

                            # 2. Article Status
                            # We leave it as PENDING or set to FAILED?
                            # Usually keeping it PENDING allows the next retry to pick it up cleanly
                            # if we rely on the job status.
                            # But let's set FAILED to indicate the *last attempt* failed.
                            job.article.summary_status = JobStatus.FAILED
                            count_failed += 1

                except Exception as e:
                    logger.opt(exception=True).error(
                        f"Save Error for Job {job.id}: {e}"
                    )
                    job.status = JobStatus.FAILED
                    job.msg = f"Save Error: {e}"
            logger.info(
                f"Analyzed {len(valid_jobs)}/{len(jobs)}: {count_success} success, {count_ignores} ignored, {count_failed} failed"
            )
            session.commit()
            return len(jobs)

    def _apply_llm_data(self, article: ArticleModel, data):
        """Maps LLM Schema to ArticleModel fields."""
        article.summary = data.summary
        article.stance = data.stance
        article.key_points = data.key_points
        article.interests = data.entities

        flat_list = []
        for group in data.entities.values():
            if isinstance(group, list):
                flat_list.extend(group)
        article.entities = list(set(flat_list))

        article.main_topics = data.main_topics
        article.stance_reasoning = data.stance_reasoning
        article.clickbait_score = data.clickbait_score
        article.clickbait_reasoning = data.clickbait_reasoning

        if data.title:
            article.title = data.title
        if data.subtitle:
            article.subtitle = data.subtitle

    def _fetch_jobs(self, session):
        stmt = (
            select(ArticlesQueueModel)
            .options(
                selectinload(ArticlesQueueModel.article).options(
                    undefer(ArticleModel.content)
                )
            )
            .join(ArticlesQueueModel.article)
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
        pass


if __name__ == "__main__":
    worker = NewsAnalystWorker()
    asyncio.run(worker.run())
