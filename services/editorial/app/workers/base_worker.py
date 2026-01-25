import abc
import asyncio
import os
import signal
from datetime import datetime, timedelta, timezone
from typing import Any, List, Type

from app.config import Settings
from loguru import logger

# Models
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueName,
    EventsQueueName,
    JobStatus,
)
from sqlalchemy import select, update
from sqlalchemy.orm import Session, sessionmaker


class BaseQueueWorker(abc.ABC):
    def __init__(
        self,
        session_maker: sessionmaker,
        queue_model: Type,
        target_queue_name: ArticlesQueueName | EventsQueueName | None,
        batch_size: int = 10,
        pending_status: JobStatus = JobStatus.PENDING,
    ):
        self.SessionLocal = session_maker
        self.QueueModel = queue_model
        self.queue_name = target_queue_name
        self.batch_size = batch_size
        self.pending_status = pending_status
        self.processing_status = JobStatus.PROCESSING
        self.settings = Settings()

        # Graceful Shutdown Flag
        self.should_exit = False

    def _register_signals(self):
        """Register signal handlers for graceful shutdown."""
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._handle_exit_signal)
            except NotImplementedError:
                # Fallback for Windows or non-main threads (rare in this setup)
                signal.signal(sig, lambda s, f: self._handle_exit_signal())

    def _handle_exit_signal(self):
        logger.warning(
            f"🛑 Received Exit Signal (PID {os.getpid()}). Stopping gracefully..."
        )
        self.should_exit = True

    async def run(self):
        """
        Main Worker Loop with Signal Handling.
        """
        self._register_signals()
        logger.info(
            f"🚀 Worker for '{self.queue_name}' started. Batch Size: {self.batch_size} (PID: {os.getpid()})"
        )

        while not self.should_exit:
            try:
                # Run one batch cycle
                processed_count = await self.process_batch()

                # If idle, sleep safely (checking for exit signal)
                if processed_count == 0:
                    logger.info("💤 No Jobs to Process. Sleeping...")
                    for _ in range(60):  # Sleep 60s total, checking every 1s
                        if self.should_exit:
                            break
                        await asyncio.sleep(1)

            except Exception as e:
                # This catches critical crashes (DB down, etc.)
                logger.opt(exception=True).critical(f"🔥 Critical Worker Crash: {e}")
                await asyncio.sleep(10)

        logger.success(f"👋 Worker '{self.queue_name}' stopped.")

    async def process_batch(self) -> int:
        """
        Fetches a batch of jobs and processes them sequentially with failure isolation.
        Handles graceful shutdown by re-queuing pending jobs in the batch.
        """
        with self.SessionLocal() as session:
            # 1. Fetch & Lock Batch
            jobs = self._fetch_jobs(session)
            if not jobs:
                return 0

            success_count = 0
            fail_count = 0
            requeue_count = 0

            # 2. Process Items Sequentially
            for job in jobs:
                # --- GRACEFUL SHUTDOWN CHECK ---
                if self.should_exit:
                    # Mark remaining jobs in this batch as PENDING so they are not stuck in PROCESSING
                    logger.warning(f"🛑 Shutdown: Re-queuing Job {job.id}")
                    job.status = JobStatus.PENDING
                    job.msg = "Worker Shutdown Re-queue"
                    requeue_count += 1
                    continue  # Skip processing, let the commit below save the PENDING state

                try:
                    # Create a SAVEPOINT. If process_item fails, we roll back ONLY this job.
                    with session.begin_nested():

                        # Call Child Implementation
                        await self.process_item(session, job)

                        # Logic Succeeded:
                        # Update metadata (if not already set by child)
                        job.updated_at = datetime.now(timezone.utc)
                        job.attempts += 1

                        # Auto-Complete if the child didn't set a specific status (like FAILED or REJECTED)
                        if job.status == self.processing_status:
                            job.status = JobStatus.COMPLETED

                    success_count += 1

                except Exception as e:
                    # Rollback happens automatically for the nested transaction on exit
                    fail_count += 1
                    logger.opt(exception=True).error(
                        f"❌ Job {job.id} Failed: {str(e)[:200]}"
                    )

                    # Mark as Failed in the main transaction
                    job.status = JobStatus.FAILED
                    job.msg = f"Worker Error: {str(e)[:500]}"
                    job.updated_at = datetime.now(timezone.utc)
                    job.attempts += 1

            # 3. Commit the Batch (Writes Successes, Failures, and Re-queues)
            try:
                session.commit()
                if success_count > 0 or fail_count > 0 or requeue_count > 0:
                    logger.info(
                        f"✅ Batch: {success_count} OK, {fail_count} Fail, {requeue_count} Re-queued"
                    )
            except Exception as e:
                logger.opt(exception=True).critical(f"💥 Commit Failed: {e}")
                session.rollback()
                return 0

            return len(jobs)

    def _fetch_jobs(self, session: Session) -> List[Any]:
        """
        Fetches jobs using SKIP LOCKED to support horizontal scaling.
        Can be overridden by child classes if complex joins are needed.
        """
        stmt = (
            select(self.QueueModel)
            .where(
                self.QueueModel.status == self.pending_status,
                self.QueueModel.queue_name == self.queue_name,
            )
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        jobs = session.execute(stmt).scalars().all()

        # Mark as Processing immediately so they are "claimed" in this transaction
        for job in jobs:
            job.status = self.processing_status

        # Flush sends the UPDATE to the DB (locks the rows) without committing
        session.flush()
        return list(jobs)

    @abc.abstractmethod
    async def process_item(self, session: Session, job):
        """
        Child classes must implement this.
        Rules:
        1. Do NOT commit. The BaseWorker commits.
        2. Raise an exception to fail the job.
        3. Return normally to complete the job.
        """
        pass


class BaseArticleQueueWorker(BaseQueueWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Track when we last cleaned up
        self.last_cleanup_time = datetime.min.replace(tzinfo=timezone.utc)
        self.cleanup_interval = timedelta(minutes=60)  # Run only once per hour

    async def run(self):
        """
        Main Worker Loop with Cleanup and Signal Handling.
        """
        self._register_signals()
        logger.info(
            f"🚀 Worker for '{self.queue_name}' started. Batch Size: {self.batch_size} (PID: {os.getpid()})"
        )

        while not self.should_exit:
            try:
                # 1. Periodically Clean Old Articles (Non-Blocking Check)
                now = datetime.now(timezone.utc)
                if now - self.last_cleanup_time > self.cleanup_interval:
                    self.clear_old_articles()
                    self.last_cleanup_time = now

                # 2. Run one batch cycle
                processed_count = await self.process_batch()

                # 3. Smart Sleep
                if processed_count == 0:
                    logger.info("💤 No Jobs to Process. Sleeping...")
                    # Safe Sleep Loop
                    for _ in range(60):
                        if self.should_exit:
                            break
                        await asyncio.sleep(1)

            except Exception as e:
                # This catches critical crashes (DB down, etc.)
                logger.opt(exception=True).critical(f"🔥 Critical Worker Crash: {e}")
                await asyncio.sleep(10)

        logger.success(f"👋 Worker '{self.queue_name}' stopped.")

    def clear_old_articles(self):
        """
        Marks pending jobs as COMPLETED if the article is older than the cutoff.
        """
        cutoff_date = datetime.now(timezone.utc) - self.settings.cutoff_period

        try:
            with self.SessionLocal() as session:
                # 1. Find articles that are too old
                subquery = (
                    select(ArticleModel.id)
                    .where(ArticleModel.published_date < cutoff_date)
                    .scalar_subquery()
                )

                # 2. Update the Queue rows that point to those articles
                stmt = (
                    update(self.QueueModel)
                    .where(
                        self.QueueModel.status == JobStatus.PENDING,
                        self.QueueModel.queue_name == self.queue_name,
                        self.QueueModel.article_id.in_(subquery),
                    )
                    .values(
                        status=JobStatus.COMPLETED,
                        msg="Article Too Old (Auto-Cleared)",
                        updated_at=datetime.now(timezone.utc),
                    )
                )

                result = session.execute(stmt)
                session.commit()

                if result.rowcount > 0:
                    logger.warning(
                        f"🧹 Auto-Cleared {result.rowcount} old articles from {self.queue_name}"
                    )
                else:
                    logger.info("🧹 Cleanup run: No old articles found.")

        except Exception as e:
            logger.opt(exception=True).error(f"Failed to clear old articles: {e}")
