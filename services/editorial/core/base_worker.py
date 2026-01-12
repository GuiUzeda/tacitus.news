# /home/guzeda/Documents/Projects/in_parcial/common/base_worker.py
import asyncio
from abc import ABC, abstractmethod
from typing import Union
from loguru import logger
from sqlalchemy import select, update
from datetime import datetime, timezone, timedelta

from news_events_lib.models import JobStatus


class BaseQueueWorker(ABC):
    def __init__(
        self, 
        session_maker, 
        queue_model, 
        target_queue_name, 
        batch_size=10, 
        pending_status: Union[JobStatus, str] = JobStatus.PENDING,
        processing_status: Union[JobStatus, str] = JobStatus.PROCESSING,
        stuck_threshold_minutes=60,
        cleanup_interval_minutes=15
    ):
        self.SessionLocal = session_maker
        self.QueueModel = queue_model
        self.queue_name = target_queue_name
        self.batch_size = batch_size
        self.pending_status = pending_status
        self.processing_status = processing_status
        self.stuck_threshold_minutes = stuck_threshold_minutes
        self.cleanup_interval_minutes = cleanup_interval_minutes
        self.last_cleanup_time = datetime.now(timezone.utc)

    async def run(self):
        logger.info(f"🚀 Worker started for queue: {self.queue_name}")
        while True:
            try:
                # Periodic cleanup of stuck jobs
                self._check_cleanup()

                count = await self.process_batch()
                if count == 0:
                    logger.debug(f"Queue {self.queue_name} empty. Sleeping...")
                    await asyncio.sleep(30)
            except Exception as e:
                logger.critical(f"Worker crashed: {e}")
                await asyncio.sleep(30)

    async def process_batch(self):
        with self.SessionLocal() as session:
            # 1. Fetch & Lock
            jobs = self._fetch_jobs(session)
            if not jobs: return 0
            
            # 2. Process
            try:
                # Note: Subclasses can override process_items for batch logic
                await self.process_items(session, jobs)
                session.commit()
            except Exception as e:
                logger.critical(f"Batch processing failed: {e}")
                session.rollback()
                # Attempt to mark all as failed to avoid stuck jobs
                for job in jobs:
                    job.status = JobStatus.FAILED
                    job.msg = f"Batch Crash: {str(e)[:50]}"
                session.commit()
                
            return len(jobs)

    def _fetch_jobs(self, session):
        # Standard locking logic
        stmt = (
            select(self.QueueModel)
            .where(self.QueueModel.status == self.pending_status, self.QueueModel.queue_name == self.queue_name)
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        jobs = session.execute(stmt).scalars().all()
        for job in jobs: 
            job.status = self.processing_status
            job.updated_at = datetime.now(timezone.utc)
        session.commit()
        return jobs

    def _check_cleanup(self):
        """Checks if it's time to clean up stuck jobs."""
        if datetime.now(timezone.utc) - self.last_cleanup_time > timedelta(minutes=self.cleanup_interval_minutes):
            try:
                with self.SessionLocal() as session:
                    self._cleanup_stuck_jobs(session)
                self.last_cleanup_time = datetime.now(timezone.utc)
            except Exception as e:
                logger.error(f"Cleanup failed: {e}")

    def _cleanup_stuck_jobs(self, session):
        threshold = datetime.now(timezone.utc) - timedelta(minutes=self.stuck_threshold_minutes)
        
        stmt = update(self.QueueModel).where(
            self.QueueModel.status == self.processing_status,
            self.QueueModel.updated_at < threshold
        )
        
        if self.queue_name:
            stmt = stmt.where(self.QueueModel.queue_name == self.queue_name)
            
        # Handle different column names for messages
        values = {"status": self.pending_status}
        if hasattr(self.QueueModel, "msg"): values["msg"] = "Auto-Reset: Stuck"
        elif hasattr(self.QueueModel, "reasoning"): values["reasoning"] = "Auto-Reset: Stuck"

        res = session.execute(stmt.values(**values))
        session.commit()
        if res.rowcount > 0:
            logger.warning(f"🧹 Reset {res.rowcount} stuck jobs in {self.queue_name or self.QueueModel.__tablename__}")

    async def process_items(self, session, jobs):
        """
        Default implementation iterates and calls process_item.
        Override this for batch processing (e.g. LLM batch calls).
        """
        for job in jobs:
            try:
                await self.process_item(session, job)
                # If status wasn't changed by process_item, mark as completed
                if job.status == self.processing_status:
                    job.status = JobStatus.COMPLETED
            except Exception as e:
                logger.error(f"Job {job.id} failed: {e}")
                job.status = JobStatus.FAILED
                job.msg = str(e)
            job.updated_at = datetime.now(timezone.utc)

    async def process_item(self, session, job):
        """Implement specific logic here for single-item processing"""
        pass
