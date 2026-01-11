# /home/guzeda/Documents/Projects/in_parcial/common/base_worker.py
import asyncio
from abc import ABC, abstractmethod
from loguru import logger
from sqlalchemy import select
from datetime import datetime, timezone

from news_events_lib.models import JobStatus


class BaseQueueWorker(ABC):
    def __init__(self, session_maker, queue_model, target_queue_name, batch_size=10, pending_status=JobStatus.PENDING):
        self.SessionLocal = session_maker
        self.QueueModel = queue_model
        self.queue_name = target_queue_name
        self.batch_size = batch_size
        self.pending_status = pending_status

    async def run(self):
        logger.info(f"🚀 Worker started for queue: {self.queue_name}")
        while True:
            try:
                count = await self.process_batch()
                if count == 0:
                    # logger.debug(f"Queue {self.queue_name} empty. Sleeping...")
                    return 
            except Exception as e:
                logger.critical(f"Worker crashed: {e}")
                await asyncio.sleep(30)

    async def process_batch(self):
        with self.SessionLocal() as session:
            # 1. Fetch & Lock
            jobs = self._fetch_jobs(session)
            if not jobs: return 0
            
            # 2. Process
            # Note: Subclasses can override process_items for batch logic
            await self.process_items(session, jobs)
            
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
            job.status = "processing"
            job.updated_at = datetime.now(timezone.utc)
        session.commit()
        return jobs

    async def process_items(self, session, jobs):
        """
        Default implementation iterates and calls process_item.
        Override this for batch processing (e.g. LLM batch calls).
        """
        for job in jobs:
            try:
                await self.process_item(session, job)
                # If status wasn't changed by process_item, mark as completed
                if job.status == "processing":
                    job.status = "completed"
            except Exception as e:
                logger.error(f"Job {job.id} failed: {e}")
                job.status = "failed"
                job.msg = str(e)
            job.updated_at = datetime.now(timezone.utc)

    async def process_item(self, session, job):
        """Implement specific logic here for single-item processing"""
        pass
