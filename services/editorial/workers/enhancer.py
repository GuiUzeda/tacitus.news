import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import asyncio
from datetime import datetime, timezone, timedelta
from loguru import logger
from sqlalchemy import create_engine, select, update, or_, exists
from sqlalchemy.orm import sessionmaker

# Models
from news_events_lib.models import (
    NewsEventModel, 
    MergeProposalModel, 
    JobStatus
)
from core.models import EventsQueueModel, EventsQueueName
from config import Settings
from core.base_worker import BaseQueueWorker

# Domain
from domain.enhancer import NewsEnhancerDomain

class NewsEnhancerWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Instantiate Domain Logic
        self.domain = NewsEnhancerDomain(concurrency_limit=5)

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=EventsQueueModel,
            target_queue_name=EventsQueueName.ENHANCER,
            batch_size=5,
            pending_status=JobStatus.PENDING
        )

    async def run(self):
        logger.info(f"🚀 Enhancer Worker started (Streaming Mode)")
        self._reset_stuck_tasks()
        
        self.queue = asyncio.Queue(maxsize=10) # Buffer size for consumer
        
        # Start Consumers
        workers = []
        for i in range(4):
            workers.append(asyncio.create_task(self._consumer_loop(i)))
            await asyncio.sleep(1.0)
        
        # Producer Loop
        while True:
            try:
                if self.queue.full():
                    await asyncio.sleep(1.0)
                    continue

                with self.SessionLocal() as session:
                    jobs = self._fetch_jobs_strategy(session)
                    
                    for job in jobs:
                        await self.queue.put(job.id)

                if not jobs:
                    if self.queue.empty():
                        self._cleanup_stuck_jobs()
                    logger.debug('No jobs. Sleeping 30s...')
                    await asyncio.sleep(30)
                        
            except Exception as e:
                logger.error(f"Producer Error: {e}")
                await asyncio.sleep(5)

    async def _consumer_loop(self, worker_id):
        while True:
            job_id = await self.queue.get()
            try:
                with self.SessionLocal() as session:
                    # Re-fetch job inside the consumer transaction
                    job = session.get(EventsQueueModel, job_id)
                    if job:
                        # Delegate to Domain
                        await self.domain.enhance_event_job(session, job)
                        session.commit()
            except Exception as e:
                logger.error(f"Worker {worker_id} error on job {job_id}: {e}")
            finally:
                self.queue.task_done()

    def _fetch_jobs_strategy(self, session):
        """
        Smart fetch strategy:
        1. Ignore events that are currently involved in Merges (Source or Target).
        2. Prioritize events with high article counts.
        """
        # Events involved in pending merges
        active_proposals = select(1).where(
            or_(
                MergeProposalModel.source_event_id == NewsEventModel.id,
                MergeProposalModel.target_event_id == NewsEventModel.id
            ),
            MergeProposalModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
        )

        stmt = (
            select(EventsQueueModel, NewsEventModel)
            .join(NewsEventModel, EventsQueueModel.event_id == NewsEventModel.id)
            .where(
                EventsQueueModel.status == self.pending_status,
                EventsQueueModel.queue_name == self.queue_name,
                ~exists(active_proposals)
            )
            .order_by(NewsEventModel.article_count.desc())
            .order_by(EventsQueueModel.created_at.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = session.execute(stmt).all()

        jobs = []
        for queue_item, event in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            queue_item.event = event
            jobs.append(queue_item)
        
        session.commit()
        return jobs

    def _reset_stuck_tasks(self):
        with self.SessionLocal() as session:
            result = session.execute(
                update(EventsQueueModel)
                .where(
                    EventsQueueModel.status == JobStatus.PROCESSING,
                    EventsQueueModel.queue_name == self.queue_name
                )
                .values(status=JobStatus.PENDING)
            )
            session.commit()
            if result.rowcount > 0:
                logger.info(f"🧹 Reset {result.rowcount} stuck enhancer jobs.")

    def _cleanup_stuck_jobs(self, session=None):
        timeout = timedelta(minutes=15)
        cutoff = datetime.now(timezone.utc) - timeout
        
        with self.SessionLocal() as session:
            result = session.execute(
                update(EventsQueueModel)
                .where(
                    EventsQueueModel.status == JobStatus.PROCESSING,
                    EventsQueueModel.queue_name == self.queue_name,
                    EventsQueueModel.updated_at < cutoff
                )
                .values(status=JobStatus.PENDING, msg="Auto-reset: Timeout")
            )
            session.commit()

if __name__ == "__main__":
    worker = NewsEnhancerWorker()
    asyncio.run(worker.run())