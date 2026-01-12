import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy import create_engine, select, or_
from sqlalchemy.orm import sessionmaker, selectinload

from config import Settings
from domain.publisher import NewsPublisherDomain

# Models
from news_events_lib.models import JobStatus
from core.models import EventsQueueModel, EventsQueueName

class NewsPublisherWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        self.domain = NewsPublisherDomain()
        self.concurrency = 2

    async def run(self):
        logger.info("📰 News Publisher Worker started")
        
        self.queue = asyncio.Queue(maxsize=20)
        
        # Start Consumers
        workers = []
        for i in range(self.concurrency):
            workers.append(asyncio.create_task(self._consumer_loop(i)))
            await asyncio.sleep(1.0)

        while True:
            try:
                # Producer: Fetch Pending Jobs
                await self._producer_cycle()
                
                # Wait for the queue to drain before starting the next cycle
                await self.queue.join()
                
                logger.info("✅ Cycle complete. Sleeping 60s...")
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Publisher Producer Error: {e}")
                await asyncio.sleep(30)

    async def _producer_cycle(self):
        with self.SessionLocal() as session:
            stmt = (
                select(EventsQueueModel)
                .options(selectinload(EventsQueueModel.event))
                .where(
                    EventsQueueModel.queue_name == EventsQueueName.PUBLISHER,
                    or_(
                        EventsQueueModel.status == JobStatus.PENDING,
                        EventsQueueModel.status == JobStatus.FAILED 
                    )
                )
                .limit(20)
                .with_for_update(skip_locked=True)
            )
            jobs = session.execute(stmt).scalars().all()
            
            if not jobs:
                logger.info("📭 No pending publish jobs.")
                return

            logger.info(f"🔍 Enqueueing {len(jobs)} publish jobs...")
            
            for job in jobs:
                job.status = JobStatus.PROCESSING
                job.updated_at = datetime.now(timezone.utc)
                await self.queue.put(job.id)
            
            session.commit()

    async def _consumer_loop(self, worker_id):
        while True:
            job_id = await self.queue.get()
            try:
                await asyncio.to_thread(self._process_single_job, job_id)
            except Exception as e:
                logger.error(f"Worker {worker_id} error on job {job_id}: {e}")
            finally:
                self.queue.task_done()

    def _process_single_job(self, job_id):
        with self.SessionLocal() as session:
            try:
                job = session.get(EventsQueueModel, job_id)
                if not job: return

                self.domain.publish_event_job(session, job)
                session.commit()
                    
            except Exception as e:
                logger.error(f"❌ Error publishing job {job_id}: {e}")
                session.rollback()
                # Refresh to update status
                job = session.get(EventsQueueModel, job_id)
                if job:
                    job.status = JobStatus.FAILED
                    job.msg = f"Crash: {str(e)[:100]}"
                    session.commit()

if __name__ == "__main__":
    worker = NewsPublisherWorker()
    asyncio.run(worker.run())