import asyncio
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload

from config import Settings
from base_worker import BaseQueueWorker
from domain.publisher import NewsPublisherDomain

# Models
from models import EventsQueueModel, JobStatus, EventsQueueName

class NewsPublisherWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        self.domain = NewsPublisherDomain()

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=EventsQueueModel,
            target_queue_name=None, # Terminal step
            batch_size=20,
            pending_status=JobStatus.PENDING
        )

    def _fetch_jobs(self, session):
        # Fetch only from PUBLISHER queue
        stmt = (
            select(EventsQueueModel)
            .options(joinedload(EventsQueueModel.event))
            .where(
                EventsQueueModel.queue_name == EventsQueueName.PUBLISHER,
                EventsQueueModel.status == JobStatus.PENDING
            )
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        jobs = session.execute(stmt).scalars().all()
        
        for job in jobs:
            job.status = JobStatus.PROCESSING
        session.commit()
        return jobs

    async def process_items(self, session, jobs):
        for job in jobs:
            try:
                self.domain.publish_event_job(session, job)
                # Commit after every item to ensure scores are saved immediately
                session.commit()
            except Exception as e:
                logger.error(f"Error publishing job {job.id}: {e}")
                session.rollback()
                job.status = JobStatus.FAILED
                job.msg = f"Crash: {str(e)}"
                session.commit()

if __name__ == "__main__":
    worker = NewsPublisherWorker()
    asyncio.run(worker.run())