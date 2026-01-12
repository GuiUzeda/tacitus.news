import asyncio
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy import create_engine, select, or_
from sqlalchemy.orm import sessionmaker, selectinload

from config import Settings
from core.base_worker import BaseQueueWorker
from domain.publisher import NewsPublisherDomain

# Models
from news_events_lib.models import JobStatus
from core.models import EventsQueueModel, EventsQueueName

class NewsPublisherWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        self.domain = NewsPublisherDomain()

        # Configura o Worker Base
        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=EventsQueueModel,
            target_queue_name=EventsQueueName.PUBLISHER,
            batch_size=10,
            pending_status=JobStatus.PENDING
        )

    def _fetch_jobs(self, session):
        """
        Busca jobs na fila PUBLISHER.
        """
        stmt = (
            select(EventsQueueModel)
            .options(selectinload(EventsQueueModel.event)) # Eager load do evento
            .where(
                EventsQueueModel.queue_name == self.queue_name,
                # Pega Pending ou Failed (Retry automático)
                or_(
                    EventsQueueModel.status == JobStatus.PENDING,
                    EventsQueueModel.status == JobStatus.FAILED 
                )
            )
            .limit(self.batch_size)
            .with_for_update(skip_locked=True) # Evita conflito com outros workers
        )
        jobs = session.execute(stmt).scalars().all()
        
        # Marca como Processing imediatamente
        for job in jobs:
            job.status = JobStatus.PROCESSING
            job.updated_at = datetime.now(timezone.utc)
        session.commit()
        return jobs

    async def process_items(self, session, jobs):
        """
        Processa o lote.
        """
        for job in jobs:
            try:
                # O Domain cuida de toda a lógica
                processed = self.domain.publish_event_job(session, job)
                # Domain now guarantees job.status is updated (COMPLETED, FAILED or WAITING)
                session.commit()
                    
            except Exception as e:
                logger.error(f"❌ Error publishing job {job.id}: {e}")
                session.rollback()
                job.status = JobStatus.FAILED
                job.msg = f"Crash: {str(e)[:100]}"
                session.commit()

if __name__ == "__main__":
    logger.info("📰 Starting Publisher Worker...")
    worker = NewsPublisherWorker()
    asyncio.run(worker.run())