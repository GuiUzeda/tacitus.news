import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy import create_engine, select, or_, exists
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
from domain.enhancing import NewsEnhancerDomain

class NewsEnhancerWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Domain
        self.domain = NewsEnhancerDomain(concurrency_limit=5)

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=EventsQueueModel,
            target_queue_name=EventsQueueName.ENHANCER,
            batch_size=5, # Small batch because LLM summarization is heavy/slow
            pending_status=JobStatus.PENDING
        )

    async def run(self):
        logger.info(f"🚀 Enhancer Worker started.")
        await super().run()

    def _fetch_jobs(self, session, limit=None):
        """
        Smart fetch strategy override:
        1. Ignore events that are currently involved in Merges (Source or Target).
        2. Prioritize events with high article counts.
        """
        fetch_limit = limit or self.batch_size

        # Subquery: Find IDs of events involved in pending/processing merges
        active_merges = select(1).where(
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
                ~exists(active_merges) # <--- Critical exclusion
            )
            .order_by(NewsEventModel.article_count.desc())
            .order_by(EventsQueueModel.created_at.asc())
            .limit(fetch_limit)
            .with_for_update(skip_locked=True)
        )
        
        rows = session.execute(stmt).all()

        jobs = []
        for queue_item, event in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            queue_item.event = event # Pre-load relation
            jobs.append(queue_item)
        
        session.commit()
        return jobs

    async def process_item(self, session, job):
        """
        Standard processing hook. Logic delegated to Domain.
        Transaction handled by BaseWorker.
        """
        try:
            status = await self.domain.enhance_event(session, job)
            
            if status in ["ENHANCED", "NO_CHANGES_PUBLISH"]:
                logger.success(f"Event {job.event.title[:30]}: {status}")
            elif status == "DEBOUNCED":
                logger.info(f"Event {job.event.title[:30]}: DEBOUNCED")
            elif "FAILED" in status:
                logger.warning(f"Event {job.event_id}: {status}")
                
        except Exception as e:
            logger.error(f"Error enhancing event {job.event_id}: {e}")
            raise e # Triggers BaseWorker rollback

if __name__ == "__main__":
    worker = NewsEnhancerWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")