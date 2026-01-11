import asyncio
import sys
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger

from config import Settings
# Assuming 'models' contains the Queue definitions as seen in cli.py
from models import EventsQueueModel, JobStatus, EventsQueueName
from news_events_lib.models import NewsEventModel, EventStatus
from base_worker import BaseQueueWorker

class NewsPublisherWorker(BaseQueueWorker):
    """
    Automatically publishes events that meet high-confidence criteria.
    Events that fail criteria are marked FAILED so a human can review them.
    """
    
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Configuration
        self.MIN_ARTICLES_FOR_AUTO = 4
        self.MIN_SUMMARY_LENGTH = 50

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=EventsQueueModel,
            target_queue_name=None, # Terminal step
            batch_size=20,
            pending_status=JobStatus.PENDING
        )

    def _fetch_jobs(self, session):
        """
        Fetch only jobs from the PUBLISHER queue.
        """
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
        
        # Mark as processing
        for job in jobs:
            job.status = JobStatus.PROCESSING
        session.commit()
        
        return jobs

    async def process_items(self, session, jobs):
        for job in jobs:
            await self.process_single_event(session, job)

    async def process_single_event(self, session, job):
        event = job.event
        
        if not event:
            job.status = JobStatus.FAILED
            job.msg = "Event not found"
            session.commit()
            return

        # --- 1. SAFETY CHECKS ---
        reasons = []
        
        # Check A: Volume (Consensus)
        if event.article_count < self.MIN_ARTICLES_FOR_AUTO:
            reasons.append(f"Low volume ({event.article_count} < {self.MIN_ARTICLES_FOR_AUTO})")

        # Check B: Completeness
        summary_text = ""
        if event.summary and isinstance(event.summary, dict):
            summary_text = event.summary.get("center") or event.summary.get("bias") or ""
        
        if len(summary_text) < self.MIN_SUMMARY_LENGTH:
            reasons.append("Summary missing or too short")

        # --- 2. DECISION ---
        if not reasons:
            # ✅ AUTO-PUBLISH
            priority = self._calculate_priority(event)
            
            event.status = EventStatus.PUBLISHED
            event.is_active = True
            event.fe_priority = priority
            
            job.status = JobStatus.COMPLETED
            job.msg = f"Auto-Published (Priority {priority})"
            
            logger.success(f"🚀 Published: {event.title} [P{priority}]")
        else:
            # ❌ MANUAL REVIEW REQUIRED
            # We mark it as FAILED so it leaves the pending loop, 
            # but the CLI user can see it in 'Queue Manager' and retry/edit it.
            job.status = JobStatus.FAILED
            job.msg = f"Auto-Skip: {', '.join(reasons)}"
            logger.info(f"⚠️ Skipped: {event.title} -> Manual Review")

        session.commit()

    def _calculate_priority(self, event: NewsEventModel) -> int:
        """
        Determines Frontend Priority (1=Top, 10=Low) based on article volume.
        """
        count = event.article_count
        
        if count >= 50: return 1  # Breaking / Massive
        if count >= 30: return 2  # Major
        if count >= 20: return 3  # High
        if count >= 10: return 5  # Standard
        if count >= 5:  return 7  # Medium
        return 10                 # Minor

if __name__ == "__main__":
    worker = NewsPublisherWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")