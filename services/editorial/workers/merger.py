import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime, timedelta, timezone
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

# Config & Models
from config import Settings
from news_events_lib.models import NewsEventModel, MergeProposalModel, JobStatus
from core.models import EventsQueueModel, EventsQueueName

# Domain
from domain.merger import NewsMergerDomain

class NewsMergerWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Domain Logic
        self.domain = NewsMergerDomain()
        
        # Worker Config
        self.SCAN_WINDOW_HOURS = 48  # Only check events updated recently
        self.concurrency = 3

    async def run(self):
        logger.info("🔄 News Merger Worker started")
        
        self.queue = asyncio.Queue(maxsize=20)
        
        # Start Consumers
        workers = []
        for i in range(self.concurrency):
            workers.append(asyncio.create_task(self._consumer_loop(i)))
            await asyncio.sleep(1.0)

        while True:
            try:
                # Producer: Fetch Active Events updated recently
                await self._producer_cycle()
                
                # Wait for the queue to drain before starting the next cycle
                await self.queue.join()
                
                logger.info("✅ Cycle complete. Sleeping 60s...")
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Merger Producer Error: {e}")
                await asyncio.sleep(30)

    async def _producer_cycle(self):
        with self.SessionLocal() as session:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self.SCAN_WINDOW_HOURS)
            
            # Subquery: Already has pending proposals?
            pending_subq = select(MergeProposalModel.source_event_id).where(
                MergeProposalModel.status == "pending",
                MergeProposalModel.source_event_id.is_not(None)
            )

            # Subquery: Currently busy in Enhancer?
            processing_subq = select(EventsQueueModel.event_id).where(
                EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                EventsQueueModel.status == JobStatus.PROCESSING
            )

            stmt = (
                select(NewsEventModel.id)
                .where(
                    NewsEventModel.is_active == True,
                    NewsEventModel.last_updated_at >= cutoff,
                    NewsEventModel.id.not_in(pending_subq),
                    NewsEventModel.id.not_in(processing_subq)
                )
                .order_by(NewsEventModel.last_updated_at.desc())
            )
            event_ids = session.execute(stmt).scalars().all()

        if not event_ids:
            logger.info("📭 No active events to scan.")
            return

        logger.info(f"🔍 Scanning {len(event_ids)} active events for duplicates...")
        for eid in event_ids:
            await self.queue.put(eid)

    async def _consumer_loop(self, worker_id):
        while True:
            event_id = await self.queue.get()
            try:
                # Run the synchronous DB domain logic in a thread
                await asyncio.to_thread(self._process_single_event, event_id)
            except Exception as e:
                logger.error(f"Worker {worker_id} error on {event_id}: {e}")
            finally:
                self.queue.task_done()

    def _process_single_event(self, event_id):
        # New session for each task to ensure thread safety
        with self.SessionLocal() as session:
            try:
                self.domain.check_and_process_duplicates(session, event_id)
            except Exception as e:
                logger.error(f"Merger failed for {event_id}: {e}")

if __name__ == "__main__":
    worker = NewsMergerWorker()
    asyncio.run(worker.run())