import asyncio
import signal
from datetime import datetime, timedelta, timezone

# Config & Models
from app.config import Settings

# Domain
from app.workers.merger.domain import MergerAction, MergerResult, NewsMergerDomain
from loguru import logger
from news_events_lib.audit import receive_after_flush  # noqa: F401
from news_events_lib.models import (
    EventsQueueModel,
    EventsQueueName,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import create_engine, exists, or_, select
from sqlalchemy.orm import sessionmaker


class NewsMergerWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        self.domain = NewsMergerDomain()
        self.SCAN_WINDOW_HOURS = 48
        self.queue = asyncio.Queue(maxsize=100)
        self.running = True  # Flag to control the loop

        # Cache to track processed state of events (Deduping)
        self.event_state_cache = {}

    async def run(self):
        logger.info("🕵️ Merger Scanner started")

        # 1. Register Signals
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)

        # 2. Start Consumer (ONCE, before the loop)
        consumer_task = asyncio.create_task(self._consumer_loop())

        # 3. Producer Loop
        while self.running:
            try:
                # Scan DB and fill queue
                await self._producer_cycle()

                # Wait for queue to drain (Consumer processes everything)
                # If shutdown signal comes during join(), we catch it or wait.
                if not self.running:
                    break

                await self.queue.join()

                # Sleep between cycles (check running flag frequently)
                for _ in range(60):
                    if not self.running:
                        break
                    await asyncio.sleep(1)

            except Exception as e:
                logger.critical(f"🔥 Merger Loop Crash: {e}")
                await asyncio.sleep(10)

        # 4. Graceful Shutdown Sequence
        logger.warning("🛑 Shutdown sequence initiated...")

        # Put "Poison Pill" to stop consumer
        await self.queue.put(None)
        await consumer_task  # Wait for consumer to finish processing and exit

        logger.success("👋 Merger Worker stopped gracefully.")

    def stop(self):
        logger.warning("🛑 Signal Received! Stopping Merger...")
        self.running = False

    async def _producer_cycle(self):
        """
        Scans for 'active' events that might have duplicates.
        Uses a state cache to avoid re-scanning unchanged events.
        """
        if not self.running:
            return

        with self.SessionLocal() as session:
            cutoff = datetime.now(timezone.utc) - timedelta(
                hours=self.SCAN_WINDOW_HOURS
            )

            # Optimization: Don't scan events that already have a pending proposal (Source OR Target)
            pending_subq = select(1).where(
                or_(
                    MergeProposalModel.source_event_id == NewsEventModel.id,
                    MergeProposalModel.target_event_id == NewsEventModel.id,
                ),
                MergeProposalModel.status.in_(
                    [JobStatus.PENDING, JobStatus.PROCESSING]
                ),
            )

            # Optimization: Don't scan events currently being Enhanced
            processing_subq = select(EventsQueueModel.event_id).where(
                EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                EventsQueueModel.status == JobStatus.PROCESSING,
            )

            stmt = (
                select(NewsEventModel.id, NewsEventModel.last_updated_at)
                .where(
                    NewsEventModel.is_active,
                    NewsEventModel.last_updated_at >= cutoff,
                    ~exists(pending_subq),
                    NewsEventModel.id.not_in(processing_subq),
                )
                .order_by(NewsEventModel.last_updated_at.desc())
            )
            # Returns list of Row(id, last_updated_at)
            rows = session.execute(stmt).all()

        if not rows:
            logger.info("No active events in window... Clearing cache.")
            self.event_state_cache.clear()
            return

        # --- Filter & Dedup Logic ---
        events_to_queue = []
        current_cycle_ids = set()

        for eid, updated_at in rows:
            current_cycle_ids.add(eid)

            # Check Cache: If we've seen this exact version of the event, skip it.
            last_seen_time = self.event_state_cache.get(eid)
            if last_seen_time and last_seen_time == updated_at:
                continue

            events_to_queue.append((eid, updated_at))

        # --- Garbage Collection ---
        # Remove IDs from cache that are no longer in the current window/query
        # (e.g., fell out of 48h window, or became locked/pending)
        cached_ids = list(self.event_state_cache.keys())
        for cached_id in cached_ids:
            if cached_id not in current_cycle_ids:
                del self.event_state_cache[cached_id]

        if not events_to_queue:
            logger.info(f"💤 All {len(rows)} active events are up-to-date. Sleeping...")
            return

        logger.info(
            f"🔎 Scanning {len(events_to_queue)} changed/new events (out of {len(rows)} active)..."
        )

        for eid, updated_at in events_to_queue:
            if not self.running:
                break
            await self.queue.put(eid)
            # Update cache immediately so we don't re-queue it next cycle
            self.event_state_cache[eid] = updated_at

    async def _consumer_loop(self):
        """
        Consumes IDs from the queue. Stops when it receives None.
        """
        while True:
            # Wait for an ID or Poison Pill
            event_id = await self.queue.get()

            # POISON PILL CHECK
            if event_id is None:
                self.queue.task_done()
                break  # Exit the loop -> Consumer task finishes

            try:
                # Run sync DB logic in thread
                await asyncio.to_thread(self._process_single_event, event_id)
            except Exception as e:
                logger.error(f"Worker Error on {event_id}: {e}")
            finally:
                self.queue.task_done()

    def _process_single_event(self, event_id):
        # (Your existing processing logic fits perfectly here)
        session = self.SessionLocal()
        try:
            event = session.get(NewsEventModel, event_id)
            if not event:
                return

            result: MergerResult = self.domain.scan_and_process_event(session, event)

            if result.action == MergerAction.MERGED and result.target:
                session.commit()
                logger.success(
                    f"⚡ AUTO-MERGE: {result.source.title[:20]} -> {result.target.title[:20]}"
                )
            elif result.action == MergerAction.PROPOSED:
                session.commit()
                logger.info(f"💡 PROPOSAL: {result.reason}")
            else:
                session.rollback()

        except Exception as e:
            session.rollback()
            logger.opt(exception=True).error(f"Merger Logic Failed for {event_id}: {e}")
        finally:
            session.close()


if __name__ == "__main__":
    worker = NewsMergerWorker()
    asyncio.run(worker.run())
