import asyncio
from datetime import datetime, timezone

from app.config import Settings
from app.utils.event_manager import EventManager
from app.workers.base_worker import BaseQueueWorker

# Domain
from app.workers.enhancer.domain import EnhancerAction, NewsEnhancerDomain
from loguru import logger
from news_events_lib.audit import receive_after_flush  # noqa: F401

# Models
from news_events_lib.models import (
    EventsQueueModel,
    EventsQueueName,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import create_engine, exists, or_, select, update
from sqlalchemy.orm import aliased, sessionmaker


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
            batch_size=5,  # Keep small, LLM is slow
            pending_status=JobStatus.PENDING,
        )

    def _fetch_jobs(self, session, limit=None):
        """
        Smart Fetch:
        1. Ignore events locked in Active Merges.
        2. Prioritize events with high article counts.
        3. Deduplicate events (process only one queue item per unique event).
        """
        target_size = limit or self.batch_size
        # Fetch extra buffer to account for duplicates we'll filter out
        fetch_limit = target_size * 10

        # Find IDs of events involved in pending/processing merges
        active_merges = select(1).where(
            or_(
                MergeProposalModel.source_event_id == NewsEventModel.id,
                MergeProposalModel.target_event_id == NewsEventModel.id,
            ),
            MergeProposalModel.status.in_(
                [JobStatus.PROCESSING]
            ),  # Only processing to speedup the publish
        )
        ProcessingQueue = aliased(EventsQueueModel)

        active_processing_events = select(1).where(
            ProcessingQueue.event_id == NewsEventModel.id,  # Correlate to Outer Event
            ProcessingQueue.queue_name == self.queue_name,
            ProcessingQueue.status == JobStatus.PROCESSING,
        )

        stmt = (
            select(EventsQueueModel, NewsEventModel)
            .join(NewsEventModel, EventsQueueModel.event_id == NewsEventModel.id)
            .where(
                EventsQueueModel.status == self.pending_status,
                EventsQueueModel.queue_name == self.queue_name,
                ~exists(active_merges),
                ~exists(active_processing_events),
            )
            .order_by(NewsEventModel.article_count.desc())
            .order_by(EventsQueueModel.created_at.desc())
            .limit(fetch_limit)
            .with_for_update(skip_locked=True)
        )

        rows = session.execute(stmt).all()

        jobs = []
        seen_events = set()

        for queue_item, event in rows:
            # If we already have a job for this event in this batch
            if event.id in seen_events:
                # Immediately retire this duplicate ticket
                queue_item.status = JobStatus.COMPLETED
                queue_item.msg = "Superseded by batch deduplication"
                queue_item.updated_at = datetime.now(timezone.utc)
                continue

            # If we've reached our target batch size, stop adding new events
            if len(jobs) >= target_size:
                break

            # This is a new event for this batch
            seen_events.add(event.id)
            queue_item.status = JobStatus.PROCESSING
            queue_item.event = event
            jobs.append(queue_item)

        session.commit()
        return jobs

    async def process_item(self, session, job):
        """
        Delegates to Domain. Handles Result Actions.
        """
        try:
            result = await self.domain.enhance_event(session, job)

            # 🛡️ DEADLOCK PREVENTION: Force Event Update First
            # We must acquire the lock on 'news_events' (via flush) BEFORE we touch 'events_queue'.
            # Otherwise, we might hold a Queue lock (via insert/prune) and wait for Event lock,
            # while Merger holds Event lock and waits for Queue lock.
            session.flush()

            # --- HANDLE OUTCOME ---
            if result.action == EnhancerAction.ENHANCED:
                logger.success(f"✨ Enhanced '{job.event.title[:20]}': {result.msg} ")

                EventManager.create_event_queue(
                    session, job.event_id, EventsQueueName.PUBLISHER, reason="Enhanced"
                )
                self._prune_superseded_tickets(session, job)
                job.status = JobStatus.COMPLETED

            elif result.action == EnhancerAction.DEBOUNCED:
                logger.info(f"⏳ Debounced '{job.event.title[:20]}': {result.msg}")
                # We mark as COMPLETED so it leaves the queue,
                # but we didn't touch the articles, so the 'Delta' remains for next time.
                self._prune_superseded_tickets(session, job)
                job.status = JobStatus.COMPLETED
                EventManager.create_event_queue(
                    session,
                    job.event_id,
                    EventsQueueName.PUBLISHER,
                    reason="Debounced: Volume Update Only",
                )

            elif result.action in [
                EnhancerAction.SKIPPED_EMPTY,
                EnhancerAction.SKIPPED_INACTIVE,
            ]:
                logger.debug(
                    f"⏩ Skipped '{job.event.title[:20] if job.event else '?'}: {result.msg}"
                )
                self._prune_superseded_tickets(session, job)
                job.status = JobStatus.COMPLETED

            elif result.action == EnhancerAction.ERROR:
                logger.error(f"❌ Error '{job.event_id}': {result.msg}")
                job.status = JobStatus.FAILED

            job.msg = f"{result.action.value}: {result.msg}"
            job.updated_at = datetime.now(timezone.utc)

            # BaseWorker will commit this transaction

        except Exception as e:
            logger.opt(exception=True).error(f"Crash enhancing {job.event_id}")
            raise e

    def _prune_superseded_tickets(self, session, current_job):
        """
        Marks older PENDING tickets for the same event as IGNORED.
        Safety: Only prunes tickets created BEFORE or AT the same time as the current job.
        """
        try:
            stmt = (
                update(EventsQueueModel)
                .where(
                    EventsQueueModel.event_id == current_job.event_id,
                    EventsQueueModel.queue_name == self.queue_name,
                    EventsQueueModel.id != current_job.id,
                    EventsQueueModel.status == JobStatus.PENDING,
                    EventsQueueModel.created_at <= current_job.created_at,
                )
                .values(
                    status=JobStatus.COMPLETED,
                    msg=f"Superseded by Job {current_job.id}",
                    updated_at=datetime.now(timezone.utc),
                )
            )
            result = session.execute(stmt)
            if result.rowcount > 0:
                logger.info(f"   -> Pruned {result.rowcount} superseded tickets.")
        except Exception as e:
            logger.warning(f"Failed to prune tickets: {e}")


if __name__ == "__main__":
    worker = NewsEnhancerWorker()
    asyncio.run(worker.run())
