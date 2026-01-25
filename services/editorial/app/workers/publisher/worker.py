import asyncio
from datetime import datetime, timezone

from app.config import Settings
from app.utils.event_manager import EventManager
from app.workers.base_worker import BaseQueueWorker
from app.workers.publisher.domain import NewsPublisherDomain, PublisherAction
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
from sqlalchemy import create_engine, exists, func, or_, select, update
from sqlalchemy.orm import aliased, sessionmaker


class NewsPublisherWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)

        self.domain = NewsPublisherDomain()

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=EventsQueueModel,
            target_queue_name=EventsQueueName.PUBLISHER,
            batch_size=10,  # Scoring is faster than LLM, can handle bigger batches
            pending_status=JobStatus.PENDING,
        )

    def _fetch_jobs(self, session, limit=None):
        fetch_limit = limit or self.batch_size

        # 1. Subquery to find the LATEST ticket ID per Event
        # This collapses all pending tickets for an event into one representative ID
        latest_ticket_ids = (
            select(
                EventsQueueModel.event_id, func.max(EventsQueueModel.id).label("max_id")
            )
            .where(
                EventsQueueModel.status == self.pending_status,
                EventsQueueModel.queue_name == self.queue_name,
            )
            .group_by(EventsQueueModel.event_id)
            .subquery()
        )

        # 2. Safety & Serialization subqueries (as you already have them)
        active_merges = select(1).where(
            or_(
                MergeProposalModel.source_event_id == NewsEventModel.id,
                MergeProposalModel.target_event_id == NewsEventModel.id,
            ),
            MergeProposalModel.status.in_(
                [JobStatus.PROCESSING]
            ),  # Only processing to speedup publish
        )

        ProcessingQueue = aliased(EventsQueueModel)
        active_processing_events = select(1).where(
            ProcessingQueue.event_id == NewsEventModel.id,
            ProcessingQueue.queue_name == self.queue_name,
            ProcessingQueue.status == JobStatus.PROCESSING,
        )

        EnhancerQueue = aliased(EventsQueueModel)
        active_enhancers = select(1).where(
            EnhancerQueue.event_id == NewsEventModel.id,
            EnhancerQueue.queue_name == EventsQueueName.ENHANCER,
            EnhancerQueue.status.in_([JobStatus.PROCESSING]),
        )

        # 3. Final Join to fetch only the "Winners" of the deduplication
        stmt = (
            select(EventsQueueModel, NewsEventModel)
            .join(latest_ticket_ids, EventsQueueModel.id == latest_ticket_ids.c.max_id)
            .join(NewsEventModel, EventsQueueModel.event_id == NewsEventModel.id)
            .where(
                ~exists(active_merges),
                ~exists(active_processing_events),
                ~exists(active_enhancers),
            )
            .order_by(EventsQueueModel.created_at.desc())
            .limit(fetch_limit)
            .with_for_update(
                skip_locked=True, of=EventsQueueModel
            )  # Only lock the queue
        )

        rows = session.execute(stmt).all()

        jobs = []
        for queue_item, event in rows:
            # Mark as processing
            queue_item.status = JobStatus.PROCESSING
            queue_item.event = event
            jobs.append(queue_item)

            # 🔥 PROACTIVE PRUNING:
            # Kill the other pending tickets for this event immediately
            # so other workers don't even see them in their "Scout" phase.
            session.execute(
                update(EventsQueueModel)
                .where(
                    EventsQueueModel.event_id == queue_item.event_id,
                    EventsQueueModel.queue_name == self.queue_name,
                    EventsQueueModel.id != queue_item.id,
                    EventsQueueModel.status == JobStatus.PENDING,
                )
                .values(
                    status=JobStatus.COMPLETED,
                    msg="Pre-emptively pruned (deduplication)",
                )
            )

        session.flush()
        return jobs

    async def process_item(self, session, job):
        try:
            # 1. Domain Logic
            result = self.domain.publish_event_job(session, job)

            # 2. Handle Actions
            if result.action == PublisherAction.WAIT:
                job.status = JobStatus.WAITING
                logger.info(f"⏳ Wait '{job.event.title[:20]}': {result.reason}")

            elif result.action == PublisherAction.IGNORE:
                job.status = JobStatus.COMPLETED
                self._prune_superseded_tickets(
                    session, job
                )  # Prune if ignored (likely stale/empty)

            elif result.action == PublisherAction.PUBLISH:
                job.status = JobStatus.COMPLETED
                logger.success(
                    f"📢 Published '{job.event.title[:20]}' Score: {result.score} {result.insights}"
                )

                # Prune older requests (We just updated the score to the latest state)
                self._prune_superseded_tickets(session, job)

            elif result.action == PublisherAction.MERGE:
                job.status = JobStatus.COMPLETED

                if result.target_event:
                    survivor = EventManager.execute_event_merge(
                        session,
                        source_event=job.event,
                        target_event=result.target_event,
                        reason="Publisher Auto-Merge",
                    )

                    EventManager.create_event_queue(
                        session,
                        survivor.id,
                        EventsQueueName.ENHANCER,
                        reason=f"Merged with {job.event.title[:20]}",
                    )

                    # Prune because the event ID effectively "changed" (merged away)
                    self._prune_superseded_tickets(session, job)

                    logger.info(
                        f"🔀 Auto-Merged '{job.event.title[:20]}' -> '{survivor.title[:20]}'"
                    )

            elif result.action == PublisherAction.PROPOSE:
                job.status = JobStatus.COMPLETED

                if result.target_event:
                    EventManager.create_merge_proposal(
                        session,
                        source=job.event,
                        target=result.target_event,
                        score=result.dist,
                        reason=result.reason,
                        status=JobStatus.PENDING,
                    )
                    self._prune_superseded_tickets(session, job)

                    logger.info(
                        f"✋ Proposed Merge '{job.event.title[:20]}' -> '{result.target_event.title[:20]}'"
                    )

            elif result.action == PublisherAction.ERROR:
                job.status = JobStatus.FAILED
                logger.error(f"❌ Publisher Error: {result.reason}")
            job.msg = result.reason
            job.updated_at = datetime.now(timezone.utc)

        except Exception as e:
            logger.opt(exception=True).error(f"Crash publishing {job.event_id}")
            raise e

    def _prune_superseded_tickets(self, session, current_job):
        """
        Marks older PENDING/WAITING tickets for the same event as IGNORED.
        Safety: Only prunes tickets created BEFORE or AT the same time as the current job.
        """
        try:
            stmt = (
                update(EventsQueueModel)
                .where(
                    EventsQueueModel.event_id == current_job.event_id,
                    EventsQueueModel.queue_name == self.queue_name,
                    EventsQueueModel.id != current_job.id,  # Don't touch self
                    EventsQueueModel.status.in_([JobStatus.PENDING, JobStatus.WAITING]),
                    # SAFETY: Only prune things older than what we just processed
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
    worker = NewsPublisherWorker()
    asyncio.run(worker.run())
