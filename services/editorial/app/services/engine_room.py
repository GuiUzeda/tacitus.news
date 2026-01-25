from datetime import datetime, timezone

from news_events_lib.models import (
    ArticlesQueueModel,
    EventsQueueModel,
    EventStatus,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import func, select, update


class EngineRoomService:
    def __init__(self, session):
        self.session = session

    def get_queue_stats(self) -> dict:
        """Returns counts for all queues and statuses."""
        a_stats = (
            self.session.query(
                ArticlesQueueModel.queue_name,
                ArticlesQueueModel.status,
                func.count(ArticlesQueueModel.id),
            )
            .group_by(ArticlesQueueModel.queue_name, ArticlesQueueModel.status)
            .all()
        )

        e_stats = (
            self.session.query(
                EventsQueueModel.queue_name,
                EventsQueueModel.status,
                func.count(EventsQueueModel.id),
            )
            .group_by(EventsQueueModel.queue_name, EventsQueueModel.status)
            .all()
        )

        p_stats = (
            self.session.query(
                MergeProposalModel.status, func.count(MergeProposalModel.id)
            )
            .group_by(MergeProposalModel.status)
            .all()
        )

        return {
            "articles": [
                {"queue": q.value, "status": s.value, "count": c} for q, s, c in a_stats
            ],
            "events": [
                {"queue": q.value, "status": s.value, "count": c} for q, s, c in e_stats
            ],
            "proposals": [{"status": s.value, "count": c} for s, c in p_stats],
        }

    def retry_failed_jobs(self) -> str:
        """Resets all FAILED jobs to PENDING."""
        now = datetime.now(timezone.utc)
        r1 = self.session.execute(
            update(ArticlesQueueModel)
            .where(ArticlesQueueModel.status == JobStatus.FAILED)
            .values(status=JobStatus.PENDING, msg="CLI Retry", updated_at=now)
        )
        r2 = self.session.execute(
            update(EventsQueueModel)
            .where(EventsQueueModel.status == JobStatus.FAILED)
            .values(status=JobStatus.PENDING, msg="CLI Retry", updated_at=now)
        )
        r3 = self.session.execute(
            update(MergeProposalModel)
            .where(MergeProposalModel.status == JobStatus.FAILED)
            .values(status=JobStatus.PENDING, reasoning="CLI Retry", updated_at=now)
        )
        self.session.commit()
        return f"✅ Retried: {r1.rowcount} Articles, {r2.rowcount} Events, {r3.rowcount} Proposals"

    def reset_stuck_jobs(self) -> str:
        """Resets all PROCESSING jobs to PENDING (use with caution)."""
        now = datetime.now(timezone.utc)
        r1 = self.session.execute(
            update(ArticlesQueueModel)
            .where(ArticlesQueueModel.status == JobStatus.PROCESSING)
            .values(status=JobStatus.PENDING, msg="CLI Reset Stuck", updated_at=now)
        )
        r2 = self.session.execute(
            update(EventsQueueModel)
            .where(EventsQueueModel.status == JobStatus.PROCESSING)
            .values(status=JobStatus.PENDING, msg="CLI Reset Stuck", updated_at=now)
        )
        self.session.commit()
        return f"✅ Reset: {r1.rowcount} Articles, {r2.rowcount} Events"

    def clear_completed_jobs(self) -> str:
        """Deletes COMPLETED queue items to free up space."""
        from sqlalchemy import delete

        r1 = self.session.execute(
            delete(ArticlesQueueModel).where(
                ArticlesQueueModel.status == JobStatus.COMPLETED
            )
        )
        r2 = self.session.execute(
            delete(EventsQueueModel).where(
                EventsQueueModel.status == JobStatus.COMPLETED
            )
        )
        r3 = self.session.execute(
            delete(MergeProposalModel).where(
                MergeProposalModel.status.in_([JobStatus.APPROVED, JobStatus.REJECTED])
            )
        )
        self.session.commit()
        return f"✅ Cleared: {r1.rowcount} Articles, {r2.rowcount} Events, {r3.rowcount} Proposals"

    def recalc_all_scores(self, publisher_domain) -> str:
        """Recalculates scores for all published active events."""
        events = self.session.scalars(
            select(NewsEventModel).where(
                NewsEventModel.is_active.is_(True),
                NewsEventModel.status == EventStatus.PUBLISHED,
            )
        ).all()

        count = 0
        for e in events:
            topics = list(e.main_topic_counts.keys()) if e.main_topic_counts else []
            new_score, insights, _ = publisher_domain.calculate_spectrum_score(
                e, topics
            )
            e.hot_score = new_score
            e.publisher_insights = insights
            count += 1

        self.session.commit()
        return f"✅ Recalculated scores for {count} events."
