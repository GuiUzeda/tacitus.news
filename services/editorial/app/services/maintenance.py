from datetime import datetime, timedelta, timezone
from typing import Dict

from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    EventsQueueModel,
    JobStatus,
    MergeProposalModel,
)
from sqlalchemy import delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert


class MaintenanceService:
    def __init__(self, session):
        self.session = session

    def clear_completed_queues(self) -> Dict[str, int]:
        """Deletes all COMPLETED/APPROVED/REJECTED items from queues."""
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
        return {
            "articles": r1.rowcount,
            "events": r2.rowcount,
            "proposals": r3.rowcount,
        }

    def prune_stale_queue(self, days: int = 7) -> str:
        """Safely cleans old queue items without user prompts."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        stmt = (
            update(EventsQueueModel)
            .where(
                EventsQueueModel.status == JobStatus.PENDING,
                EventsQueueModel.created_at < cutoff,
            )
            .values(status=JobStatus.COMPLETED, msg="Auto-Pruned by MCP")
        )
        result = self.session.execute(stmt)
        self.session.commit()

        return f"✅ Pruned {result.rowcount} stale jobs older than {days} days."

    def emergency_unlock_db(self) -> str:
        """Kills active DB connections to release locks."""
        try:
            # We use a separate connection for this usually, but via session execute is fine
            # provided the user has permissions.
            self.session.execute(text("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = current_database()
                AND pid <> pg_backend_pid();
            """))
            return "✅ All external database connections have been terminated."
        except Exception as e:
            return f"❌ Failed to release locks: {str(e)}"

    def requeue_untitled_articles(self) -> str:
        """Scans for articles with 'No Title' or 'Unknown' and requeues them for enrichment."""
        stmt = select(ArticleModel.id).where(
            or_(
                func.lower(ArticleModel.title) == "no title",
                func.lower(ArticleModel.title) == "unknown",
                ArticleModel.title == "",
            )
        )
        article_ids = self.session.scalars(stmt).all()

        if not article_ids:
            return "✅ No untitled articles found."

        now = datetime.now(timezone.utc)
        count = 0

        for aid in article_ids:
            stmt = (
                insert(ArticlesQueueModel)
                .values(
                    article_id=aid,
                    queue_name=ArticlesQueueName.ENRICHER,
                    status=JobStatus.PENDING,
                    created_at=now,
                    updated_at=now,
                    attempts=0,
                    msg="Manual Requeue: Untitled",
                )
                .on_conflict_do_update(
                    index_elements=["article_id"],
                    set_={
                        "queue_name": ArticlesQueueName.ENRICHER,
                        "status": JobStatus.PENDING,
                        "updated_at": now,
                        "attempts": 0,
                        "msg": "Manual Requeue: Untitled",
                    },
                )
            )
            self.session.execute(stmt)
            count += 1

        self.session.commit()
        return f"🚀 Successfully re-queued {count} untitled articles to ENRICH queue."
