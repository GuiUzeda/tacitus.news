from typing import Any, Dict, List, Optional

from app.services.base_service import BaseService
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    AuditLogModel,
    EventsQueueModel,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import func, select


class AuditService(BaseService):
    """
    Service for Audit logs and system-wide overview.
    Domain Model: AuditLogModel
    """

    def get_largest_events(self, limit: int = 10) -> Dict[str, Any]:
        """Finds top N largest active events and details the largest one."""
        stmt = (
            select(NewsEventModel)
            .where(NewsEventModel.is_active.is_(True))
            .order_by(NewsEventModel.article_count.desc())
            .limit(limit)
        )
        events = self.session.scalars(stmt).all()

        if not events:
            return {"events": [], "largest_event_articles": []}

        event_list = [
            {"id": str(e.id), "count": e.article_count, "title": e.title}
            for e in events
        ]

        largest = events[0]
        art_stmt = (
            select(ArticleModel)
            .where(ArticleModel.event_id == largest.id)
            .order_by(ArticleModel.published_date.desc())
            .limit(100)
        )
        articles = self.session.scalars(art_stmt).all()

        art_list = [
            {
                "title": a.title,
                "date": a.published_date.isoformat() if a.published_date else None,
            }
            for a in articles
        ]

        return {
            "events": event_list,
            "largest_event": {"id": str(largest.id), "title": largest.title},
            "largest_event_articles": art_list,
        }

    def search_logs(
        self, filters: Dict[str, Any], limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        return self._search(AuditLogModel, filters, limit, offset)

    def get_queue_overview(self, queue_filter: Optional[str] = None) -> Dict[str, Any]:
        """Summarizes states and Queue stats across all domains."""
        result = {"articles": [], "events": [], "proposals": []}

        def matches(name: str):
            if not queue_filter or queue_filter.lower() == "all":
                return True
            return queue_filter.lower() in name.lower()

        # Articles
        if matches("articles"):
            q_articles = select(
                ArticlesQueueModel.queue_name,
                ArticlesQueueModel.status,
                func.count(ArticlesQueueModel.id),
            ).group_by(ArticlesQueueModel.queue_name, ArticlesQueueModel.status)
            for q, s, c in self.session.execute(q_articles).all():
                result["articles"].append(
                    {"queue": q.value, "status": s.value, "count": c}
                )

        # Events
        if matches("events"):
            q_events = select(
                EventsQueueModel.queue_name,
                EventsQueueModel.status,
                func.count(EventsQueueModel.id),
            ).group_by(EventsQueueModel.queue_name, EventsQueueModel.status)
            for q, s, c in self.session.execute(q_events).all():
                result["events"].append(
                    {"queue": q.value, "status": s.value, "count": c}
                )

        # Proposals
        if matches("proposals"):
            q_props = select(
                MergeProposalModel.status, func.count(MergeProposalModel.id)
            ).group_by(MergeProposalModel.status)
            for s, c in self.session.execute(q_props).all():
                result["proposals"].append({"status": s.value, "count": c})

        return result
