from datetime import datetime, timezone
from typing import List

from news_events_lib.models import (
    EventsQueueModel,
    EventStatus,
    JobStatus,
    NewsEventModel,
)
from sqlalchemy import desc, select


class WaitingRoomService:
    def __init__(self, session, publisher_domain):
        self.session = session
        self.publisher_domain = publisher_domain

    def get_waiting_events(self) -> List[dict]:
        """Returns events waiting for approval."""
        events = self.session.scalars(
            select(NewsEventModel)
            .join(EventsQueueModel, NewsEventModel.id == EventsQueueModel.event_id)
            .where(EventsQueueModel.status == JobStatus.WAITING)
            .order_by(desc(NewsEventModel.created_at))
        ).all()

        return [
            {
                "id": str(e.id),
                "title": e.title,
                "article_count": e.article_count,
                "reason": "Topic/Niche" if e.article_count > 5 else "Low Volume",
            }
            for e in events
        ]

    def approve_event(self, event_id: str) -> str:
        event = self.session.get(NewsEventModel, event_id)
        if not event:
            return "❌ Event not found."

        event.status = EventStatus.PUBLISHED
        event.published_at = datetime.now(timezone.utc)

        # Recalc Score
        topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []
        new_score, insights, _ = self.publisher_domain.calculate_spectrum_score(
            event, topics
        )
        event.hot_score = new_score
        event.publisher_insights = insights

        # We should also update the queue item to COMPLETED, but assuming implicit handling or separate job
        # Ideally:
        q_item = self.session.scalar(
            select(EventsQueueModel).where(
                EventsQueueModel.event_id == event.id,
                EventsQueueModel.status == JobStatus.WAITING,
            )
        )
        if q_item:
            q_item.status = JobStatus.COMPLETED
            q_item.msg = "Approved via MCP/CLI"

        self.session.commit()
        return f"✅ Event '{event.title}' Published!"
