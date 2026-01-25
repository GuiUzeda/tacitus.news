from typing import List, Optional

from news_events_lib.models import EventStatus, NewsEventModel
from sqlalchemy import desc, select


class LiveDeskService:
    def __init__(self, session, publisher_domain):
        self.session = session
        self.publisher_domain = publisher_domain

    def get_top_events(self, limit: int = 50) -> List[dict]:
        """Returns top published events for the dashboard."""
        events = self.session.scalars(
            select(NewsEventModel)
            .where(NewsEventModel.status == EventStatus.PUBLISHED)
            .order_by(desc(NewsEventModel.hot_score))
            .limit(limit)
        ).all()

        return [
            {
                "id": str(e.id),
                "title": e.title,
                "hot_score": e.hot_score,
                "editorial_score": e.editorial_score,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ]

    def get_event_details(self, event_id: str) -> Optional[dict]:
        event = self.session.get(NewsEventModel, event_id)
        if not event:
            return None

        return {
            "id": str(event.id),
            "title": event.title,
            "status": event.status.value,
            "hot_score": event.hot_score,
            "editorial_score": event.editorial_score,
            "ai_impact_score": event.ai_impact_score,
            "insights": event.publisher_insights,
        }

    def boost_event(self, event_id: str, amount: int = 10) -> str:
        event = self.session.get(NewsEventModel, event_id)
        if not event:
            return "❌ Event not found."

        event.editorial_score = (event.editorial_score or 0) + amount

        # Recalc
        topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []
        new_score, insights, _ = self.publisher_domain.calculate_spectrum_score(
            event, topics
        )
        event.hot_score = new_score
        event.publisher_insights = insights

        self.session.commit()
        return f"✅ Event boosted by {amount}. New Score: {new_score:.2f}"

    def kill_event(self, event_id: str) -> str:
        event = self.session.get(NewsEventModel, event_id)
        if not event:
            return "❌ Event not found."

        event.status = EventStatus.ARCHIVED
        event.is_active = False
        self.session.commit()
        return "✅ Event killed (Archived)."
