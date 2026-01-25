from typing import List, Optional

from news_events_lib.models import FeedModel, NewspaperModel
from sqlalchemy import select


class FeedManagementService:
    def __init__(self, session):
        self.session = session

    def list_newspapers(self, search: Optional[str] = None) -> List[dict]:
        stmt = select(NewspaperModel).order_by(NewspaperModel.name)
        if search:
            stmt = stmt.where(NewspaperModel.name.ilike(f"%{search}%"))

        newspapers = self.session.scalars(stmt).all()
        return [
            {"id": str(n.id), "name": n.name, "feeds": len(n.feeds)} for n in newspapers
        ]

    def get_feeds_for_newspaper(self, newspaper_id: str) -> List[dict]:
        stmt = select(FeedModel).where(FeedModel.newspaper_id == newspaper_id)
        feeds = self.session.scalars(stmt).all()
        return [
            {
                "id": str(f.id),
                "url": f.url,
                "type": f.feed_type,
                "is_ranked": f.is_ranked,
                "use_browser": f.use_browser_render,
                "pattern": f.url_pattern,
            }
            for f in feeds
        ]

    def update_feed_config(
        self,
        feed_id: str,
        pattern: str = None,
        use_browser: bool = None,
        is_ranked: bool = None,
    ) -> str:
        stmt = select(FeedModel).where(FeedModel.id == feed_id)
        feed = self.session.scalar(stmt)
        if not feed:
            return "❌ Feed not found."

        changes = []
        if pattern is not None:
            feed.url_pattern = pattern
            changes.append(f"Pattern='{pattern}'")

        if use_browser is not None:
            feed.use_browser_render = use_browser
            changes.append(f"Browser={use_browser}")

        if is_ranked is not None:
            feed.is_ranked = is_ranked
            changes.append(f"Ranked={is_ranked}")

        if not changes:
            return "⚠️ No changes requested."

        self.session.add(feed)
        self.session.commit()
        return f"✅ Updated Feed: {', '.join(changes)}"
