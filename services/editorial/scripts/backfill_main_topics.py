import asyncio
import sys
import os
from sqlalchemy import create_engine, select, or_
from sqlalchemy.orm import sessionmaker
from loguru import logger

# Add service root to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Settings
from news_events_lib.models import NewsEventModel, ArticleModel

class BackfillMainTopicsWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)

    def calculate_topic_counts(self, articles: list[ArticleModel]) -> dict[str, int]:
        """Aggregates main_topics from a list of articles."""
        counts = {}
        for article in articles:
            if not article.main_topics:
                continue
            
            for topic in article.main_topics:
                counts[topic] = counts.get(topic, 0) + 1
        return counts

    async def process_event(self, session, event: NewsEventModel):
        # Fetch articles linked to this event
        articles = (
            session.query(ArticleModel)
            .filter(ArticleModel.event_id == event.id)
            .all()
        )

        if not articles:
            return

        # Calculate new counts
        new_counts = self.calculate_topic_counts(articles)

        if new_counts:
            event.main_topic_counts = new_counts
            session.add(event)
        elif event.main_topic_counts is None:
            # Initialize empty dict if it was None to avoid future null checks
            event.main_topic_counts = {}
            session.add(event)

    async def run(self):
        logger.info("🚀 Starting Main Topics Backfill...")

        with self.SessionLocal() as session:
            # Find events where main_topic_counts is NULL or Empty JSON
            stmt = select(NewsEventModel).where(
                or_(
                    NewsEventModel.main_topic_counts.is_(None),
                    NewsEventModel.main_topic_counts == {},
                )
            )

            # Fetch all candidates
            events = session.execute(stmt).scalars().all()
            total = len(events)
            logger.info(f"Found {total} events to process.")

            for i, event in enumerate(events):
                try:
                    await self.process_event(session, event)

                    # Commit in chunks
                    if (i + 1) % 50 == 0:
                        session.commit()
                        logger.info(f"Progress: {i + 1}/{total}")

                except Exception as e:
                    logger.error(f"Error processing event {event.id}: {e}")
                    session.rollback()

            # Final commit
            session.commit()
            logger.success(f"✅ Backfill Complete. Processed {total} events.")

if __name__ == "__main__":
    worker = BackfillMainTopicsWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Backfill stopped by user.")