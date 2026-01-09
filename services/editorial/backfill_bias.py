import asyncio
import sys
import os
from uuid import UUID
from sqlalchemy import create_engine, select, or_
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger


from config import Settings
from news_events_lib.models import NewsEventModel, ArticleModel, NewspaperModel


class BiasBackfillWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)

    def calculate_bias_distribution(
        self, articles: list[ArticleModel]
    ) -> dict[str, list[str]]:
        """Aggregates the bias from a list of articles."""
        distribution = {}
        for article in articles:
            if not article.newspaper:
                continue

            bias = article.newspaper.bias
            if bias:

                news_distribution = distribution.get(bias, set())
                news_distribution.add(str(article.newspaper.id))
                distribution[bias] = news_distribution

        return {k: list(v) for k, v in distribution.items()}


    async def process_event(self, session, event: NewsEventModel):
        # Fetch articles linked to this event, joining newspaper to get the bias data
        articles = (
            session.query(ArticleModel)
            .options(joinedload(ArticleModel.newspaper))
            .filter(ArticleModel.event_id == event.id)
            .all()
        )

        if not articles:
            return

        # Calculate new distribution
        new_dist = self.calculate_bias_distribution(articles)

        if new_dist:
            event.bias_distribution = new_dist
            
            # Determine/Update the 'ownership_stats' while we are at it?
            # (Optional, but efficient since we have the data loaded)
            # event.ownership_stats = ...

            session.add(event)
            # logger.debug(f"Updated '{event.title[:20]}...': {new_dist}")
        else:
            logger.warning(
                f"Event '{event.title}' has articles but no bias data found."
            )

    async def run(self):
        logger.info("🚀 Starting Bias Distribution Backfill...")

        with self.SessionLocal() as session:
            # Find events where bias_distribution is NULL or Empty JSON
            stmt = select(NewsEventModel).where(
                or_(
                    NewsEventModel.bias_distribution.is_(None),
                    NewsEventModel.bias_distribution == {},
                )
            )

            # Fetch all candidates
            events = session.execute(stmt).scalars().all()
            total = len(events)
            logger.info(f"Found {total} events with missing bias distribution.")

            if total == 0:
                return

            for i, event in enumerate(events):
                try:
                    await self.process_event(session, event)

                    # Commit in chunks to avoid massive transactions
                    if (i + 1) % 50 == 0:
                        session.commit()
                        logger.info(f"Progress: {i + 1}/{total}")

                except Exception as e:
                    logger.error(f"Error processing event {event.id}: {e}")
                    session.rollback()

            # Final commit for remaining items
            session.commit()
            logger.success(f"✅ Backfill Complete. Processed {total} events.")


if __name__ == "__main__":
    worker = BiasBackfillWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Backfill stopped by user.")
