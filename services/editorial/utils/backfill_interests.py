import asyncio
import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger


from editorial.config import Settings
from editorial.llm_parser import CloudNewsAnalyzer
from news_events_lib.models import NewsEventModel, ArticleModel, ArticleContentModel

class BackfillWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.analyzer = CloudNewsAnalyzer()
        
        # Semaphore to prevent hitting rate limits too hard
        self.semaphore = asyncio.Semaphore(2) 

    def _map_stance_to_bucket(self, score: float) -> str:
        """Helper to map float stance to bucket for Event aggregation."""
        if score is None:
            return "neutral"
        if score <= -0.35:
            return "critical"
        elif score >= 0.35:
            return "supportive"
        return "neutral"

    async def analyze_article(self, article: ArticleModel) -> bool:
        """
        Runs the LLM on a single article and updates its fields.
        Returns True if successful, False otherwise.
        """
        if not article.contents:
            logger.warning(f"⚠️ Article {article.id} has no content. Skipping.")
            return False

        content_text = article.contents[0].content
        
        async with self.semaphore:
            try:
                # Call LLM
                result = await self.analyzer.analyze_article(content_text)
                
                if not result:
                    logger.warning(f"❌ Failed to analyze article {article.title[:20]}")
                    return False

                # --- Update Article Fields ---
                article.interests = result.entities
                article.main_topics = result.main_topics
                article.key_points = result.key_points
                article.summary = result.summary
                article.stance = result.stance  # Float
                article.stance_reasoning = result.stance_reasoning

                # Maintain legacy flat list for search compatibility
                all_entities = []
                if result.entities:
                    for entity_list in result.entities.values():
                        all_entities.extend(entity_list)
                article.entities = list(set(all_entities))

                logger.info(f"✅ Updated Article: {article.title[:30]}")
                return True

            except Exception as e:
                logger.error(f"Error processing article {article.id}: {e}")
                return False

    async def process_event(self, event_id):
        """
        Process all articles in an event, then re-aggregate the event stats.
        """
        with self.SessionLocal() as session:
            event = session.get(NewsEventModel, event_id)
            if not event:
                return

            # Fetch articles with content and newspaper data
            articles = (
                session.query(ArticleModel)
                .filter(ArticleModel.event_id == event.id)
                .options(joinedload(ArticleModel.contents))
                .options(joinedload(ArticleModel.newspaper))
                .all()
            )

            logger.info(f"📂 Event '{event.title}' - Processing {len(articles)} articles...")

            # 1. Analyze all articles concurrently
            tasks = [self.analyze_article(article) for article in articles]
            results = await asyncio.gather(*tasks)
            
            success_count = sum(results)
            if success_count == 0 and len(articles) > 0:
                logger.error(f"Skipping Event Aggregation for '{event.title}' (No articles updated)")
                return

            # 2. Re-Aggregate Event Stats (From Scratch)
            # This ensures the Event is perfectly synced with the new Article data
            agg_interests = {}
            agg_bias_dist = {}
            agg_stance_dist = {}
            agg_ownership = {}

            for article in articles:
                # A. Interests
                if article.interests:
                    for category, items in article.interests.items():
                        if category not in agg_interests:
                            agg_interests[category] = {}
                        for item in items:
                            agg_interests[category][item] = agg_interests[category].get(item, 0) + 1
                
                # B. Bias Distribution
                if article.newspaper and article.newspaper.bias:
                    bias = article.newspaper.bias
                    agg_bias_dist[bias] = agg_bias_dist.get(bias, 0) + 1

                    # C. Stance Distribution (Dependent on Bias)
                    if article.stance is not None:
                        bucket = self._map_stance_to_bucket(article.stance)
                        if bias not in agg_stance_dist:
                            agg_stance_dist[bias] = {}
                        agg_stance_dist[bias][bucket] = agg_stance_dist[bias].get(bucket, 0) + 1

                # D. Ownership Stats
                if article.newspaper and article.newspaper.ownership_type:
                    otype = article.newspaper.ownership_type
                    agg_ownership[otype] = agg_ownership.get(otype, 0) + 1

            # Apply Updates
            event.interest_counts = agg_interests
            event.bias_distribution = agg_bias_dist
            event.stance_distribution = agg_stance_dist
            event.ownership_stats = agg_ownership
            event.last_updated_at = datetime.utcnow()

            session.add(event)
            session.commit()
            logger.success(f"🏁 Finished Event: {event.title} (Aggregated {success_count} articles)")

    async def run(self):
        logger.info("🚀 Starting Backfill of Interests & Stance...")
        
        # Fetch all Event IDs
        with self.SessionLocal() as session:
            events = session.execute(select(NewsEventModel.id)).scalars().all()
        
        logger.info(f"Found {len(events)} events to process.")

        for i, event_id in enumerate(events):
            await self.process_event(event_id)
            logger.info(f"--- Progress: {i+1}/{len(events)} ---")
            
            # Sleep briefly to be nice to the API
            await asyncio.sleep(2)

if __name__ == "__main__":
    worker = BackfillWorker()
    asyncio.run(worker.run())