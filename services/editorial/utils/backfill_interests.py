import asyncio
import sys
import os
from datetime import datetime
from typing import List

# NLP Imports
import spacy

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger



from editorial.config import Settings
from editorial.llm_parser import CloudNewsAnalyzer
from news_events_lib.models import NewsEventModel, ArticleModel

class BackfillWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.analyzer = CloudNewsAnalyzer()
        
        # Semaphore to control concurrency
        self.semaphore = asyncio.Semaphore(3) 

        # Initialize spaCy
        try:
            logger.info("⏳ Loading spaCy model (pt_core_news_lg)...")
            self.nlp = spacy.load("pt_core_news_lg")
            logger.info("✅ spaCy loaded.")
        except OSError:
            logger.warning("⚠️ 'pt_core_news_lg' not found. Falling back to 'pt_core_news_sm'.")
            logger.warning("👉 Recommended: python -m spacy download pt_core_news_lg")
            try:
                self.nlp = spacy.load("pt_core_news_sm")
            except:
                logger.error("❌ No Portuguese spaCy model found. Exiting.")
                sys.exit(1)

    def _map_stance_to_bucket(self, score: float) -> str:
        """Helper to map float stance to bucket for Event aggregation."""
        if score is None:
            return "neutral"
        if score <= -0.35:
            return "critical"
        elif score >= 0.35:
            return "supportive"
        return "neutral"

    def _extract_spacy_entities(self, text: str, existing_topics: list) -> dict[str, List[str]]:
        """
        Hybrid extraction: Uses spaCy for hard entities, keeps LLM for topics.
        """
        doc = self.nlp(text)
        
        entities = {
            "person": set(),
            "place": set(),
            "org": set(),
            "topic": set(existing_topics or []) # Start with LLM topics
        }

        for ent in doc.ents:
            clean_text = ent.text.strip()
            if len(clean_text) < 3: # Ignore noise
                continue

            if ent.label_ == "PER":
                entities["person"].add(clean_text)
            elif ent.label_ in ["LOC", "GPE"]:
                entities["place"].add(clean_text)
            elif ent.label_ == "ORG":
                entities["organization"].add(clean_text)
            elif ent.label_ == "MISC":
                entities["topic"].add(clean_text)

        return {k: sorted(list(v)) for k, v in entities.items() if v}

    async def analyze_article(self, article: ArticleModel) -> bool:
        """
        Runs the LLM + spaCy pipeline on a single article.
        """
        if not article.contents:
            return False

        # Limit text size for spaCy performance
        content_text = article.contents[0].content
        content_text_limited = content_text[:100000]

        async with self.semaphore:
            try:
                # 1. Run LLM
                result = await self.analyzer.analyze_article(content_text_limited)
                
                if not result:
                    logger.warning(f"❌ Failed to analyze article {article.title[:20]}")
                    return False

                # 2. Run spaCy (Hybrid Extraction)
                structured_interests = self._extract_spacy_entities(
                    content_text_limited, 
                    result.main_topics
                )

                # 3. Update Article Fields
                article.interests = structured_interests
                article.main_topics = result.main_topics
                article.key_points = result.key_points
                article.summary = result.summary
                article.stance = float(result.stance)  # Float
                article.stance_reasoning = result.stance_reasoning

                # Legacy Flat List (for search compatibility)
                all_entities = []
                for entity_list in structured_interests.values():
                    all_entities.extend(entity_list)
                article.entities = list(set(all_entities))

                logger.info(f"✅ Updated Article: {article.title[:30]}")
                return True

            except Exception as e:
                logger.error(f"Error processing article {article.id}: {e}")
                return False

    async def process_event(self, event_id):
        """
        Phase 1: Process linked articles and re-aggregate the event.
        """
        with self.SessionLocal() as session:
            event = session.get(NewsEventModel, event_id)
            if not event:
                return

            articles = (
                session.query(ArticleModel)
                .filter(ArticleModel.event_id == event.id)
                .options(joinedload(ArticleModel.contents))
                .options(joinedload(ArticleModel.newspaper))
                .all()
            )

            if not articles:
                return

            logger.info(f"📂 Event '{event.title}' - Processing {len(articles)} articles...")

            # 1. Update Articles
            tasks = [self.analyze_article(article) for article in articles]
            results = await asyncio.gather(*tasks)
            
            success_count = sum(results)
            if success_count == 0:
                return

            # 2. Re-Aggregate Event Stats
            agg_interests = {}
            agg_bias_dist = {}
            agg_stance_dist = {}
            agg_ownership = {}

            for article in articles:
                # Interests
                if article.interests:
                    for category, items in article.interests.items():
                        if category not in agg_interests:
                            agg_interests[category] = {}
                        for item in items:
                            agg_interests[category][item] = agg_interests[category].get(item, 0) + 1
                
                # Newspaper Meta
                if article.newspaper:
                    if article.newspaper.bias:
                        bias = article.newspaper.bias
                        agg_bias_dist[bias] = agg_bias_dist.get(bias, 0) + 1

                        if article.stance is not None:
                            bucket = self._map_stance_to_bucket(article.stance)
                            if bias not in agg_stance_dist:
                                agg_stance_dist[bias] = {}
                            agg_stance_dist[bias][bucket] = agg_stance_dist[bias].get(bucket, 0) + 1

                    if article.newspaper.ownership_type:
                        otype = article.newspaper.ownership_type
                        agg_ownership[otype] = agg_ownership.get(otype, 0) + 1

            event.interest_counts = agg_interests
            event.bias_distribution = agg_bias_dist
            event.stance_distribution = agg_stance_dist
            event.ownership_stats = agg_ownership
            event.last_updated_at = datetime.utcnow()

            session.add(event)
            session.commit()
            logger.success(f"🏁 Aggregated Event: {event.title}")

    async def process_orphans(self):
        """
        Phase 2: Process articles that are NOT linked to any event.
        """
        logger.info("🕵️ Starting Phase 2: Processing Orphan Articles...")
        
        with self.SessionLocal() as session:
            # Fetch IDs of orphans
            stmt = select(ArticleModel.id).where(ArticleModel.event_id.is_(None))
            orphan_ids = session.execute(stmt).scalars().all()
        
        logger.info(f"Found {len(orphan_ids)} orphan articles.")

        # Process in batches to avoid holding too many objects in memory
        BATCH_SIZE = 10
        for i in range(0, len(orphan_ids), BATCH_SIZE):
            batch_ids = orphan_ids[i : i + BATCH_SIZE]
            
            with self.SessionLocal() as batch_session:
                articles = (
                    batch_session.query(ArticleModel)
                    .filter(ArticleModel.id.in_(batch_ids))
                    .options(joinedload(ArticleModel.contents))
                    .all()
                )
                
                tasks = [self.analyze_article(article) for article in articles]
                await asyncio.gather(*tasks)
                
                batch_session.commit()
                logger.info(f"--- Orphans Progress: {i + len(articles)}/{len(orphan_ids)} ---")
                await asyncio.sleep(1)

    async def run(self):
        logger.info("🚀 Starting Comprehensive Backfill (Articles + Events)...")
        
        # --- PHASE 1: EVENTS ---
        logger.info("=== PHASE 1: Events & Linked Articles ===")
        with self.SessionLocal() as session:
            # Sort by recent updates to prioritize active news
            events = session.execute(
                select(NewsEventModel.id)
                .order_by(NewsEventModel.last_updated_at.desc())
            ).scalars().all()
        
        logger.info(f"Found {len(events)} events to process.")
        for i, event_id in enumerate(events):
            await self.process_event(event_id)
            if i % 5 == 0:
                logger.info(f"--- Event Progress: {i+1}/{len(events)} ---")
                await asyncio.sleep(1)

        # --- PHASE 2: ORPHANS ---
        logger.info("=== PHASE 2: Orphan Articles ===")
        await self.process_orphans()
        
        logger.success("✅ Backfill Complete.")

if __name__ == "__main__":
    worker = BackfillWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Backfill stopped by user.")