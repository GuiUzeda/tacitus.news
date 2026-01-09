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


from config import Settings
from news_events_lib.models import NewsEventModel, ArticleModel

class BackfillWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)
        
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

    def _extract_spacy_entities(self, text: str) -> dict:
        """
        Pure spaCy extraction for entities.
        """
        # Increase limit if needed, but 100k is safe for memory
        doc = self.nlp(text[:100000]) 
        
        entities = {
            "person": set(),
            "place": set(),
            "org": set(),
            "topic": set()
        }

        for ent in doc.ents:
            clean_text = ent.text.strip()
            # Filter noise (short words)
            if len(clean_text) < 3: 
                continue

            if ent.label_ == "PER":
                entities["person"].add(clean_text)
            elif ent.label_ in ["LOC", "GPE"]:
                entities["place"].add(clean_text)
            elif ent.label_ == "ORG":
                entities["org"].add(clean_text)
            elif ent.label_ == "MISC":
                entities["topic"].add(clean_text)

        # Return sorted lists
        return {k: sorted(list(v)) for k, v in entities.items() if v}

    async def analyze_article(self, article: ArticleModel) -> bool:
        """
        Updates ONLY the entities/interests of the article using spaCy.
        """
        if not article.contents:
            return False

        try:
            content_text = article.contents[0].content
            
            # 1. Run spaCy Extraction
            structured_interests = self._extract_spacy_entities(f"{article.title} {article.summary}")

            # 2. Update Article Fields
            # We DO NOT touch stance, summary, or main_topics
            article.interests = structured_interests

            # 3. Update Legacy Flat List (for search compatibility)
            all_entities = []
            for entity_list in structured_interests.values():
                all_entities.extend(entity_list)
            article.entities = list(set(all_entities))

            logger.info(f"✅ Updated Interests: {article.title[:30]}")
            return True

        except Exception as e:
            logger.error(f"Error processing article {article.id}: {e}")
            return False

    async def process_event(self, event_id):
        """
        Phase 1: Process linked articles and re-aggregate ONLY interests.
        """
        with self.SessionLocal() as session:
            event = session.get(NewsEventModel, event_id)
            if not event:
                return

            articles = (
                session.query(ArticleModel)
                .filter(ArticleModel.event_id == event.id)
                .options(joinedload(ArticleModel.contents))
                .all()
            )

            if not articles:
                return

            logger.info(f"📂 Event '{event.title}' - Processing {len(articles)} articles...")

            # 1. Update Articles (Interests Only)
            # Since this is CPU bound (spaCy), we can run sequentially or offload to thread.
            # For simplicity in this script, we run sequentially.
            updated_count = 0
            for article in articles:
                if await self.analyze_article(article):
                    updated_count += 1
            
            if updated_count == 0:
                return

            # 2. Re-Aggregate ONLY Interest Counts
            agg_interests = {}

            for article in articles:
                if article.interests:
                    for category, items in article.interests.items():
                        if category not in agg_interests:
                            agg_interests[category] = {}
                        for item in items:
                            agg_interests[category][item] = agg_interests[category].get(item, 0) + 1
            
            # Update Event
            event.interest_counts = agg_interests
            event.last_updated_at = datetime.utcnow()

            session.add(event)
            session.commit()
            logger.success(f"🏁 Refreshed Interests for Event: {event.title}")

    async def process_orphans(self):
        """
        Phase 2: Process articles that are NOT linked to any event.
        """
        logger.info("🕵️ Starting Phase 2: Processing Orphan Articles...")
        
        with self.SessionLocal() as session:
            stmt = select(ArticleModel.id).where(ArticleModel.event_id.is_(None))
            orphan_ids = session.execute(stmt).scalars().all()
        
        logger.info(f"Found {len(orphan_ids)} orphan articles.")

        BATCH_SIZE = 20
        for i in range(0, len(orphan_ids), BATCH_SIZE):
            batch_ids = orphan_ids[i : i + BATCH_SIZE]
            
            with self.SessionLocal() as batch_session:
                articles = (
                    batch_session.query(ArticleModel)
                    .filter(ArticleModel.id.in_(batch_ids))
                    .options(joinedload(ArticleModel.contents))
                    .all()
                )
                
                for article in articles:
                    await self.analyze_article(article)
                
                batch_session.commit()
                logger.info(f"--- Orphans Progress: {i + len(articles)}/{len(orphan_ids)} ---")

    async def run(self):
        logger.info("🚀 Starting Entities-Only Backfill (spaCy)...")
        
        # --- PHASE 1: EVENTS ---
        logger.info("=== PHASE 1: Events & Linked Articles ===")
        with self.SessionLocal() as session:
            # Process most recent events first
            events = session.execute(
                select(NewsEventModel.id)
                .order_by(NewsEventModel.last_updated_at.desc())
            ).scalars().all()
        
        logger.info(f"Found {len(events)} events to process.")
        for i, event_id in enumerate(events):
            await self.process_event(event_id)
            if i % 10 == 0:
                logger.info(f"--- Event Progress: {i+1}/{len(events)} ---")

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