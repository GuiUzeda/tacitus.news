import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import asyncio
from datetime import datetime, timedelta, timezone
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session

# Existing Domains
from domain.clustering import NewsCluster
from domain.enhancer import NewsEnhancerDomain
from domain.publisher import NewsPublisherDomain
from news_events_lib.models import NewsEventModel, EventStatus, ArticleModel

class EventSplitterService:
    def __init__(self):
        self.cluster_domain = NewsCluster()
        self.enhancer_domain = NewsEnhancerDomain()
        self.publisher_domain = NewsPublisherDomain()
        
        # Configuration for "Mega Events"
        self.SPLIT_THRESHOLD_COUNT = 30  # Events with > 30 articles
        self.SPLIT_THRESHOLD_HOURS = 24  # Older than 24h

    async def run_cycle(self, session):
        logger.info("🔪 Starting Splitter Cycle...")
        
        # 1. FIND CANDIDATES
        # We look for published active events that are too big/old
        candidates = session.scalars(
            select(NewsEventModel)
            .where(
                NewsEventModel.status == EventStatus.PUBLISHED,
                NewsEventModel.is_active == True,
                NewsEventModel.article_count >= self.SPLIT_THRESHOLD_COUNT
            )
        ).all()

        for parent_event in candidates:
            await self._process_split(session, parent_event)

    async def _process_split(self, session: Session, parent_event):
        logger.info(f"⚡ Analyzing {parent_event.title} ({parent_event.article_count} articles)...")
        
        # 2. PERFORM SPLIT (Logic needed in Clustering Domain)
        # This function needs to be implemented (see below)
        # It should return a list of lists: [[art1, art2], [art3, art4, art5]]
        sub_clusters = self.cluster_domain.calculate_sub_clusters(
            session, parent_event, method="DBSCAN_WITH_TIME"
        )
        
        if len(sub_clusters) < 2:
            logger.info("   -> Solid event, no split needed.")
            return

        logger.warning(f"   -> SPLITTING into {len(sub_clusters)} sub-events!")

        new_events = []
        
        # 3. CREATE CHILDREN & RE-HOME ARTICLES
        for articles_group in sub_clusters:
            # Create new event from the group (uses clustering logic)
            # We assume the first article is the 'seed'
            seed_article = articles_group[0]
            new_event_id = self.cluster_domain.execute_new_event_action(
                session, seed_article, reason=f"Split from {parent_event.title}"
            )
            new_event = session.get(NewsEventModel, new_event_id)
            
            # Move remaining articles
            for art in articles_group[1:]:
                self.cluster_domain._link_to_event(session, new_event, art, art.embedding)
            session.refresh(new_event)
            
            new_events.append(new_event)

        # 4. ENHANCE & PUBLISH (In One Go)
        for child_event in new_events:
            # A. Enhance (Summarize)
            # We construct a fake 'job' object if the domain requires it, 
            # or refactor enhance_event_job to accept an event directly.
            await self.enhancer_domain.enhance_event_direct(session, child_event)
            
            # B. Publish (Score)
            # We calculate score and set status
            self.publisher_domain.publish_event_direct(session, child_event)
            
            logger.success(f"   -> Created Child: {child_event.title} (Score: {child_event.hot_score})")

        # 5. RETIRE PARENT
        parent_event.status = EventStatus.ARCHIVED
        parent_event.is_active = False
        parent_event.title = f"[SPLIT] {parent_event.title}"
        session.add(parent_event)
        session.commit()
        
if __name__ == "__main__":
    from config import Settings

    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    splitter = EventSplitterService()
    
    async def run_splitter_standalone():
        with SessionLocal() as session:
            await splitter.run_cycle(session)

    asyncio.run(run_splitter_standalone())
    