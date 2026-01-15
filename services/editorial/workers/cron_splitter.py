import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session

# Existing Domains
from domain.clustering import NewsCluster
from domain.enhancer import NewsEnhancerDomain
from domain.publisher import NewsPublisherDomain
from news_events_lib.models import (
    NewsEventModel, 
    EventStatus, 
    ArticleModel, 
    MergeProposalModel, 
    JobStatus
)

class EventSplitterService:
    def __init__(self):
        self.cluster_domain = NewsCluster()
        self.enhancer_domain = NewsEnhancerDomain()
        self.publisher_domain = NewsPublisherDomain()
        
        # Configuration for "Mega Events"
        self.SPLIT_THRESHOLD_COUNT = 30 
        self.SPLIT_THRESHOLD_HOURS = 24

    async def run_cycle(self, session):
        logger.info("🔪 Starting Splitter Cycle...")
        
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
        
        # 1. CLUSTER ANALYSIS
        sub_clusters = self.cluster_domain.calculate_sub_clusters(
            session, parent_event, method="DBSCAN_WITH_TIME"
        )
        
        if len(sub_clusters) < 2:
            logger.info("   -> Solid event, no split needed.")
            return

        # 2. IDENTIFY SURVIVOR VS. DISSIDENTS (Pruning Strategy)
        sub_clusters.sort(key=len, reverse=True)
        
        dominant_group = sub_clusters[0]      # Stays in Parent
        dissident_groups = sub_clusters[1:]   # Ejected to New Events

        logger.warning(
            f"   -> PRUNING: Keeping {len(dominant_group)} arts in Parent, "
            f"ejecting {len(dissident_groups)} groups."
        )

        # 3. EJECT DISSIDENTS
        new_events = []
        for group in dissident_groups:
            seed_article = group[0]
            
            new_event_id = self.cluster_domain.execute_new_event_action(
                session, seed_article, reason=f"Split from {parent_event.title}"
            )
            new_event = session.get(NewsEventModel, new_event_id)
            
            for art in group[1:]:
                self.cluster_domain._link_to_event(session, new_event, art, art.embedding)
            
            session.refresh(new_event)
            new_events.append(new_event)

        # 4. PROCESS EJECTED EVENTS (Enhance & Publish - No Commit)
        for child_event in new_events:
            await self.enhancer_domain.enhance_event_direct(session, child_event, commit=False)
            self.publisher_domain.publish_event_direct(session, child_event, commit=False)
            logger.success(f"   -> Created Child: {child_event.title}")

        # 5. REFINE PARENT (The Survivor)
        session.refresh(parent_event)
        
        # Reset & Re-aggregate
        parent_event.article_count = 0
        parent_event.editorial_score = 0.0
        parent_event.interest_counts = {}
        parent_event.main_topic_counts = {}
        parent_event.article_counts_by_bias = {}
        
        # Re-run aggregation for survivor articles
        from domain.aggregator import EventAggregator
        for art in parent_event.articles:
            EventAggregator.aggregate_basic_stats(parent_event, art)
            EventAggregator.aggregate_interests(parent_event, art.interests)
            EventAggregator.aggregate_main_topics(parent_event, art.main_topics)
            EventAggregator.update_centroid(parent_event, art.embedding)
            if art.newspaper:
                 EventAggregator.aggregate_metadata(parent_event, art)

        await self.enhancer_domain.enhance_event_direct(session, parent_event, commit=False)
        self.publisher_domain.publish_event_direct(session, parent_event, commit=False)

        # 6. IMMUNITY: CREATE "ANTI-MERGE" PROPOSALS (The Loop Fix)
        # We explicitly tell the Merger: "We checked these, and they are NOT duplicates."
        for child in new_events:
            immunity_proposal = MergeProposalModel(
                id=uuid.uuid4(),
                proposal_type="event_merge",
                source_event_id=child.id,
                target_event_id=parent_event.id,
                distance_score=0.0, # Irrelevant, but required
                status=JobStatus.REJECTED, # <--- The Shield
                reasoning="Split Origin: Explicitly separated by Splitter."
            )
            session.add(immunity_proposal)

        # 7. ATOMIC COMMIT
        session.commit()
        logger.success(f"✂️ Pruned {parent_event.title} & Immunized {len(new_events)} children.")

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