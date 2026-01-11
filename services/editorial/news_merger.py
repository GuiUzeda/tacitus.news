"""
News Merger Service
Scans active events to find duplicates that should be merged.
Generates MergeProposals for the NewsReviewer or auto-merges if confidence is very high.
"""
import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import create_engine, select, and_, or_, update
from sqlalchemy.orm import sessionmaker
from loguru import logger

from config import Settings
from news_events_lib.models import NewsEventModel, MergeProposalModel, EventStatus
from news_cluster import NewsCluster

class NewsMergerScanner:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.cluster = NewsCluster()
        
        # Thresholds
        self.AUTO_MERGE_THRESHOLD = 0.05 # Very strict distance (Lower is better for Cosine Distance)
        self.PROPOSAL_THRESHOLD = 0.28    # Looser distance for proposals
        self.SCAN_WINDOW_HOURS = 48      # Only check events updated recently

    def run(self):
        logger.info("🔄 News Merger Scanner started (Manual Run)...")
        try:
            self.scan_cycle()
        except Exception as e:
            logger.error(f"Merger Error: {e}")
        logger.info("✅ Merger run complete. Exiting.")

    def scan_cycle(self):
        with self.SessionLocal() as session:
            # 1. Fetch Active Events updated recently
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self.SCAN_WINDOW_HOURS)
            events = session.scalars(
                select(NewsEventModel)
                .where(
                    NewsEventModel.is_active == True,
                    NewsEventModel.last_updated_at >= cutoff,
                    NewsEventModel.status != EventStatus.DRAFT
                )
                .order_by(NewsEventModel.last_updated_at.desc())
            ).all()

            if not events:
                logger.info("📭 No active events to scan in window.")
                return

            logger.info(f"🔍 Scanning {len(events)} active events for duplicates (Window: {self.SCAN_WINDOW_HOURS}h)...")

            for event in events:
                # Refresh/Check if event was merged in a previous iteration of this loop
                if not event.is_active:
                    continue
                self._check_event_duplicates(session, event)

    def _check_event_duplicates(self, session, event: NewsEventModel):
        # Search for similar events (excluding self)
        # We use the cluster's hybrid search
        candidates = self.cluster.search_news_events_hybrid(
            session,
            query_text=event.title,
            query_vector=event.embedding_centroid,
            target_date=event.created_at,
            diff_date=timedelta(days=3), # Look at 3 day window
            limit=5
        )

        for candidate, rrf_score, vec_dist in candidates:
            if candidate.id == event.id:
                continue
            
            # Skip if candidate is already merged/inactive
            if not candidate.is_active:
                continue

            # Check if we already have a pending/rejected proposal for this pair
            existing_proposal = session.scalar(
                select(MergeProposalModel)
                .where(
                    MergeProposalModel.source_event_id == event.id,
                    MergeProposalModel.target_event_id == candidate.id
                )
            )
            if existing_proposal:
                continue

            # --- DECISION LOGIC ---
            
            # 1. Auto-Merge (Very High Confidence)
            # Note: Vector distance is Cosine Distance (0 = identical, 1 = opposite)
            if vec_dist < self.AUTO_MERGE_THRESHOLD:
                logger.warning(f"⚡ AUTO-MERGE: {event.title} -> {candidate.title} (Dist: {vec_dist:.3f})")
                self.cluster.execute_event_merge(session, event, candidate)
                session.commit()
                return # Event is now dead, stop scanning it

            # 2. Propose (Ambiguous)
            elif vec_dist < self.PROPOSAL_THRESHOLD:
                logger.info(f"💡 PROPOSAL: {event.title} -> {candidate.title} (Dist: {vec_dist:.3f})")
                self._create_proposal(session, event, candidate, vec_dist)
                session.commit()

    def _create_proposal(self, session, source: NewsEventModel, target: NewsEventModel, score: float):
        proposal = MergeProposalModel(
            id=uuid.uuid4(),
            proposal_type="event_merge",
            source_event_id=source.id,
            target_event_id=target.id,
            similarity_score=float(score),
            status="pending",
            reasoning=f"MergerScanner: Vector Dist {score:.3f}"
        )
        session.add(proposal)

if __name__ == "__main__":
    scanner = NewsMergerScanner()
    scanner.run()
