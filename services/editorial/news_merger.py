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
from news_events_lib.models import NewsEventModel, MergeProposalModel, EventStatus, JobStatus
from news_cluster import NewsCluster
from models import EventsQueueModel, EventsQueueName

class NewsMergerScanner:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.cluster = NewsCluster()
        
        # Thresholds
        self.AUTO_MERGE_THRESHOLD = 0.05 # Very strict distance (Lower is better for Cosine Distance)
        self.PROPOSAL_THRESHOLD = 0.15    # Looser distance for proposals
        self.RRF_THRESHOLD = 0.12       # Minimum RRF score to consider keyword-heavy matches
        self.SCAN_WINDOW_HOURS = 48      # Only check events updated recently

    async def run(self):
        logger.info("🔄 News Merger Scanner started (Async Producer-Consumer)...")
        
        self.queue = asyncio.Queue(maxsize=20)
        
        # Start Consumers (Staggered)
        workers = []
        for i in range(3):
            workers.append(asyncio.create_task(self._consumer_loop(i)))
            await asyncio.sleep(1.0)

        while True:
            try:
                # Producer: Fetch Active Events updated recently
                with self.SessionLocal() as session:
                    cutoff = datetime.now(timezone.utc) - timedelta(hours=self.SCAN_WINDOW_HOURS)
                    
                    pending_subq = select(MergeProposalModel.source_event_id).where(
                        MergeProposalModel.status == "pending",
                        MergeProposalModel.source_event_id.is_not(None)
                    )

                    # Filter out events processing in enhancer
                    processing_subq = select(EventsQueueModel.event_id).where(
                        EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                        EventsQueueModel.status == JobStatus.PROCESSING
                    )

                    # Fetch IDs only to keep memory light
                    stmt = (
                        select(NewsEventModel.id)
                        .where(
                            NewsEventModel.is_active == True,
                            NewsEventModel.last_updated_at >= cutoff,
                            # NewsEventModel.status == EventStatus.DRAFT,
                            NewsEventModel.id.not_in(pending_subq),
                            NewsEventModel.id.not_in(processing_subq)
                        )
                        .order_by(NewsEventModel.last_updated_at.desc())
                    )
                    event_ids = session.execute(stmt).scalars().all()

                if not event_ids:
                    logger.info("📭 No active events to scan. Sleeping 60s...")
                    await asyncio.sleep(60)
                    continue

                logger.info(f"🔍 Scanning {len(event_ids)} active events for duplicates...")

                for eid in event_ids:
                    await self.queue.put(eid)
                
                # Wait for the queue to drain before starting the next cycle
                # This prevents re-scanning the same events immediately
                await self.queue.join()
                
                logger.info("✅ Cycle complete. Sleeping 60s...")
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Merger Producer Error: {e}")
                await asyncio.sleep(30)

    async def _consumer_loop(self, worker_id):
        while True:
            event_id = await self.queue.get()
            try:
                await asyncio.to_thread(self._process_single_event, event_id)
            except Exception as e:
                logger.error(f"Worker {worker_id} error on {event_id}: {e}")
            finally:
                self.queue.task_done()

    def _process_single_event(self, event_id):
        with self.SessionLocal() as session:
            # Check if source became busy in enhancer since fetch
            is_busy = session.scalar(
                select(1).where(
                    EventsQueueModel.event_id == event_id,
                    EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                    EventsQueueModel.status == JobStatus.PROCESSING
                )
            )
            if is_busy:
                return

            event = session.get(NewsEventModel, event_id)
            if event and event.is_active:
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
            limit=10
        )

        for candidate, rrf_score, vec_dist in candidates:
            if candidate.id == event.id:
                continue
            
            # Skip if candidate is already merged/inactive
            if not candidate.is_active:
                continue

            # Check if candidate is busy in enhancer
            candidate_busy = session.scalar(
                select(1).where(
                    EventsQueueModel.event_id == candidate.id,
                    EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                    EventsQueueModel.status == JobStatus.PROCESSING
                )
            )
            if candidate_busy:
                continue

            # Check if we already have a pending/rejected proposal for this pair (BIDIRECTIONAL)
            existing_proposal = session.scalar(
                select(MergeProposalModel)
                .where(
                    or_(
                        and_(
                            MergeProposalModel.source_event_id == event.id,
                            MergeProposalModel.target_event_id == candidate.id
                        ),
                        and_(
                            MergeProposalModel.source_event_id == candidate.id,
                            MergeProposalModel.target_event_id == event.id
                        )
                    )
                )
            )
            if existing_proposal:
                continue

            # --- DECISION LOGIC ---
            
            # 1. Auto-Merge (Very High Confidence)
            # Note: Vector distance is Cosine Distance (0 = identical, 1 = opposite)
            if vec_dist < self.AUTO_MERGE_THRESHOLD:
                # Re-check if candidate became busy during the search/calculation window
                candidate_busy_now = session.scalar(
                    select(1).where(
                        EventsQueueModel.event_id == candidate.id,
                        EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                        EventsQueueModel.status == JobStatus.PROCESSING
                    )
                )
                if candidate_busy_now:
                    return

                logger.warning(f"⚡ AUTO-MERGE: {event.title} -> {candidate.title} (Dist: {vec_dist:.3f})")
                self.cluster.execute_event_merge(session, event, candidate)
                # Ensure target is queued for enhancement
                self.cluster._trigger_event_enhancement(session, candidate.id)
                session.commit()
                return # Event is now dead, stop scanning it

            # 2. Smart Proposal Logic
            # Condition A: Standard Vector Proximity
            is_vector_match = vec_dist < self.PROPOSAL_THRESHOLD
            
            # Condition B: "Yellow Zone" - Vector is slightly off, but Keywords are very strong (High RRF)
            # We allow a looser vector threshold if the RRF score indicates strong keyword overlap.
            is_keyword_rescue = (vec_dist < (self.PROPOSAL_THRESHOLD + 0.08)) and (rrf_score > self.RRF_THRESHOLD)

            if is_vector_match or is_keyword_rescue:
                reason = f"Vector Dist {vec_dist:.3f}"
                if is_keyword_rescue and not is_vector_match:
                    reason = f"Strong Keyword Match (RRF {rrf_score:.3f}). Dist {vec_dist:.3f}"
                
                logger.info(f"💡 PROPOSAL: {event.title} -> {candidate.title} | {reason}")
                self._create_proposal(session, event, candidate, vec_dist, reason)
                session.commit()

    def _create_proposal(self, session, source: NewsEventModel, target: NewsEventModel, score: float, reason: str|None = None):
        if not reason:
            reason = f"MergerScanner: Vector Dist {score:.3f}"
            
        proposal = MergeProposalModel(
            id=uuid.uuid4(),
            proposal_type="event_merge",
            source_event_id=source.id,
            target_event_id=target.id,
            distance_score=float(score),
            status="pending",
            reasoning=reason
        )
        session.add(proposal)

if __name__ == "__main__":
    scanner = NewsMergerScanner()
    try:
        asyncio.run(scanner.run())
    except KeyboardInterrupt:
        logger.info("Merger stopped by user")
