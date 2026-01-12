import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import uuid
from datetime import timedelta
from typing import Optional

from loguru import logger
from sqlalchemy import select, and_, or_
from sqlalchemy.orm import Session

# Models
from news_events_lib.models import (
    NewsEventModel, 
    MergeProposalModel, 
    JobStatus
)
from core.models import EventsQueueModel, EventsQueueName
from domain.clustering import NewsCluster

class NewsMergerDomain:
    def __init__(self):
        self.cluster = NewsCluster()
        
        # Thresholds
        self.AUTO_MERGE_THRESHOLD = 0.05  # Very strict distance (Lower is better)
        self.PROPOSAL_THRESHOLD = 0.15    # Looser distance for proposals
        self.RRF_THRESHOLD = 0.12         # Minimum RRF score to consider keyword-heavy matches

    def check_and_process_duplicates(self, session: Session, event_id: uuid.UUID):
        """
        Scans a specific event against the database to find duplicates.
        Decides whether to Auto-Merge or Create a Proposal.
        """
        # 1. Fetch & Validate Source Event
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
        if not event or not event.is_active:
            return

        # 2. Search Candidates (Excluding self)
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
            
            if not candidate.is_active:
                continue

            # Check safeguards on candidate
            candidate_busy = session.scalar(
                select(1).where(
                    EventsQueueModel.event_id == candidate.id,
                    EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                    EventsQueueModel.status == JobStatus.PROCESSING
                )
            )
            if candidate_busy:
                continue

            # Check if bidirectional proposal already exists
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

            # A. Auto-Merge (Very High Confidence)
            if vec_dist < self.AUTO_MERGE_THRESHOLD:
                # Double-check concurrent locks
                if self._is_event_busy(session, candidate.id):
                    return

                logger.warning(f"⚡ AUTO-MERGE: {event.title} -> {candidate.title} (Dist: {vec_dist:.3f})")
                self.cluster.execute_event_merge(session, event, candidate)


                session.commit()
                return # Source event is now dead, stop scanning

            # B. Smart Proposal Logic
            is_vector_match = vec_dist < self.PROPOSAL_THRESHOLD
            
            # "Yellow Zone" - Vector slightly off, but Keywords strong (High RRF)
            is_keyword_rescue = (vec_dist < (self.PROPOSAL_THRESHOLD + 0.08)) and (rrf_score > self.RRF_THRESHOLD)

            if is_vector_match or is_keyword_rescue:
                reason = f"Vector Dist {vec_dist:.3f}"
                if is_keyword_rescue and not is_vector_match:
                    reason = f"Strong Keyword Match (RRF {rrf_score:.3f}). Dist {vec_dist:.3f}"
                
                logger.info(f"💡 PROPOSAL: {event.title} -> {candidate.title} | {reason}")
                self._create_proposal(session, event, candidate, vec_dist, reason)
                session.commit()

    def _create_proposal(self, session, source: NewsEventModel, target: NewsEventModel, score: float, reason: str):
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

    def _is_event_busy(self, session, event_id):
        return session.scalar(
            select(1).where(
                EventsQueueModel.event_id == event_id,
                EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                EventsQueueModel.status == JobStatus.PROCESSING
            )
        )