import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Set

from loguru import logger
from sqlalchemy import select, update, and_, or_
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
        self.AUTO_MERGE_THRESHOLD = 0.05  # Very strict
        self.PROPOSAL_THRESHOLD = 0.15    # Looser
        self.RRF_THRESHOLD = 0.12         # Keyword rescue

    def check_and_process_duplicates(self, session: Session, event_id: uuid.UUID):
        """
        Scans a specific event against the database to find duplicates.
        """
        # 1. Validation checks (Busy/Active)
        is_busy = self._is_event_busy(session, event_id)
        if is_busy: return

        event = session.get(NewsEventModel, event_id)
        if not event or not event.is_active: return

        # 2. Search Candidates
        candidates = self.cluster.search_news_events_hybrid(
            session,
            query_text=event.title,
            query_vector=event.embedding_centroid,
            target_date=event.created_at,
            diff_date=timedelta(days=3),
            limit=10
        )

        # Pre-calculate source entities for the heuristic check
        source_ents = self._extract_event_entities(event)

        for candidate, rrf_score, vec_dist in candidates:
            if candidate.id == event.id: continue
            if not candidate.is_active: continue
            if self._is_event_busy(session, candidate.id): continue

            # Check existing proposals (Bi-directional)
            if self._proposal_exists(session, event.id, candidate.id):
                continue

            # --- OPTIMIZATION: ENTITY GATEKEEPER ---
            # If we are in the "Yellow Zone" (Proposal), verify entity overlap
            # We skip this check for Auto-Merge (0.05) as vectors are nearly identical
            if vec_dist >= self.AUTO_MERGE_THRESHOLD:
                target_ents = self._extract_event_entities(candidate)
                if not self._passes_heuristic_check(source_ents, target_ents):
                    # Skip creating a proposal that the Reviewer would just auto-reject
                    continue

            # --- DECISION LOGIC ---

            # A. Auto-Merge (Very High Confidence)
            if vec_dist < self.AUTO_MERGE_THRESHOLD:
                logger.warning(f"⚡ AUTO-MERGE: {event.title} -> {candidate.title} (Dist: {vec_dist:.3f})")
                self.cluster.execute_event_merge(session, event, candidate)
                session.commit()
                return # Source event is dead

            # B. Smart Proposal Logic
            is_vector_match = vec_dist < self.PROPOSAL_THRESHOLD
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
            status=JobStatus.PENDING,
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

    def _proposal_exists(self, session, id_a, id_b):
        return session.scalar(
            select(1).where(
                or_(
                    and_(
                        MergeProposalModel.source_event_id == id_a,
                        MergeProposalModel.target_event_id == id_b
                    ),
                    and_(
                        MergeProposalModel.source_event_id == id_b,
                        MergeProposalModel.target_event_id == id_a
                    )
                )
            )
        )

    def _extract_event_entities(self, event) -> Set[str]:
        """Helper to flatten interest_counts into a set of entity names."""
        ents = set()
        if getattr(event, 'entities', None):
            ents.update(event.entities)
        
        if event.interest_counts:
            for cat, items in event.interest_counts.items():
                ents.update(items.keys())
        return ents

    def _passes_heuristic_check(self, source_ents: Set[str], target_ents: Set[str]) -> bool:
        """Returns True if there is at least 1 shared entity (or if data is missing)."""
        if not source_ents or not target_ents:
            return True # Fail Open
            
        return not source_ents.isdisjoint(target_ents)