import uuid
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Optional, Set

from app.config import Settings
from app.utils.event_manager import EventManager
from loguru import logger

# Project Imports
from news_events_lib.models import (
    EventsQueueModel,
    EventsQueueName,
    EventStatus,
    JobStatus,
    NewsEventModel,
)
from sqlalchemy import select
from sqlalchemy.orm import Session


class MergerAction(str, Enum):
    MERGED = "MERGED"  # Auto-merged (High confidence)
    PROPOSED = "PROPOSED"  # Proposal created (Ambiguous)
    SKIPPED = "SKIPPED"  # No match found
    BUSY = "BUSY"  # Target is locked
    ERROR = "ERROR"  # Technical fail


@dataclass
class MergerResult:
    action: MergerAction
    source: NewsEventModel
    target: Optional[NewsEventModel] = None
    dist: float = 0.0
    reason: str = ""


class NewsMergerDomain:
    def __init__(self):
        # Configuration
        self.settings = Settings()
        self.AUTO_MERGE_THRESHOLD = self.settings.similarity_strict
        self.PROPOSAL_THRESHOLD = self.settings.similarity_loose_merger
        self.RRF_THRESHOLD = (
            0.02  # Keyword Rescue Score (Max possible is ~0.032 with k=60)
        )

    def scan_and_process_event(
        self, session: Session, event: NewsEventModel
    ) -> MergerResult:
        """
        Scans a specific event against the database to find duplicates.
        Performs logic updates in memory (Merge/Propose) but DOES NOT COMMIT.
        """
        # 1. Validation checks
        if not event or not event.is_active:
            return MergerResult(
                MergerAction.SKIPPED, event, reason="Event inactive/missing"
            )

        if self._is_event_busy(session, event.id):
            return MergerResult(
                MergerAction.BUSY, event, reason="Source Event is processing"
            )

        # 2. Search Candidates (Hybrid: Vector + Keyword)
        # We look back 3 days to catch "Twin Events" that started slightly apart
        candidates = EventManager.search_news_events_hybrid(
            session,
            query_text=event.title,
            query_vector=event.embedding_centroid,
            target_date=event.created_at,
            diff_date=timedelta(days=3),
            limit=10,
        )
        # exclude events that have reject merge proposal between them

        # Pre-calculate Source Entities for the Gatekeeper
        source_ents = self._extract_event_entities(event)

        for candidate, rrf_score, vec_dist in candidates:
            # Skip self
            if candidate.id == event.id:
                continue

            # Skip inactive/busy targets
            if not candidate.is_active:
                continue
            if self._is_event_busy(session, candidate.id):
                continue

            # Skip if a proposal already exists (Avoid spamming the Reviewer)
            if EventManager.merge_proposal_exists(session, event.id, candidate.id):
                continue

            # --- 🛡️ ENTITY GATEKEEPER ---
            # If the vector distance suggests a merge, we verify it with Named Entities.
            if vec_dist < self.PROPOSAL_THRESHOLD:
                target_ents = self._extract_event_entities(candidate)

                # Case A: Missing Entities (Risk of Hallucination)
                # If either side lacks entities, we can't verify the match.
                # Action: Enforce STRICTER threshold (0.08) instead of loose (0.15).
                if not source_ents or not target_ents:
                    if vec_dist > (self.AUTO_MERGE_THRESHOLD + 0.03):  # ~0.08
                        continue

                # Case B: Disjoint Entities (Contradiction)
                # If both have entities, but share NONE -> BLOCK.
                elif source_ents.isdisjoint(target_ents):
                    # Exception: If RRF (Keyword) score is high, allow it (maybe entity extraction failed but keywords match)
                    if rrf_score < self.RRF_THRESHOLD:
                        continue

            # --- DECISION LOGIC ---

            # A. Auto-Merge (Very High Confidence: < 0.05)
            if vec_dist < self.AUTO_MERGE_THRESHOLD:

                # 🛡️ CRITICAL: SURVIVOR SELECTION (Anti-Flicker)
                # Default: Candidate is survivor (Target), Event is victim (Source)
                source_evt = event
                target_evt = candidate

                # Check 1: If Source is PUBLISHED and Target is NOT, we MUST swap.
                # We merge the Candidate INTO the Event.
                if (
                    source_evt.status == EventStatus.PUBLISHED
                    and target_evt.status != EventStatus.PUBLISHED
                ):
                    source_evt, target_evt = target_evt, source_evt
                    logger.info(
                        f"🔄 Swapped merge order: Keeping Published Event {target_evt.id} alive."
                    )

                # Check 2: If BOTH are PUBLISHED...
                # This is a severe data issue. We ideally shouldn't auto-merge two live events
                # without human oversight, as it changes history.
                # BETTER ACTION: Force a Proposal instead of Auto-Merge.
                if (
                    source_evt.status == EventStatus.PUBLISHED
                    and target_evt.status == EventStatus.PUBLISHED
                ):
                    logger.warning(
                        "⚠️ Attempted Auto-Merge on two PUBLISHED events. Downgrading to Proposal."
                    )
                    EventManager.create_merge_proposal(
                        session,
                        source=event,
                        target=candidate,
                        reason="Double-Published Auto-Merge Risk",
                        score=vec_dist,
                    )
                    return MergerResult(
                        MergerAction.PROPOSED,
                        event,
                        candidate,
                        dist=vec_dist,
                        reason="Double-Published Risk",
                    )

                # Execute Logic (Memory Only)
                survivor = EventManager.execute_event_merge(
                    session, source_evt, target_evt
                )

                # Trigger Enhancer for the survivor
                EventManager.create_event_queue(
                    session,
                    survivor.id,
                    EventsQueueName.ENHANCER,
                    reason="Merger: Auto-Merge",
                )

                return MergerResult(
                    MergerAction.MERGED,
                    source=source_evt,
                    target=survivor,
                    reason=f"Auto-Merge (Dist: {vec_dist:.3f})",
                )

            # B. Proposal (Ambiguous: 0.05 - 0.15) OR Keyword Rescue
            is_vector_match = vec_dist < self.PROPOSAL_THRESHOLD
            is_keyword_rescue = (vec_dist < (self.PROPOSAL_THRESHOLD + 0.08)) and (
                rrf_score > self.RRF_THRESHOLD
            )

            if is_vector_match or is_keyword_rescue:
                reason = f"Vector Dist {vec_dist:.3f}"
                if is_keyword_rescue and not is_vector_match:
                    reason = f"Keyword Rescue (RRF {rrf_score:.3f})"

                # Create Proposal (Memory Only)
                EventManager.create_merge_proposal(
                    session,
                    source=event,
                    target=candidate,
                    reason=reason,
                    score=vec_dist,
                )

                return MergerResult(
                    MergerAction.PROPOSED,
                    source=event,
                    target=candidate,
                    reason=reason,
                    dist=vec_dist,
                )

        return MergerResult(MergerAction.SKIPPED, event, reason="No matches found")

    def _is_event_busy(self, session: Session, event_id: uuid.UUID):
        """Checks if the event is currently locked by another worker."""
        return session.scalar(
            select(1).where(
                EventsQueueModel.event_id == event_id,
                EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                EventsQueueModel.status == JobStatus.PROCESSING,
            )
        )

    def _extract_event_entities(self, event) -> Set[str]:
        """Collects all entities (NER + Topics) for overlap checking."""
        ents = set()

        # 1. Direct Entities (from Articles)
        # Note: event.interest_counts stores entity counts like {"Politics": {"Trump": 10}}
        if event.interest_counts:
            for cat, items in event.interest_counts.items():
                ents.update(items.keys())

        # 2. Main Topics
        if event.main_topic_counts:
            ents.update(event.main_topic_counts.keys())

        return ents
