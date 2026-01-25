import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import List

from app.config import Settings
from app.core.llm_parser import CloudNewsAnalyzer
from app.utils.event_manager import EventManager
from loguru import logger

# Project Imports
from news_events_lib.models import (
    ArticleModel,
    EventsQueueModel,
    EventsQueueName,
    EventStatus,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import desc, select, update
from sqlalchemy.orm import Session, joinedload


class ReviewerAction(str, Enum):
    MERGED = "MERGED"
    REJECTED = "REJECTED"
    NEW_EVENT = "NEW_EVENT"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


@dataclass
class ReviewerResult:
    action: ReviewerAction
    source_id: uuid.UUID
    reason: str = ""


class NewsReviewerDomain:
    def __init__(self):
        self.settings = Settings()
        self.llm = CloudNewsAnalyzer()
        self.CONFIDENCE_THRESHOLD = 0.85

    async def review_proposals(
        self,
        session: Session,
        source_id: uuid.UUID,
        proposal_ids: List[uuid.UUID],
        is_event_merge: bool,
    ) -> ReviewerResult:

        # 1. Build Context & Lazy Cleanup
        # If source is dead, this method will reject proposals and return None
        work_data = self._get_work_data(
            session, source_id, proposal_ids, is_event_merge
        )
        if not work_data:
            return ReviewerResult(
                ReviewerAction.SKIPPED,
                source_id,
                "Source invalid/merged (Lazy Cleanup)",
            )

        source_data, proposals_data = work_data

        logger.info(
            f"⚖️ Reviewing Source'{source_data['title'][:10]}...' vs {len(proposals_data)} Candidates"
        )

        # 2. Sort candidates by distance
        proposals_data.sort(key=lambda x: x["score"])

        match_found = False
        BATCH_SIZE = 5

        for i in range(0, len(proposals_data), BATCH_SIZE):
            if match_found:
                break

            valid_batch = proposals_data[i : i + BATCH_SIZE]

            # Prepare Payload for LLM
            candidates_payload = [
                {"id": str(p["proposal_id"]), "text": p["event_context"]}
                for p in valid_batch
            ]

            try:
                llm_results = await self.llm.verify_batch_matches(
                    reference_text=source_data["context_str"],
                    candidates=candidates_payload,
                )
            except Exception as e:
                logger.opt(exception=True).error(f"LLM Batch Review Failed: {e}")
                continue

            results_map = {res.proposal_id: res for res in llm_results}

            for prop_data in valid_batch:
                pid = str(prop_data["proposal_id"])

                if pid not in results_map:
                    continue  # Should fail item individually, skipping for brevity

                result = results_map[pid]

                # --- CHECK FOR MATCH ---
                if (
                    result.same_event
                    and result.confidence_score >= self.CONFIDENCE_THRESHOLD
                ):
                    # ✅ POSITIVE MATCH
                    success = self._execute_positive_match(
                        session,
                        source_id=source_id,
                        target_id=prop_data["target_id"],
                        winning_proposal_id=prop_data["proposal_id"],
                        reason=result.reasoning,
                        is_event_merge=is_event_merge,
                    )

                    if success:
                        match_found = True
                        break
                    else:
                        # Target Locked -> Retry Later
                        self._update_proposal_status(
                            session, pid, JobStatus.PENDING, "Target Busy"
                        )

                else:
                    # ❌ REJECT INDIVIDUAL
                    self._update_proposal_status(
                        session,
                        pid,
                        JobStatus.REJECTED,
                        f"LLM Reject ({result.confidence_score})",
                    )

        if match_found:
            return ReviewerResult(
                ReviewerAction.MERGED, source_id, "Merged successfully"
            )

        # Fallback Logic
        if not is_event_merge and len(proposals_data) > 0:
            logger.info(f"🆕 Creating NEW EVENT for  '{source_data['title'][:20]}'")
            self._execute_new_event_fallback(session, source_id, is_event_merge)
            return ReviewerResult(
                ReviewerAction.NEW_EVENT, source_id, "Converted to New Event"
            )

        return ReviewerResult(
            ReviewerAction.REJECTED, source_id, "All proposals rejected"
        )

    # --- EXECUTION ---

    def _execute_positive_match(
        self, session, source_id, target_id, winning_proposal_id, reason, is_event_merge
    ) -> bool:
        # 1. Lock Check
        is_busy = session.scalar(
            select(1).where(
                EventsQueueModel.event_id == target_id,
                EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                EventsQueueModel.status == JobStatus.PROCESSING,
            )
        )
        if is_busy:
            return False

        # 2. Perform Merge
        if is_event_merge:
            source = session.get(NewsEventModel, source_id)
            target = session.get(NewsEventModel, target_id)

            # Survivor Logic
            victim, survivor = source, target
            if (
                source.status == EventStatus.PUBLISHED
                and target.status != EventStatus.PUBLISHED
            ):
                victim, survivor = target, source
            elif (
                source.status == EventStatus.PUBLISHED
                and target.status == EventStatus.PUBLISHED
            ):
                if (source.hot_score or 0) > (target.hot_score or 0):
                    victim, survivor = target, source

            survivor_final = EventManager.execute_event_merge(session, victim, survivor)
            prefix = "Auto-Merged Event"
        else:
            article = session.get(ArticleModel, source_id)
            target = session.get(NewsEventModel, target_id)
            survivor_final = EventManager.link_article_to_event(
                session, target, article
            )
            prefix = "Auto-Merged Article"

        # 3. Queue Enhancer
        EventManager.create_event_queue(
            session,
            survivor_final.id,
            EventsQueueName.ENHANCER,
            reason=f"{prefix}: {reason}",
        )

        # 4. Approve Winner
        self._update_proposal_status(
            session, winning_proposal_id, JobStatus.APPROVED, f"{prefix}: {reason}"
        )

        # 5. REJECT ALL OTHERS (Siblings + Zombies)
        self._reject_all_pending_for_source(
            session,
            source_id,
            is_event_merge,
            exclude_id=winning_proposal_id,
            reason="Sibling won",
        )

        return True

    def _execute_new_event_fallback(self, session, source_id, is_event_merge):
        # Only for articles
        if is_event_merge:
            return

        article = session.get(ArticleModel, source_id)
        if article:
            new_event = EventManager.execute_new_event_action(session, article)
            session.flush()
            EventManager.create_event_queue(
                session,
                new_event.id,
                EventsQueueName.ENHANCER,
                reason="Reviewer: New Event",
            )
            # Source is now handled (it's in a new event). Reject all pending proposals.
            self._reject_all_pending_for_source(
                session, source_id, is_event_merge, reason="Fallback to New Event"
            )

    def _reject_all_pending_for_source(
        self, session, source_id, is_event_merge, reason="Cleanup", exclude_id=None
    ):
        """
        Broad cleanup: Rejects ALL pending proposals for this source.
        This handles zombies effectively.
        """
        col = (
            MergeProposalModel.source_event_id
            if is_event_merge
            else MergeProposalModel.source_article_id
        )

        stmt = update(MergeProposalModel).where(
            col == source_id,
            MergeProposalModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]),
        )

        if exclude_id:
            stmt = stmt.where(MergeProposalModel.id != exclude_id)

        session.execute(
            stmt.values(
                status=JobStatus.REJECTED,
                reasoning=reason,
                updated_at=datetime.now(timezone.utc),
            )
        )

    def _update_proposal_status(self, session, pid, status, reasoning):
        session.execute(
            update(MergeProposalModel)
            .where(MergeProposalModel.id == pid)
            .values(
                status=status,
                reasoning=reasoning,
                updated_at=datetime.now(timezone.utc),
            )
        )

    # --- CONTEXT ---

    def _get_work_data(self, session, source_id, proposal_ids, is_event_merge):
        if is_event_merge:
            return self._get_event_merge_context(session, source_id, proposal_ids)
        else:
            return self._get_article_merge_context(session, source_id, proposal_ids)

    def _get_article_merge_context(self, session, article_id, proposal_ids):
        article = session.get(ArticleModel, article_id)

        # LAZY CLEANUP: If article is already merged, kill ALL proposals
        if not article or article.event_id:
            self._reject_all_pending_for_source(
                session, article_id, False, "Lazy Cleanup: Already Merged"
            )
            return None

        content_txt = (
            article.content[:2000] if article.content else (article.summary or "")
        )
        source_context = f"TITLE: {article.title}\nDATE: {article.published_date}\nTEXT: {content_txt}"
        return self._fetch_proposals_and_targets(
            session,
            {"title": article.title, "context_str": source_context},
            proposal_ids,
        )

    def _get_event_merge_context(self, session, event_id, proposal_ids):
        event = session.get(NewsEventModel, event_id)
        # LAZY CLEANUP: If event dead, kill ALL proposals
        if not event or not event.is_active:
            self._reject_all_pending_for_source(
                session, event_id, True, "Lazy Cleanup: Event Inactive"
            )
            return None

        source_context = self._build_event_text_representation(session, event)
        return self._fetch_proposals_and_targets(
            session, {"title": event.title, "context_str": source_context}, proposal_ids
        )

    def _fetch_proposals_and_targets(self, session, source_data, proposal_ids):
        # (Same as before)
        proposals = (
            session.execute(
                select(MergeProposalModel)
                .options(joinedload(MergeProposalModel.target_event))
                .where(MergeProposalModel.id.in_(proposal_ids))
            )
            .scalars()
            .all()
        )

        proposals_data = []
        for p in proposals:
            if not p.target_event:
                continue
            target_context = self._build_event_text_representation(
                session, p.target_event
            )
            proposals_data.append(
                {
                    "proposal_id": p.id,
                    "target_id": p.target_event_id,
                    "score": p.distance_score,
                    "event_context": target_context,
                }
            )

        return source_data, proposals_data

    def _build_event_text_representation(self, session, event: NewsEventModel) -> str:
        # (Same as before)
        lines = [f"EVENT TITLE: {event.title}"]
        if event.subtitle:
            lines.append(f"SUBTITLE: {event.subtitle}")

        try:
            recent = session.execute(
                select(ArticleModel.title, ArticleModel.subtitle)
                .where(ArticleModel.event_id == event.id)
                .order_by(desc(ArticleModel.published_date))
                .limit(3)
            ).all()
            if recent:
                lines.append("RECENT COVERAGE SNAPSHOT:")
                for title, subtitle in recent:
                    sub_text = f" - {subtitle}" if subtitle else ""
                    lines.append(f"* {title}{sub_text}")
        except Exception:
            pass
        return "\n".join(lines)
