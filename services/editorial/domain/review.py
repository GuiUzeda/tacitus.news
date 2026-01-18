import uuid
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Optional
from loguru import logger
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import select, update, desc, func

# Models
from news_events_lib.models import (
    MergeProposalModel,
    ArticleModel,
    NewsEventModel,
    ArticleContentModel,
    JobStatus,
)
from core.models import EventsQueueModel, EventsQueueName

# Core & Domain Services
from core.llm_parser import CloudNewsAnalyzer
from domain.clustering import NewsCluster


class NewsReviewerDomain:
    def __init__(self):
        self.llm = CloudNewsAnalyzer()
        self.cluster = NewsCluster()
        self.CONFIDENCE_THRESHOLD = 0.85

    async def review_proposals(
        self,
        session: Session,
        source_id: uuid.UUID,
        proposal_ids: List[uuid.UUID],
        is_event_merge: bool,
    ) -> str:
        """
        Reviews a batch of proposals for a single source.
        Returns a status string ('MERGED', 'REJECTED_ALL', 'NEW_EVENT', 'SKIPPED').
        Does NOT commit.
        """
        # 1. Build Context Data (Read-Only)
        work_data = self._get_work_data(
            session, source_id, proposal_ids, is_event_merge
        )
        if not work_data:
            return "SKIPPED"

        source_data, proposals_data = work_data

        # 2. LLM Review Loop
        match_found = False
        BATCH_SIZE = 5

        logger.info(
            f"🔎 Reviewing '{source_data['title'][:20]}...' vs {len(proposals_data)} candidates"
        )

        # Sort by distance score (Review best matches first)
        proposals_data.sort(key=lambda x: x["score"])

        for i in range(0, len(proposals_data), BATCH_SIZE):
            if match_found:
                break

            valid_batch = proposals_data[i : i + BATCH_SIZE]

            # A. Heuristic Filter (Save LLM tokens)

            # B. Prepare LLM Payload
            candidates_payload = [
                {"id": str(p["proposal_id"]), "text": p["event_context"]}
                for p in valid_batch
            ]

            try:
                # C. Call LLM
                llm_results = await self.llm.verify_batch_matches(
                    source_data["context_str"], candidates_payload
                )
            except Exception as e:
                logger.error(f"LLM Batch failed: {e}")
                continue

            # D. Apply Decisions (Memory/Session only)
            results_map = {res.proposal_id: res for res in llm_results}

            for prop_data in valid_batch:
                pid = str(prop_data["proposal_id"])
                if pid not in results_map:
                    # LLM Hallucinated or skipped
                    self._update_proposal_status(
                        session, pid, JobStatus.FAILED, "LLM Missed Item"
                    )
                    continue

                result = results_map[pid]

                if (
                    result.same_event
                    and result.confidence_score >= self.CONFIDENCE_THRESHOLD
                ):
                    # ✅ MATCH!
                    success = self._execute_positive_match(
                        session,
                        source_id,
                        prop_data["target_id"],
                        prop_data["proposal_id"],
                        result.reasoning,
                        is_event_merge,
                    )
                    if success:
                        match_found = True
                        break
                    else:
                        # Target Busy - Reset to PENDING to avoid stuck PROCESSING state
                        self._update_proposal_status(
                            session, pid, JobStatus.PENDING, "Target Busy - Retry"
                        )
                else:
                    # ❌ REJECT
                    self._update_proposal_status(
                        session,
                        pid,
                        JobStatus.REJECTED,
                        f"LLM Reject: {result.reasoning}",
                    )

        # 3. Post-Review Logic
        if match_found:
            return "MERGED"

        # If we reviewed everything and found nothing, creates a NEW EVENT (if it's an article)
        # Events don't get "New Event" fallback, they just stay alone.
        if not is_event_merge and len(proposals_data) > 0:
            logger.info(f"🆕 Creating NEW EVENT for: {source_data['title']}")
            self._execute_new_event_fallback(session, source_id)
            return "NEW_EVENT"

        return "REJECTED_ALL"

    # --- EXECUTION HELPERS (NO COMMIT) ---

    def _execute_positive_match(
        self, session, source_id, target_id, proposal_id, reason, is_event_merge
    ):
        # Check Busy Lock
        is_busy = session.scalar(
            select(1).where(
                EventsQueueModel.event_id == target_id,
                EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                EventsQueueModel.status == JobStatus.PROCESSING,
            )
        )
        if is_busy:
            logger.warning(f"⚠️ Target busy. Skipping merge.")
            return False

        # Execute Merge (Delegates to Cluster Domain)
        survivor_id = target_id
        if is_event_merge:
            source = session.get(NewsEventModel, source_id)
            target = session.get(NewsEventModel, target_id)
            survivor_id = self.cluster.execute_event_merge(session, source, target)
            reason_prefix = "Auto-Merged Events"
        else:
            article = session.get(ArticleModel, source_id)
            event = session.get(NewsEventModel, target_id)
            # Use internal method to link without search
            self.cluster.link_article_to_event(
                session, event, article, article.embedding
            )
            reason_prefix = "Auto-Merged Article"

        # Trigger Enhancer for the survivor
        self._queue_event_job(session, survivor_id, EventsQueueName.ENHANCER)

        # Approve winning proposal
        self._update_proposal_status(
            session, proposal_id, JobStatus.APPROVED, f"{reason_prefix}: {reason}"
        )

        # Reject all other pending proposals for this source
        col_id = (
            MergeProposalModel.source_event_id
            if is_event_merge
            else MergeProposalModel.source_article_id
        )
        session.execute(
            update(MergeProposalModel)
            .where(
                col_id == source_id,
                MergeProposalModel.id != proposal_id,
                MergeProposalModel.status.in_(
                    [JobStatus.PENDING, JobStatus.PROCESSING]
                ),
            )
            .values(
                status=JobStatus.REJECTED,
                reasoning="Merged into another target.",
                updated_at=datetime.now(timezone.utc),
            )
        )
        return True

    def _execute_new_event_fallback(self, session, article_id):
        article = session.get(ArticleModel, article_id)
        if article:
            # Use public method
            new_id = self.cluster.execute_new_event_action(
                session, article, reason="Reviewer: All proposals rejected"
            )
            self._queue_event_job(session, new_id, EventsQueueName.ENHANCER)

            # Reject remaining proposals (PENDING/PROCESSING)
            # We do NOT overwrite REJECTED ones (which contain LLM reasoning)
            session.execute(
                update(MergeProposalModel)
                .where(
                    MergeProposalModel.source_article_id == article_id,
                    MergeProposalModel.status.in_(
                        [JobStatus.PENDING, JobStatus.PROCESSING]
                    ),
                )
                .values(status=JobStatus.REJECTED, reasoning="Converted to New Event")
            )

    def _update_proposal_status(self, session, pid_or_list, status, reasoning):
        stmt = update(MergeProposalModel).values(
            status=status, reasoning=reasoning, updated_at=datetime.now(timezone.utc)
        )

        if isinstance(pid_or_list, list):
            stmt = stmt.where(MergeProposalModel.id.in_(pid_or_list))
        else:
            stmt = stmt.where(MergeProposalModel.id == pid_or_list)

        session.execute(stmt)

    def _queue_event_job(self, session, event_id, queue_name):
        """Idempotent queue insert/update."""
        now = datetime.now(timezone.utc)
        existing = session.scalar(
            select(EventsQueueModel).where(EventsQueueModel.event_id == event_id)
        )
        if existing:
            existing.status = JobStatus.PENDING
            existing.queue_name = queue_name
            existing.updated_at = now
            session.add(existing)
        else:
            session.add(
                EventsQueueModel(
                    event_id=event_id,
                    queue_name=queue_name,
                    status=JobStatus.PENDING,
                    created_at=now,
                    updated_at=now,
                )
            )

    # --- DATA FETCHING (Read Only) ---

    def _get_work_data(
        self,
        session: Session,
        source_id: uuid.UUID,
        proposal_ids: List[uuid.UUID],
        is_event_merge: bool,
    ):
        if is_event_merge:
            return self._get_event_merge_data(session, source_id, proposal_ids)
        else:
            return self._get_article_merge_data(session, source_id, proposal_ids)

    def _get_article_merge_data(self, session, article_id, proposal_ids):
        article = session.get(ArticleModel, article_id)
        if not article or article.event_id:
            # cleanup if already merged
            self._update_proposal_status(
                session, proposal_ids, JobStatus.REJECTED, "Article already merged"
            )
            return None

        proposals = (
            session.execute(
                select(MergeProposalModel)
                .options(joinedload(MergeProposalModel.target_event))
                .where(MergeProposalModel.id.in_(proposal_ids))
            )
            .scalars()
            .all()
        )

        source_data = {
            "id": article.id,
            "title": article.title,
            "context_str": self._build_article_context(session, article),
            "entities": article.entities or [],
        }

        proposals_data = []
        for p in proposals:
            if not p.target_event:
                continue
            proposals_data.append(
                {
                    "proposal_id": p.id,
                    "target_id": p.target_event_id,
                    "target_obj": p.target_event,
                    "score": p.distance_score,
                    "event_context": self._build_event_context(
                        session, p.target_event, article.embedding
                    ),
                }
            )
        return source_data, proposals_data

    def _get_event_merge_data(self, session, source_id, proposal_ids):
        source = session.get(NewsEventModel, source_id)
        if not source or not source.is_active:
            return None

        proposals = (
            session.execute(
                select(MergeProposalModel)
                .options(joinedload(MergeProposalModel.target_event))
                .where(MergeProposalModel.id.in_(proposal_ids))
            )
            .scalars()
            .all()
        )

        source_data = {
            "id": source.id,
            "title": source.title,
            "context_str": self._build_event_context(session, source),
        }

        proposals_data = []
        for p in proposals:
            if not p.target_event:
                continue
            proposals_data.append(
                {
                    "proposal_id": p.id,
                    "target_id": p.target_event_id,
                    "target_obj": p.target_event,
                    "score": p.distance_score,
                    "event_context": self._build_event_context(
                        session, p.target_event, source.embedding_centroid
                    ),
                }
            )
        return source_data, proposals_data

    # --- CONTEXT BUILDERS & HEURISTICS ---

    def _build_article_context(self, session, article) -> str:
        content_txt = ""
        if article.contents:
            content_txt = article.contents[0].content[:2000]
        return f"ARTICLE: {article.title}\nDATE: {article.published_date}\nTEXT: {content_txt}"

    def _build_event_context(
        self, session, event, source_vector: Optional[list] = None
    ) -> str:
        text = f"EVENT: {event.title}\n"
        # Add summary logic here...
        return text
