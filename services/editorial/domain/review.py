import uuid
import asyncio
from datetime import datetime
from typing import List, Tuple, Dict, Optional
from loguru import logger
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import select, update, desc

# Models
from news_events_lib.models import (
    MergeProposalModel, 
    ArticleModel, 
    NewsEventModel, 
    ArticleContentModel,
    JobStatus
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
        is_event_merge: bool
    ) -> bool:
        """
        Main entry point. Reviews a batch of proposals for a single source (Article or Event).
        """
        # 1. Build Context Data (DB IO)
        # We do this synchronously as it's fast DB lookups
        work_data = self._get_work_data(session, source_id, proposal_ids, is_event_merge)
        
        if not work_data:
            return False

        source_data, proposals_data = work_data
        
        # 2. Call LLM (Async IO)
        # We process in sub-batches of 5 to fit in context window if necessary
        # (Though the new batch_verify can handle ~10 easily, 5 is safe)
        match_found = False
        BATCH_SIZE = 5
        
        logger.info(f"🔎 Reviewing '{source_data['title'][:20]}...' vs {len(proposals_data)} candidates")

        for i in range(0, len(proposals_data), BATCH_SIZE):
            if match_found: break # Stop if we already merged

            batch = proposals_data[i:i + BATCH_SIZE]
            
            candidates_payload = [
                {'id': str(p['proposal_id']), 'text': p['event_context']} 
                for p in batch
            ]
            
            try:
                llm_results = await self.llm.verify_batch_matches(
                    source_data['context_str'],
                    candidates_payload
                )
            except Exception as e:
                logger.error(f"LLM Batch failed: {e}")
                continue

            # 3. Process Decisions
            results_map = {res.proposal_id: res for res in llm_results}

            for prop_data in batch:
                pid = str(prop_data['proposal_id'])
                if pid not in results_map:
                    self._update_proposal_status(session, pid, JobStatus.FAILED, "LLM Missed Item")
                    continue

                result = results_map[pid]
                
                if result.same_event and result.confidence_score >= self.CONFIDENCE_THRESHOLD:
                    # MATCH FOUND!
                    success = self._execute_positive_match(
                        session, 
                        source_id, 
                        prop_data['target_id'], 
                        prop_data['proposal_id'], 
                        result.reasoning,
                        is_event_merge
                    )
                    if success:
                        match_found = True
                        break # Stop processing other candidates
                else:
                    # REJECT
                    self._update_proposal_status(session, pid, JobStatus.REJECTED, f"Auto-Rejected: {result.reasoning}")

        # 4. Fallback: Create New Event?
        # Only if it was an ARTICLE, no match was found, and we actually reviewed proposals.
        if not is_event_merge and not match_found and len(proposals_data) > 0:
            logger.info(f"🆕 Creating NEW EVENT for: {source_data['title']}")
            self._execute_new_event_fallback(session, source_id)
            
        return True

    # --- DATA PREPARATION ---

    def _get_work_data(self, session: Session, source_id: uuid.UUID, proposal_ids: List[uuid.UUID], is_event_merge: bool):
        """
        Fetches the Source Entity and all Candidate Events, generating their text contexts.
        """
        if is_event_merge:
            return self._get_event_merge_data(session, source_id, proposal_ids)
        else:
            return self._get_article_merge_data(session, source_id, proposal_ids)

    def _get_article_merge_data(self, session, article_id, proposal_ids):
        article = session.get(ArticleModel, article_id)
        if not article: return None

        # Optimization: Is it already merged?
        if getattr(article, 'event_id', None) is not None:
            self._bulk_reject(session, proposal_ids, "Auto-Cleanup: Article already merged.")
            return None

        # Fetch Proposals
        stmt = (
            select(MergeProposalModel)
            .options(joinedload(MergeProposalModel.target_event))
            .where(MergeProposalModel.id.in_(proposal_ids))
            .order_by(MergeProposalModel.distance_score)
        )
        proposals = session.execute(stmt).scalars().all()
        if not proposals: return None

        # Build Data
        source_ctx = self._build_article_context(session, article)
        proposals_data = []
        for p in proposals:
            if not p.target_event: continue
            proposals_data.append({
                'proposal_id': p.id,
                'target_id': p.target_event_id,
                'event_context': self._build_event_context(session, p.target_event)
            })
            
        return {'id': article.id, 'title': article.title, 'context_str': source_ctx}, proposals_data

    def _get_event_merge_data(self, session, source_id, proposal_ids):
        source = session.get(NewsEventModel, source_id)
        if not source or not source.is_active:
            self._bulk_reject(session, proposal_ids, "Auto-Cleanup: Source inactive.")
            return None

        stmt = (
            select(MergeProposalModel)
            .options(joinedload(MergeProposalModel.target_event)) 
            .where(MergeProposalModel.id.in_(proposal_ids))
            .order_by(MergeProposalModel.distance_score)
        )
        proposals = session.execute(stmt).scalars().all()
        
        # Treat Source Event as the "Text"
        source_ctx = self._build_event_context(session, source)
        proposals_data = []
        for p in proposals:
            if not p.target_event: continue
            proposals_data.append({
                'proposal_id': p.id,
                'target_id': p.target_event_id,
                'event_context': self._build_event_context(session, p.target_event)
            })

        return {'id': source.id, 'title': source.title, 'context_str': source_ctx}, proposals_data

    # --- EXECUTION ---

    def _execute_positive_match(self, session, source_id, target_id, proposal_id, reason, is_event_merge):
        # 1. Check if Target is locked (Busy)
        is_busy = session.scalar(select(1).where(
            EventsQueueModel.event_id == target_id,
            EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
            EventsQueueModel.status == JobStatus.PROCESSING
        ))
        if is_busy:
            logger.warning(f"⚠️ Skipping merge: Target Event is busy in Enhancer.")
            return False

        # 2. Execute Merge via Clustering Logic
        if is_event_merge:
            source = session.get(NewsEventModel, source_id)
            target = session.get(NewsEventModel, target_id)
            self.cluster.execute_event_merge(session, source, target)
            reason_prefix = "Auto-Merged Events"
        else:
            article = session.get(ArticleModel, source_id)
            event = session.get(NewsEventModel, target_id)
            self.cluster.execute_merge_action(session, article, event)
            reason_prefix = "Auto-Merged Article"

        # 3. Update Winning Proposal
        self._update_proposal_status(session, proposal_id, JobStatus.APPROVED, f"{reason_prefix}: {reason}")

        logger.success(f"✅ {reason_prefix}: {source_id} -> {target_id} | Reason: {reason}")

        # 4. Reject Losers (Concurrent proposals for same source)
        col_id = MergeProposalModel.source_event_id if is_event_merge else MergeProposalModel.source_article_id
        session.execute(
            update(MergeProposalModel)
            .where(
                col_id == source_id,
                MergeProposalModel.id != proposal_id,
                MergeProposalModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
            )
            .values(status=JobStatus.REJECTED, reasoning="Merged into another target (Auto-Cleanup).")
        )
        session.commit()
        return True

    def _execute_new_event_fallback(self, session, article_id):
        article = session.get(ArticleModel, article_id)
        if article:
            self.cluster.execute_new_event_action(
                session, article, reason="Auto-Review: All proposals rejected"
            )
            session.commit()

    # --- HELPERS ---

    def _update_proposal_status(self, session, pid, status, reasoning):
        session.execute(
            update(MergeProposalModel)
            .where(MergeProposalModel.id == pid)
            .values(status=status, reasoning=reasoning)
        )
        session.commit()

    def _bulk_reject(self, session, pids, reason):
        session.execute(
            update(MergeProposalModel)
            .where(MergeProposalModel.id.in_(pids))
            .values(status=JobStatus.REJECTED, reasoning=reason)
        )
        session.commit()

    def _build_article_context(self, session, article) -> str:
        content_txt = ""
        # Try to get content from relation or query
        if article.contents:
            content_txt = article.contents[0].content[:2000]
        else:
            rec = session.scalar(select(ArticleContentModel).where(ArticleContentModel.article_id == article.id))
            if rec: content_txt = rec.content[:2000]
        return f"ARTICLE TITLE: {article.title}\nDATE: {article.published_date}\nTEXT: {content_txt}"

    def _build_event_context(self, session, event) -> str:
        text = f"EVENT TITLE: {event.title}\n"
        if event.summary and isinstance(event.summary, dict):
            text += f"SUMMARY: {event.summary.get('center') or event.summary.get('bias') or ''}\n"

        # Optimization: Fetch only top 3 articles
        stmt = (
            select(ArticleModel)
            .where(ArticleModel.event_id == event.id)
            .order_by(desc(ArticleModel.published_date))
            .limit(3)
        )
        arts = session.scalars(stmt).all()
        
        text += "RELATED ARTICLES:\n"
        for art in arts:
            content = "..."
            rec = session.scalar(select(ArticleContentModel.content).where(ArticleContentModel.article_id == art.id))
            if rec: content = rec[:200]
            text += f"- [{art.published_date}] {art.title} : {content}...\n"
        return text