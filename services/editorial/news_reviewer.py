import asyncio
import sys
import os
import re
import time
import uuid
from typing import List, Dict, Tuple, Optional
from itertools import groupby

from sqlalchemy import create_engine, select, update, and_
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger


from base_worker import BaseQueueWorker
from config import Settings
from news_events_lib.models import MergeProposalModel, ArticleModel, NewsEventModel, JobStatus
from llm_parser import CloudNewsAnalyzer
from news_cluster import NewsCluster

class TokenBucket:
    """
    Manages API Rate Limits (TPM) allowing for bursts.
    Refills tokens over time to strictly respect the limit.
    """
    def __init__(self, max_tokens_per_minute: int):
        self.capacity = max_tokens_per_minute
        self.tokens = max_tokens_per_minute
        self.rate = max_tokens_per_minute / 60.0 # Tokens per second
        self.last_update = time.time()
        self.lock = asyncio.Lock()

    async def consume(self, cost: int):
        """
        Attempts to consume tokens. If not enough, waits until available.
        """
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            # Refill tokens based on elapsed time
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now

            if self.tokens >= cost:
                self.tokens -= cost
                return # Approved
            else:
                # Calculate wait time
                deficit = cost - self.tokens
                wait_time = deficit / self.rate
                # Add a small buffer to avoid float precision issues
                wait_time += 0.1 
                logger.debug(f"⏳ Rate Limit: Waiting {wait_time:.2f}s for {cost} tokens...")
                await asyncio.sleep(wait_time)
                
                # After waiting, we consume the tokens
                self.tokens = 0
                self.last_update = time.time()

class NewsReviewerWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Tools
        self.llm = CloudNewsAnalyzer()
        self.cluster = NewsCluster()
        
        # Configuration
        self.CONFIDENCE_THRESHOLD = 0.85
        
        # RATE LIMITER: 
        # Gemma-3-12b limit is ~15,000 TPM. 
        # We set it slightly lower (14k) to be safe.
        self.limiter = TokenBucket(max_tokens_per_minute=14000)

        # Initialize BaseWorker
        # We use MergeProposalModel as the queue model.
        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=MergeProposalModel,
            target_queue_name=None, # Not used for proposals
            batch_size=10, # Number of ARTICLES to process per batch
            pending_status=JobStatus.PENDING
        )
        
        # Startup cleanup
        self._reset_stuck_tasks()

    def _reset_stuck_tasks(self):
        with self.SessionLocal() as session:
            result = session.execute(
                update(MergeProposalModel)
                .where(MergeProposalModel.status == "processing")
                .values(status="pending")
            )
            session.commit()
            if result.rowcount > 0:
                logger.info(f"🧹 Reset {result.rowcount} stuck 'processing' proposals to 'pending'.")

    def _fetch_jobs(self, session):
        # 1. Fetch Distinct Article IDs (Limit by batch_size)
        subq = (
            select(MergeProposalModel.source_article_id)
            .where(MergeProposalModel.status == "pending")
            .distinct()
            .limit(self.batch_size)
        )
        article_ids = session.execute(subq).scalars().all()
        
        if not article_ids:
            return []

        # 2. Fetch ALL proposals for these articles and LOCK them
        stmt = (
            select(MergeProposalModel)
            .where(MergeProposalModel.source_article_id.in_(article_ids))
            .where(MergeProposalModel.status == "pending")
            .with_for_update(skip_locked=True)
        )
        proposals = session.execute(stmt).scalars().all()

        # 3. Mark as processing
        for p in proposals:
            p.status = "processing"
        
        session.commit()
        return proposals

    async def process_items(self, session, jobs):
        """
        Override to handle grouping and concurrency.
        'jobs' here is a list of MergeProposalModel objects marked as 'processing'.
        """
        if not jobs: return

        # Group by article_id
        # Note: jobs might be detached if session.commit() expired them. 
        # But we just need IDs for the concurrent workers.
        jobs.sort(key=lambda x: str(x.source_article_id))
        
        tasks = []
        for article_id, group in groupby(jobs, key=lambda x: x.source_article_id):
            # We pass the IDs of the proposals to the worker method
            proposal_ids = [p.id for p in group]
            tasks.append(self.process_single_article_concurrent(article_id, proposal_ids))

        # Run concurrently
        if tasks:
            await asyncio.gather(*tasks)

    async def process_single_article_concurrent(self, article_id: uuid.UUID, proposal_ids: List[uuid.UUID]) -> bool:
        """
        Orchestrates the Review -> LLM -> Write flow for one article.
        Uses asyncio.to_thread for ALL database operations to avoid blocking.
        """
        
        # --- PHASE 1: READ DATA (Threaded) ---
        work_data = await asyncio.to_thread(self._get_work_data, article_id, proposal_ids)
        
        if not work_data:
            return False
            
        article_context, proposals_data = work_data
        
        logger.info(f"🔎 Reviewing '{article_context['title'][:20]}...' ({len(proposals_data)} proposals)")

        match_found = False
        all_rejected = True
        
        # Estimated cost: Prompt (~700) + Output (~100)
        TOKEN_COST_PER_CALL = 800

        # --- PHASE 2: EVALUATE (Async) ---
        for prop in proposals_data:
            
            # Rate Limit Check
            await self.limiter.consume(TOKEN_COST_PER_CALL)

            # Call LLM
            # Retry logic is handled by @with_retry in llm_parser.py
            try:
                result = await self.llm.verify_event_match(
                    prop['event_context'], 
                    article_context['context_str']
                )
            except Exception as e:
                logger.error(f"LLM Verification Failed after retries: {e}")
                result = None
            
            if not result:
                all_rejected = False # Error occurred, play it safe
                continue

            # --- DECISION LOGIC ---
            
            # CASE A: MATCH
            if result.same_event and result.confidence_score >= self.CONFIDENCE_THRESHOLD:
                logger.success(f"✅ Auto-MERGE: {article_context['title']} ")
                
                # --- PHASE 3: WRITE MATCH (Threaded) ---
                await asyncio.to_thread(
                    self._execute_merge, 
                    article_id, 
                    prop['event_id'], 
                    prop['proposal_id'], 
                    result.reasoning
                )
                match_found = True
                all_rejected = False
                break 
            
            # CASE B: REJECT
            elif (not result.same_event) and result.confidence_score >= self.CONFIDENCE_THRESHOLD:
                # --- PHASE 3: WRITE REJECT (Threaded) ---
                # We save rejections immediately so we don't re-process them if the worker restarts
                await asyncio.to_thread(
                    self._mark_proposal_rejected,
                    prop['proposal_id'],
                    result.reasoning
                )
            
            # CASE C: UNSURE
            else:
                all_rejected = False

        # --- PHASE 4: NEW EVENT (Threaded) ---
        if not match_found and all_rejected:
            logger.info(f"🆕 Creating NEW EVENT for: {article_context['title']}")
            await asyncio.to_thread(
                self._execute_new_event,
                article_id
            )

        return True

    # =========================================================================
    # SYNCHRONOUS DB HELPERS (Run in Threads)
    # =========================================================================

    def _get_work_data(self, article_id: uuid.UUID, proposal_ids: List[uuid.UUID]) -> Optional[Tuple[Dict, List[Dict]]]:
        """
        Fetches all necessary data to process an article.
        Checks if the article is already merged (optimization).
        Returns detached dictionaries/strings.
        """
        with self.SessionLocal() as session:
            # 1. Fetch Article
            article = session.get(ArticleModel, article_id)
            if not article: return None

            # 2. Optimization: Is it already merged?
            # Assuming ArticleModel has 'event_id'
            if getattr(article, 'event_id', None) is not None:
                logger.warning(f"Article {article.id} already merged. Obsoleting pending proposals.")
                # Cleanup
                session.execute(
                    update(MergeProposalModel)
                    .where(
                        MergeProposalModel.id.in_(proposal_ids),
                        MergeProposalModel.status == "pending"
                    )
                    .values(status="rejected", reasoning="Auto-Cleanup: Article already merged.")
                )
                session.commit()
                return None

            # 3. Fetch Proposals
            # Eager load target event and its articles to build context strings
            stmt = (
                select(MergeProposalModel)
                .options(
                    joinedload(MergeProposalModel.target_event).joinedload(NewsEventModel.articles).joinedload(ArticleModel.contents)
                )
                .where(
                    MergeProposalModel.id.in_(proposal_ids)
                )
                .order_by(MergeProposalModel.similarity_score.desc())
            )
            proposals = session.execute(stmt).unique().scalars().all()
            
            if not proposals: return None

            # 4. Build Context Strings (Inside the session while objects are attached)
            article_context_str = self._build_article_context(article)
            
            proposals_data = []
            for prop in proposals:
                if not prop.target_event: continue
                proposals_data.append({
                    'proposal_id': prop.id,
                    'event_id': prop.target_event_id,
                    'event_context': self._build_event_context(prop.target_event),
                    'similarity': prop.similarity_score
                })

            return (
                {'id': article.id, 'title': article.title, 'context_str': article_context_str}, 
                proposals_data
            )

    def _execute_merge(self, article_id: uuid.UUID, event_id: uuid.UUID, proposal_id: uuid.UUID, reason: str):
        """Executes the merge and updates the proposal."""
        with self.SessionLocal() as session:
            # Re-fetch objects to attach to this session
            article = session.get(ArticleModel, article_id)
            event = session.get(NewsEventModel, event_id)
            prop = session.get(MergeProposalModel, proposal_id)
            
            if article and event:
                # 1. Execute Logic
                self.cluster.execute_merge_action(session, article, event)
                
                # 2. Update Winning Proposal
                if prop:
                    prop.status = "approved"
                    prop.reasoning = f"Auto-Verified: {reason}"
                    session.add(prop)
                
                # 3. Cleanup Losers (Obsolete other pending proposals for this article)
                session.execute(
                    update(MergeProposalModel)
                    .where(
                        MergeProposalModel.source_article_id == article_id,
                        MergeProposalModel.status == "pending"
                    )
                    .values(status="rejected", reasoning="Article merged into another event.")
                )
                session.commit()

    def _mark_proposal_rejected(self, proposal_id: uuid.UUID, reason: str):
        with self.SessionLocal() as session:
            prop = session.get(MergeProposalModel, proposal_id)
            if prop:
                prop.status = "rejected"
                prop.reasoning = f"Auto-Rejected: {reason}"
                session.commit()

    def _execute_new_event(self, article_id: uuid.UUID):
        with self.SessionLocal() as session:
            article = session.get(ArticleModel, article_id)
            if article:
                self.cluster.execute_new_event_action(
                    session, article, reason="Auto-Review: All proposals rejected by LLM"
                )
                session.commit()

    # =========================================================================
    # STRING BUILDERS (Called inside DB Thread)
    # =========================================================================

    def _build_event_context(self, event: NewsEventModel) -> str:
        text = f"EVENT TITLE: {event.title}\n"
        
        if event.summary and isinstance(event.summary, dict):
            summary_text = event.summary.get("center") or event.summary.get("bias") or ""
            text += f"CURRENT SUMMARY: {summary_text}\n"
            
        text += "RELATED ARTICLES:\n"
        # Accessing relations here works because we used joinedload or are inside the session
        for art in event.articles[:5]: 
            # Safe access to attributes
            sum_txt = art.summary[:100] if art.summary else ""
            try:
                content_snippet = art.contents[0].content[:100] if art.contents else ""
            except (IndexError, AttributeError):
                content_snippet = ""
                
            text += f"- {art.title} : {sum_txt}... : {content_snippet}... ({art.published_date})\n"
        return text

    def _build_article_context(self, article: ArticleModel) -> str:
        content = ""
        if article.contents:
            content = article.contents[0].content[:2000] 
        
        return f"""
        CANDIDATE ARTICLE:
        Title: {article.title}
        Date: {article.published_date}
        Summary: {article.summary}
        Content Snippet: {content}
        """

if __name__ == "__main__":
    worker = NewsReviewerWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")