import asyncio
import sys
import os
import re
import time
import uuid
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
from itertools import groupby

from sqlalchemy import create_engine, select, update, and_
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger


from base_worker import BaseQueueWorker
from config import Settings
from news_events_lib.models import MergeProposalModel, ArticleModel, NewsEventModel, JobStatus
from llm_parser import CloudNewsAnalyzer, EventMatchSchema
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
        Sleeps OUTSIDE the lock to allow other tasks to proceed if they have tokens 
        (or to queue up without blocking the event loop logic).
        """
        while True:
            wait_time = 0
            async with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= cost:
                    self.tokens -= cost
                    return
                
                # Calculate deficit
                deficit = cost - self.tokens
                wait_time = deficit / self.rate + 0.1
            
            if wait_time > 0:
                logger.debug(f"⏳ Rate Limit: Waiting {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)

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
        self.concurrency = 10  # Increased from 3 to 10 for higher throughput

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

    async def run(self):
        """
        Overrides BaseQueueWorker.run to implement a Streaming/Producer-Consumer pattern.
        Instead of processing batches strictly, we feed a Queue and have workers consume continuously.
        """
        logger.info(f"🚀 Worker started (Streaming Mode)")
        self.queue = asyncio.Queue(maxsize=10) 
        
        # Start Consumers (Staggered to avoid rate limit bursts)
        workers = []
        for i in range(self.concurrency):
            workers.append(asyncio.create_task(self._consumer_loop(i)))
            # Stagger startup to avoid thundering herd on API limits
            await asyncio.sleep(1.0)
        
        while True:
            try:
                # Flow Control: Don't fetch if queue is full
                if self.queue.full():
                    await asyncio.sleep(1.0)
                    continue

                with self.SessionLocal() as session:
                    # Fetch jobs (Commits 'processing' status, so they are safe to pass around)
                    jobs = self._fetch_jobs(session) 
                    
                    if not jobs:
                        logger.info('No more jobs found. Waiting for queue to drain...')
                        await self.queue.join()
                        logger.info('Queue drained. Stopping workers...')
                        for w in workers:
                            w.cancel()
                        await asyncio.gather(*workers, return_exceptions=True)
                        logger.info('All workers stopped. Exiting.')
                        return

                    # Grouping Logic
                    # Sort by source ID (handle None for mixed types)
                    jobs.sort(key=lambda x: str(x.source_article_id or x.source_event_id))
                    
                    for source_id, group in groupby(jobs, key=lambda x: x.source_article_id or x.source_event_id):
                        items = list(group)
                        if not items: continue
                        
                        # Determine type based on the first item
                        is_event_merge = (items[0].source_event_id is not None)
                        proposal_ids = [p.id for p in items]
                        
                        await self.queue.put((source_id, proposal_ids, is_event_merge))
                        
            except Exception as e:
                logger.error(f"Producer Error: {e}")
                await asyncio.sleep(5)

    async def _consumer_loop(self, worker_id):
        while True:
            source_id, proposal_ids, is_event_merge = await self.queue.get()
            try:
                if is_event_merge:
                    await self.process_single_event_merge_concurrent(source_id, proposal_ids)
                else:
                    await self.process_single_article_concurrent(source_id, proposal_ids)
            except Exception as e:
                logger.error(f"Worker {worker_id} error on {source_id}: {e}")
            finally:
                self.queue.task_done()

    def _reset_stuck_tasks(self):
        with self.SessionLocal() as session:
            result = session.execute(
                update(MergeProposalModel)
                .where(MergeProposalModel.status == "processing")
                .values(status="pending")
            )
            session.commit()
            if result.rowcount > 0: # type: ignore
                logger.info(f"🧹 Reset {result.rowcount} stuck 'processing' proposals to 'pending'.") # type: ignore

    def _fetch_jobs(self, session):
        # 1. Try Fetch Distinct Article IDs (Priority)
        subq_art = (
            select(MergeProposalModel.source_article_id)
            .where(MergeProposalModel.status == "pending", MergeProposalModel.source_article_id.is_not(None))
            .distinct()
            .limit(self.batch_size)
        )
        article_ids = session.execute(subq_art).scalars().all()
        
        if article_ids:
            stmt = (
                select(MergeProposalModel)
                .where(MergeProposalModel.source_article_id.in_(article_ids))
                .where(MergeProposalModel.status == "pending")
                .with_for_update(skip_locked=True)
            )
            proposals = session.execute(stmt).scalars().all()
            for p in proposals: p.status = "processing"
            session.commit()
            return proposals

        # 2. If no articles, try Event Merges
        subq_evt = (
            select(MergeProposalModel.source_event_id)
            .where(MergeProposalModel.status == "pending", MergeProposalModel.source_event_id.is_not(None))
            .distinct()
            .limit(self.batch_size)
        )
        event_ids = session.execute(subq_evt).scalars().all()
        
        if event_ids:
            stmt = (
                select(MergeProposalModel)
                .where(MergeProposalModel.source_event_id.in_(event_ids))
                .where(MergeProposalModel.status == "pending")
                .with_for_update(skip_locked=True)
            )
            proposals = session.execute(stmt).scalars().all()
            for p in proposals: p.status = "processing"
            session.commit()
            return proposals
            
        return []

    async def _verify_proposal_llm(self, prop: dict, article_context_str: str) -> Tuple[dict, EventMatchSchema|None]:
        """Helper to run a single LLM verification with rate limiting."""
        # Estimated cost: Prompt (~700) + Output (~100)

        try:
            result = await self.llm.verify_event_match(
                prop['event_context'], 
                article_context_str
            )
            return prop, result
        except Exception as e:
            logger.error(f"LLM Verification Failed: {e}")
            return prop, None

    async def _verify_event_merge_llm(self, prop: dict, source_dict: dict) -> Tuple[dict, EventMatchSchema|None]:
        """Helper for Event-Event verification."""

        try:
            result = await self.llm.verify_event_merge(source_dict, prop['target_dict'])
            return prop, result
        except Exception as e:
            logger.error(f"LLM Event Merge Verify Failed: {e}")
            return prop, None

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
        
        # --- PHASE 2: PARALLEL EVALUATION ---
        # Launch all LLM checks concurrently to reduce latency
        tasks = [self._verify_proposal_llm(p, article_context['context_str']) for p in proposals_data]
        results = await asyncio.gather(*tasks)

        # Process results in original order (by similarity score)
        for prop, result in results:
            if not result:
                await asyncio.to_thread(
                    self._mark_proposal_failed,
                    prop['proposal_id'],
                    "LLM Verification Failed (Max Retries)"
                )
                continue

            # CASE A: MATCH
            if result.same_event and result.confidence_score >= self.CONFIDENCE_THRESHOLD:
                logger.success(f"✅ Auto-MERGE: {article_context['title']} ")
                
                # --- PHASE 3: WRITE MATCH ---
                await asyncio.to_thread(
                    self._execute_merge, 
                    article_id, 
                    prop['event_id'], 
                    prop['proposal_id'], 
                    result.reasoning
                )
                match_found = True
                break # Stop at the first (highest similarity) confirmed match
            
            # CASE B: REJECT
            else:
                # --- PHASE 3: WRITE REJECT ---
                await asyncio.to_thread(
                    self._mark_proposal_rejected,
                    prop['proposal_id'],
                    result.reasoning
                )

        # --- PHASE 4: NEW EVENT (Threaded) ---
        if not match_found:
            # Only create new event if we had at least one valid LLM response (to avoid creating dupes on network error)
            valid_responses = sum(1 for _, r in results if r is not None)
            if valid_responses > 0:
                logger.info(f"🆕 Creating NEW EVENT for: {article_context['title']}")
                await asyncio.to_thread(self._execute_new_event, article_id)

        return True

    async def process_single_event_merge_concurrent(self, source_event_id: uuid.UUID, proposal_ids: List[uuid.UUID]) -> bool:
        """
        Orchestrates the Review -> LLM -> Write flow for EVENT-TO-EVENT merges.
        """
        # --- PHASE 1: READ DATA ---
        work_data = await asyncio.to_thread(self._get_event_merge_work_data, source_event_id, proposal_ids)
        if not work_data: return False
        
        source_dict, proposals_data = work_data
        logger.info(f"🔗 Reviewing Event Merge: '{source_dict['title'][:20]}...' ({len(proposals_data)} candidates)")

        # --- PHASE 2: PARALLEL EVALUATION ---
        tasks = [self._verify_event_merge_llm(p, source_dict) for p in proposals_data]
        results = await asyncio.gather(*tasks)

        for prop, result in results:
            if not result:
                await asyncio.to_thread(self._mark_proposal_failed, prop['proposal_id'], "LLM Verification Failed")
                continue

            # CASE A: MATCH
            if result.same_event and result.confidence_score >= self.CONFIDENCE_THRESHOLD:
                logger.success(f"⚡ Auto-MERGE EVENTS: {source_dict['title']} -> Target")
                await asyncio.to_thread(
                    self._execute_event_merge_action,
                    source_event_id,
                    prop['target_event_id'],
                    prop['proposal_id'],
                    result.reasoning
                )
                break # Stop after first successful merge (source is now dead)
            
            # CASE B: REJECT
            else:
                await asyncio.to_thread(self._mark_proposal_rejected, prop['proposal_id'], result.reasoning)

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
                        MergeProposalModel.status.in_(["pending", "processing"])
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

    def _get_event_merge_work_data(self, source_event_id: uuid.UUID, proposal_ids: List[uuid.UUID]) -> Optional[Tuple[Dict, List[Dict]]]:
        with self.SessionLocal() as session:
            # Load Source with Articles
            source = session.query(NewsEventModel).options(
                joinedload(NewsEventModel.articles).joinedload(ArticleModel.contents)
            ).filter(NewsEventModel.id == source_event_id).first()
            
            if not source or not source.is_active:
                # Cleanup proposals if source is dead/inactive
                session.execute(
                    update(MergeProposalModel)
                    .where(
                        MergeProposalModel.id.in_(proposal_ids),
                        MergeProposalModel.status.in_(["pending", "processing"])
                    )
                    .values(status="rejected", reasoning="Auto-Cleanup: Source event inactive.")
                )
                session.commit()
                return None

            # Load Proposals with Targets
            stmt = (
                select(MergeProposalModel)
                .options(joinedload(MergeProposalModel.target_event).joinedload(NewsEventModel.articles).joinedload(ArticleModel.contents))
                .where(MergeProposalModel.id.in_(proposal_ids))
                .order_by(MergeProposalModel.similarity_score.desc())
            )
            proposals = session.execute(stmt).unique().scalars().all()
            if not proposals: return None

            source_dict = self._build_event_dict(source)
            proposals_data = []
            for p in proposals:
                if not p.target_event: continue
                proposals_data.append({
                    'proposal_id': p.id,
                    'target_event_id': p.target_event_id,
                    'target_dict': self._build_event_dict(p.target_event),
                    'similarity': p.similarity_score
                })
            return source_dict, proposals_data

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
                # FIX: Include 'processing' to clean up concurrent jobs in this batch
                session.execute(
                    update(MergeProposalModel)
                    .where(
                        MergeProposalModel.source_article_id == article_id,
                        MergeProposalModel.id != proposal_id,
                        MergeProposalModel.status.in_(["pending", "processing"])
                    )
                    .values(status="rejected", reasoning="Article merged into another event.")
                )
                session.commit()

    def _execute_event_merge_action(self, source_id: uuid.UUID, target_id: uuid.UUID, proposal_id: uuid.UUID, reason: str):
        with self.SessionLocal() as session:
             source = session.get(NewsEventModel, source_id)
             target = session.get(NewsEventModel, target_id)
             prop = session.get(MergeProposalModel, proposal_id)
             
             if source and target and source.is_active:
                 self.cluster.execute_event_merge(session, source, target)
                 
                 if prop:
                     prop.status = "approved"
                     prop.reasoning = f"Auto-Verified: {reason}"
                     session.add(prop)
                 
                 # Reject other proposals for this source
                 session.execute(
                     update(MergeProposalModel)
                     .where(
                         MergeProposalModel.source_event_id == source_id,
                         MergeProposalModel.id != proposal_id,
                         MergeProposalModel.status.in_(["pending", "processing"])
                     )
                     .values(status="rejected", reasoning="Event merged into another.")
                 )
                 session.commit()

    def _mark_proposal_rejected(self, proposal_id: uuid.UUID, reason: str):
        with self.SessionLocal() as session:
            prop = session.get(MergeProposalModel, proposal_id)
            if prop:
                prop.status = "rejected"
                prop.reasoning = f"Auto-Rejected: {reason}"
                session.commit()

    def _mark_proposal_failed(self, proposal_id: uuid.UUID, reason: str):
        with self.SessionLocal() as session:
            prop = session.get(MergeProposalModel, proposal_id)
            if prop:
                prop.status = "failed"
                prop.reasoning = f"Error: {reason}"
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

    def _build_event_dict(self, event: NewsEventModel) -> Dict:
        """Builds the dictionary structure expected by verify_event_merge."""
        articles_data = []
        # Sort by date desc, take top 5
        sorted_arts = sorted(event.articles, key=lambda x: x.published_date or datetime.min, reverse=True)[:5]
        
        for art in sorted_arts:
            snippet = art.summary or ""
            if art.contents:
                snippet = art.contents[0].content[:300]
            
            articles_data.append({
                "title": art.title,
                "date": str(art.published_date),
                "snippet": snippet
            })
            
        return {
            "title": event.title,
            "date": str(event.created_at),
            "articles": articles_data
        }

if __name__ == "__main__":
    worker = NewsReviewerWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")