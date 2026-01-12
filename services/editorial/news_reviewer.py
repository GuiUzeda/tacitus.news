import asyncio
import sys
import os
import re
import time
import uuid
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
from itertools import groupby

from sqlalchemy import create_engine, desc, select, update, and_
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger


from base_worker import BaseQueueWorker
from config import Settings
from news_events_lib.models import MergeProposalModel, ArticleModel, NewsEventModel, JobStatus, ArticleContentModel
from llm_parser import CloudNewsAnalyzer, EventMatchSchema, BatchMatchResult
from news_cluster import NewsCluster



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

    # =========================================================================
    # SYNCHRONOUS DB HELPERS (Run in Threads)
    # =========================================================================

    def _get_work_data(self, article_id: uuid.UUID, proposal_ids: List[uuid.UUID]) -> Optional[Tuple[Dict, List[Dict]]]:
        with self.SessionLocal() as session:
            article = session.get(ArticleModel, article_id)
            if not article: return None

            # Optimization: Is it already merged?
            if getattr(article, 'event_id', None) is not None:
                # Obsolete pending proposals
                session.execute(
                    update(MergeProposalModel)
                    .where(MergeProposalModel.id.in_(proposal_ids))
                    .values(status="rejected", reasoning="Auto-Cleanup: Article already merged.")
                )
                session.commit()
                return None

            # 1. Fetch Proposals with Target Event ONLY (No deep join on articles)
            stmt = (
                select(MergeProposalModel)
                .options(joinedload(MergeProposalModel.target_event)) # Shallow Load
                .where(MergeProposalModel.id.in_(proposal_ids))
                .order_by(MergeProposalModel.similarity_score.desc())
            )
            proposals = session.execute(stmt).scalars().all()
            if not proposals: return None

            # 2. Build Contexts efficiently
            article_context_str = self._build_article_context(session, article)
            
            proposals_data = []
            for prop in proposals:
                if not prop.target_event: continue
                
                # Manual efficient fetch of top 3 articles for context
                event_context = self._build_event_context_optimized(session, prop.target_event)
                
                proposals_data.append({
                    'proposal_id': prop.id,
                    'event_id': prop.target_event_id,
                    'event_context': event_context,
                    'similarity': prop.similarity_score
                })

            return (
                {'id': article.id, 'title': article.title, 'context_str': article_context_str}, 
                proposals_data
            )
    def _get_event_merge_work_data(self, source_event_id: uuid.UUID, proposal_ids: List[uuid.UUID]) -> Optional[Tuple[Dict, List[Dict]]]:
        with self.SessionLocal() as session:
            source = session.get(NewsEventModel, source_event_id)
            if not source or not source.is_active:
                session.execute(
                    update(MergeProposalModel)
                    .where(MergeProposalModel.id.in_(proposal_ids))
                    .values(status="rejected", reasoning="Auto-Cleanup: Source inactive.")
                )
                session.commit()
                return None

            # 1. Fetch Proposals (Shallow)
            stmt = (
                select(MergeProposalModel)
                .options(joinedload(MergeProposalModel.target_event)) 
                .where(MergeProposalModel.id.in_(proposal_ids))
                .order_by(MergeProposalModel.similarity_score.desc())
            )
            proposals = session.execute(stmt).scalars().all()
            
            # 2. Build Contexts
            # We treat the Source Event effectively as a "Big Article" text
            source_context_str = self._build_event_context_optimized(session, source)
            
            proposals_data = []
            for prop in proposals:
                if not prop.target_event: continue
                
                target_context_str = self._build_event_context_optimized(session, prop.target_event)
                
                proposals_data.append({
                    'proposal_id': prop.id,
                    'target_event_id': prop.target_event_id,
                    'event_context': target_context_str, # Uniform key for batching
                    'similarity': prop.similarity_score
                })

            return {'id': source.id, 'title': source.title, 'context_str': source_context_str}, proposals_data
        
    async def process_single_article_concurrent(self, article_id: uuid.UUID, proposal_ids: List[uuid.UUID]) -> bool:
        work_data = await asyncio.to_thread(self._get_work_data, article_id, proposal_ids)
        if not work_data: return False
        
        # Reuse common logic
        return await self._process_batch_verification(
            entity_id=article_id,
            work_data=work_data,
            is_event_merge=False
        )

    async def process_single_event_merge_concurrent(self, source_event_id: uuid.UUID, proposal_ids: List[uuid.UUID]) -> bool:
        work_data = await asyncio.to_thread(self._get_event_merge_work_data, source_event_id, proposal_ids)
        if not work_data: return False

        # Reuse common logic - Event Merges can be batched just like articles!
        return await self._process_batch_verification(
            entity_id=source_event_id,
            work_data=work_data,
            is_event_merge=True
        )

    async def _process_batch_verification(self, entity_id, work_data, is_event_merge: bool):
        """
        Unified Batch Processor for both Articles and Events.
        """
        source_data, proposals_data = work_data
        logger.info(f"🔎 Reviewing '{source_data['title'][:20]}...' vs {len(proposals_data)} candidates")

        match_found = False
        BATCH_SIZE = 5
        all_results = []

        # Batch Loop
        for i in range(0, len(proposals_data), BATCH_SIZE):
            batch = proposals_data[i:i + BATCH_SIZE]
            



            candidates_payload = [
                {'id': str(p['proposal_id']), 'text': p['event_context']} 
                for p in batch
            ]
            
            # Call LLM
            # Note: We reuse verify_batch_matches even for events. 
            # Ideally rename the method in LLM to verify_batch_similarity, but this works.
            batch_results = await self.llm.verify_batch_matches(
                source_data['context_str'],
                candidates_payload
            )
            all_results.extend(batch_results)

        # Process Results
        results_map = {res.proposal_id: res for res in all_results}

        for prop_data in proposals_data:
            pid = str(prop_data['proposal_id'])
            
            if pid not in results_map:
                await asyncio.to_thread(self._mark_proposal_failed, prop_data['proposal_id'], "Batch LLM Missed Item")
                continue

            result = results_map[pid]
            
            if result.same_event and result.confidence_score >= self.CONFIDENCE_THRESHOLD:
                
                if is_event_merge:
                    logger.success(f"⚡ Auto-MERGE EVENTS: {source_data['title']} -> Target")
                    await asyncio.to_thread(
                        self._execute_event_merge_action,
                        entity_id, # source_id
                        prop_data['target_event_id'],
                        prop_data['proposal_id'],
                        result.reasoning
                    )
                else:
                    logger.success(f"✅ Auto-MERGE ARTICLE: {source_data['title']}")
                    await asyncio.to_thread(
                        self._execute_merge, 
                        entity_id, # article_id
                        prop_data['event_id'], 
                        prop_data['proposal_id'], 
                        result.reasoning
                    )
                
                match_found = True
                break # Stop after first match
            else:
                await asyncio.to_thread(self._mark_proposal_rejected, prop_data['proposal_id'], result.reasoning)

        # Handle New Event (Only for Articles, usually we don't create new events from failed Event-Event merges)
        if not is_event_merge and not match_found and len(all_results) > 0:
            logger.info(f"🆕 Creating NEW EVENT for: {source_data['title']}")
            await asyncio.to_thread(self._execute_new_event, entity_id)

        return True
    def _build_article_context(self, session, article: ArticleModel) -> str:
        # Fetch content explicitly if missing
        content_txt = ""
        if not article.contents:
             # Lazy load or query
             content_rec = session.scalar(select(ArticleContentModel).where(ArticleContentModel.article_id == article.id))
             if content_rec: content_txt = content_rec.content[:2000]
        else:
             content_txt = article.contents[0].content[:2000]
             
        return f"ARTICLE TITLE: {article.title}\nDATE: {article.published_date}\nTEXT: {content_txt}"

    def _build_event_context_optimized(self, session, event: NewsEventModel) -> str:
        """
        Fetches ONLY top 3 articles for the event to save tokens and DB memory.
        """
        text = f"EVENT TITLE: {event.title}\n"
        if event.summary and isinstance(event.summary, dict):
            text += f"SUMMARY: {event.summary.get('center') or event.summary.get('bias') or ''}\n"

        # Explicit Query for context articles
        stmt = (
            select(ArticleModel)
            .where(ArticleModel.event_id == event.id)
            .order_by(desc(ArticleModel.published_date))
            .limit(3)
        )
        context_articles = session.scalars(stmt).all()
        
        text += "RELATED ARTICLES:\n"
        for art in context_articles:
            # Quick fetch content
            content_snippet = "No Content"
            content_rec = session.scalar(select(ArticleContentModel.content).where(ArticleContentModel.article_id == art.id))
            if content_rec:
                content_snippet = content_rec[:200]
            
            text += f"- [{art.published_date}] {art.title} : {content_snippet}...\n"
            
        return text
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