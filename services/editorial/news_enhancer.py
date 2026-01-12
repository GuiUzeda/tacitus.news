import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from sqlalchemy import create_engine, select, update, func, or_, exists
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger

# Add common to path if needed, though usually handled by env
from base_worker import BaseQueueWorker
from event_aggregator import EventAggregator

# Project Imports
from config import Settings
from models import EventsQueueModel, EventsQueueName
from llm_parser import CloudNewsAnalyzer, LLMNewsOutputSchema

from news_events_lib.models import NewsEventModel, ArticleModel, JobStatus, MergeProposalModel


class NewsEnhancerWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.enhancer = CloudNewsAnalyzer()
        
        # Allow some concurrency for article processing within an event
        self.semaphore = asyncio.Semaphore(5) 

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=EventsQueueModel,
            target_queue_name=EventsQueueName.ENHANCER,
            batch_size=5,
            pending_status=JobStatus.PENDING
        )

    async def run(self):
        """
        Overrides BaseQueueWorker.run to implement a Streaming/Producer-Consumer pattern.
        """
        logger.info(f"🚀 Worker started (Streaming Mode)")
        self._reset_stuck_tasks()
        self.queue = asyncio.Queue(maxsize=1)
        
        # Start Consumers (Staggered)
        workers = []
        for i in range(4):
            workers.append(asyncio.create_task(self._consumer_loop(i)))
            await asyncio.sleep(1.0)
        
        while True:
            try:
                if self.queue.full():
                    await asyncio.sleep(1.0)
                    continue

                with self.SessionLocal() as session:
                    jobs = self._fetch_jobs(session)
                    
                    for job in jobs:
                        await self.queue.put(job.id)

                if not jobs:
                    # If DB has no pending jobs, and our internal queue is empty, check for stuck jobs
                    if self.queue.empty():
                        self._cleanup_stuck_jobs()

                    logger.info('No more jobs found. Sleeping 60s...')
                    await asyncio.sleep(60)
                        
            except Exception as e:
                logger.error(f"Producer Error: {e}")
                await asyncio.sleep(5)

    def _reset_stuck_tasks(self):
        with self.SessionLocal() as session:
            result = session.execute(
                update(EventsQueueModel)
                .where(
                    EventsQueueModel.status == JobStatus.PROCESSING,
                    EventsQueueModel.queue_name == self.queue_name
                )
                .values(status=JobStatus.PENDING)
            )
            session.commit()
            if result.rowcount > 0: # type: ignore
                logger.info(f"🧹 Reset {result.rowcount} stuck 'processing' enhancer jobs.") # type: ignore

    def _cleanup_stuck_jobs(self):
        """
        Resets jobs that have been stuck in PROCESSING state for too long (e.g. worker crash).
        """
        timeout = timedelta(minutes=10)
        cutoff = datetime.now(timezone.utc) - timeout
        
        with self.SessionLocal() as session:
            result = session.execute(
                update(EventsQueueModel)
                .where(
                    EventsQueueModel.status == JobStatus.PROCESSING,
                    EventsQueueModel.queue_name == self.queue_name,
                    EventsQueueModel.updated_at < cutoff
                )
                .values(status=JobStatus.PENDING, msg="Auto-reset: Stuck in processing")
            )
            session.commit()
            if result.rowcount > 0:
                logger.warning(f"🧹 Reset {result.rowcount} stuck jobs (timeout > 10m).")

    async def _consumer_loop(self, worker_id):
        while True:
            job_id = await self.queue.get()
            try:
                with self.SessionLocal() as session:
                    job = session.get(EventsQueueModel, job_id)
                    if job:
                        await self.process_item(session, job)
                        session.commit()
            except Exception as e:
                logger.error(f"Worker {worker_id} error on job {job_id}: {e}")
            finally:
                self.queue.task_done()

    async def _analyze_wrapper(self, text: str) -> Optional[LLMNewsOutputSchema]:
        """
        Wraps the LLM call with a Semaphore for concurrency control.
        Retries are handled by the decorator in llm_parser.
        """
        async with self.semaphore:
            return await self.enhancer.analyze_article(text)
            
    def _fetch_jobs(self, session):
        # Override to join NewsEventModel
        # Filter out events that have pending/processing merge proposals (Source or Target)
        active_proposals = select(1).where(
            or_(
                MergeProposalModel.source_event_id == NewsEventModel.id,
                MergeProposalModel.target_event_id == NewsEventModel.id
            ),
            MergeProposalModel.status.in_(['pending', 'processing'])
        )

        stmt = (
            select(EventsQueueModel, NewsEventModel)
            .join(NewsEventModel, EventsQueueModel.event_id == NewsEventModel.id)
            .where(
                EventsQueueModel.status == self.pending_status,
                EventsQueueModel.queue_name == self.queue_name,
                ~exists(active_proposals)
            )
            .order_by(EventsQueueModel.created_at.asc(), NewsEventModel.article_count.desc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = session.execute(stmt).all()

        jobs = []
        for queue_item, event in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            queue_item.event = event # Attach event to job
            jobs.append(queue_item)
        
        session.commit()
        return jobs

    async def process_item(self, session, job):
        event = job.event
        
        # 1. Fetch articles (Existing Logic)
        # Filter out articles that have pending/processing merge proposals
        article_proposals = select(1).where(
            MergeProposalModel.source_article_id == ArticleModel.id,
            MergeProposalModel.status.in_(['pending', 'processing'])
        )

        stmt = (
            select(ArticleModel)
            .filter(ArticleModel.event_id == event.id)
            .options(joinedload(ArticleModel.contents))
            .options(joinedload(ArticleModel.newspaper))
            .filter(ArticleModel.summary_status == JobStatus.PENDING)
            .filter(~exists(article_proposals))
        )
        articles = session.scalars(stmt).unique().all()

        summary_inputs = []
        processed_ids = set()

        # --- PHASE 1: Process Pending Articles ---
        if articles:
            logger.info(f"Enhancing '{event.title}' ({len(articles)} pending articles) in batches...")
            
            # 2. Batch Processing Configuration
            BATCH_SIZE = 4  # Process 4 articles at a time (Optimal for ~30k context limit)
            
            # Helper to chunk the list of articles
            article_batches = [articles[i:i + BATCH_SIZE] for i in range(0, len(articles), BATCH_SIZE)]

            for batch in article_batches:
                # Prepare texts, filtering out empty ones
                valid_articles = []
                batch_texts = []
                
                for article in batch:
                    if article.contents and article.contents[0].content:
                        # Truncate here to save tokens before sending (Limit to ~6k chars per article)
                        # This removes footers/comments which are irrelevant for editorial analysis
                        clean_content = article.contents[0].content[:6000]
                        batch_texts.append(clean_content)
                        valid_articles.append(article)
                
                if not batch_texts:
                    continue

                # 3. Call the Batch API
                # We use the semaphore to ensure we don't run too many *batches* concurrently
                # if multiple workers are running.
                try:
                    async with self.semaphore:
                        # NOTE: This calls the new method in CloudNewsAnalyzer
                        batch_results = await self.enhancer.analyze_articles_batch(batch_texts)
                except Exception as e:
                    logger.error(f"Batch analysis failed: {e}")
                    batch_results = None

                if not batch_results:
                    logger.warning("Empty results from batch analysis. Marking articles as FAILED.")
                    for article in valid_articles:
                        article.summary_status = JobStatus.FAILED
                        session.add(article)
                    continue

                # 4. Map Results back to Articles
                # We assume the LLM returns results in the same order as inputs.
                # If the lengths mismatch, we zip safely to avoid crashes.
                for article, result in zip(valid_articles, batch_results):
                    if not result: 
                        continue

                    # Update Article Fields
                    article.main_topics = result.main_topics
                    article.interests = result.entities 
                    
                    all_entities = []
                    if result.entities:
                        for entity_list in result.entities.values():
                            all_entities.extend(entity_list)
                    article.entities = list(set(all_entities))

                    article.summary = result.summary
                    article.key_points = result.key_points
                    article.stance = result.stance
                    article.stance_reasoning = result.stance_reasoning
                    article.clickbait_score = result.clickbait_score
                    article.clickbait_reasoning = result.clickbait_reasoning
                    
                    article.summary_status = JobStatus.COMPLETED
                    article.summary_date = datetime.now(timezone.utc)
                    session.add(article)

                    # Collect data for Event Summary
                    summary_inputs.append({
                        "bias": article.newspaper.bias,
                        "key_points": result.key_points
                    })
                    processed_ids.add(article.id)

                    # 5. Aggregate Statistics (EventAggregator)
                    # Since we are inside the loop, we update the event incrementally
                    EventAggregator.aggregate_interests(event, result.entities)
                    EventAggregator.aggregate_stance(event, article.newspaper.bias, result.stance)
                    EventAggregator.aggregate_clickbait(event, article.newspaper.bias, result.clickbait_score)

        # --- PHASE 2: Check for External Updates (Merges / Manual Edits) ---
        last_sum = event.last_summarized_at or datetime.min
        if last_sum.tzinfo is None: last_sum = last_sum.replace(tzinfo=timezone.utc)
        
        job_updated = job.updated_at or datetime.min
        if job_updated.tzinfo is None: job_updated = job_updated.replace(tzinfo=timezone.utc)
        
        has_external_update = job_updated > last_sum

        # Early exit check
        if not summary_inputs and not has_external_update:
             # Handle empty event or just no work
             if event.article_count > 0:
                 # Just move to publisher if nothing to do
                 job.status = JobStatus.PENDING
                 job.queue_name = EventsQueueName.PUBLISHER
             else:
                 job.status = JobStatus.FAILED
                 job.msg = "No articles"
             return

        # --- PHASE 3: Gather Context from Merges ---
        force_context_fetch = False
        if has_external_update:
            stmt_merges = select(NewsEventModel).where(
                NewsEventModel.merged_into_id == event.id,
                NewsEventModel.last_updated_at > last_sum
            )
            merged_events = session.scalars(stmt_merges).all()
            for m_ev in merged_events:
                s_text = ""
                if m_ev.summary and isinstance(m_ev.summary, dict):
                    s_text = m_ev.summary.get("center") or m_ev.summary.get("bias")
                
                if s_text:
                    summary_inputs.append({
                        "bias": "merged_context",
                        "key_points": [f"Context from merged event '{m_ev.title}': {s_text}"]
                    })
                else:
                    force_context_fetch = True

        # --- PHASE 4: Fallback Context (If needed) ---
        # If we have an update but no inputs yet, or if we detected a raw merge (no summary)
        if (not summary_inputs or force_context_fetch) and has_external_update:
             stmt_ctx = (
                select(ArticleModel)
                .filter(ArticleModel.event_id == event.id)
                .filter(ArticleModel.summary_status == JobStatus.COMPLETED)
                .options(joinedload(ArticleModel.newspaper))
                .order_by(ArticleModel.published_date.desc())
                .limit(10)
            )
             ctx_articles = session.scalars(stmt_ctx).unique().all()
             for art in ctx_articles:
                 if art.id not in processed_ids:
                     points = art.key_points or ([art.summary] if art.summary else [])
                     if points:
                         summary_inputs.append({
                             "bias": art.newspaper.bias,
                             "key_points": points
                         })

        # --- PHASE 5: Generate Summary ---
        session.add(event)
        session.flush()

        if summary_inputs:
            event_summary = await self.enhancer.summarize_event(
                summary_inputs, 
                event.summary 
            )
            if event_summary:
                event.subtitle = event_summary.get("subtitle")
                event.title = event_summary.get("title")
                event.summary = event_summary.get("summary")
                event.last_summarized_at = datetime.now(timezone.utc)
                event.articles_at_last_summary = event.article_count 
                session.add(event)

        # Move to Publisher
        # CRITICAL: Refresh job to check if NewsCluster/Merger flagged it as PENDING 
        # while we were processing (e.g. new article arrived).
        session.refresh(job)
        if job.status == JobStatus.PENDING :
            logger.info(f"🔄 Job {(event.title or '')[:20]}... was flagged for re-processing. Keeping in ENHANCER queue.")
            return

        job.status = JobStatus.PENDING
        job.queue_name = EventsQueueName.PUBLISHER
        job.updated_at = datetime.now(timezone.utc)
        
        logger.success(f"✅ Enhanced: {event.title} (+{len(summary_inputs)} inputs processed)")
if __name__ == "__main__":
    worker = NewsEnhancerWorker()
    asyncio.run(worker.run())