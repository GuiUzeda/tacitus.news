import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from sqlalchemy import select, or_, exists
from sqlalchemy.orm import Session, joinedload
from loguru import logger

# Models
from news_events_lib.models import (
    NewsEventModel,
    ArticleModel,
    JobStatus,
    MergeProposalModel,
)
from news_events_lib.models import EventStatus
from core.models import EventsQueueModel, EventsQueueName

# Core/Services
from core.llm_parser import CloudNewsAnalyzer
from domain.aggregator import EventAggregator


class NewsEnhancerDomain:
    def __init__(self, concurrency_limit: int = 5):
        self.enhancer = CloudNewsAnalyzer()
        # Semaphore to limit concurrent heavy LLM batches across threads/tasks
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        
        # CONFIG
        self.DEBOUNCE_MINUTES = 10  # Don't re-summarize if updated < 10 mins ago

    async def enhance_event_job(self, session: Session, job: EventsQueueModel):
        """
        Main logic for enhancing an event.
        1. Identifies new/pending articles.
        2. Batches them for LLM analysis (Entity/Stance extraction).
        3. Aggregates stats (Bias/Interests).
        4. Generates/Updates the Event Summary if needed.
        5. Moves to Publisher queue (unless new data arrived during process).
        """
        event: NewsEventModel | None = job.event
        if not event:
            job.status = JobStatus.FAILED
            job.msg = "Event Not Found"
            return

        # 1. Fetch & Filter Articles
        # We skip articles involved in active merge proposals to avoid race conditions
        article_proposals = select(1).where(
            MergeProposalModel.source_article_id == ArticleModel.id,
            MergeProposalModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]),
        )

        stmt = (
            select(ArticleModel)
            .filter(ArticleModel.event_id == event.id)
            .options(
                joinedload(ArticleModel.contents), joinedload(ArticleModel.newspaper)
            )
            .filter(ArticleModel.summary_status == JobStatus.PENDING)
            .filter(~exists(article_proposals))
        )
        articles = session.scalars(stmt).unique().all()

        summary_inputs = []
        processed_ids = set()

        # --- PHASE 1: Process Pending Articles (Batched) ---
        if articles:
            logger.info(f"Enhancing '{event.title}' ({len(articles)} new articles)...")

            BATCH_SIZE = 4
            article_batches = [
                articles[i : i + BATCH_SIZE]
                for i in range(0, len(articles), BATCH_SIZE)
            ]

            for batch in article_batches:
                valid_articles = []
                batch_texts = []

                for article in batch:
                    if article.contents and article.contents[0].content:
                        # Truncate handled by CloudNewsAnalyzer.smart_truncate internally now
                        batch_texts.append(f"{article.title}\n\n{article.contents[0].content}")
                        valid_articles.append(article)

                if not batch_texts:
                    continue

                # Concurrent Batch Analysis
                try:
                    async with self.semaphore:
                        batch_results = await self.enhancer.analyze_articles_batch(
                            batch_texts
                        )
                except Exception as e:
                    logger.error(f"Batch analysis failed: {e}")
                    # Mark batch as failed
                    for art in valid_articles:
                        art.summary_status = JobStatus.FAILED
                    continue

                if not batch_results:
                    continue

                # Map Results
                for article, result in zip(valid_articles, batch_results):
                    if not result:
                        continue

                    # Update Article
                    self._update_article_from_llm(article, result)
                    session.add(article)

                    # Collect for Summary
                    summary_inputs.append(
                        {
                            "bias": article.newspaper.bias,
                            "key_points": result.key_points,
                        }
                    )
                    processed_ids.add(article.id)

                    # Aggregate Stats to Event
                    EventAggregator.aggregate_interests(event, result.entities)
                    EventAggregator.aggregate_main_topics(event, result.main_topics)
                    EventAggregator.aggregate_stance(
                        event, article.newspaper.bias, result.stance
                    )
                    EventAggregator.aggregate_clickbait(
                        event, article.newspaper.bias, result.clickbait_score
                    )

        # --- PHASE 2: Check Logic for Re-Summarization ---
        last_sum = event.last_summarized_at or datetime.min
        if last_sum.tzinfo is None:
            last_sum = last_sum.replace(tzinfo=timezone.utc)

        job_updated = job.updated_at or datetime.min
        if job_updated.tzinfo is None:
            job_updated = job_updated.replace(tzinfo=timezone.utc)

        # If job is newer than last summary, it implies a Merge or Manual trigger occurred
        has_external_update = job_updated > last_sum

        # IMPROVEMENT: Debounce Check
        # If summarized very recently, we SKIP Phase 5 (Synthesis) unless it's a "First Summary"
        should_skip_synthesis = False
        now = datetime.now(timezone.utc)
        time_since_last = now - last_sum
        
        if event.summary and time_since_last < timedelta(minutes=self.DEBOUNCE_MINUTES):
             # Debounce active: We gathered stats (Phase 1), but we won't rewrite the summary text.
             logger.info(f"⏳ Debouncing Synthesis for '{event.title}' (Last sum: {time_since_last.seconds}s ago)")
             should_skip_synthesis = True

        if not summary_inputs and not has_external_update:
            # Nothing new to do
            if event.article_count > 0:
                job.status = JobStatus.PENDING
                job.queue_name = EventsQueueName.PUBLISHER
            else:
                job.status = JobStatus.FAILED
                job.msg = "No articles"
            return

        # --- PHASE 3: Gather Context from Merges ---
        force_context_fetch = False
        if has_external_update and not should_skip_synthesis:
            # Fetch summaries from events recently merged into this one
            stmt_merges = select(NewsEventModel).where(
                NewsEventModel.merged_into_id == event.id,
                NewsEventModel.last_updated_at > last_sum,
            )
            merged_events = session.scalars(stmt_merges).all()
            for m_ev in merged_events:
                s_text = self._extract_summary_text(m_ev)
                if s_text:
                    summary_inputs.append(
                        {
                            "bias": "merged_context",
                            "key_points": [
                                f"Context from merged event '{m_ev.title}': {s_text}"
                            ],
                        }
                    )
                else:
                    force_context_fetch = True

        # --- PHASE 4: Fallback Context ---
        # If we need to re-summarize but have no *new* inputs, fetch *recent* articles
        if (not summary_inputs or force_context_fetch) and has_external_update and not should_skip_synthesis:
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
                        summary_inputs.append(
                            {"bias": art.newspaper.bias, "key_points": points}
                        )

        # --- PHASE 5: Generate & Save Summary ---
        session.add(event)
        session.flush()  # Ensure aggregations are visible

        if summary_inputs and not should_skip_synthesis:
            logger.info(
                f"Summarizing Event {event.id} with {len(summary_inputs)} inputs."
            )
            event_summary = await self.enhancer.summarize_event(
                summary_inputs, event.summary
            )
            if event_summary:
                event.subtitle = event_summary.subtitle
                event.title = event_summary.title or event.title
                event.summary = event_summary.summary
                event.last_summarized_at = datetime.now(timezone.utc)
                event.articles_at_last_summary = event.article_count
                event.last_updated_at = datetime.now(timezone.utc)
                event.ai_impact_score = event_summary.impact_score or 50 
                event.ai_impact_reasoning = event_summary.impact_reasoning or "" 
                event.category_tag = event_summary.category or "GENERAL"
                session.add(event)

        # --- PHASE 6: Transition & Race Condition Check ---
        # CRITICAL: We check if another process (Cluster/Merger) flagged this job
        # as PENDING while we were working. If so, we do NOT move to publisher.
        session.refresh(job)
        if job.status == JobStatus.PENDING:
            logger.info(
                f"🔄 Job for {event.title[:20]} was re-queued during processing. Keeping in ENHANCER."
            )
            return

        job.status = JobStatus.PENDING
        job.queue_name = EventsQueueName.PUBLISHER
        job.updated_at = datetime.now(timezone.utc)
        logger.success(f"✅ Enhanced: {event.title} (Debounced: {should_skip_synthesis}, articles: {event.article_count}, impact:{event.ai_impact_score})")

    def _update_article_from_llm(self, article, result):
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

    def _extract_summary_text(self, event):
        if event.summary and isinstance(event.summary, dict):
            return event.summary.get("center") or event.summary.get("bias")
        return None

# [Add this to NewsEnhancerDomain class]

    async def enhance_event_direct(self, session: Session, event: NewsEventModel):
        """
        Forces an immediate enhancement (Analysis + Summarization) for a specific event,
        bypassing queues and debounce timers. Used by the Splitter.
        """
        logger.info(f"⚡ Direct Enhance started for: {event.title}")

        # 1. PHASE 1: Micro-Analysis (Ensure all articles have metadata)
        # The splitter moved articles here, but they might already be analyzed.
        # We check for any articles missing 'key_points' or 'stance'.
        unprocessed_articles = [
            a for a in event.articles 
            if not a.key_points or not a.stance_reasoning
        ]

        if unprocessed_articles:
            logger.info(f"   -> Analyzing {len(unprocessed_articles)} moved articles...")
            # Reuse the existing batch logic
            # We assume self.batch_size exists or hardcode it to 5
            BATCH_SIZE = 5
            for i in range(0, len(unprocessed_articles), BATCH_SIZE):
                batch =  unprocessed_articles[i : i + BATCH_SIZE]
                try:
                    # We await the LLM call directly
                    outputs = await self.enhancer.analyze_articles_batch([f"{a.title}\n\n{a.contents[0].content}" for a in batch])
                    
                    # Save results to DB immediately
                    for article, output in zip(batch, outputs):
                        article.stance = output.stance
                        article.stance_reasoning = output.stance_reasoning
                        article.clickbait_score = output.clickbait_score
                        article.clickbait_reasoning = output.clickbait_reasoning
                        article.key_points = output.key_points
                        all_entities = []
                        if output.entities:
                            for entity_list in output.entities.values():
                                all_entities.extend(entity_list)
                        article.entities = list(set(all_entities))  
                        article.summary = output.summary # Short summary
                        session.add(article)
                    session.commit()
                except Exception as e:
                    logger.error(f"   -> Batch analysis failed: {e}")

        # 2. PHASE 2: Aggregation (Math)
        # Recalculate stats based on the new grouping
        from domain.aggregator import EventAggregator # Local import to avoid circular dep
        
        # Reset aggregates before rebuilding (Optional, but safer for splits)
        event.interest_counts = {}
        event.main_topic_counts = {}
        summary_inputs = []
        for art in event.articles:
            EventAggregator.aggregate_basic_stats(event, art) # Re-sums counts
            EventAggregator.aggregate_interests(event, art.interests)
            EventAggregator.aggregate_main_topics(event, art.main_topics)
            if art.key_points and art.newspaper:
                # Group by bias (simplified for direct mode)
                summary_inputs.append({
                    "bias": art.newspaper.bias or "center",
                    "key_points": art.key_points
                })
        
        session.add(event)
        session.commit()

        
        if not summary_inputs:
            event.is_active = False
            event.status = EventStatus.ARCHIVED
            session.add(event)
            session.commit()
            logger.warning("   -> No key points available for summary.")
            return

        try:
            # Call LLM to write the Title and Summary
            # We pass 'None' as previous_summary to force a fresh perspective
            event_summary = await self.enhancer.summarize_event(summary_inputs, previous_summary=None)
            if not event_summary:
                event.is_active = False
                event.status = EventStatus.ARCHIVED
                session.add(event)
                session.commit()
                logger.error(f"   -> Summary generation failed: No Summary!")
                return
            # Apply results
            event.title = event_summary.title # Crucial: New Event needs a New Title
            event.subtitle = event_summary.subtitle
            event.summary = event_summary.summary
            event.ai_impact_score = event_summary.impact_score
            event.ai_impact_reasoning = event_summary.impact_reasoning
            event.last_summarized_at = datetime.now(timezone.utc)
            event.status = EventStatus.ENHANCED # Ready for Publisher
            
            session.add(event)
            session.commit()
            logger.success(f"   -> Enhanced: '{event.title}' (Impact: {event.ai_impact_score})")
            
        except Exception as e:
            logger.error(f"   -> Summary generation failed: {e}")