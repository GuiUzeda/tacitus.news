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
        event: NewsEventModel | None = job.event
        if not event:
            job.status = JobStatus.FAILED
            job.msg = "Event Not Found"
            return

        # 1. FETCH THE "DELTA" (Articles ready to be integrated)
        # We strictly look for COMPLETED articles. 
        # - PENDING: Not ready (waiting for Enricher)
        # - APPROVED: Already inside the summary (skip)
        # - COMPLETED: Enriched by Enricher, waiting for us!
        
        article_proposals = select(1).where(
            MergeProposalModel.source_article_id == ArticleModel.id,
            MergeProposalModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]),
        )

        stmt = (
            select(ArticleModel)
            .filter(ArticleModel.event_id == event.id)
            .filter(ArticleModel.summary_status == JobStatus.COMPLETED) # 👈 The Filter
            .options(
                joinedload(ArticleModel.contents), joinedload(ArticleModel.newspaper)
            )
            .filter(~exists(article_proposals))
        )
        new_articles = session.scalars(stmt).unique().all()

        summary_inputs = []
        articles_to_mark_integrated = []

        # --- PHASE 1: Collect Inputs from the Delta ---
        if new_articles:
            logger.info(f"Enhancing '{event.title}' ({len(new_articles)} new items)...")
            
            # Since these are COMPLETED, we trust they have data.
            # No need to run batches or call LLM here (Enricher did it).
            for article in new_articles:
                if article.key_points:
                    summary_inputs.append({
                        "bias": article.newspaper.bias,
                        "key_points": article.key_points,
                    })
                    articles_to_mark_integrated.append(article)
                    
                    # Update Aggregates incrementally
                    EventAggregator.aggregate_interests(event, article.interests)
                    EventAggregator.aggregate_main_topics(event, article.main_topics)
                    EventAggregator.aggregate_stance(event, article.newspaper.bias, article.stance or 0.0)
                    EventAggregator.aggregate_clickbait(event, article.newspaper.bias, article.clickbait_score or 0.0)
                    EventAggregator.aggregate_basic_stats(event, article)


        # --- PHASE 2: Debounce & Context ---
        last_sum = event.last_summarized_at or datetime.min
        if last_sum.tzinfo is None: last_sum = last_sum.replace(tzinfo=timezone.utc)
        
        job_updated = job.updated_at or datetime.min
        if job_updated.tzinfo is None: job_updated = job_updated.replace(tzinfo=timezone.utc)

        has_external_update = job_updated > last_sum
        
        should_skip_synthesis = False
        now = datetime.now(timezone.utc)
        if event.summary and (now - last_sum) < timedelta(minutes=self.DEBOUNCE_MINUTES):
             should_skip_synthesis = True

        # Check for Merged Context (The Merger might have dumped info here)
        if has_external_update and not should_skip_synthesis:
            stmt_merges = select(NewsEventModel).where(
                NewsEventModel.merged_into_id == event.id,
                NewsEventModel.last_updated_at > last_sum,
            )
            merged_events = session.scalars(stmt_merges).all()
            for m_ev in merged_events:
                s_text = self._extract_summary_text(m_ev)
                if s_text:
                    summary_inputs.append({
                        "bias": "merged_context",
                        "key_points": [f"Context from merged event '{m_ev.title}': {s_text}"],
                    })

        if not summary_inputs and not has_external_update:
            # Done
            if event.article_count > 0:
                job.status = JobStatus.PENDING
                job.queue_name = EventsQueueName.PUBLISHER
            else:
                job.status = JobStatus.FAILED
                job.msg = "No articles"
            return

        # --- PHASE 3: Synthesis ---
        session.add(event)
        session.flush()

        if summary_inputs and not should_skip_synthesis:
            logger.info(f"Summarizing {event.title} with {len(summary_inputs)} new inputs.")
            
            # Pass the previous summary to maintain context!
            event_summary = await self.enhancer.summarize_event(summary_inputs, event.summary)
            
            if event_summary:
                event.title = event_summary.title or event.title
                event.subtitle = event_summary.subtitle
                event.summary = event_summary.summary
                event.last_summarized_at = datetime.now(timezone.utc)
                event.ai_impact_score = event_summary.impact_score
                event.ai_impact_reasoning = event_summary.impact_reasoning
                event.category_tag = event_summary.category
                
                # ✅ STATE TRANSITION: COMPLETED -> APPROVED
                # This effectively "signs off" that these articles are in the summary.
                for article in articles_to_mark_integrated:
                    article.summary_status = JobStatus.APPROVED
                    session.add(article)
                
                session.add(event)

        # --- PHASE 4: Transition ---
        session.refresh(job)
        if job.status == JobStatus.PENDING:
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
            event.status = EventStatus.ENHANCED # Ready for Publishe
            
            
            session.add(event)
            session.commit()
            logger.success(f"   -> Enhanced: '{event.title}' (Impact: {event.ai_impact_score})")
            
        except Exception as e:
            logger.error(f"   -> Summary generation failed: {e}")