import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict

from sqlalchemy import select, or_, exists
from sqlalchemy.orm import Session, joinedload
from loguru import logger

# Models
from news_events_lib.models import (
    NewsEventModel,
    ArticleModel,
    JobStatus,
    MergeProposalModel,
    EventStatus
)
from core.models import EventsQueueModel, EventsQueueName

# Core/Services
from core.llm_parser import CloudNewsAnalyzer
from domain.aggregator import EventAggregator


class NewsEnhancerDomain:
    def __init__(self, concurrency_limit: int = 5):
        self.enhancer = CloudNewsAnalyzer()
        # CONFIG
        self.DEBOUNCE_MINUTES = 10  # Don't re-summarize if updated < 10 mins ago

    async def enhance_event(self, session: Session, job: EventsQueueModel) -> str:
        """
        Main entry point for Worker.
        Aggregates new articles, updates stats, and calls LLM if debounce allows.
        DOES NOT COMMIT.
        """
        event: NewsEventModel | None = job.event
        
        # 1. Validation
        if not event:
            job.status = JobStatus.FAILED
            job.updated_at = datetime.now(timezone.utc)
            job.msg = "Event Not Found"
            return "FAILED_MISSING"
            
        if not event.is_active:
            job.status = JobStatus.COMPLETED
            job.updated_at = datetime.now(timezone.utc)
            job.msg = "Event Inactive"
            return "SKIPPED_INACTIVE"

        # 2. FETCH THE "DELTA" (Articles ready to be integrated)
        # We look for articles enriched by the Enricher (COMPLETED) 
        # but not yet integrated into this event (APPROVED).
        
        # Guard: Don't touch articles involved in active merge proposals
        active_proposals = select(1).where(
            MergeProposalModel.source_article_id == ArticleModel.id,
            MergeProposalModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]),
        )

        stmt = (
            select(ArticleModel)
            .filter(ArticleModel.event_id == event.id)
            .filter(ArticleModel.summary_status == JobStatus.COMPLETED) # <-- The Delta
            .options(
                joinedload(ArticleModel.contents), joinedload(ArticleModel.newspaper)
            )
            .filter(~exists(active_proposals))
        )
        new_articles = session.scalars(stmt).unique().all()

        summary_inputs = []
        articles_to_mark_integrated = []

        # --- PHASE 1: Collect Inputs from the Delta ---
        if new_articles:
            # Aggregate stats incrementally (Lightweight)
            for article in new_articles:
                if article.key_points:
                    summary_inputs.append({
                        "bias": article.newspaper.bias if article.newspaper else "center",
                        "key_points": article.key_points,
                    })
                    articles_to_mark_integrated.append(article)
                    
                    # Update Aggregates
                    if article.newspaper:
                        EventAggregator.aggregate_stance(event, article.newspaper.bias, article.stance or 0.0)
                        EventAggregator.aggregate_clickbait(event, article.newspaper.bias, article.clickbait_score or 0.0)

        # --- PHASE 2: Check Debounce & External Updates ---
        
        # Check if a Merge occurred recently (Merged events dump their context here)
        last_sum = event.last_summarized_at or datetime.min
        if last_sum.tzinfo is None: last_sum = last_sum.replace(tzinfo=timezone.utc)
        
        job_updated = job.updated_at or datetime.min
        if job_updated.tzinfo is None: job_updated = job_updated.replace(tzinfo=timezone.utc)
        
        has_external_update = job_updated > last_sum
        
        # Should we run the heavy LLM?
        now = datetime.now(timezone.utc)
        is_debounced = False
        if event.summary and (now - last_sum) < timedelta(minutes=self.DEBOUNCE_MINUTES):
             is_debounced = True

        # If Debounced, we ONLY run if forced by an external update (like a manual merge)
        # Otherwise we skip synthesis to save costs.
        should_run_synthesis = (not is_debounced) or has_external_update

        # Collect Context from recently merged events (if any)
        if should_run_synthesis:
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

        # --- PHASE 3: Exit if Nothing to Do ---
        if not summary_inputs and not has_external_update:
            # No new articles, no merges, nothing to update.
            if event.article_count > 0:
                job.status = JobStatus.PENDING
                job.queue_name = EventsQueueName.PUBLISHER
                return "NO_CHANGES_PUBLISH"
            else:
                job.status = JobStatus.FAILED
                job.msg = "No articles in event"
                return "FAILED_EMPTY"

        # --- PHASE 4: Synthesis (The Heavy Lift) ---
        outcome = "AGGREGATED_ONLY"
        
        if summary_inputs and should_run_synthesis:
            # Pass the previous summary to maintain context
            event_summary = await self.enhancer.summarize_event(summary_inputs, event.summary)
            
            if event_summary:
                event.title = event_summary.title or event.title
                event.subtitle = event_summary.subtitle
                event.summary = event_summary.summary
                event.last_summarized_at = datetime.now(timezone.utc)
                
                # Metadata updates
                event.ai_impact_score = event_summary.impact_score
                event.ai_impact_reasoning = event_summary.impact_reasoning
                event.category_tag = event_summary.primary_category.value if event_summary.primary_category else None
                event.is_international = event_summary.is_international
                event.publisher_isights = event_summary.secondary_topics
                
                # Status Transition
                if event.status not in (EventStatus.PUBLISHED, EventStatus.ARCHIVED):
                    event.status = EventStatus.ENHANCED
                
                session.add(event)
                outcome = "ENHANCED"
        elif is_debounced:
            outcome = "DEBOUNCED"

        # --- PHASE 5: Mark Articles Integrated ---
        # CRITICAL: We mark them APPROVED even if we Debounced the LLM.
        # This prevents the "Delta" from finding them again next loop.
        # Their stats (stance, bias) were already aggregated in Phase 1.
        for article in articles_to_mark_integrated:
            article.summary_status = JobStatus.APPROVED
            session.add(article)

        # --- PHASE 6: Transition Job ---
        job.status = JobStatus.PENDING
        job.queue_name = EventsQueueName.PUBLISHER
        job.updated_at = datetime.now(timezone.utc)
        job.msg = f"Enhancer: {outcome}"
        
        # Safe Flush
        session.flush()
        
        return outcome

    async def enhance_event_direct(self, session: Session, event: NewsEventModel) -> bool: 
        """
        Forces an immediate full refresh of an event. 
        Used by CLI or manual triggers.
        NO COMMIT.
        """
        # 1. Micro-Analysis of Raw Articles (if missing data)
        unprocessed = [a for a in event.articles if not a.key_points]
        if unprocessed:
            BATCH = 5
            for i in range(0, len(unprocessed), BATCH):
                batch = unprocessed[i:i+BATCH]
                inputs = [f"{a.title}\n{a.contents[0].content}" for a in batch if a.contents]
                if inputs:
                    try:
                        outputs = await self.enhancer.analyze_articles_batch(inputs)
                        for art, out in zip(batch, outputs):
                            if out.status == "valid":
                                self._update_article_from_llm(art, out)
                                session.add(art)
                    except Exception as e:
                        logger.error(f"Batch analysis failed: {e}")

        # 2. Re-Aggregate EVERYTHING
        # Reset counters
        event.article_count = 0
        event.editorial_score = 0.0
        event.interest_counts = {}
        event.main_topic_counts = {}
        event.bias_distribution = {}
        
        summary_inputs = []
        for art in event.articles:
            EventAggregator.aggregate_basic_stats(event, art) 
            EventAggregator.aggregate_interests(event, art.interests)
            EventAggregator.aggregate_main_topics(event, art.main_topics)
            EventAggregator.aggregate_metadata(event, art)
            EventAggregator.aggregate_bias_counts(event, art)
            EventAggregator.aggregate_source_snapshot(event, art)
            
            if art.key_points and art.newspaper:
                summary_inputs.append({
                    "bias": art.newspaper.bias,
                    "key_points": art.key_points
                })
        
        # 3. Full Synthesis
        if not summary_inputs:
            return False

        event_summary = await self.enhancer.summarize_event(summary_inputs, None) # None = Fresh start
        if event_summary:
            event.title = event_summary.title 
            event.subtitle = event_summary.subtitle
            event.summary = event_summary.summary
            event.ai_impact_score = event_summary.impact_score
            event.ai_impact_reasoning = event_summary.impact_reasoning
            event.category_tag = event_summary.primary_category.value
            event.is_international = event_summary.is_international
            event.last_summarized_at = datetime.now(timezone.utc)
            
            if event.status not in (EventStatus.PUBLISHED, EventStatus.ARCHIVED):
                event.status = EventStatus.ENHANCED
            
            session.add(event)
            session.flush()
            return True
        return False

    def _update_article_from_llm(self, article, result):
        article.main_topics = result.main_topics
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
        article.summar_date = datetime.now(timezone.utc)


    def _extract_summary_text(self, event):
        if event.summary and isinstance(event.summary, dict):
            return event.summary.get("center") or event.summary.get("bias")
        return None