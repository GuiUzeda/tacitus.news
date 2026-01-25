from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Optional

# Core/Services
from app.core.llm_parser import CloudNewsAnalyzer, EventSummaryInput
from loguru import logger

# Models
from news_events_lib.models import (
    ArticleModel,
    EventsQueueModel,
    EventStatus,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import and_, exists, select
from sqlalchemy.orm import Session, joinedload


class EnhancerAction(str, Enum):
    ENHANCED = "ENHANCED"
    DEBOUNCED = "DEBOUNCED"
    SKIPPED_EMPTY = "SKIPPED_EMPTY"
    SKIPPED_INACTIVE = "SKIPPED_INACTIVE"
    ERROR = "ERROR"


@dataclass
class EnhancerResult:
    action: EnhancerAction
    event: Optional[NewsEventModel] = None
    articles_processed: int = 0
    events_processed: int = 0
    msg: str = ""


class NewsEnhancerDomain:
    def __init__(self, concurrency_limit: int = 5):
        self.enhancer = CloudNewsAnalyzer()
        self.DEBOUNCE_MINUTES = 15
        self.BATCH_SIZE = 5

    async def enhance_merged(
        self, target: NewsEventModel, sources: List[NewsEventModel]
    ) -> tuple[EventSummaryInput, int]:
        """
        Phase 0: Structurally merges historical events into the target's baseline.
        Returns a Pydantic Object (not a dict) to prevent AttributeErrors later.
        """

        processed_count = 0
        # Initialize baseline from target
        current_state = EventSummaryInput(
            title=target.title,
            subtitle=target.subtitle or "New Event",
            summary=target.summary or {"bias": "New Event"},
            impact_score=target.ai_impact_score or 0,
            impact_reasoning=target.ai_impact_reasoning or "New Event",
        )

        if not sources:
            return current_state, processed_count

        batches = range(0, len(sources), self.BATCH_SIZE)
        logger.info(f"🔗 Merging {len(sources)} events into '{target.title[:20]}'...")

        for i in batches:
            batch = sources[i : i + self.BATCH_SIZE]
            batch_inputs = []

            for m_ev in batch:
                batch_inputs.append(
                    EventSummaryInput(
                        title=m_ev.title,
                        subtitle=m_ev.subtitle or "",
                        summary=m_ev.summary or {},
                        impact_score=m_ev.ai_impact_score or 0,
                        impact_reasoning=m_ev.ai_impact_reasoning or "",
                    )
                )
            if not batch_inputs:

                continue
            processed_count += len(batch)
            try:
                # FIX 1: Ensure we get an Object back, not a dict.
                # (You must update llm_parser.py to return result, NOT result.model_dump())
                merged_result = await self.enhancer.merge_event_summaries(
                    target=current_state, sources=batch_inputs
                )

                if merged_result:
                    # If merged_result is EventSummaryOutput, it's compatible with Input
                    current_state = merged_result

            except Exception as e:
                logger.error(f"Merge Summaries Failed: {e}")
                raise e

        return current_state, processed_count

    async def enhance_event(
        self, session: Session, job: EventsQueueModel
    ) -> EnhancerResult:
        """
        Refines the Event Summary by iteratively processing new 'Delta' articles.
        """
        event: NewsEventModel | None = job.event

        # 1. Validation
        if not event:
            return EnhancerResult(EnhancerAction.ERROR, msg="Event Not Found")
        if not event.is_active:
            return EnhancerResult(
                EnhancerAction.SKIPPED_INACTIVE, event, msg="Event Inactive"
            )

        # 2. Check Debounce
        last_sum = event.last_summarized_at or datetime.min
        if last_sum.tzinfo is None:
            last_sum = last_sum.replace(tzinfo=timezone.utc)

        # 3. Fetch Context (Merges)
        merged_events_db = session.scalars(
            select(NewsEventModel).where(
                NewsEventModel.merged_into_id == event.id,
                NewsEventModel.last_updated_at > last_sum,
            )
        ).all()

        # 4. Fetch Delta (New Articles)
        active_proposals = select(1).where(
            and_(
                MergeProposalModel.source_article_id == ArticleModel.id,
                MergeProposalModel.status.in_(
                    [JobStatus.PENDING, JobStatus.PROCESSING]
                ),
            )
        )

        stmt = (
            select(ArticleModel)
            .filter(ArticleModel.event_id == event.id)
            .filter(ArticleModel.summary_status == JobStatus.COMPLETED)
            .options(joinedload(ArticleModel.newspaper))
            .filter(~exists(active_proposals))
            .order_by(ArticleModel.published_date.asc())
        )
        delta_articles = session.scalars(stmt).unique().all()

        # Debounce Logic
        is_forced = "FORCE" in (job.msg or "")
        is_backlogged = len(delta_articles) > 10
        now = datetime.now(timezone.utc)
        is_too_soon = (now - last_sum) < timedelta(minutes=self.DEBOUNCE_MINUTES)

        if is_too_soon and not is_forced and not is_backlogged:
            return EnhancerResult(
                EnhancerAction.DEBOUNCED, event, msg=f"Debounced (Last: {last_sum})"
            )

        if not delta_articles and not merged_events_db and not is_forced:
            return EnhancerResult(
                EnhancerAction.SKIPPED_EMPTY, event, msg="No new articles/merges"
            )

        # --- PROCESS START ---

        # PHASE 0: Structural Merge (Baseline)
        events_processed = 0
        try:
            current_state, events_processed = await self.enhance_merged(
                event, list(merged_events_db)
            )
        except Exception as e:
            return EnhancerResult(
                EnhancerAction.ERROR, event, msg=f"Merge Phase Failed: {e}"
            )

        # Track fields not present in EventSummaryInput base
        current_primary_category = event.category_tag
        current_is_international = event.is_international

        # PHASE 1: Iterative Refinement
        processed_count = 0
        batches = range(0, len(delta_articles), self.BATCH_SIZE)

        if len(batches) > 0:
            logger.info(
                f"🔄 Refining Event '{event.title[:20]}' in {len(batches)} batches"
            )

        for x, i in enumerate(batches):
            batch = delta_articles[i : i + self.BATCH_SIZE]
            batch_inputs = []

            for article in batch:
                if article.key_points:
                    batch_inputs.append(
                        {
                            "bias": article.newspaper.bias,
                            "key_points": article.key_points,
                            "source": article.newspaper.name,
                        }
                    )

            # Skip empty batches (e.g. articles without key_points)
            if not batch_inputs:
                processed_count += len(batch)
                continue

            # Only ask for metadata on the first valid pass to save tokens
            get_category = x == 0 and event.articles_at_last_summary == 0
            get_international = x == 0 and event.articles_at_last_summary == 0

            try:
                llm_response = await self.enhancer.summarize_event(
                    batch_inputs,
                    current_state,
                    get_category,
                    get_international,
                )

                if llm_response:
                    # Update state
                    current_state.summary = (
                        llm_response.summary or current_state.summary
                    )
                    current_state.title = llm_response.title or current_state.title
                    current_state.subtitle = (
                        llm_response.subtitle or current_state.subtitle
                    )

                    if llm_response.impact_score:
                        current_state.impact_score = llm_response.impact_score
                    if llm_response.impact_reasoning:
                        current_state.impact_reasoning = llm_response.impact_reasoning

                    # Update optional metadata
                    if llm_response.primary_category:
                        # FIX 2: Use .value for Enums
                        current_primary_category = llm_response.primary_category.value
                    if llm_response.is_international is not None:
                        current_is_international = llm_response.is_international

                    # Success: Mark this batch as processed
                    processed_count += len(batch)

            except Exception as e:
                logger.opt(exception=True).error(f"LLM Refine Failed on Batch {x}: {e}")
                # FIX 3: Break loop on error to avoid marking failed articles as Approved
                break

        # 6. Save State to DB
        event.title = current_state.title
        event.subtitle = current_state.subtitle
        event.summary = current_state.summary
        event.ai_impact_score = current_state.impact_score
        event.ai_impact_reasoning = current_state.impact_reasoning

        if current_primary_category:
            event.category_tag = current_primary_category
        if current_is_international is not None:
            event.is_international = current_is_international

        event.last_summarized_at = datetime.now(timezone.utc)
        event.articles_at_last_summary += processed_count

        if event.status not in (EventStatus.PUBLISHED, EventStatus.ARCHIVED):
            event.status = EventStatus.ENHANCED

        # 7. Drain Inbox (Only for successfully processed articles)
        # FIX 3: Slice the list using processed_count
        for article in delta_articles[:processed_count]:
            article.summary_status = JobStatus.APPROVED
            session.add(article)

        return EnhancerResult(
            EnhancerAction.ENHANCED,
            event,
            articles_processed=processed_count,
            events_processed=events_processed,
            msg=f"Refinement Complete ({processed_count}/{len(delta_articles)} arts) ({events_processed}/{len(merged_events_db)} merges)",
        )

    def _extract_summary_text(self, event):
        """Helper to get clean text from the summary JSON."""
        if not event.summary:
            return None
        if isinstance(event.summary, dict):
            return (
                event.summary.get("center")
                or event.summary.get("bias")
                or str(event.summary)
            )
        return str(event.summary)
