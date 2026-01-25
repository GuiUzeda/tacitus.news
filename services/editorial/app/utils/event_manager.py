import uuid
from datetime import datetime, timedelta, timezone
from typing import List

from app.utils.event_aggregator import EventAggregator  # <--- Your new file
from loguru import logger

# Project Imports
from news_events_lib.models import (
    ArticleModel,
    EventsQueueModel,
    EventsQueueName,
    EventStatus,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import and_, desc, func, or_, select, update
from sqlalchemy.orm import Session, aliased


class EventManager:

    # --- 1. WRITE OPERATIONS (Using Aggregator) ---

    @staticmethod
    def execute_new_event_action(
        session: Session, article: ArticleModel, commit: bool = False
    ) -> NewsEventModel:
        """Creates a fresh event and initializes stats from the first article."""

        if article.embedding is None or len(article.embedding) == 0:
            raise Exception("Cannot create event: Article missing embedding")

        # Create Skeleton
        new_event = NewsEventModel(
            id=uuid.uuid4(),
            title=article.title,
            # Initialize with first article's data
            embedding_centroid=article.embedding,
            article_count=0,  # Will be incremented by aggregator
            is_active=True,
            created_at=article.published_date or datetime.now(timezone.utc),
            last_updated_at=datetime.now(timezone.utc),
            search_text=EventManager.derive_search_query(article),
        )

        # Link & Aggregate
        # We reuse the logic in link_article_to_event to avoid duplicating aggregation code
        # However, since the event isn't in DB yet, we manually attach it first.
        session.add(new_event)

        # We call the aggregator helpers directly to initialize the state
        article.event_id = new_event.id

        EventAggregator.aggregate_basic_stats(new_event, article)
        # Note: Centroid is already set to article.embedding, so we skip update_centroid for the first one to avoid /1 math noise
        EventAggregator.aggregate_interests(new_event, article.interests)
        EventAggregator.aggregate_main_topics(new_event, article.main_topics)
        EventAggregator.aggregate_metadata(new_event, article)
        EventAggregator.aggregate_bias_counts(new_event, article)
        EventAggregator.aggregate_source_snapshot(new_event, article)

        session.add(article)

        logger.info(f"🆕 New Event created: {new_event.title[:30]}")

        if commit:
            session.commit()

        return new_event

    @staticmethod
    def recalculate_event_metrics(
        session: Session, event: NewsEventModel, commit: bool = False
    ):

        # set to default all event metrics
        event.article_count = 0
        event.first_article_date = None
        event.last_article_date = None
        event.articles_at_last_summary = 0
        event.interest_counts = {}
        event.main_topic_counts = {}
        event.sources_snapshot = {}
        event.stance = 0.0
        event.stance_distribution = {}
        event.clickbait_distribution = {}
        event.embedding_centroid = [0.0] * 768

        # Re-aggregate from all linked articles
        articles = session.scalars(
            select(ArticleModel).where(ArticleModel.event_id == event.id)
        ).all()

        if not articles:
            # If no articles, the event might be empty or invalid, consider deactivating
            event.is_active = False
            logger.warning(
                f"Event {event.id} has no articles after recalculation. Deactivating."
            )
            if commit:
                session.rollback()
            return

        # Use the first article to re-initialize some fields
        first_article = min(articles, key=lambda a: a.published_date or datetime.min)
        event.title = first_article.title
        event.created_at = first_article.published_date or datetime.now(timezone.utc)
        event.embedding_centroid = [0.0] * 768

        # Aggregate each article
        for article in articles:
            EventAggregator.aggregate_basic_stats(event, article)
            EventAggregator.update_centroid(
                event,
                article.embedding if article.embedding is not None else [0.0] * 768,
            )
            EventAggregator.aggregate_interests(event, article.interests)
            EventAggregator.aggregate_main_topics(event, article.main_topics)
            EventAggregator.aggregate_metadata(event, article)
            EventAggregator.aggregate_bias_counts(event, article)
            EventAggregator.aggregate_source_snapshot(event, article)

            # Add Stance and Clickbait if they exist (enhanced articles)
            if article.newspaper and article.newspaper.bias:
                if article.stance is not None:
                    EventAggregator.aggregate_stance(
                        event, article.newspaper.bias, article.stance
                    )
                if article.clickbait_score is not None:
                    EventAggregator.aggregate_clickbait(
                        event, article.newspaper.bias, article.clickbait_score
                    )

            article.summary_status = JobStatus.COMPLETED

            # Text Optimization: Rebuild cumulative search text
            current_text = event.search_text or ""
            # Use the rich derived query (Title + Topics + Entities)
            new_tokens = EventManager.derive_search_query(article).split()
            all_tokens = f"{current_text} ".split() + new_tokens

            # Keep last 100 unique words (reversed to keep newest first and implicitly prioritize recent topics)
            unique_words = list(dict.fromkeys(reversed(all_tokens)))[:100]
            event.search_text = " ".join(reversed(unique_words))

        event.last_updated_at = datetime.now(timezone.utc)
        session.add(event)

        if commit:
            session.commit()

        return event

    @staticmethod
    def link_article_to_event(
        session: Session,
        event: NewsEventModel,
        article: ArticleModel,
        commit: bool = False,
    ) -> NewsEventModel:
        """Links article and runs incremental aggregation."""

        if article.embedding is None:
            raise Exception("Article missing embedding")

        # Explicit Lock to prevent race conditions during math updates
        # Only needed if this transaction is long-lived, but good safety.
        # Note: For strict locking, the caller usually handles 'with_for_update' on fetch.

        # 1. Run Aggregations (The Math)
        EventAggregator.aggregate_basic_stats(event, article)
        EventAggregator.update_centroid(event, article.embedding)
        EventAggregator.aggregate_interests(event, article.interests)
        EventAggregator.aggregate_main_topics(event, article.main_topics)
        EventAggregator.aggregate_metadata(event, article)
        EventAggregator.aggregate_bias_counts(event, article)
        EventAggregator.aggregate_source_snapshot(event, article)

        # Link Stance/Clickbait if available
        if article.newspaper and article.newspaper.bias:
            if article.stance is not None:
                EventAggregator.aggregate_stance(
                    event, article.newspaper.bias, article.stance
                )
            if article.clickbait_score is not None:
                EventAggregator.aggregate_clickbait(
                    event, article.newspaper.bias, article.clickbait_score
                )

        # 2. Text Optimization (Search)
        # Append new rich tokens (Title + Topics + Entities) to search_text
        current_text = event.search_text or ""
        new_tokens = EventManager.derive_search_query(article).split()

        # Combine and Deduplicate
        all_tokens = f"{current_text} ".split() + new_tokens

        # Keep last 100 unique words (reversed to keep newest first)
        unique_words = list(dict.fromkeys(reversed(all_tokens)))[:100]
        event.search_text = " ".join(reversed(unique_words))

        event.last_updated_at = datetime.now(timezone.utc)

        # 3. Database Link
        article.event_id = event.id
        session.add(event)
        session.add(article)

        if commit:
            session.commit()

        return event

    @staticmethod
    def execute_event_merge(
        session: Session,
        source_event: NewsEventModel,
        target_event: NewsEventModel,
        reason: str = "",
        commit: bool = False,
    ):
        """Merges Source -> Target and aggregates their stats."""
        logger.info(f"MERGING: {source_event.title} -> {target_event.title}")
        now = datetime.now(timezone.utc)

        # 1. Re-link Articles
        articles = session.scalars(
            select(ArticleModel).where(ArticleModel.event_id == source_event.id)
        ).all()

        for art in articles:
            art.event_id = target_event.id
            art.updated_at = now
            session.add(art)

        # 2. Merge Stats (CRITICAL STEP)
        EventAggregator.merge_event_stats(target_event, source_event)

        # 3. Tombstone Source
        source_event.is_active = False
        source_event.status = EventStatus.MERGED
        source_event.merged_into_id = target_event.id
        source_event.last_updated_at = now

        # 4. Cleanup Proposals (Reject siblings)
        session.execute(
            update(MergeProposalModel)
            .where(
                or_(
                    MergeProposalModel.target_event_id == source_event.id,
                    MergeProposalModel.source_event_id == source_event.id,
                )
            )
            .values(
                status=JobStatus.REJECTED,
                reasoning=f"Event merged: {reason}",
                updated_at=now,
            )
        )

        # 5. Cleanup Queue
        session.execute(
            update(EventsQueueModel)
            .where(EventsQueueModel.event_id == source_event.id)
            .values(
                status=JobStatus.REJECTED,
                msg=f"Event merged: {reason}",
                updated_at=now,
            )
        )

        target_event.last_updated_at = now
        session.add(target_event)
        session.add(source_event)

        if commit:
            session.commit()

        return target_event

    # --- 2. HELPERS & SEARCH ---

    @staticmethod
    def derive_search_query(article: ArticleModel) -> str:
        """
        Generates a rich keyword string for search/indexing.
        Includes Title + Main Topics + Top Entities.
        """
        parts = [article.title]

        # Add Main Topics (High signal for categorization)
        if article.main_topics:
            parts.extend(article.main_topics)

        # Add Top Entities (High signal for specificity)
        if article.entities:
            parts.extend(article.entities[:5])

        return " ".join(parts)

    @staticmethod
    def create_event_queue(
        session,
        event_id: uuid.UUID,
        queue_name: EventsQueueName,
        reason: str = "",
        commit: bool = False,
    ) -> EventsQueueModel:
        # Idempotency check handled by caller or DB constraints typically,
        # but safe to just add here if not checking.
        now = datetime.now(timezone.utc)
        event_queue_entry = EventsQueueModel(
            event_id=event_id,
            queue_name=queue_name,
            status=JobStatus.PENDING,
            created_at=now,
            updated_at=now,
            msg=reason,
        )
        session.add(event_queue_entry)
        if commit:
            session.commit()
        return event_queue_entry

    @staticmethod
    def create_merge_proposal(
        session: Session,
        source: NewsEventModel,
        target: NewsEventModel,
        score: float,
        ambiguous: bool = False,
        reason: str = "",
        status=JobStatus.PENDING,
        commit: bool = False,
    ):
        if EventManager.merge_proposal_exists(session, source.id, target.id):
            return None

        final_reason = reason + (" (AMBIGUOUS)" if ambiguous else "")

        prop = MergeProposalModel(
            id=uuid.uuid4(),
            proposal_type="event_merge",
            source_event_id=source.id,
            target_event_id=target.id,
            distance_score=float(score),
            status=status,
            reasoning=final_reason,
        )
        session.add(prop)
        logger.info(f"⚠️ Proposal created: {source.title[:20]} -> {target.title[:20]}")

        if commit:
            session.commit()
        return prop

    @staticmethod
    def merge_proposal_exists(session, id_a, id_b):
        return session.scalar(
            select(1).where(
                or_(
                    and_(
                        MergeProposalModel.source_event_id == id_a,
                        MergeProposalModel.target_event_id == id_b,
                    ),
                    and_(
                        MergeProposalModel.source_event_id == id_b,
                        MergeProposalModel.target_event_id == id_a,
                    ),
                )
            )
        )

    @staticmethod
    def search_news_events_hybrid(
        session: Session,
        query_text: str,
        query_vector: List[float],
        target_date: datetime,
        diff_date: timedelta = timedelta(days=5),
        limit: int = 10,
        rrf_k: int = 60,
        decay_rate: float = 0.05,
    ):
        """
        Performs Hybrid Search (Vector + Keyword) with Reciprocal Rank Fusion (RRF).
        Includes Time Decay to prefer newer events.
        """
        # 1. Semantic Search CTE
        semantic_subq = (
            select(
                NewsEventModel.id,
                NewsEventModel.embedding_centroid.cosine_distance(query_vector).label(
                    "dist"
                ),
                func.row_number()
                .over(
                    order_by=NewsEventModel.embedding_centroid.cosine_distance(
                        query_vector
                    )
                )
                .label("rank"),
            )
            .where(NewsEventModel.is_active.is_(True))
            .order_by(NewsEventModel.embedding_centroid.cosine_distance(query_vector))
            .limit(50)
        )

        if target_date:
            semantic_subq = semantic_subq.where(
                and_(
                    NewsEventModel.created_at >= target_date - diff_date,
                    NewsEventModel.created_at <= target_date,
                )
            )
        semantic_subq = semantic_subq.cte("semantic_results")

        # 2. Keyword Search CTE
        ts_query = func.websearch_to_tsquery("portuguese", query_text)

        keyword_subq = (
            select(
                NewsEventModel.id,
                func.row_number()
                .over(
                    order_by=desc(
                        func.ts_rank_cd(NewsEventModel.search_vector_ts, ts_query)
                    )
                )
                .label("rank"),
            )
            .where(
                and_(
                    NewsEventModel.is_active.is_(True),
                    NewsEventModel.search_vector_ts.op("@@")(ts_query),
                )
            )
            .limit(50)
        )

        if target_date:
            keyword_subq = keyword_subq.where(
                and_(
                    NewsEventModel.created_at >= target_date - diff_date,
                    NewsEventModel.created_at <= target_date,
                )
            )
        keyword_subq = keyword_subq.cte("keyword_results")

        # 3. RRF Calculation
        sem_alias = aliased(semantic_subq, name="sem")
        kw_alias = aliased(keyword_subq, name="kw")

        # Time Decay Factor
        date_ref = func.coalesce(
            NewsEventModel.last_updated_at, NewsEventModel.created_at
        )
        hours_diff = func.abs(func.extract("epoch", target_date - date_ref)) / 3600.0
        decay_factor = 1.0 / (1.0 + (decay_rate * hours_diff))

        score_expression = (
            func.coalesce(1.0 / (rrf_k + sem_alias.c.rank), 0.0)
            + func.coalesce(1.0 / (rrf_k + kw_alias.c.rank), 0.0)
        ) * decay_factor

        distance_expression = func.coalesce(sem_alias.c.dist, 1.0)

        stmt = (
            select(
                NewsEventModel,
                score_expression.label("rrf_score"),
                distance_expression.label("vector_dist"),
            )
            .join_from(sem_alias, kw_alias, sem_alias.c.id == kw_alias.c.id, full=True)
            .join(
                NewsEventModel,
                NewsEventModel.id == func.coalesce(sem_alias.c.id, kw_alias.c.id),
            )
            .order_by(desc("rrf_score"))
            .where(NewsEventModel.is_active.is_(True))
            .limit(limit)
        )

        return session.execute(stmt).all()
