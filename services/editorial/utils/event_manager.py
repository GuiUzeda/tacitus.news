from datetime import datetime, timedelta, timezone
import stat
from types import NoneType
from typing import List, Optional
import uuid
from loguru import logger
from news_events_lib.models import (
    EventStatus,
    JobStatus,
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    EventsQueueModel,
    EventsQueueName,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import and_, delete, desc, func, or_, select, update
from sqlalchemy.orm import Session, aliased
from wasabi import msg

from services.editorial.utils.event_aggregator import EventAggregator


class EventManager:
    @staticmethod
    def derive_search_query(article: ArticleModel) -> str:
        parts = [article.title]
        if article.entities:
            parts.extend(article.entities[:5])
        return " ".join(parts)

    @staticmethod
    def execute_new_event_action(
        session: Session, article: ArticleModel, commit: bool = False
    ) -> NewsEventModel:
        # 1. Prepare Initial Aggregations
        init_interests = {}
        init_bias = {}
        init_counts = {}
        init_ownership = {}
        init_main_topics = {}
        init_snapshot = {}
        if type(article.embedding) == NoneType or sum(article.embedding) == 0:
            raise Exception("To create an event article must have embedding")

        if article.newspaper:
            init_snapshot[article.newspaper.name] = {
                "icon": article.newspaper.icon_url,
                "name": article.newspaper.name,
                "id": str(article.newspaper.id),
                "logo": article.newspaper.logo_url,
                "bias": article.newspaper.bias,
            }
            if article.newspaper.bias:
                init_bias[article.newspaper.bias] = [str(article.newspaper.id)]
                init_counts[article.newspaper.bias] = 1

            if article.newspaper.ownership_type:
                init_ownership[article.newspaper.ownership_type] = 1

        if article.interests:
            for category, items in article.interests.items():
                init_interests[category] = {}
                for item in items:
                    init_interests[category][item] = 1

        if article.main_topics:
            for topic in article.main_topics:
                init_main_topics[topic] = 1

        # 2. Create Event
        init_score = 0.0
        init_rank = None
        if article.source_rank:
            init_rank = article.source_rank
            init_score = 10.0 / float(article.source_rank)

        new_event = NewsEventModel(
            id=uuid.uuid4(),
            title=article.title,
            embedding_centroid=article.embedding,
            article_count=1,
            is_active=True,
            created_at=article.published_date,
            last_updated_at=datetime.now(timezone.utc),
            search_text=f"{article.title} {EventManager.derive_search_query(article)}",
            # Aggregations
            interest_counts=init_interests,
            main_topic_counts=init_main_topics,
            article_counts_by_bias=init_counts,
            bias_distribution=init_bias,
            ownership_stats=init_ownership,
            sources_snapshot=init_snapshot,
            # Rank & Score
            best_source_rank=init_rank,
            editorial_score=init_score,
            # Date Range
            first_article_date=article.published_date,
            last_article_date=article.published_date,
        )
        session.add(new_event)
        if commit:

            session.commit()

        logger.info(
            f"🆕 New Event created: {new_event.title[:20]} {'and commited' if commit else ''} "
        )
        return new_event

    @staticmethod
    def execute_event_merge(
        session: Session,
        source_event: NewsEventModel,
        target_event: NewsEventModel,
        reason: str = "",
        commit: bool = False,
    ):
        """
        Merges Source Event -> Target Event.
        Updates DB state but does NOT commit.
        """
        logger.info(f"MERGING: {source_event.title} -> {target_event.title}")
        now = datetime.now(timezone.utc)

        # 1. Re-link Articles
        articles = session.scalars(
            select(ArticleModel).where(ArticleModel.event_id == source_event.id)
        ).all()
        for art in articles:
            art.event_id = target_event.id
            art.updated_at = now
            if art.summary_status == JobStatus.APPROVED:
                # we have to signal the enhancer that this article has to be reviewed
                art.summary_status = JobStatus.COMPLETED

            session.add(art)

        # 2. Merge Stats
        EventAggregator.merge_event_stats(target_event, source_event)

        # 3. Tombstone Source
        source_event.is_active = False
        source_event.status = EventStatus.MERGED
        source_event.merged_into_id = target_event.id
        source_event.last_updated_at = now
        session.add(source_event)

        # 4. Cleanup Proposals
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
                reasoning="Event merged: " + reason,
                updated_at=now,
            )
        )

        # 5. Cleanup Queue
        session.execute(
            update(EventsQueueModel)
            .where(EventsQueueModel.event_id == source_event.id)
            .values(
                status=JobStatus.REJECTED,
                msg="Event merged: " + reason,
                updated_at=now,
            )
        )

        target_event.last_updated_at = now
        session.add(target_event)

        if commit:
            session.commit()

        # Return survivor ID so worker can queue it
        return target_event

    @staticmethod
    def link_article_to_event(
        session: Session,
        event: NewsEventModel,
        article: ArticleModel,
        commit: bool = False,
    ) -> NewsEventModel:
        if type(article.embedding) == NoneType or sum(article.embedding) == 0:
            raise Exception("To create an event article must have embedding")

        # Lock explicitly
        session.execute(
            select(NewsEventModel.id)
            .where(NewsEventModel.id == event.id)
            .with_for_update()
        )
        session.refresh(event)

        # 1. Aggregations (Delegated to Aggregator)
        EventAggregator.aggregate_basic_stats(event, article)
        EventAggregator.update_centroid(event, article.embedding)
        EventAggregator.aggregate_interests(event, article.interests)
        EventAggregator.aggregate_main_topics(event, article.main_topics)
        EventAggregator.aggregate_metadata(event, article)
        EventAggregator.aggregate_bias_counts(event, article)
        EventAggregator.aggregate_source_snapshot(event, article)

        # 2. Search Text Optimization
        current_text = event.search_text or ""
        new_keywords = f"{current_text} {article.title}".split()
        unique_words = list(dict.fromkeys(reversed(new_keywords)))[:50]
        event.search_text = " ".join(reversed(unique_words))

        event.last_updated_at = datetime.now(timezone.utc)

        # 3. Link
        article.event_id = event.id
        event.articles.append(article)

        session.add(event)
        session.add(article)
        if commit:
            session.commit()

        return event

  

    @staticmethod
    def create_event_queue(
        session,
        event_id: uuid.UUID,
        queue_name: EventsQueueName,
        reason: str = "",
        commit: bool = False,
    ) -> EventsQueueModel:
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
            .where(NewsEventModel.is_active == True)
            .order_by(NewsEventModel.embedding_centroid.cosine_distance(query_vector))
            .limit(50)
        )

        if target_date and diff_date:
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
                    NewsEventModel.is_active == True,
                    NewsEventModel.search_vector_ts.op("@@")(ts_query),
                )
            )
            .limit(50)
        )

        if target_date and diff_date:
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
            .where(
                and_(
                    NewsEventModel.created_at >= target_date - diff_date,
                    NewsEventModel.created_at <= target_date,
                )
            )
            .limit(limit)
        )

        return session.execute(stmt).all()