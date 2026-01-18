from types import NoneType
import uuid
import zlib
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from dataclasses import dataclass

from loguru import logger
from sqlalchemy import delete, select, func, and_, desc, update, or_, text
from sqlalchemy.orm import Session, aliased, sessionmaker, joinedload
from sqlalchemy import create_engine
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_distances

# Shared Libraries
from news_events_lib.models import (
    NewsEventModel,
    ArticleModel,
    MergeProposalModel,
    JobStatus,
    EventStatus
)
from core.models import (
    ArticlesQueueModel,
    EventsQueueModel,
    EventsQueueName,
)
from config import Settings
from domain.aggregator import EventAggregator


@dataclass
class ClusterResult:
    action: str  # 'MERGE', 'PROPOSE', 'PROPOSE_MULTI', 'NEW', 'IGNORED', 'ERROR'
    event_id: Optional[uuid.UUID]
    candidates: List[dict]
    reason: str


class NewsCluster:
    def __init__(self):
        self.settings = Settings()
        # Engine is only needed if this class runs standalone logic, 
        # but here we rely on the passed 'session', so we don't strictly need self.engine 
        # unless for specific utility methods not shown here.
        self.SIMILARITY_STRICT = self.settings.similarity_strict
        self.SIMILARITY_LOOSE = self.settings.similarity_loose

    # --- UTILITIES ---

    def _get_advisory_lock(self, session: Session, key_str: str):
        """Acquires a Postgres Transaction-level Advisory Lock based on the string hash."""
        lock_id = zlib.crc32(key_str.encode('utf-8')) 
        # pg_advisory_xact_lock automatically releases at the end of the transaction
        session.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": lock_id})

    def derive_search_query(self, article: ArticleModel) -> str:
        parts = [article.title]
        if article.entities:
            parts.extend(article.entities[:5])
        return " ".join(parts)
    
    # --- CORE CLUSTERING LOGIC ---

    def cluster_existing_article(
        self, session: Session, article: ArticleModel
    ) -> ClusterResult:
        # 0. Age Check: Ignore articles older than 7 days
        if article.published_date:
            pub_date = article.published_date
            if pub_date.tzinfo is None:
                pub_date = pub_date.replace(tzinfo=timezone.utc)
            
            if pub_date < datetime.now(timezone.utc) - timedelta(days=7):
                return ClusterResult("IGNORED", None, [], "Article too old (> 7 days)")

        # 1. LOCKING PHASE
        self._get_advisory_lock(session, article.title)

        text_query = self.derive_search_query(article)
        vector = article.embedding

        if vector is None or len(vector) == 0:
            return ClusterResult("ERROR", None, [], "Missing Vector")

        target_dt = article.published_date or datetime.now(timezone.utc)
        candidates = self.search_news_events_hybrid(
            session, text_query, vector, target_dt
        )

        if not candidates:
            new_id = self._create_new_event(
                session, article, vector, reason="No Candidates Found"
            )
            return ClusterResult("NEW", new_id, [], "No Matches")

        best_ev, rrf_score, vec_dist = candidates[0]

        # Case A: Too Distant -> New Event
        if vec_dist > self.SIMILARITY_LOOSE:
            # Check if RRF gave a strong keyword match despite vector distance
            if rrf_score > 0.05:
                return ClusterResult(
                    "PROPOSE",
                    best_ev.id,
                    [{"title": best_ev.title, "score": vec_dist}],
                    f"Yellow Zone ({vec_dist:.3f}) - RRF Strong",
                )
            reason = f"Vector Dist {vec_dist:.2f} too high"
            new_id = self._create_new_event(session, article, vector, reason=reason)
            return ClusterResult("NEW", new_id, [], reason)

        # Case B: Ambiguous Zone -> Heuristics or Proposal
        if self.SIMILARITY_STRICT < vec_dist < self.SIMILARITY_LOOSE:
            # HEURISTIC: "The Hot Hand"
            is_fresh = False
            if best_ev.last_updated_at:
                last_upd = best_ev.last_updated_at
                if last_upd.tzinfo is None:
                    last_upd = last_upd.replace(tzinfo=timezone.utc)
                is_fresh = (datetime.now(timezone.utc) - last_upd).total_seconds() < 14400 # 4 hours
            
            is_big = best_ev.article_count > 10
            
            if is_fresh and is_big and vec_dist < 0.12:
                 self.link_article_to_event(session, best_ev, article, vector)
                 return ClusterResult("MERGE", best_ev.id, [], "Auto-Merge: Hot Topic Heuristic")

            self._create_proposal(session, article, best_ev, vec_dist)
            return ClusterResult(
                "PROPOSE",
                best_ev.id,
                [{"title": best_ev.title, "score": vec_dist}],
                f"Yellow Zone ({vec_dist:.3f})",
            )

        # Case C: Multiple Strong Candidates -> Ambiguity
        if len(candidates) > 1:
            second_ev, _, second_dist = candidates[1]
            if (second_dist - vec_dist) < 0.05:
                options = []
                for ev, _, dist in candidates[:3]:
                    if dist < self.SIMILARITY_LOOSE:
                        self._create_proposal(
                            session, article, ev, dist, ambiguous=True
                        )
                        options.append({"title": ev.title, "score": dist})
                return ClusterResult("PROPOSE_MULTI", None, options, "Ambiguous Match")

        # Case D: Perfect Match -> Merge
        self.link_article_to_event(session, best_ev, article, vector)
        return ClusterResult("MERGE", best_ev.id, [], "Perfect Match")

    # --- SEARCH LOGIC ---

    def search_news_events_hybrid(
        self,
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

        date_ref = func.coalesce(NewsEventModel.last_updated_at, NewsEventModel.created_at)
        hours_diff = func.abs(func.extract('epoch', target_date - date_ref)) / 3600.0
        decay_factor = 1.0 / (1.0 + (decay_rate * hours_diff))

        score_expression = (
            func.coalesce(1.0 / (rrf_k + sem_alias.c.rank), 0.0) + 
            func.coalesce(1.0 / (rrf_k + kw_alias.c.rank), 0.0)
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

    # --- ACTIONS (NO COMMIT) ---

    def link_article_to_event(
        self,
        session: Session,
        event: Optional[NewsEventModel],
        article: Optional[ArticleModel],
        vector: Optional[list[float]],
    ):
        if event is None or article is None or vector is None:
            return None

        # Lock explicitly
        session.execute(
            select(NewsEventModel.id)
            .where(NewsEventModel.id == event.id)
            .with_for_update()
        )
        session.refresh(event)

        # 1. Aggregations (Delegated to Aggregator)
        EventAggregator.aggregate_basic_stats(event, article)
        EventAggregator.update_centroid(event, vector)
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
        return event.id

    def execute_new_event_action(self, session: Session, article: ArticleModel, reason: str = "") -> uuid.UUID:
        """
        Public wrapper for creating a new event from an article.
        Used by Gardner/Splitter.
        """
        return self._create_new_event(session, article, article.embedding, reason)

    def _create_new_event(self, session: Session, article, vector, reason=""):
        # 1. Prepare Initial Aggregations
        init_interests = {}
        init_bias = {}
        init_counts = {}
        init_ownership = {}
        init_main_topics = {}
        init_snapshot = {}
        
        if article.newspaper:
            init_snapshot[article.newspaper.name] = {
                "icon": article.newspaper.icon_url,
                "name": article.newspaper.name,
                "id": str(article.newspaper.id),
                "logo": article.newspaper.logo_url,
                "bias": article.newspaper.bias
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
            embedding_centroid=vector,
            article_count=1,
            is_active=True,
            created_at=article.published_date,
            last_updated_at=datetime.now(timezone.utc),
            search_text=f"{article.title} {self.derive_search_query(article)}",
            
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
            last_article_date=article.published_date
        )
        session.add(new_event)
        
        # We flush to get the ID, but do NOT commit.
        session.flush()

        # 3. Link Article
        article = session.merge(article)
        article.event_id = new_event.id
        session.add(article)

        logger.info(f"🆕 New Event created: {new_event.title[:20]} | Reason: {reason}")
        return new_event.id

    def _create_proposal(self, session, article, event, score, ambiguous=False):
        exists = session.execute(
            select(MergeProposalModel).where(
                and_(
                    MergeProposalModel.source_article_id == article.id,
                    MergeProposalModel.target_event_id == event.id,
                )
            )
        ).scalar()

        if not exists:
            reason = f"RRF Match. Vector Dist {score:.3f}"
            if ambiguous:
                reason += " (AMBIGUOUS)"

            prop = MergeProposalModel(
                id=uuid.uuid4(),
                source_article_id=article.id,
                target_event_id=event.id,
                distance_score=float(score),
                status=JobStatus.PENDING,
                reasoning=reason,
            )
            session.add(prop)
            logger.info(
                f"⚠️ Proposal created: {article.title[:20]} -> {event.title[:20]} ({reason})"
            )
    
    # --- MAINTENANCE (Event Merging / Sub-Clustering) ---
    
    def execute_event_merge(
        self,
        session: Session,
        source_event: NewsEventModel,
        target_event: NewsEventModel,
    ):
        """
        Merges Source Event -> Target Event. 
        Updates DB state but does NOT commit.
        """
        logger.info(f"🧬 MERGING: {source_event.title} -> {target_event.title}")

        # 1. Re-link Articles
        articles = session.scalars(
            select(ArticleModel).where(ArticleModel.event_id == source_event.id)
        ).all()
        for art in articles:
            art.event_id = target_event.id
            session.add(art)
        
        # 2. Merge Stats
        EventAggregator.merge_event_stats(target_event, source_event)

        # 3. Tombstone Source
        source_event.is_active = False
        source_event.status = EventStatus.MERGED
        source_event.merged_into_id = target_event.id
        source_event.last_updated_at = datetime.now(timezone.utc)
        session.add(source_event)

        # 4. Cleanup Proposals
        session.execute(
            update(MergeProposalModel)
            .where(or_(
                MergeProposalModel.target_event_id == source_event.id,
                MergeProposalModel.source_event_id == source_event.id
            ))
            .values(status=JobStatus.REJECTED, reasoning="Event merged.", updated_at=datetime.now(timezone.utc))
        )

        # 5. Cleanup Queue
        session.execute(
            delete(EventsQueueModel).where(EventsQueueModel.event_id == source_event.id)
        )

        target_event.last_updated_at = datetime.now(timezone.utc)
        session.add(target_event)
        
        # Return survivor ID so worker can queue it
        return target_event.id