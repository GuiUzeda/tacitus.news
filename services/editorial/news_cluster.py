import asyncio
import uuid
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from loguru import logger
from sqlalchemy import select, func, and_, desc, update, or_
from sqlalchemy.orm import Session, aliased, sessionmaker, joinedload
from sqlalchemy import create_engine
from dataclasses import dataclass

# Models
from news_events_lib.models import (
    NewsEventModel,
    ArticleModel,
    MergeProposalModel,
    JobStatus,
)
from models import (
    ArticlesQueueModel,
    ArticlesQueueName,
    EventsQueueModel,
    EventsQueueName,
)
from config import Settings


from base_worker import BaseQueueWorker
from event_aggregator import EventAggregator

@dataclass
class ClusterResult:
    action: str  # 'MERGE', 'PROPOSE', 'PROPOSE_MULTI', 'NEW'
    event_id: Optional[uuid.UUID]
    candidates: List[dict]
    reason: str


class NewsCluster:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        self.SIMILARITY_STRICT = self.settings.similarity_strict
        self.SIMILARITY_LOOSE = self.settings.similarity_loose

    # --- STANDARD ACTIONS (Used by CLI & Worker) ---

    def execute_merge_action(
        self, session: Session, article: ArticleModel, event: NewsEventModel
    ):
        """
        Standardizes the 'Merge' operation:
        1. Links article to event (updating centroid).
        2. Rejects conflicting proposals for this article.
        3. Updates ArticlesQueue -> COMPLETED.
        4. triggers EventsQueue -> ENHANCER.
        """
        # 1. Link
        self._link_to_event(session, event, article, article.embedding)

        # 2. Reject other proposals for this article
        session.execute(
            update(MergeProposalModel)
            .where(
                MergeProposalModel.source_article_id == article.id,
                MergeProposalModel.target_event_id != event.id,
                MergeProposalModel.status == "pending",
            )
            .values(status="rejected")
        )
        # Approve the specific proposal if it exists
        session.execute(
            update(MergeProposalModel)
            .where(
                MergeProposalModel.source_article_id == article.id,
                MergeProposalModel.target_event_id == event.id,
            )
            .values(status="approved")
        )

        # 3. Update Article Queue
        self._mark_article_queue_completed(session, article.id)

        # 4. Trigger Event Enhancement
        self._trigger_event_enhancement(session, event.id)

        return event.id

    def execute_new_event_action(
        self, session: Session, article: ArticleModel, reason: str = ""
    ):
        """
        Standardizes the 'New Event' operation.
        """
        # 1. Create Event
        # (Note: _create_new_event internally commits to reserve ID, so we re-fetch if needed)
        new_event_id = self._create_new_event(
            session, article, article.embedding, reason
        )

        # 2. Reject ALL proposals for this article (since we made a new event)
        session.execute(
            update(MergeProposalModel)
            .where(MergeProposalModel.source_article_id == article.id)
            .values(status="rejected")
        )

        # 3. Update Article Queue
        self._mark_article_queue_completed(session, article.id)

        # 4. Trigger Event Enhancement
        self._trigger_event_enhancement(session, new_event_id)

        return new_event_id

    def execute_event_merge(
        self,
        session: Session,
        source_event: NewsEventModel,
        target_event: NewsEventModel,
    ):
        """
        Merges two events (Source -> Target).
        1. Moves all articles.
        2. Updates target centroid (simple average or re-calc).
        3. Deactivates source event.
        4. Triggers enhancer for target.
        """
        # Move articles
        articles = session.scalars(
            select(ArticleModel)
            .options(joinedload(ArticleModel.newspaper))
            .where(ArticleModel.event_id == source_event.id)
        ).all()

        for art in articles:
            # We treat this like a new link to update centroids/search text
            # Note: This might be heavy if merging massive events, but accurate.
            self._link_to_event(session, target_event, art, art.embedding)

        # Deactivate Source
        source_event.is_active = False
        source_event.title = f"{source_event.title}"
        source_event.merged_into_id = target_event.id
        source_event.last_updated_at = datetime.now(timezone.utc)
        session.add(source_event)

        # Reject proposals pointing to the dead event
        session.execute(
            update(MergeProposalModel)
            .where(MergeProposalModel.target_event_id == source_event.id)
            .values(status="rejected")
        )

        # Trigger Enhancement
        self._trigger_event_enhancement(session, target_event.id)

    # --- HELPERS ---

    def _mark_article_queue_completed(self, session: Session, article_id: uuid.UUID):
        session.execute(
            update(ArticlesQueueModel)
            .where(ArticlesQueueModel.article_id == article_id)
            .values(status=JobStatus.COMPLETED, updated_at=datetime.now(timezone.utc))
        )

    def _trigger_event_enhancement(self, session: Session, event_id: uuid.UUID):
        # 1. Check local session cache for pending inserts (to avoid duplicates in same batch)
        for obj in session.new:
            if isinstance(obj, EventsQueueModel) and obj.event_id == event_id:
                return

        # 2. Check Database (Any status)
        existing = session.scalar(
            select(EventsQueueModel).where(
                EventsQueueModel.event_id == event_id
            )
        )

        if existing:
            # If it exists, ensure it is PENDING in the ENHANCER queue
            if existing.queue_name != EventsQueueName.ENHANCER or existing.status != JobStatus.PENDING:
                existing.status = JobStatus.PENDING
                existing.queue_name = EventsQueueName.ENHANCER
                existing.updated_at = datetime.now(timezone.utc)
                existing.msg = ""
                session.add(existing)
        else:
            new_job = EventsQueueModel(
                event_id=event_id,
                queue_name=EventsQueueName.ENHANCER,
                status=JobStatus.PENDING,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            session.add(new_job)

    # --- CORE SEARCH & LOGIC (Existing) ---

    def search_news_events_hybrid(
        self,
        session: Session,
        query_text: str,
        query_vector: List[float],
        target_date: datetime = datetime.now(timezone.utc),
        diff_date: timedelta = timedelta(days=5),
        limit: int = 10,
        rrf_k: int = 60,
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

        score_expression = func.coalesce(
            1.0 / (rrf_k + sem_alias.c.rank), 0.0
        ) + func.coalesce(1.0 / (rrf_k + kw_alias.c.rank), 0.0)

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

    def derive_search_query(self, article: ArticleModel) -> str:
        parts = [article.title]
        if article.entities:
            parts.extend(article.entities[:5])
        if article.main_topics and len(parts) < 5:
            parts.extend(article.main_topics[:2])
        return " ".join(parts)

    def cluster_existing_article(
        self, session: Session, article: ArticleModel
    ) -> ClusterResult:
        text_query = self.derive_search_query(article)
        vector = article.embedding

        if vector is None or len(vector) == 0:
            return ClusterResult("ERROR", None, [], "Missing Vector")

        candidates = self.search_news_events_hybrid(
            session, text_query, vector, article.published_date
        )

        if not candidates:
            # We still call internal create because we are in the middle of logic flow,
            # but the RUN loop will call execute_new_event_action to finalize queues.
            new_id = self._create_new_event(
                session, article, vector, reason="No Candidates Found"
            )
            return ClusterResult("NEW", new_id, [], "No Matches")

        best_ev, rrf_score, vec_dist = candidates[0]

        if vec_dist > self.SIMILARITY_LOOSE:
            if rrf_score > 0.05:  # Arbitrary threshold implying strong keyword rank
                return ClusterResult(
                    "PROPOSE",
                    best_ev.id,
                    [{"title": best_ev.title, "score": vec_dist}],
                    f"Yellow Zone ({vec_dist:.3f})",
                )
            reason = (
                f"Vector Dist {vec_dist:.2f} too high (Buzzword Trap) {best_ev.title}"
            )
            new_id = self._create_new_event(session, article, vector, reason=reason)
            return ClusterResult("NEW", new_id, [], reason)

        if self.SIMILARITY_STRICT < vec_dist < self.SIMILARITY_LOOSE:
            self._create_proposal(session, article, best_ev, vec_dist)
            return ClusterResult(
                "PROPOSE",
                best_ev.id,
                [{"title": best_ev.title, "score": vec_dist}],
                f"Yellow Zone ({vec_dist:.3f})",
            )

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

        self._link_to_event(session, best_ev, article, vector)
        return ClusterResult("MERGE", best_ev.id, [], "Perfect Match")

    def _link_to_event(
        self,
        session: Session,
        event: Optional[NewsEventModel],
        article: Optional[ArticleModel],
        vector: Optional[list[float]],
    ):
        if event is None or article is None or vector is None:
            return None
        
        # Lock explicitly to avoid "FOR UPDATE on outer join" error if event was loaded with joinedload
        session.execute(select(NewsEventModel.id).where(NewsEventModel.id == event.id).with_for_update())
        session.refresh(event)

        # 1. Update Centroid
        EventAggregator.update_centroid(event, vector)
        
        # 2. Update Basic Stats
        event.article_count += 1
        event.last_updated_at = datetime.now(timezone.utc)
        
        if event.search_text:
            event.search_text += f" {article.title}"

        # 3. Aggregate Interests
        EventAggregator.aggregate_interests(event, article.interests)

        # 4. Aggregate Newspaper Metadata (Bias/Ownership)
        EventAggregator.aggregate_metadata(event, article)

        # Explicitly update article_counts_by_bias
        if article.newspaper and article.newspaper.bias:
            bias = article.newspaper.bias
            # Clone dict to ensure SQLAlchemy detects change on JSONB
            counts = dict(event.article_counts_by_bias or {})
            counts[bias] = counts.get(bias, 0) + 1
            event.article_counts_by_bias = counts

        # 5. Link
        article.event_id = event.id
        event.articles.append(article)
        session.add(event)
        session.add(article)
        return event.id

    def _create_new_event(self, session: Session, article, vector, reason=""):
        # 1. Prepare Initial Aggregations
        init_interests = {}
        init_bias = {}
        init_counts = {}
        init_ownership = {}

        # Interests
        if article.interests:
            for category, items in article.interests.items():
                init_interests[category] = {}
                for item in items:
                    init_interests[category][item] = 1

        # Newspaper Metadata
        if article.newspaper:
            if article.newspaper.bias:
                init_bias[article.newspaper.bias] = [str(article.newspaper.id)]
                init_counts[article.newspaper.bias] = 1

            if article.newspaper.ownership_type:
                init_ownership[article.newspaper.ownership_type] = 1

        # 2. Create Event
        new_event = NewsEventModel(
            id=uuid.uuid4(),
            title=article.title,
            embedding_centroid=vector,
            article_count=1,
            is_active=True,
            created_at=article.published_date,
            last_updated_at=datetime.now(timezone.utc),
            search_text=f"{article.title} {self.derive_search_query(article)}",
            # New Aggregated Fields
            interest_counts=init_interests,
            article_counts_by_bias=init_counts,
            bias_distribution=init_bias,
            ownership_stats=init_ownership,
        )
        session.add(new_event)
        session.flush()  # Generate ID without breaking transaction

        # 3. Link Article
        article = session.merge(article)
        article.event_id = new_event.id
        session.add(article)
        session.flush()

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
                status="pending",
                reasoning=reason,
            )
            session.add(prop)
            logger.info(
                f"⚠️ Proposal created: {article.title[:20]} -> {event.title[:20]} ({reason})"
            )

class NewsClusterWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine, expire_on_commit=False
        )
        self.cluster = NewsCluster()
        
        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.CLUSTER,
            batch_size=100,
            pending_status=JobStatus.PENDING
        )

    async def run(self):
        logger.info(f"🚀 Worker started for queue: {self.queue_name}")
        while True:
            try:
                count = await self.process_batch()
                if count == 0:
                    logger.info(f"Queue {self.queue_name} empty. Sleeping 60s...")
                    await asyncio.sleep(60)
            except Exception as e:
                logger.critical(f"Worker crashed: {e}")
                await asyncio.sleep(30)

    def _fetch_jobs(self, session):
        # Override to join with ArticleModel
        stmt = (
            select(ArticlesQueueModel, ArticleModel)
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .options(joinedload(ArticleModel.newspaper, innerjoin=True))
            .where(ArticlesQueueModel.status == self.pending_status)
            .where(ArticlesQueueModel.queue_name == self.queue_name)
            .order_by(ArticleModel.published_date.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = session.execute(stmt).all()
        
        jobs = []
        for queue_item, article in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            queue_item.article = article 
            jobs.append(queue_item)
            
        session.commit()
        return jobs

    async def process_item(self, session, job):
        try:
            article = job.article
            
            decision: ClusterResult = self.cluster.cluster_existing_article(
                session, article
            )

            if decision.action in ["MERGE", "NEW"]:
                # The event creation/linking happened inside cluster_existing_article
                # We just need to trigger enhancement and mark queue completed.
                if decision.event_id:
                    self.cluster._trigger_event_enhancement(session, decision.event_id)
                
                job.status = JobStatus.COMPLETED
                logger.success(f"Action {decision.action}: {article.title[:20]} -> ENHANCE")

            elif decision.action in ["PROPOSE", "PROPOSE_MULTI"]:
                job.status = JobStatus.COMPLETED
                job.msg = f"Parked: {decision.reason}"
                logger.warning(f"Action {decision.action}: {article.title} -> PARKED")

            else:
                job.status = JobStatus.FAILED
                job.msg = f"Unknown Action: {decision.action}"
        except Exception as e:
            logger.error(f"Error processing article {job.article_id}: {e}")
            session.rollback()
            raise e

if __name__ == "__main__":
    worker = NewsClusterWorker()
    asyncio.run(worker.run())
