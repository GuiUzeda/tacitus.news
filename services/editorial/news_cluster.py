import asyncio
import uuid
import numpy as np
from datetime import datetime
from typing import List, Optional
from loguru import logger
from sqlalchemy import select, func, text, and_, desc, update
from sqlalchemy.orm import Session, aliased, sessionmaker
from sqlalchemy import create_engine

# Models
from news_events_lib.models import NewsEventModel, ArticleModel, MergeProposalModel
from models import ArticlesQueueModel, ArticlesQueueName, ClusterResult, JobStatus, EventsQueueName
from config import Settings

class NewsCluster:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        
        # --- TUNING KNOBS ---
        # 0.12 is very strict (good for auto-merge)
        # 0.23 covers "same topic, different angle" (good for proposals)
        self.SIMILARITY_STRICT = 0.12 
        self.SIMILARITY_LOOSE = 0.23   

    def search_news_events_hybrid(
        self,
        session: Session,
        query_text: str, 
        query_vector: List[float], 
        limit: int = 10,
        rrf_k: int = 60
    ):
        """
        Returns list of (NewsEventModel, rrf_score, vector_distance)
        """
        # 1. Semantic Search CTE (Now returns DISTANCE)
        semantic_subq = (
            select(
                NewsEventModel.id,
                NewsEventModel.embedding_centroid.cosine_distance(query_vector).label("dist"),
                func.row_number().over(
                    order_by=NewsEventModel.embedding_centroid.cosine_distance(query_vector)
                ).label("rank")
            )
            .where(NewsEventModel.is_active == True)
            .order_by(NewsEventModel.embedding_centroid.cosine_distance(query_vector))
            .limit(50)
            .cte("semantic_results")
        )

        # 2. Keyword Search CTE
        ts_query = func.plainto_tsquery('portuguese', query_text)
        
        keyword_subq = (
            select(
                NewsEventModel.id,
                func.row_number().over(
                    order_by=desc(func.ts_rank_cd(NewsEventModel.search_vector_ts, ts_query))
                ).label("rank")
            )
            .where(
                and_(
                    NewsEventModel.is_active == True,
                    NewsEventModel.search_vector_ts.op("@@")(ts_query)
                )
            )
            .limit(50)
            .cte("keyword_results")
        )

        # 3. RRF Calculation
        sem_alias = aliased(semantic_subq, name="sem")
        kw_alias = aliased(keyword_subq, name="kw")
        
        score_expression = (
            func.coalesce(1.0 / (rrf_k + sem_alias.c.rank), 0.0) +
            func.coalesce(1.0 / (rrf_k + kw_alias.c.rank), 0.0)
        )

        # IMPORTANT: If 'dist' is NULL (meaning it was found ONLY by keyword search),
        # we treat distance as 1.0 (Max Distance/Irrelevant). 
        # We only auto-merge if the VECTOR confirms it.
        distance_expression = func.coalesce(sem_alias.c.dist, 1.0)

        stmt = (
            select(
                NewsEventModel,
                score_expression.label("rrf_score"),
                distance_expression.label("vector_dist")
            )
            .join_from(
                sem_alias, 
                kw_alias, 
                sem_alias.c.id == kw_alias.c.id, 
                full=True
            )
            .join(
                NewsEventModel,
                NewsEventModel.id == func.coalesce(sem_alias.c.id, kw_alias.c.id)
            )
            .order_by(desc("rrf_score"))
            .limit(limit)
        )

        return session.execute(stmt).all()

    def derive_search_query(self, article: ArticleModel) -> str:
        parts = []
        if article.entities:
            parts.extend(article.entities[:5]) # Strongest signal
        if article.main_topics and len(parts) < 3:
            parts.extend(article.main_topics[:2])
        if not parts:
            return article.title # Fallback
        return " ".join(parts)

    def cluster_existing_article(self, session: Session, article: ArticleModel) -> ClusterResult:
        text_query = self.derive_search_query(article)
        vector = article.embedding
        
        # 1. Search (Returns: Event, RRF_Score, Vector_Dist, Text_Rank)
        # Note: You need to update search_news_events_hybrid to return Text Rank too
        candidates = self.search_news_events_hybrid(session, text_query, vector)
        
        if not candidates:
            return self._create_new_event(session, article, vector)

        best_ev, rrf_score, vec_dist = candidates[0]
        
        # --- SCENARIO 2: THE BUZZWORD TRAP (Keyword High, Vector Low) ---
        # Even if RRF picked it, if the vector distance is Red, trust the vector.
        if vec_dist > self.SIMILARITY_LOOSE: # > 0.23
            # It's a different topic involving the same entities.
            return self._create_new_event(session, article, vector, 
                                          reason=f"Vector Dist {vec_dist:.2f} too high (Buzzword Trap)")

        # --- SCENARIO 3: THE VIBE TRAP (Vector High, Keyword Low) ---
        # This requires checking the 'Text Rank'. 
        # If we are Green (<0.12) but Keywords are weak, we pause.
        # (Assuming you add a check for text match quality, simplified here as:)
        # strict_text_match = (text_query in best_ev.search_text) 
        
        # --- SCENARIO 4: THE EVOLUTION (Medium Vector, High Keyword) ---
        if self.SIMILARITY_STRICT < vec_dist < self.SIMILARITY_LOOSE:
            # This is the classic "Yellow Zone"
            self._create_proposal(session, article, best_ev, vec_dist)
            return ClusterResult('PROPOSE', best_ev.id, [], f"Yellow Zone ({vec_dist:.3f})")

        # --- AMBIGUITY CHECK (Safety Brake) ---
        if len(candidates) > 1:
            second_ev, _, second_dist = candidates[1]
            if (second_dist - vec_dist) < 0.05:
                # Create Multiple Proposals
                options = []
                for ev, _, dist in candidates[:3]:
                    if dist < self.SIMILARITY_LOOSE:
                        self._create_proposal(session, article, ev, dist, ambiguous=True)
                        options.append({'title': ev.title, 'score': dist})
                
                return ClusterResult('PROPOSE_MULTI', None, options, "Ambiguous Match")

        # --- SCENARIO 1: PERFECT MATCH ---
        self._link_to_event(session, best_ev, article, vector)
        return ClusterResult('MERGE', best_ev.id, [], "Perfect Match")

    def _link_to_event(self, session, event: NewsEventModel, article: ArticleModel, vector: list[float]):
        """Updates centroid and links article"""
        # Weighted Average Centroid Update
        current_centroid = np.array(event.embedding_centroid)
        new_vector = np.array(vector)
        n = event.article_count
        updated_centroid = ((current_centroid * n) + new_vector) / (n + 1)
        
        event.embedding_centroid = updated_centroid.tolist()
        event.article_count += 1
        event.last_updated_at = datetime.utcnow()
        
        # Simple text append for search (Postgres handles dedupe in tsvector usually)
        if event.search_text:
            event.search_text += f" {article.title}"
            
        article.event_id = event.id
        return event.id

    def _create_proposal(self, session, article, event, score):
        prop = MergeProposalModel(
            id=uuid.uuid4(),
            source_article_id=article.id,
            target_event_id=event.id,
            similarity_score=float(score),
            status="pending",
            reasoning=f"RRF Match. Vector Dist {score:.3f} in Yellow Zone."
        )
        session.add(prop)
        logger.info(f"⚠️ Proposal created: {article.title[:20]} -> {event.title[:20]}")

    def _create_new_event(self, session, article, vector):
        new_event = NewsEventModel(
            id=uuid.uuid4(),
            title=article.title,
            embedding_centroid=vector,
            article_count=1,
            is_active=True,
            created_at=datetime.utcnow(),
            last_updated_at=datetime.utcnow(),
            search_text=f"{article.title} {self.derive_search_query(article)}"
        )
        session.add(new_event)
        session.flush() # Get ID
        
        article.event_id = new_event.id
        logger.info(f"🆕 New Event created: {new_event.title[:20]}")
        return new_event.id

    async def run(self):
        while True:
            # 1. Fetch Batch (Locking)
            with self.SessionLocal() as session:
                stmt = (
                    select(ArticleModel, ArticlesQueueModel)
                    .join(ArticlesQueueModel, ArticlesQueueModel.article_id == ArticleModel.id)
                    .where(ArticlesQueueModel.status == JobStatus.PENDING)
                    .where(ArticlesQueueModel.queue_name == ArticlesQueueName.CLUSTER)
                    .order_by(ArticlesQueueModel.created_at.asc())
                    .limit(50)
                    .with_for_update(skip_locked=True)
                )
                result = session.execute(stmt).all()
                
                if not result:
                    logger.info("Cluster queue empty. Sleeping...")
                    await asyncio.sleep(10)
                    continue

                for _, queue in result:
                    queue.status = JobStatus.PROCESSING
                session.commit()

            # 2. Process Batch
            with self.SessionLocal() as session:
                for article, queue in result:
                    try:
                        # Re-merge into new session
                        article = session.merge(article)
                        queue = session.merge(queue)
                        
                        event_id = self.cluster_existing_article(session, article)
                        
                        if event_id:
                            # Green/Red -> Move to Enhancer
                            queue.status = JobStatus.PENDING
                            queue.queue_name = EventsQueueName.ENHANCER
                            # IMPORTANT: Queue needs event_id reference now? 
                            # If your EventsQueue is separate, you might need to insert there instead.
                            # Assuming ArticlesQueue just flows through:
                        else:
                            # Yellow -> Park it
                            queue.status = JobStatus.COMPLETED
                            queue.msg = "Waiting for Proposal"
                            
                        queue.updated_at = datetime.utcnow()
                        
                    except Exception as e:
                        logger.error(f"Cluster failed: {e}")
                        queue.status = JobStatus.FAILED
                        queue.msg = str(e)
                
                session.commit()
                logger.success(f"Processed {len(result)} articles.")