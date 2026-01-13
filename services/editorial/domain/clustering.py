import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from dataclasses import dataclass

from loguru import logger
from sqlalchemy import delete, select, func, and_, desc, update, or_
from sqlalchemy.orm import Session, aliased, sessionmaker, joinedload
from sqlalchemy import create_engine

# Shared Libraries
from news_events_lib.models import (
    NewsEventModel,
    ArticleModel,
    MergeProposalModel,
    JobStatus,
)

# Service Modules
from news_events_lib.models import EventStatus
from core.models import (
    ArticlesQueueModel,
    EventsQueueModel,
    EventsQueueName,
)
from config import Settings
from domain.aggregator import EventAggregator


@dataclass
class ClusterResult:
    action: str  # 'MERGE', 'PROPOSE', 'PROPOSE_MULTI', 'NEW'
    event_id: Optional[uuid.UUID]
    candidates: List[dict]
    reason: str


class NewsCluster:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        self.SIMILARITY_STRICT = self.settings.similarity_strict
        self.SIMILARITY_LOOSE = self.settings.similarity_loose

    # --- STANDARD ACTIONS ---

    def execute_merge_action(
        self, session: Session, article: ArticleModel, event: NewsEventModel
    ):
        """
        Standardizes the 'Merge' operation:
        1. Links article to event.
        2. Rejects conflicting proposals.
        3. Updates ArticlesQueue -> COMPLETED.
        4. Triggers EventsQueue -> ENHANCER.
        """
        self._link_to_event(session, event, article, article.embedding)

        session.execute(
            update(MergeProposalModel)
            .where(
                MergeProposalModel.source_article_id == article.id,
                MergeProposalModel.target_event_id != event.id,
                MergeProposalModel.status == JobStatus.PENDING,
            )
            .values(status=JobStatus.REJECTED, updated_at=datetime.now(timezone.utc))
        )
        session.execute(
            update(MergeProposalModel)
            .where(
                MergeProposalModel.source_article_id == article.id,
                MergeProposalModel.target_event_id == event.id,
            )
            .values(status=JobStatus.APPROVED, updated_at=datetime.now(timezone.utc))
        )

        self._mark_article_queue_completed(session, article.id)
        self._trigger_event_enhancement(session, event.id)
        return event.id

    def execute_new_event_action(
        self, session: Session, article: ArticleModel, reason: str = ""
    ):
        new_event_id = self._create_new_event(
            session, article, article.embedding, reason
        )

        session.execute(
            update(MergeProposalModel)
            .where(MergeProposalModel.source_article_id == article.id)
            .values(status=JobStatus.REJECTED, updated_at=datetime.now(timezone.utc))
        )

        self._mark_article_queue_completed(session, article.id)
        self._trigger_event_enhancement(session, new_event_id)
        return new_event_id

    def _calculate_editorial_score(self, current_score: float, rank: Optional[int]) -> float:
        if not rank:
            return current_score
        # Rank 1 = 10pts, Rank 2 = 9pts ... Rank 10+ = 1pt
        points = max(11 - rank, 1)
        return current_score + float(points)

    # --- MÉTODO DE FUSÃO CENTRALIZADO E SEGURO ---
    def execute_event_merge(
        self,
        session: Session,
        source_event: NewsEventModel,
        target_event: NewsEventModel,
    ):
        """
        Funde o Evento A (Source) no Evento B (Target) preservando TODO o histórico.
        """
        logger.info(f"🧬 MERGING: {source_event.title} -> {target_event.title}")

        # 1. Mover Artigos (Re-link)
        articles = session.scalars(
            select(ArticleModel).where(ArticleModel.event_id == source_event.id)
        ).all()

        for art in articles:
            art.event_id = target_event.id
            session.add(art)
            target_event.articles.append(art)
        
        # 2. HERANÇA DE DADOS (Critical Step)
        
        # A. Datas (Expandir a linha do tempo)
        if source_event.first_article_date:
            if not target_event.first_article_date or source_event.first_article_date < target_event.first_article_date:
                target_event.first_article_date = source_event.first_article_date
        
        if source_event.last_article_date:
            if not target_event.last_article_date or source_event.last_article_date > target_event.last_article_date:
                target_event.last_article_date = source_event.last_article_date

        # B. Scores (Soma)
        target_event.editorial_score = (target_event.editorial_score or 0.0) + (source_event.editorial_score or 0.0)
        target_event.article_count += source_event.article_count
        
        # C. Rank (Melhor rank vence)
        if source_event.best_source_rank:
            if target_event.best_source_rank is None or source_event.best_source_rank < target_event.best_source_rank:
                target_event.best_source_rank = source_event.best_source_rank

        # D. Viés / Spectrum (Merge de Dicionários)
        if source_event.article_counts_by_bias:
            target_counts = dict(target_event.article_counts_by_bias or {})
            for bias, count in source_event.article_counts_by_bias.items():
                target_counts[bias] = target_counts.get(bias, 0) + count
            target_event.article_counts_by_bias = target_counts

        # E. Main Topics (Merge)
        if source_event.main_topic_counts:
            target_topics = dict(target_event.main_topic_counts or {})
            for topic, count in source_event.main_topic_counts.items():
                target_topics[topic] = target_topics.get(topic, 0) + count
            target_event.main_topic_counts = target_topics

        # 3. Tombstone (Enterrar o Source)
        source_event.is_active = False
        source_event.status = EventStatus.MERGED
        source_event.merged_into_id = target_event.id
        source_event.last_updated_at = datetime.now(timezone.utc)
        session.add(source_event)

        # 4. Limpar Propostas e Filas do Morto
        session.execute(
            update(MergeProposalModel)
            .where(MergeProposalModel.target_event_id == source_event.id)
            .values(status=JobStatus.REJECTED, reasoning="Target event was merged.", updated_at=datetime.now(timezone.utc))
        )
        session.execute(
            update(MergeProposalModel)
            .where(MergeProposalModel.source_event_id == source_event.id)
            .values(status=JobStatus.REJECTED, reasoning="Source event was merged.", updated_at=datetime.now(timezone.utc))
        )

        # Remove jobs pendentes do evento morto para não travar workers
        session.execute(
            delete(EventsQueueModel).where(EventsQueueModel.event_id == source_event.id)
        )

        # 5. Trigger Next Steps (Para o Sobrevivente)
        target_event.last_updated_at = datetime.now(timezone.utc)
        session.add(target_event)

        # Se o alvo já estava publicado, força re-publicação para atualizar Hot Score e Posição
        if target_event.status == EventStatus.PUBLISHED:
            logger.info(f"🔄 Re-Queueing Published Event: {target_event.title}")
            self._trigger_event_publisher(session, target_event.id)
        else:
            # Fluxo normal: vai para o Enhancer (Resumo/LLM)
            self._trigger_event_enhancement(session, target_event.id)

    # --- HELPERS DE FILA ---


    def _trigger_event_publisher(self, session: Session, event_id: uuid.UUID):
        # Novo método que faltava!
        self._upsert_queue_job(session, event_id, EventsQueueName.PUBLISHER)

    def _upsert_queue_job(self, session: Session, event_id: uuid.UUID, queue_name: EventsQueueName):
        """Helper genérico para evitar duplicação de código"""
        # Verifica se já existe na memória da sessão (evita flush prematuro)
        for obj in session.new:
            if isinstance(obj, EventsQueueModel) and obj.event_id == event_id:
                return

        existing = session.scalar(
            select(EventsQueueModel).where(EventsQueueModel.event_id == event_id)
        )

        if existing:
            # Se já existe, reseta para Pending e atualiza a fila alvo
            if existing.queue_name != queue_name or existing.status != JobStatus.PENDING:
                existing.status = JobStatus.PENDING
                existing.queue_name = queue_name
                existing.updated_at = datetime.now(timezone.utc)
                existing.msg = "Re-queued by Merge"
                session.add(existing)
        else:
            new_job = EventsQueueModel(
                event_id=event_id,
                queue_name=queue_name, # Pode ser ENHANCER ou PUBLISHER
                status=JobStatus.PENDING,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            session.add(new_job)
    def _mark_article_queue_completed(self, session: Session, article_id: uuid.UUID):
        session.execute(
            update(ArticlesQueueModel)
            .where(ArticlesQueueModel.article_id == article_id)
            .values(status=JobStatus.COMPLETED, updated_at=datetime.now(timezone.utc))
        )

    def _trigger_event_enhancement(self, session: Session, event_id: uuid.UUID):
        for obj in session.new:
            if isinstance(obj, EventsQueueModel) and obj.event_id == event_id:
                return

        existing = session.scalar(
            select(EventsQueueModel).where(EventsQueueModel.event_id == event_id)
        )

        if existing:
            if (
                existing.queue_name != EventsQueueName.ENHANCER
                or existing.status != JobStatus.PENDING
            ):
                existing.status = JobStatus.PENDING
                existing.queue_name = EventsQueueName.ENHANCER
                existing.updated_at = datetime.now(timezone.utc)
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
        return " ".join(parts)

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

        text_query = self.derive_search_query(article)
        vector = article.embedding

        if vector is None or len(vector) == 0:
            return ClusterResult("ERROR", None, [], "Missing Vector")

        candidates = self.search_news_events_hybrid(
            session, text_query, vector, article.published_date
        )

        if not candidates:
            new_id = self._create_new_event(
                session, article, vector, reason="No Candidates Found"
            )
            return ClusterResult("NEW", new_id, [], "No Matches")

        best_ev, rrf_score, vec_dist = candidates[0]

        if vec_dist > self.SIMILARITY_LOOSE:
            if rrf_score > 0.05:
                return ClusterResult(
                    "PROPOSE",
                    best_ev.id,
                    [{"title": best_ev.title, "score": vec_dist}],
                    f"Yellow Zone ({vec_dist:.3f})",
                )
            reason = f"Vector Dist {vec_dist:.2f} too high"
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

        # Lock explicitly
        session.execute(
            select(NewsEventModel.id)
            .where(NewsEventModel.id == event.id)
            .with_for_update()
        )
        session.refresh(event)
        if not event.first_article_date or article.published_date < event.first_article_date:
            event.first_article_date = article.published_date
        
        if not event.last_article_date or article.published_date > event.last_article_date:
            event.last_article_date = article.published_date

        # 2. Agregar Score Editorial e Rank
        if article.source_rank:
            # Melhor Rank Vence (Menor é melhor)
            if event.best_source_rank is None or article.source_rank < event.best_source_rank:
                event.best_source_rank = article.source_rank
            
            # Acumula pontos
            event.editorial_score = self._calculate_editorial_score(
                event.editorial_score, article.source_rank
            )   
        # --- AGGREGATION PIPELINE (Updated) ---
        
        # 1. Basic Stats (Counts + Dates + Rank Score)
        # This handles article_count+=1 and the hot_score logic
        EventAggregator.aggregate_basic_stats(event, article)
        
        # 2. Centroids
        EventAggregator.update_centroid(event, vector)

        # 3. Interests & Metadata
        EventAggregator.aggregate_interests(event, article.interests)
        EventAggregator.aggregate_main_topics(event, article.main_topics)
        EventAggregator.aggregate_metadata(event, article)

        # 4. Local Search Optimization (Keep search_text updated)
        if event.search_text:
            event.search_text += f" {article.title}"
        event.last_updated_at = datetime.now(timezone.utc)

        # 5. Helper for Bias Distribution (Specific to Clustering logic)
        if article.newspaper and article.newspaper.bias:
            bias = article.newspaper.bias
            counts = dict(event.article_counts_by_bias or {})
            counts[bias] = counts.get(bias, 0) + 1
            event.article_counts_by_bias = counts

        # 6. Link in DB
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
        init_main_topics = {}

        if article.interests:
            for category, items in article.interests.items():
                init_interests[category] = {}
                for item in items:
                    init_interests[category][item] = 1

        if article.newspaper:
            if article.newspaper.bias:
                init_bias[article.newspaper.bias] = [str(article.newspaper.id)]
                init_counts[article.newspaper.bias] = 1

            if article.newspaper.ownership_type:
                init_ownership[article.newspaper.ownership_type] = 1

        if article.main_topics:
            for topic in article.main_topics:
                init_main_topics[topic] = 1

        # 2. Create Event (With initial Score values)
        # Rank Logic: If the first article has a rank, use it.
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
            
            # Rank & Score
            best_source_rank=init_rank,
            editorial_score=init_score,
            
            # Date Range
            first_article_date=article.published_date,
            last_article_date=article.published_date
        )
        session.add(new_event)
        session.flush()

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
                status=JobStatus.PENDING,
                reasoning=reason,
            )
            session.add(prop)
            logger.info(
                f"⚠️ Proposal created: {article.title[:20]} -> {event.title[:20]} ({reason})"
            )