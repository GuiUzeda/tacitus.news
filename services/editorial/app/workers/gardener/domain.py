from datetime import datetime, timedelta, timezone
from typing import List

import numpy as np
from app.utils.event_manager import EventManager
from app.workers.publisher.domain import NewsPublisherDomain  # Only for score calc
from loguru import logger

# Models
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    AuditLogModel,
    EventsQueueModel,
    EventsQueueName,
    EventStatus,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_distances
from sqlalchemy import and_, delete, or_, select, update
from sqlalchemy.orm import Session


class NewsGardenerDomain:
    def __init__(self):
        # Configuration
        self.publisher_domain = NewsPublisherDomain()  # Used for score decay calc only

        # Policies
        self.ARCHIVE_AFTER_DAYS = 7
        self.SOFT_DELETE_MONTHS = 6
        self.AUDIT_RETENTION_DAYS = 7

        # Splitter Config
        self.SPLIT_THRESHOLD_COUNT = 10
        self.CHECK_SPLIT_DAYS = 3

    def run_maintenance_cycle(self, session: Session):
        """
        Runs all maintenance tasks in sequence.
        """
        stats = {}

        # 1. Archive Old Events
        stats["archived"] = self._archive_stale_events(session)

        stats["cleaned_queues"] = self._run_janitor(session)
        stats["stale_deltas"] = self._rescue_stale_deltas(session)

        return stats

    async def run_score_decay(self, session: Session):
        # 1. Score Decay
        await self._run_score_decay(session)

    async def run_splitter_cycle(self, session: Session):
        """
        Async loop for heavy logic (Splitting & Score Decay).
        """

        # 2. Split Mega Events
        await self._run_splitter(session)

    # --- 1. ARCHIVIST ---

    def _archive_stale_events(self, session: Session) -> int:
        now = datetime.now(timezone.utc)

        # Policy: Archive if > 7 days AND Low Score OR > 14 days regardless
        stmt = (
            update(NewsEventModel)
            .where(
                NewsEventModel.status == EventStatus.PUBLISHED,
                NewsEventModel.is_active.is_(True),
                or_(
                    and_(
                        NewsEventModel.last_updated_at < now - timedelta(days=7),
                        NewsEventModel.hot_score < 50.0,
                    ),
                    NewsEventModel.last_updated_at < now - timedelta(days=14),
                ),
            )
            .values(status=EventStatus.ARCHIVED)
        )
        result = session.execute(stmt)
        return result.rowcount  # type: ignore

    # --- 2. JANITOR ---

    def _run_janitor(self, session: Session) -> int:
        """
        Deletes completed/failed queue items and old logs.
        NEVER DELETES ARTICLES.
        """
        now = datetime.now(timezone.utc)
        retention_cutoff = now - timedelta(days=self.AUDIT_RETENTION_DAYS)
        total_deleted = 0

        # 1. Clean Queues (Merge Proposals, Event Queue, Article Queue)
        # We only delete items that are in a terminal state AND older than the cutoff
        for model in [ArticlesQueueModel, EventsQueueModel, MergeProposalModel]:
            res = session.execute(
                delete(model).where(
                    model.status.in_(
                        [
                            JobStatus.COMPLETED,
                            JobStatus.REJECTED,
                            JobStatus.APPROVED,
                        ]
                    ),
                    model.updated_at < retention_cutoff,
                )
            )
            total_deleted += res.rowcount  # type: ignore

        # 2. Clean Audit Logs (The requested part)
        # Audit logs don't have a 'status', so we rely purely on created_at
        res_audit = session.execute(
            delete(AuditLogModel).where(AuditLogModel.created_at < retention_cutoff)
        )
        total_deleted += res_audit.rowcount  # type: ignore

        # 3. Optional: Clean 'Dead' processing items
        # If a worker crashes, an item might stay 'PROCESSING' forever.
        # This resets items that have been stuck for > 6 hours.
        stuck_cutoff = now - timedelta(hours=6)
        session.execute(
            update(EventsQueueModel)
            .where(
                EventsQueueModel.status == JobStatus.PROCESSING,
                EventsQueueModel.updated_at < stuck_cutoff,
            )
            .values(
                status=JobStatus.PENDING, msg="Gardener: Reset stuck processing job"
            )
        )

        if total_deleted > 0:
            logger.info(
                f"🧹 Janitor: Purged {total_deleted} stale records (Queues + Audit Logs)."
            )

        return total_deleted

    # --- 3. SCORE DECAY ---

    async def _run_score_decay(self, session: Session):
        """
        Recalculates hot_score for all published events to apply time decay.
        """
        logger.info("📉 Running Score Decay...")
        events = session.scalars(
            select(NewsEventModel).where(
                NewsEventModel.status == EventStatus.PUBLISHED,
                NewsEventModel.is_active.is_(True),
            )
        ).all()

        updated_count = 0
        for event in events:
            topics = (
                list(event.main_topic_counts.keys()) if event.main_topic_counts else []
            )

            # Re-calculate using standard domain logic
            new_score, new_insights = self.publisher_domain.calculate_spectrum_score(
                event, topics
            )

            # Update if changed significantly
            if abs(event.hot_score - new_score) > 0.01:
                event.hot_score = new_score
                event.publisher_insights = new_insights
                updated_count += 1

        if updated_count > 0:
            session.commit()
            logger.info(f"   -> Decayed scores for {updated_count} events.")

    # --- 4. SPLITTER (Complex Logic) ---

    async def _run_splitter(self, session: Session):
        logger.info("🔪 Running Splitter Check...")
        candidates = session.scalars(
            select(NewsEventModel).where(
                NewsEventModel.status == EventStatus.PUBLISHED,
                NewsEventModel.is_active.is_(True),
                NewsEventModel.article_count >= self.SPLIT_THRESHOLD_COUNT,
                NewsEventModel.last_updated_at
                >= datetime.now(timezone.utc) - timedelta(days=self.CHECK_SPLIT_DAYS),
            )
        ).all()

        for parent in candidates:
            await self._process_split(session, parent)

    def calculate_sub_clusters(
        self,
        session: Session,
        event: NewsEventModel,
        eps: float = 0.22,
        min_samples: int = 2,
    ) -> List[List[ArticleModel]]:

        # 1. Fetch Data
        # We need all articles with their embeddings
        articles = session.scalars(
            select(ArticleModel)
            .where(ArticleModel.event_id == event.id)
            .order_by(ArticleModel.published_date)
        ).all()

        if len(articles) < 3:
            return []  # Too small to split

        # 2. Prepare Vectors & Timestamps
        valid_articles = []
        vectors = []
        timestamps = []

        for art in articles:
            if (
                art.embedding is not None and len(art.embedding) == 768
            ):  # Ensure valid vector
                valid_articles.append(art)
                vectors.append(art.embedding)

                # Normalize time to "Hours since first article"
                dt = art.published_date
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                timestamps.append(dt.timestamp() / 3600.0)  # Hours

        if not valid_articles:
            return []

        # Convert to Numpy
        X_vec = np.array(vectors)
        # FIX: Keep shape as (N, 1)
        X_time = np.array(timestamps).reshape(-1, 1)

        # 3. Compute Distance Matrix
        # A. Semantic Distance (0.0 to 1.0+)
        dist_semantic = cosine_distances(X_vec)

        # B. Temporal Distance (FIXED BROADCASTING)
        # We perform (N, 1) - (1, N) to get (N, N) matrix
        t_diff = np.abs(X_time - X_time.T)

        # Scale: 24h gap adds 0.15 distance
        # CAP: Max penalty 0.5 (prevent splitting purely on time for long threads)
        dist_time = np.minimum((t_diff / 24.0) * 0.15, 0.5)

        # C. Combined Distance
        # Shape is now safely (N, N)
        dist_final = dist_semantic + dist_time

        # 4. Run DBSCAN
        # eps=0.22 -> Cluster Radius.
        # min_samples=2 -> Even a pair can form a cluster.
        db = DBSCAN(eps=eps, min_samples=min_samples, metric="precomputed")
        labels = db.fit_predict(dist_final)

        # 5. Group Results
        clusters = {}
        noise = []

        for idx, label in enumerate(labels):
            art = valid_articles[idx]
            if label == -1:
                noise.append(idx)
            else:
                if label not in clusters:
                    clusters[label] = []
                clusters[label].append(art)

        # 6. Handle Noise (The "Smart Orphan" Logic)
        # Threshold: 0.25 (Same as your Loose Collision/Proposal threshold)
        NOISE_MERGE_THRESHOLD = 0.25

        if clusters and noise:
            for noise_idx in noise:
                min_dist = 999.0
                best_cluster = None

                # Compare noise item against valid cluster members
                for label, cluster_arts in clusters.items():
                    # Heuristic: Compare against the first member (Representative)
                    # Ideally, compare against all and take average, but this is fast.
                    member_idx = valid_articles.index(cluster_arts[0])
                    d = dist_final[noise_idx][member_idx]

                    if d < min_dist:
                        min_dist = d
                        best_cluster = label

                # --- LOGIC CHANGE HERE ---
                if best_cluster is not None and min_dist <= NOISE_MERGE_THRESHOLD:
                    # It's close enough, just an outlier. Pull it in.
                    clusters[best_cluster].append(valid_articles[noise_idx])
                else:
                    # It is TOO FAR. Orphan it.
                    # We create a new unique label for this orphan
                    new_orphan_label = 1000 + noise_idx
                    clusters[new_orphan_label] = [valid_articles[noise_idx]]
                    logger.info(
                        f"✂️ Orphaned noise article: '{valid_articles[noise_idx].title[:20]}' (Dist {min_dist:.2f} > {NOISE_MERGE_THRESHOLD})"
                    )

        # 7. Format Output
        result_groups = list(clusters.values())

        # Note: We allow groups of size 1 (orphans) to be returned now.
        if not result_groups:
            return []

        # Sort groups by time (Oldest event first)
        result_groups.sort(key=lambda arts: min(a.published_date for a in arts))

        return result_groups

    async def _process_split(self, session: Session, parent_event: NewsEventModel):
        # 0. Busy Check (Avoid Carpet Pulling from Enhancer)
        is_busy = session.scalar(
            select(1).where(
                EventsQueueModel.event_id == parent_event.id,
                EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                EventsQueueModel.status == JobStatus.PROCESSING,
            )
        )
        if is_busy:
            logger.info(
                f"⏳ Splitter skipped '{parent_event.title[:20]}': Enhancer is busy."
            )
            return

        # 1. ACQUIRE LOCK (Block ClusterWorker from adding articles during split)
        session.refresh(parent_event, with_for_update=True)

        # Dynamic Epsilon based on density
        eps = min(0.3 * 30 / max(parent_event.article_count, 1), 0.7)

        sub_clusters = self.calculate_sub_clusters(session, parent_event, eps=eps)
        if len(sub_clusters) < 2:
            return

        # Sort: Largest stays, others leave
        sub_clusters.sort(key=len, reverse=True)
        dissident_groups = sub_clusters[1:]  # Eject these

        logger.warning(
            f"✂️ Splitting '{parent_event.title}': Ejecting {len(dissident_groups)} clusters."
        )

        new_events = []
        for group in dissident_groups:
            # 1. Select Best Seed (Best Source Rank)
            # We want the title of the new event to come from a reputable source
            seed = min(
                group, key=lambda a: a.source_rank if a.source_rank is not None else 999
            )
            seed.summary_status = JobStatus.COMPLETED

            new_event = EventManager.execute_new_event_action(session, seed)

            # 2. Move Remaining Articles
            for art in group:
                if art.id == seed.id:
                    continue  # Skip seed, already linked

                art.summary_status = JobStatus.COMPLETED
                EventManager.link_article_to_event(session, new_event, art)

            # 3. Queue for Enhancement (The correct way to update it)
            EventManager.create_event_queue(
                session,
                new_event.id,
                EventsQueueName.ENHANCER,
                reason="FORCE",
            )
            new_events.append(new_event)

        # 4. Refine Parent (The Survivor)
        # We need to forcefully re-aggregate stats since we lost articles
        # Ideally, we queue it for Enhancer too, but Enhancer is additive (Delta).
        # So we trigger a special "Resync" enhancement?
        # Actually, standard Enhancer will re-read current state.
        # But we need to reset stats first.

        # Reset Aggregations manually for safety
        parent_event.article_count = len(sub_clusters[0])
        # (Re-calculating centroid is expensive, maybe skip or do approx)
        EventManager.recalculate_event_metrics(session, parent_event)
        # Queue Parent for re-enhancement to update summary
        EventManager.create_event_queue(
            session,
            parent_event.id,
            EventsQueueName.ENHANCER,
            reason="FORCE",
        )

        # 5. Immunity Proposals (Prevent immediate re-merge)
        for child in new_events:
            EventManager.create_merge_proposal(
                session,
                child,
                parent_event,
                0.0,
                reason="Split Origin Immunity",
                status=JobStatus.REJECTED,  # Pre-rejected
            )

        session.commit()

    def _rescue_stale_deltas(self, session: Session) -> int:
        """
        Finds events that have new articles but haven't been summarized
        for a long time (e.g. because they were debounced and then traffic stopped).
        """
        # Criteria:
        # 1. Has unsummarized articles (article_count > articles_at_last_summary)
        # 2. Last summary was > 30 mins ago (Safe margin over 15m debounce)
        # 3. No active job in Enhancer queue

        cutoff = datetime.now(timezone.utc) - timedelta(minutes=30)

        # Subquery: Events currently in queue (Pending/Processing)
        active_jobs = select(EventsQueueModel.event_id).where(
            EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
            EventsQueueModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]),
        )

        stmt = (
            select(NewsEventModel)
            .where(
                NewsEventModel.is_active.is_(True),
                NewsEventModel.article_count > NewsEventModel.articles_at_last_summary,
                NewsEventModel.last_summarized_at < cutoff,
                NewsEventModel.id.not_in(active_jobs),
            )
            .limit(50)  # Batch limit
        )

        events = session.scalars(stmt).all()
        count = 0

        for event in events:
            EventManager.create_event_queue(
                session,
                event.id,
                EventsQueueName.ENHANCER,
                reason="Gardener: Stale Delta Rescue",
            )
            count += 1

        return count
