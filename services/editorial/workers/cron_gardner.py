import sys
import os
from typing import List

import numpy as np

from utils.event_manager import EventManager

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from loguru import logger
from sqlalchemy import create_engine, select, delete, or_, and_
from sqlalchemy.orm import sessionmaker, Session

# Existing Domains
from domain.clustering import NewsCluster
from domain.enhancing import NewsEnhancerDomain
from domain.publisher import NewsPublisherDomain
from services.editorial.utils.event_aggregator import EventAggregator
from news_events_lib.models import (
    EventsQueueName,
    NewsEventModel,
    EventStatus,
    ArticleModel,
    MergeProposalModel,
    JobStatus,
    EventsQueueModel,
    ArticlesQueueModel,
)

from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_distances


class GardnerService:
    def __init__(self):
        self.cluster_domain = NewsCluster()
        self.enhancer_domain = NewsEnhancerDomain()
        self.publisher_domain = NewsPublisherDomain()

        # Configuration for "Mega Events"
        self.SPLIT_THRESHOLD_COUNT = 30
        self.SPLIT_THRESHOLD_HOURS = 24
        self.CHECK_SPLIT_DAYS = 5

    def calculate_sub_clusters(
        self,
        session: Session,
        event: NewsEventModel,
        eps: float = 0.22,
        min_samples: int = 2,
    ) -> List[List[ArticleModel]]:
        """
        Analyzes an event's articles and splits them into cohesive sub-groups
        using DBSCAN on a custom (Semantic + Temporal) distance matrix.
        """
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
        dist_time = (t_diff / 24.0) * 0.15

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

    async def run_cycle(self, session):
        logger.info("🌿 Gardner Cycle Started...")

        # 1. Split Mega Events
        await self._run_splitter(session)

        # 2. Update Hot Scores (Decay)
        await self._run_score_decay(session)

        # 3. Archive Stale Events
        await self._run_archivist(session)

        # 4. Clean Queues
        await self._run_janitor(session)

        logger.success("🌿 Gardner Cycle Complete.")

    async def _run_splitter(self, session):
        logger.info("🔪 Starting Splitter Check...")
        candidates = session.scalars(
            select(NewsEventModel).where(
                NewsEventModel.status == EventStatus.PUBLISHED,
                NewsEventModel.is_active == True,
                NewsEventModel.article_count >= self.SPLIT_THRESHOLD_COUNT,
                NewsEventModel.last_updated_at
                >= datetime.now(timezone.utc) - timedelta(days=self.CHECK_SPLIT_DAYS),
            )
        ).all()

        for parent_event in candidates:
            try:
                await self._process_split(session, parent_event)
            except Exception as e:
                logger.error(f"Failed to split event {parent_event.id}: {e}")
                session.rollback()

    async def _run_score_decay(self, session):
        logger.info("📉 Running Score Decay...")
        events = session.scalars(
            select(NewsEventModel).where(
                NewsEventModel.status == EventStatus.PUBLISHED,
                NewsEventModel.is_active == True,
            )
        ).all()

        updated_count = 0
        for event in events:
            topics = (
                list(event.main_topic_counts.keys()) if event.main_topic_counts else []
            )
            new_score, new_insights, _ = self.publisher_domain.calculate_spectrum_score(
                event, topics
            )

            if abs(event.hot_score - new_score) > 0.01 or set(
                event.publisher_isights or []
            ) != set(new_insights):
                event.hot_score = new_score
                event.publisher_isights = new_insights

                # Sync Blind Spot Flags
                event.is_blind_spot = "BLIND_SPOT" in new_insights
                event.blind_spot_side = None
                if event.is_blind_spot:
                    for tag in new_insights:
                        if tag.startswith("BS_"):
                            event.blind_spot_side = tag.replace("BS_", "").lower()
                            break

                updated_count += 1

        if updated_count > 0:
            session.commit()
            logger.info(f"   -> Updated scores for {updated_count} events.")

    async def _run_archivist(self, session):
        logger.info("📦 Running Archivist...")
        now = datetime.now(timezone.utc)

        # Policy A: > 7 days old AND score < 50 OR Policy B: > 14 days old
        stmt = select(NewsEventModel).where(
            NewsEventModel.status == EventStatus.PUBLISHED,
            NewsEventModel.is_active == True,
            or_(
                and_(
                    NewsEventModel.last_updated_at < now - timedelta(days=7),
                    NewsEventModel.hot_score < 50.0,
                ),
                NewsEventModel.last_updated_at < now - timedelta(days=14),
            ),
        )

        events = session.scalars(stmt).all()
        if events:
            for event in events:
                event.status = EventStatus.ARCHIVED
                # event.is_active = False   Maintain the event active
            session.commit()
            logger.info(f"   -> Archived {len(events)} stale events.")

    async def _run_janitor(self, session):
        logger.info("🧹 Running Queue Janitor...")
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)

        # Handle potential missing Enum members gracefully
        statuses = [JobStatus.COMPLETED]
        if hasattr(JobStatus, "APPROVED"):
            statuses.append(JobStatus.APPROVED)
        if hasattr(JobStatus, "REJECTED"):
            statuses.append(JobStatus.REJECTED)

        # Clean Articles Queue
        res_art = session.execute(
            delete(ArticlesQueueModel).where(
                ArticlesQueueModel.status.in_(statuses),
                ArticlesQueueModel.updated_at < cutoff,
            )
        )

        # Clean Events Queue
        res_evt = session.execute(
            delete(EventsQueueModel).where(
                EventsQueueModel.status.in_(statuses),
                EventsQueueModel.updated_at < cutoff,
            )
        )

        if res_art.rowcount > 0 or res_evt.rowcount > 0:
            session.commit()
            logger.info(
                f"   -> Deleted {res_art.rowcount} article jobs and {res_evt.rowcount} event jobs."
            )

    async def _process_split(self, session: Session, parent_event):
        logger.info(
            f"⚡ Analyzing {parent_event.title} ({parent_event.article_count} articles)..."
        )
        eps = min(0.3 * 50 / max(parent_event.article_count, 1), 0.7)

        # 1. CLUSTER ANALYSIS
        sub_clusters = self.calculate_sub_clusters(session, parent_event, eps=eps)

        if len(sub_clusters) < 2:
            logger.info("   -> Solid event, no split needed.")
            return

        # 2. IDENTIFY SURVIVOR VS. DISSIDENTS (Pruning Strategy)
        sub_clusters.sort(key=len, reverse=True)

        dominant_group = sub_clusters[0]  # Stays in Parent
        dissident_groups = sub_clusters[1:]  # Ejected to New Events

        logger.warning(
            f"   -> PRUNING: Keeping {len(dominant_group)} arts in Parent, "
            f"ejecting {len(dissident_groups)} groups."
        )

        # 3. EJECT DISSIDENTS
        new_events = []
        for group in dissident_groups:
            seed_article = group[0]

            new_event = EventManager.execute_new_event_action(
                session,
                seed_article,
            )
            EventManager.create_event_queue(
                session,
                new_event.id,
                EventsQueueName.ENHANCER,
                reason="Ejected from Splitter",
            )

            for art in group[1:]:
                EventManager.link_article_to_event(session, new_event, art)

            session.refresh(new_event)
            new_events.append(new_event)

        # 4. PROCESS EJECTED EVENTS (Enhance & Publish - No Commit)
        for child_event in new_events:
            await self.enhancer_domain.enhance_event_direct(session, child_event)
            self.publisher_domain.publish_event_direct(
                session, child_event, commit=False
            )
            logger.success(f"   -> Created Child: {child_event.title}")

        # 5. REFINE PARENT (The Survivor)
        session.flush()
        session.expire(parent_event, ["articles"])
        session.refresh(parent_event)

        # Re-calculate Centroid & Search Text (Accumulators not handled by Enhancer)
        parent_event.embedding_centroid = None
        parent_event.search_text = ""
        parent_event.stance = 0.0

        # We must manually simulate the incremental build for Centroid/Search Text
        local_count = 0
        for art in parent_event.articles:
            local_count += 1
            parent_event.article_count = (
                local_count  # Temporarily set for update_centroid logic
            )
            EventAggregator.update_centroid(parent_event, art.embedding)

            # Rebuild Search Text (Keep last 50 unique words)
            current_text = parent_event.search_text or ""
            new_keywords = f"{current_text} {art.title}".split()
            unique_words = list(dict.fromkeys(reversed(new_keywords)))[:50]
            parent_event.search_text = " ".join(reversed(unique_words))

        await self.enhancer_domain.enhance_event_direct(session, parent_event)
        self.publisher_domain.publish_event_direct(session, parent_event, commit=False)

        # 6. IMMUNITY: CREATE "ANTI-MERGE" PROPOSALS (The Loop Fix)
        # We explicitly tell the Merger: "We checked these, and they are NOT duplicates."
        for child in new_events:
            immunity_proposal = MergeProposalModel(
                id=uuid.uuid4(),
                proposal_type="event_merge",
                source_event_id=child.id,
                target_event_id=parent_event.id,
                distance_score=0.0,  # Irrelevant, but required
                status=JobStatus.REJECTED,  # <--- The Shield
                reasoning="Split Origin: Explicitly separated by Splitter.",
            )
            session.add(immunity_proposal)

        # 7. ATOMIC COMMIT
        session.commit()
        logger.success(
            f"✂️ Pruned {parent_event.title} & Immunized {len(new_events)} children."
        )


if __name__ == "__main__":
    from config import Settings

    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)
    splitter = GardnerService()

    async def run_splitter_standalone():
        with SessionLocal() as session:
            await splitter.run_cycle(session)

    asyncio.run(run_splitter_standalone())
