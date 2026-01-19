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
    EventStatus,
    ArticlesQueueModel,
    EventsQueueModel,
    EventsQueueName,
)

from config import Settings
from utils.event_manager import EventManager


@dataclass
class Candidate:
    article: ArticleModel
    dist: float
    rank: int


@dataclass
class ClusterResult:
    action: str  # 'MERGE', 'PROPOSE', 'PROPOSE_MULTI', 'NEW', 'IGNORED', 'ERROR'
    event: Optional[NewsEventModel]

    candidates: List[Candidate]
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
        lock_id = zlib.crc32(key_str.encode("utf-8"))
        # pg_advisory_xact_lock automatically releases at the end of the transaction
        session.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": lock_id})

    # --- CORE CLUSTERING LOGIC ---

    def cluster_existing_article(
        self, session: Session, article: ArticleModel
    ) -> ClusterResult:
        # 0. Age Check: Ignore articles older than 7 days

        pub_date = article.published_date
        if pub_date.tzinfo is None:
            pub_date = pub_date.replace(tzinfo=timezone.utc)

        if pub_date < datetime.now(timezone.utc) - self.settings.cutoff_period:
            return ClusterResult("IGNORED", None, [], "Article too old (> 7 days)")

        # 1. LOCKING PHASE
        self._get_advisory_lock(session, article.title)

        text_query = EventManager.derive_search_query(article)
        vector = article.embedding

        if vector is None or len(vector) == 0:
            logger.error("Missing Embedding Vector")
            return ClusterResult("ERROR", None, [], "Missing Embedding Vector")

        candidates = EventManager.search_news_events_hybrid(
            session, text_query, vector, article.published_date
        )

        if not candidates:
            new_event = EventManager.execute_new_event_action(session, article)
            return ClusterResult("NEW", new_event, [], "No Matches")

        best_ev: NewsEventModel = candidates[0][0]
        rrf_score: float = candidates[0][1]
        vec_dist: float = candidates[0][2]

        # Case A: Too Distant -> New Event
        if vec_dist > self.SIMILARITY_LOOSE:
            # Check if RRF gave a strong keyword match despite vector distance
            if rrf_score > 0.05:
                return ClusterResult(
                    "PROPOSE",
                    best_ev,
                    [Candidate(article, vec_dist, 0)],
                    f"Yellow Zone ({vec_dist:.3f}) - RRF Strong",
                )
            reason = f"Vector Dist {vec_dist:.2f} too high"
            new_event = EventManager.execute_new_event_action(session, article)
            # TOODO add to queue?
            return ClusterResult("NEW", new_event, [], reason)

        # Case B: Ambiguous Zone -> Heuristics or Proposal
        if self.SIMILARITY_STRICT < vec_dist < self.SIMILARITY_LOOSE:
            # HEURISTIC: "The Hot Hand"
            is_fresh = False
            if best_ev.last_updated_at:
                last_upd = best_ev.last_updated_at
                if last_upd.tzinfo is None:
                    last_upd = last_upd.replace(tzinfo=timezone.utc)
                is_fresh = (
                    datetime.now(timezone.utc) - last_upd
                ).total_seconds() < 14400  # 4 hours

            is_big = best_ev.article_count > 10

            if is_fresh and is_big and vec_dist < 0.12:
                target_event = EventManager.link_article_to_event(
                    session, best_ev, article
                )
                return ClusterResult(
                    "MERGE", target_event, [], "Auto-Merge: Hot Topic Heuristic"
                )

            # EventManager.create_merge_proposal(session, article, best_ev, vec_dist)
            return ClusterResult(
                "PROPOSE",
                best_ev,
                [Candidate(article, vec_dist, 0)],
                f"Yellow Zone ({vec_dist:.3f})",
            )

        # Case C: Multiple Strong Candidates -> Ambiguity
        if len(candidates) > 1:
            second_ev, _, second_dist = candidates[1]
            if (second_dist - vec_dist) < 0.05:
                options = []
                for i, [ev, _, dist] in enumerate(candidates[:3]):
                    if dist < self.SIMILARITY_LOOSE:

                        options.append(Candidate(article, dist, i))
                return ClusterResult("PROPOSE_MULTI", None, options, "Ambiguous Match")

        # Case D: Perfect Match -> Merge
        target_event = EventManager.link_article_to_event(session, best_ev, article)
        return ClusterResult("MERGE", target_event, [], "Perfect Match")

    # --- SEARCH LOGIC ---
