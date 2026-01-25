import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional

from app.config import Settings
from app.utils.event_manager import EventManager

# Shared Libraries
from news_events_lib.models import ArticleModel, NewsEventModel
from sqlalchemy import text
from sqlalchemy.orm import Session


# --- ENUM DEFINITION ---
class ClusterAction(str, Enum):
    MERGE = "MERGE"  # High confidence link (Strict)
    PROPOSE = "PROPOSE"  # Ambiguous link (Loose) -> Reviewer
    PROPOSE_MULTI = "PROPOSE_MULTI"  # Confused between multiple events -> Reviewer
    NEW = "NEW"  # No match found -> Create New Event
    IGNORED = "IGNORED"  # Article too old or invalid
    ERROR = "ERROR"  # Technical failure (missing vector, etc.)


@dataclass
class Candidate:
    target_event: NewsEventModel
    article: ArticleModel
    dist: float
    rank: int


@dataclass
class ClusterResult:
    action: ClusterAction  # <--- Now uses Enum
    event: Optional[NewsEventModel]
    candidates: List[Candidate]
    reason: str


class NewsCluster:
    def __init__(self):
        self.settings = Settings()
        self.SIMILARITY_STRICT = self.settings.similarity_strict
        self.SIMILARITY_LOOSE = self.settings.similarity_loose

    def _get_advisory_lock(self, session: Session, key_str: str):
        """Acquires a Postgres Transaction-level Advisory Lock."""
        lock_id = zlib.crc32(key_str.encode("utf-8"))
        session.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": lock_id})

    def cluster_existing_article(
        self, session: Session, article: ArticleModel
    ) -> ClusterResult:

        # 0. Age Check
        pub_date = article.published_date
        if pub_date and pub_date.tzinfo is None:
            pub_date = pub_date.replace(tzinfo=timezone.utc)

        if (
            pub_date
            and pub_date < datetime.now(timezone.utc) - self.settings.cutoff_period
        ):
            return ClusterResult(
                ClusterAction.IGNORED,
                None,
                [],
                f"Article too old (> {self.settings.cutoff_period.days} days) - {pub_date}",
            )

        # 1. LOCKING PHASE
        self._get_advisory_lock(session, article.title)

        # 2. HYBRID SEARCH
        text_query = EventManager.derive_search_query(article)
        vector = article.embedding

        if vector is None or len(vector) == 0:
            return ClusterResult(
                ClusterAction.ERROR, None, [], "Missing Embedding Vector"
            )

        candidates = EventManager.search_news_events_hybrid(
            session, text_query, vector, article.published_date
        )

        # Case 0: No Matches -> New Event
        if not candidates:
            # We use commit=False so the Worker manages the transaction
            new_event = EventManager.execute_new_event_action(
                session, article, commit=False
            )
            return ClusterResult(ClusterAction.NEW, new_event, [], "No Matches Found")

        best_ev: NewsEventModel = candidates[0][0]
        rrf_score: float = candidates[0][1]
        vec_dist: float = candidates[0][2]

        # Case A: Too Distant -> Check RRF or New
        if vec_dist > self.SIMILARITY_LOOSE:
            # RRF Override: Vector says different, Keywords say same
            if rrf_score > 0.05:
                return ClusterResult(
                    ClusterAction.PROPOSE,
                    None,
                    [Candidate(best_ev, article, vec_dist, 0)],
                    f"Vector Dist ({vec_dist:.3f}) High but RRF Strong",
                )

            # Truly New
            new_event = EventManager.execute_new_event_action(
                session, article, commit=False
            )
            return ClusterResult(
                ClusterAction.NEW, new_event, [], f"Vector Dist {vec_dist:.2f} too high"
            )

        # Case B: Ambiguous Zone (Strict < Dist < Loose)
        if self.SIMILARITY_STRICT < vec_dist < self.SIMILARITY_LOOSE:
            # "Hot Hand" Heuristic
            is_fresh = False
            if best_ev.last_updated_at:
                last_upd = best_ev.last_updated_at
                if last_upd.tzinfo is None:
                    last_upd = last_upd.replace(tzinfo=timezone.utc)
                is_fresh = (
                    datetime.now(timezone.utc) - last_upd
                ).total_seconds() < 14400  # 4h

            is_big = best_ev.article_count > 10

            if is_fresh and is_big and vec_dist < 0.12:
                target_event = EventManager.link_article_to_event(
                    session, best_ev, article, commit=False
                )
                return ClusterResult(
                    ClusterAction.MERGE,
                    target_event,
                    [],
                    "Auto-Merge: Hot Topic Heuristic",
                )

            # Otherwise: Propose
            return ClusterResult(
                ClusterAction.PROPOSE,
                best_ev,
                [Candidate(best_ev, article, vec_dist, 0)],
                f"Ambiguous Zone ({vec_dist:.3f})",
            )

        # Case C: Confusion (Multiple Close Candidates)
        if len(candidates) > 1:
            second_dist = candidates[1][2]
            if (second_dist - vec_dist) < 0.05:
                options = []
                for i, (ev, _, dist) in enumerate(candidates[:3]):
                    if dist < self.SIMILARITY_LOOSE:
                        options.append(Candidate(ev, article, dist, i))
                return ClusterResult(
                    ClusterAction.PROPOSE_MULTI,
                    None,
                    options,
                    "Ambiguous Match (Close Candidates)",
                )

        # Case D: Perfect Match
        target_event = EventManager.link_article_to_event(
            session, best_ev, article, commit=False
        )
        return ClusterResult(ClusterAction.MERGE, target_event, [], "Perfect Match")
