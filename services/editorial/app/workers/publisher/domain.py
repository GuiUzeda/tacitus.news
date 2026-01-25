import enum
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from app.config import Settings
from app.utils.event_manager import EventManager

# Models
from news_events_lib.models import (
    EventsQueueModel,
    EventStatus,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import select
from sqlalchemy.orm import Session


class PublisherAction(str, enum.Enum):
    WAIT = "WAIT"
    PUBLISH = "PUBLISH"
    MERGE = "MERGE"  # Found a better parent (Auto-merge)
    PROPOSE = "PROPOSE"  # Found a potential duplicate (Ask human)
    IGNORE = "IGNORE"  # Event is dead/invalid
    ERROR = "ERROR"


@dataclass
class PublisherResult:
    action: PublisherAction
    target_event: Optional[NewsEventModel] = None
    reason: str = ""
    dist: float = 0.0
    score: float = 0.0
    insights: list[str] | None = None


class NewsPublisherDomain:
    def __init__(self):
        self.settings = Settings()

        # --- SCORING CONSTANTS ---
        self.WEIGHT_EDITORIAL_LOG_FACTOR = 40.0
        self.WEIGHT_VOLUME_LOG_FACTOR = 100.0
        self.RECENCY_MAX_BONUS = 150.0
        self.RECENCY_HALFLIFE_HOURS = 3.0

        self.CONTROVERSY_BONUS = 1.15
        self.INDEPENDENT_BONUS = 1.1
        self.FULL_SPECTRUM_BONUS = 1.5
        self.BLIND_SPOT_BONUS = 1.2

        # Collision Thresholds (Strict for Auto-Merge)
        self.COLLISION_THRESHOLD_STRICT = 0.05
        self.COLLISION_THRESHOLD_LOOSE = 0.15

        self.TOPIC_MULTIPLIERS = {
            "Politics": 1.5,
            "Economy": 1.4,
            "World": 1.3,
            "Crime": 1.2,
            "Science": 0.8,
            "Technology": 0.9,
            "Entertainment": 0.6,
            "Sports": 0.5,
            "Lifestyle": 0.4,
            "Nature": 0.4,
            "Mercado da Bola": 0.1,
            "Futebol Brasileiro": 0.1,
        }
        self.CLICKBAIT_PENALTY_THRESHOLD = 0.7

    def publish_event_job(
        self, session: Session, job: EventsQueueModel
    ) -> PublisherResult:
        event = job.event
        event.status = EventStatus.ARCHIVED
        # 1. Validation
        if not event or not event.is_active:
            return PublisherResult(PublisherAction.IGNORE, reason="Event Inactive/Dead")

        # Check for future dates (Time Travelers)
        now = datetime.now(timezone.utc)
        check_date = event.first_article_date
        if check_date and check_date.tzinfo is None:
            check_date = check_date.replace(tzinfo=timezone.utc)

        if check_date and check_date > now + timedelta(hours=1):  # Tolerance 1h
            return PublisherResult(
                PublisherAction.WAIT, reason=f"Future Event ({check_date})"
            )

        # 2. Gatekeeper: Collision Check
        # Before we publish, we check if this event is actually a duplicate of something ALREADY published.
        # This prevents flickering: We prefer to Merge INTO the published event rather than creating a new one.
        collision = self._check_collision_smart(session, event)

        if collision:
            if collision["status"] == "WAITING":
                return PublisherResult(
                    PublisherAction.WAIT, reason="Pending Merge Proposal Exists"
                )

            target_event = session.get(NewsEventModel, collision["id"])

            # Safety Check: Is target still alive?
            if target_event and target_event.is_active:
                if collision["auto_merge"]:
                    return PublisherResult(
                        PublisherAction.MERGE,
                        target_event=target_event,
                        reason="Gatekeeper: Auto-Merge",
                        dist=float(collision["score"]),
                    )
                else:
                    return PublisherResult(
                        PublisherAction.PROPOSE,
                        target_event=target_event,
                        reason="Gatekeeper: Potential Duplicate",
                        dist=float(collision["score"]),
                    )

        # 3. Scoring & Logic
        topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []
        hot_score, insights = self.calculate_spectrum_score(event, topics)

        # 4. Filters (Soft News / Low Volume)
        is_breaking = "BREAKING" in insights
        is_high_impact = "HIGH_IMPACT" in insights
        is_blind_spot = "BLIND_SPOT" in insights

        is_soft_news = any(
            t in topics for t in ["Lifestyle", "Entertainment", "Nature", "Sports"]
        )

        # Rule: Soft news needs volume or impact
        if (
            is_soft_news
            and not (is_breaking or is_high_impact)
            and event.article_count < 5
        ):
            return PublisherResult(
                PublisherAction.WAIT, reason="Soft News - Low Volume", score=hot_score
            )

        # Rule: Hard news needs at least 2 sources unless breaking/blindspot
        if (
            not (is_breaking or is_blind_spot or is_high_impact)
            and event.article_count < 2
        ):
            return PublisherResult(
                PublisherAction.WAIT,
                reason="Low Volume (Waiting for confirmation)",
                score=hot_score,
            )

        # 5. Execute State Changes (In Memory)
        event.status = EventStatus.PUBLISHED
        event.hot_score = hot_score
        event.publisher_insights = insights
        event.published_at = now

        # Blind Spot Metadata
        event.is_blind_spot = is_blind_spot
        event.blind_spot_side = None
        if is_blind_spot:
            for tag in insights:
                if tag.startswith("BS_"):
                    event.blind_spot_side = tag.replace("BS_", "").lower()

        return PublisherResult(
            PublisherAction.PUBLISH,
            reason="Published",
            score=hot_score,
            insights=insights,
        )

    def _check_collision_smart(self, session: Session, candidate: NewsEventModel):
        """
        Checks if the candidate event collides with any currently PUBLISHED event.
        CRITICAL: We ONLY return PUBLISHED events as targets to avoid frontend flickering.
        """
        if candidate.embedding_centroid is None:
            return None

        # 1. Check if already part of a proposal (don't spam)
        # Note: We check 'source_event_id' because we are the source (the new guy)
        pending_proposal = session.execute(
            select(1).where(
                MergeProposalModel.source_event_id == candidate.id,
                MergeProposalModel.status.in_(
                    [JobStatus.PENDING, JobStatus.PROCESSING]
                ),
            )
        ).scalar()

        if pending_proposal:
            return {"status": "WAITING"}

        # 2. Vector Search against PUBLISHED events only
        dist_expr = NewsEventModel.embedding_centroid.cosine_distance(
            candidate.embedding_centroid
        )

        # Exclude self and ensure target is published
        stmt = (
            select(NewsEventModel)
            .where(
                NewsEventModel.id != candidate.id,
                NewsEventModel.status
                == EventStatus.PUBLISHED,  # <--- STRICT ANTI-FLICKER FILTER
                NewsEventModel.is_active.is_(True),
                dist_expr <= self.COLLISION_THRESHOLD_LOOSE,
            )
            .order_by(dist_expr.asc())
            .limit(1)
        )

        target = session.scalars(stmt).first()
        if not target:
            return None

        # 🛡️ SPLIT IMMUNITY / HISTORY CHECK
        # If a proposal (Rejected or Pending) already exists between these two,
        # we respect that history and do NOT trigger a new collision.
        # This prevents the Publisher from re-merging events that were just Split.
        if EventManager.merge_proposal_exists(session, candidate.id, target.id):
            return None

        # Calculate Distance (Use Scalar subquery for precision)
        dist: float = (
            session.scalar(select(dist_expr).where(NewsEventModel.id == target.id))
            or 1.0
        )

        # Time check
        t_ref = target.first_article_date or target.created_at
        c_ref = candidate.first_article_date or candidate.created_at
        if t_ref.tzinfo is None:
            t_ref = t_ref.replace(tzinfo=timezone.utc)
        if c_ref.tzinfo is None:
            c_ref = c_ref.replace(tzinfo=timezone.utc)

        hours_diff = abs((c_ref - t_ref).total_seconds() / 3600.0)

        # Decision
        if dist <= self.COLLISION_THRESHOLD_STRICT and hours_diff <= 72:
            return {
                "id": target.id,
                "score": dist,
                "auto_merge": True,
                "status": "FOUND",
            }

        if dist <= self.COLLISION_THRESHOLD_LOOSE and hours_diff <= 24:
            return {
                "id": target.id,
                "score": dist,
                "auto_merge": False,
                "status": "FOUND",
            }

        return None

    def calculate_spectrum_score(
        self, event: NewsEventModel, topics: List[str]
    ) -> Tuple[float, List[str]]:
        score = 0.0
        insights = []

        # A. Editorial & Volume
        if event.editorial_score > 0:
            score += (
                math.log1p(event.editorial_score) * self.WEIGHT_EDITORIAL_LOG_FACTOR
            )

        unique_src = (
            len(event.sources_snapshot)
            if event.sources_snapshot
            else event.article_count
        )
        score += math.log1p(unique_src) * self.WEIGHT_VOLUME_LOG_FACTOR

        # B. Recency
        ref_date = event.first_article_date or event.created_at
        if ref_date.tzinfo is None:
            ref_date = ref_date.replace(tzinfo=timezone.utc)
        age_hours = max(
            0.0, (datetime.now(timezone.utc) - ref_date).total_seconds() / 3600.0
        )

        if age_hours < 72:
            recency = self.RECENCY_MAX_BONUS * (
                0.5 ** (age_hours / self.RECENCY_HALFLIFE_HOURS)
            )
            score += recency
        else:
            score -= age_hours * 0.1  # Penalty for zombie news

        # C. Topics & Impact
        if topics:
            mult = 1.0
            for t in topics:
                mult *= self.TOPIC_MULTIPLIERS.get(t.capitalize(), 1.0)
            score *= mult

        impact = event.ai_impact_score or 10

        # International Dampening
        if event.is_international and impact < 70:
            score *= 0.85

        # Impact Multiplier (Sigmoid-like)
        if impact < 30:
            semantic_mult = 0.01 + (impact / 30.0) * 0.09
        elif impact < 50:
            semantic_mult = 0.1 + ((impact - 30) / 20.0) * 0.7
        else:
            semantic_mult = 0.8 + ((impact - 50) / 50.0) * 2.2
        score *= semantic_mult

        # D. Time Gravity (Decay for old heavy stories)
        if age_hours > 6.0:
            time_decay = 1.0 / (1.0 + 0.05 * (age_hours - 6.0) ** 1.5)
            score *= time_decay

        # E. Clickbait Penalty
        if event.clickbait_distribution:
            vals = list(event.clickbait_distribution.values())
            if vals and (sum(vals) / len(vals)) > self.CLICKBAIT_PENALTY_THRESHOLD:
                score *= 0.7
                insights.append("CLICKBAIT_RISK")

        # F. Insights
        if impact >= 80:
            insights.append("HIGH_IMPACT")
        elif impact <= 30:
            insights.append("LOW_IMPACT")

        if age_hours < 6.0 and impact >= 60:
            insights.append("BREAKING")
        elif age_hours < 2.0:
            insights.append("BREAKING")

        # G. Stance/Controversy
        if event.stance_distribution:
            sup = sum(
                s.get("supportive", 0) for s in event.stance_distribution.values()
            )
            crit = sum(s.get("critical", 0) for s in event.stance_distribution.values())
            if sup > 1 and crit > 1:
                score *= self.CONTROVERSY_BONUS
                insights.append("CONTROVERSIAL")

        # H. Independent Coverage
        if event.ownership_stats and event.ownership_stats.get("independent", 0) >= 2:
            score *= self.INDEPENDENT_BONUS
            insights.append("INDEPENDENT_COVERAGE")

        # I. Bias Spectrum & Blind Spots (Tripartite Model: Progressive / Conservative / Consensus)
        bias_counts = event.article_counts_by_bias or {}

        # 1. Strict Bucketing
        # "Center" is strictly Consensus. "Leans" is strictly Partisan.
        prog_count = sum(
            bias_counts.get(k, 0) for k in ["left", "far_left", "leans_left"]
        )
        cons_count = sum(
            bias_counts.get(k, 0) for k in ["right", "far_right", "leans_right"]
        )
        census_count = sum(
            bias_counts.get(k, 0) for k in ["center", "neutral", "mainstream"]
        )

        has_prog = prog_count > 0
        has_cons = cons_count > 0
        has_census = census_count > 0

        # 2. Scoring Bonuses
        # Culture War: Both sides fighting (High Heat)
        if has_prog and has_cons:
            score *= 1.25  # Bubble Burst
            insights.append("CROSS_BUBBLE")

        # Consensus Validation: Mainstream has picked it up
        if has_census:
            score *= 1.1
            # Not explicitly an insight, just a reliability boost

        # Full Spectrum: Everyone is talking about it
        if has_prog and has_cons and has_census:
            score *= 1.3  # Replaces previous FULL_SPECTRUM_BONUS
            insights.append("FULL_SPECTRUM")

        # 3. Blind Spot Logic
        # A Blind Spot is defined by WHO IS IGNORING IT.
        if event.article_count >= 2:
            bs = False

            # BS_RIGHT (The Right is Blind): Only Progressives are talking.
            # Condition: Prog active, Cons silent, Census silent.
            if has_prog and not has_cons and not has_census:
                insights.append("BS_RIGHT")
                bs = True

            # BS_LEFT (The Left is Blind): Only Conservatives are talking.
            # Condition: Cons active, Prog silent, Census silent.
            elif has_cons and not has_prog and not has_census:
                insights.append("BS_LEFT")
                bs = True

            # BS_CENTER (The Establishment is Blind): Fringes active, Mainstream silent.
            # Condition: (Prog OR Cons active) AND Census silent.
            # Note: If ONLY Prog is active, it's BS_RIGHT (stronger signal).
            # So BS_CENTER implies purely: Mainstream is ignoring a story that has some traction elsewhere.
            # Let's refine: BS_CENTER if Census is 0, but total articles > 2 (somebody is talking).
            elif not has_census and (has_prog or has_cons):
                # Only tag BS_CENTER if it's NOT already tagged as a specific side blindspot
                # OR allow it as a secondary tag?
                # Let's make it exclusive for the "Anti-Establishment" case:
                # Both Prog AND Cons are talking, but Center is silent.
                if has_prog and has_cons:
                    insights.append("BS_CENTER")
                    bs = True

            if bs:
                score *= self.BLIND_SPOT_BONUS
                insights.append("BLIND_SPOT")

        return max(0.0, round(score, 2)), list(set(insights))
