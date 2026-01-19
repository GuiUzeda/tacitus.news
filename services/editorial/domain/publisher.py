import math
from datetime import datetime, timezone, timedelta
from typing import Tuple, List, Dict
from utils.event_manager import EventManager
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import exists, not_, or_, select, and_, desc
from news_events_lib.models import (
    NewsEventModel,
    EventStatus,
    JobStatus,
    MergeProposalModel,
)
from config import Settings
from domain.clustering import NewsCluster


class NewsPublisherDomain:
    def __init__(self):
        self.settings = Settings()
        self.cluster = NewsCluster()

        # --- CONFIGURATION (EQUILIBRADA) ---
        self.WEIGHT_EDITORIAL_LOG_FACTOR = 40.0
        self.WEIGHT_VOLUME_LOG_FACTOR = 300.0

        self.RECENCY_MAX_BONUS=150.0
        self.RECENCY_HALFLIFE_HOURS = 6.0

        # Collision Thresholds
        self.COLLISION_THRESHOLD_STRICT = 0.05
        self.COLLISION_THRESHOLD_LOOSE = 0.15

        # New Scoring Constants
        self.CONTROVERSY_BONUS = 1.15
        self.INDEPENDENT_BONUS = 1.1

        self.BASELINE_BIAS = {"left": 0.3, "center": 0.4, "right": 0.3}

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
            "Oddities": 0.3,
            "Mercado da Bola": 0.1,
            "Futebol Brasileiro": 0.1,
        }

        self.MIN_ARTICLES_FOR_BLIND_SPOT = 2
        self.CLICKBAIT_PENALTY_THRESHOLD = 0.7

    def publish_event_job(self, session: Session, job) -> bool:
        event = job.event

        if not event or not event.is_active:
            logger.warning(f"👻 Publisher ignored dead event: {job.event_id}")
            job.status = JobStatus.COMPLETED
            return False

        check_date = event.first_article_date
        if check_date and check_date.tzinfo is None:
            check_date = check_date.replace(tzinfo=timezone.utc)

        if check_date and check_date > datetime.now(timezone.utc):
            job.status = JobStatus.WAITING
            job.msg = "Future event"
            return False
        # --- 1. SMART COLLISION CHECK ---
        collision = self._check_collision_smart(session, event)

        if collision:
            if collision.get("status") == "WAITING":
                job.status = JobStatus.WAITING
                job.msg = "Paused: Pending Merge Proposal"
                return False

            existing_id = collision["id"]
            distance = collision["score"]
            is_auto = collision["auto_merge"]
            existing_event = session.get(NewsEventModel, existing_id)

            if is_auto and existing_event:
                logger.warning(
                    f"⚡ PUBLISHER AUTO-MERGE: '{event.title}' -> '{existing_event.title}' (Dist: {distance:.3f})"
                )
                EventManager.execute_event_merge(session, event, existing_event)
                job.msg = f"Auto-Merged into {existing_event.title}"
                return False

            logger.info(
                f"💡 PUBLISHER PROPOSAL: '{event.title}' ?= '{collision['title']}' (Dist: {distance:.3f})"
            )

            proposal = MergeProposalModel(
                proposal_type="event_merge",
                source_event_id=event.id,
                target_event_id=existing_id,
                distance_score=distance,
                reasoning=f"Publisher Gatekeeper (Dist: {distance:.3f})",
                status=JobStatus.PENDING,
            )
            session.add(proposal)
            job.status = JobStatus.COMPLETED
            job.msg = f"Blocked by Proposal -> {existing_id}"
            return False

        # --- 2. STANDARD PUBLISHING FLOW ---
        event_topics = (
            list(event.main_topic_counts.keys()) if event.main_topic_counts else []
        )

        hot_score, insights, metadata = self.calculate_spectrum_score(
            event, event_topics
        )

        is_breaking = "BREAKING" in insights
        is_blind_spot = "BLIND_SPOT" in insights
        is_high_impact = "HIGH_IMPACT" in insights
        is_low_impact = "LOW_IMPACT" in insights

        is_soft_news = any(
            t in event_topics
            for t in ["Lifestyle", "Entertainment", "Nature", "Sports"]
        )

        if (
            is_soft_news
            and not (is_breaking or is_high_impact)
            and event.article_count < 5
        ):
            job.status = JobStatus.WAITING
            job.msg = "Soft News: Low Volume"
            return False

        if (
            not (is_breaking or is_blind_spot or is_high_impact)
            and event.article_count < 2
        ):
            job.status = JobStatus.WAITING
            job.msg = "Low Volume: Waiting"
            return False

        self._execute_publish(session, event, job, hot_score, insights, metadata)
        return True

    def _check_collision_smart(self, session: Session, candidate_event: NewsEventModel):
        if candidate_event.embedding_centroid is None:
            return None

        pending_proposal = session.execute(
            select(MergeProposalModel.id).where(
                MergeProposalModel.source_event_id == candidate_event.id,
                MergeProposalModel.status.in_(
                    [JobStatus.PENDING, JobStatus.PROCESSING]
                ),
            )
        ).scalar()

        if pending_proposal:
            logger.info(
                f"⏸️ Event {candidate_event.id} is pending merge review. Pausing publish."
            )
            return {
                "id": None,
                "title": "Pending Proposal",
                "score": 0.0,
                "auto_merge": False,
                "status": "WAITING",
            }

        distance_expr = NewsEventModel.embedding_centroid.cosine_distance(
            candidate_event.embedding_centroid
        )

        has_proposal = exists(
            select(1).where(
                or_(
                    and_(
                        MergeProposalModel.source_event_id == candidate_event.id,
                        MergeProposalModel.target_event_id == NewsEventModel.id,
                    ),
                    and_(
                        MergeProposalModel.source_event_id == NewsEventModel.id,
                        MergeProposalModel.target_event_id == candidate_event.id,
                    ),
                )
            )
        )

        stmt = (
            select(
                NewsEventModel.id,
                NewsEventModel.title,
                NewsEventModel.first_article_date,
                NewsEventModel.created_at,
                distance_expr.label("distance"),
            )
            .where(NewsEventModel.status == EventStatus.PUBLISHED)
            .where(NewsEventModel.is_active == True)
            .where(distance_expr <= self.COLLISION_THRESHOLD_LOOSE)
            .where(~has_proposal)
            .order_by(distance_expr)
            .limit(1)
        )

        result = session.execute(stmt).first()
        if not result:
            return None

        existing_id, existing_title, existing_first_date, existing_created, distance = (
            result
        )

        existing_ref = existing_first_date or existing_created
        if existing_ref.tzinfo is None:
            existing_ref = existing_ref.replace(tzinfo=timezone.utc)

        candidate_ref = (
            candidate_event.first_article_date
            or candidate_event.created_at
            or datetime.now(timezone.utc)
        )
        if candidate_ref.tzinfo is None:
            candidate_ref = candidate_ref.replace(tzinfo=timezone.utc)

        time_diff_hours = abs((candidate_ref - existing_ref).total_seconds() / 3600.0)

        if distance <= self.COLLISION_THRESHOLD_STRICT and time_diff_hours <= 72:
            return {
                "id": existing_id,
                "title": existing_title,
                "score": distance,
                "auto_merge": True,
            }

        if distance <= self.COLLISION_THRESHOLD_LOOSE and time_diff_hours <= 24:
            return {
                "id": existing_id,
                "title": existing_title,
                "score": distance,
                "auto_merge": False,
            }

        return None

    def calculate_spectrum_score(
        self, event: NewsEventModel, topics: List[str]
    ) -> Tuple[float, List[str], Dict]:
        score = 0.0
        insights = []

        # --- A. BASE METRICS (LOGARITHMIC) ---
        if event.editorial_score > 0:
            score += (
                math.log1p(event.editorial_score) * self.WEIGHT_EDITORIAL_LOG_FACTOR
            )

        # Use unique sources count if available for volume score, falling back to article_count
        unique_sources = len(event.sources_snapshot) if event.sources_snapshot else event.article_count
        score += math.log1p(unique_sources) * self.WEIGHT_VOLUME_LOG_FACTOR

        # --- B. RECENCY CURVE (SANITIZED) ---
        ref_date = event.first_article_date or event.created_at
        if ref_date.tzinfo is None:
            ref_date = ref_date.replace(tzinfo=timezone.utc)

        # FIX 1: Prevent negative age (Future dates) exploding the score
        age_hours = (datetime.now(timezone.utc) - ref_date).total_seconds() / 3600.0
        age_hours = max(0.0, age_hours)

        # Decay logic
        if age_hours < 72:
            recency_score = self.RECENCY_MAX_BONUS * (
                0.5 ** (age_hours / self.RECENCY_HALFLIFE_HOURS)
            )
            score += recency_score
        else:
            score -= age_hours * 0.1

        # --- C. SEMANTIC & IMPACT ---
        if topics:
            best_multiplier = 1.0
            for topic in topics:
                norm_topic = topic.capitalize()
                mult = self.TOPIC_MULTIPLIERS.get(norm_topic, 1.0)
                best_multiplier *= mult
            score *= best_multiplier

        impact = event.ai_impact_score or 10
        if event.is_international:
            # Check impact to decide penalty
            impact = event.ai_impact_score or 0
            if impact < 70:
                score *= 0.85  # Dampen routine international news
            else:
                score *= 1.0   # Keep major global events full strength
        # FIX 2: Steeper Impact Curve
        # Old: 0.4 + (impact/100 * 1.6) -> Range [0.4 ... 2.0]
        # New:
        #   Impact 20 -> 0.4 (Punishment)
        #   Impact 50 -> 1.0 (Neutral)
        #   Impact 90 -> 2.5 (High Boost)
        #   Impact 100 -> 3.0
        if impact < 30:
            # Range 0.01 to 0.1
            semantic_multiplier = 0.01 + (impact / 30.0) * 0.09  # Max 0.1

        elif impact < 50:
            # Range 0.1 to .8
            semantic_multiplier = 0.1 + ((impact - 30.0) / 20.0) * 0.7  # Max 0.8
        else:
            # Range .8 to 3.0
            semantic_multiplier = 0.8 + ((impact - 50.0) / 50.0) * 2.2  # Max 3.0

        score *= semantic_multiplier

        # ---------------------------------------------------------
        # 🆕 NEW LOGIC: GLOBAL TIME GRAVITY
        # ---------------------------------------------------------
        # This ensures that even "Mega Events" eventually yield to new stories.
        # After 12 hours, the TOTAL score starts shrinking by 5% every hour.

        # Gravity kicks in after 6 hours
        if age_hours > 6.0:
            # Formula: Decay factor that gets stronger with time
            # At 6h: 1.0 (No penalty)
            # At 12h: 0.85 (15% penalty)
            # At 24h: 0.50 (50% penalty)
            time_decay = 1.0 / (1.0 + 0.05 * (age_hours - 6.0)**1.5)
            score *= time_decay
        

        # Clickbait Penalty
        if event.clickbait_distribution:
            values = list(event.clickbait_distribution.values())
            if values:
                avg_clickbait = sum(values) / len(values)
                if avg_clickbait > self.CLICKBAIT_PENALTY_THRESHOLD:
                    score *= 0.7
                    insights.append("CLICKBAIT_RISK")

        # Insights Generation
        if impact >= 80:
            insights.append("HIGH_IMPACT")
        elif impact <= 30:
            insights.append("LOW_IMPACT")

        # FIX 3: Stricter "BREAKING"
        # Must be very fresh (< 6h) AND have moderate relevance (> 60)
        # Or be extremely fresh (< 2h)
        if age_hours < 6.0 and impact >= 60:
            insights.append("BREAKING")
        elif age_hours < 2.0:
            # Freshness override for very new stuff, even if low impact
            insights.append("BREAKING")

        # --- NEW: CONTROVERSY DETECTION (Stance) ---
        if event.stance_distribution:
            total_sup = 0
            total_crit = 0
            for stats in event.stance_distribution.values():
                total_sup += stats.get("supportive", 0)
                total_crit += stats.get("critical", 0)
            
            if total_sup > 1 and total_crit > 1:
                score *= self.CONTROVERSY_BONUS
                insights.append("CONTROVERSIAL")

        # --- NEW: OWNERSHIP DIVERSITY ---
        if event.ownership_stats and event.ownership_stats.get("independent", 0) >= 2:
            score *= self.INDEPENDENT_BONUS
            insights.append("INDEPENDENT_COVERAGE")

        # --- D. SPECTRUM BONUS ---
        bias_counts = event.article_counts_by_bias or {}
        
        has_left = any("left" in k for k, v in bias_counts.items() if v > 0)
        has_right = any("right" in k for k, v in bias_counts.items() if v > 0)
        has_center = any("center" in k for k, v in bias_counts.items() if v > 0)
        
        sides_count = sum([has_left, has_right, has_center])

        if sides_count == 3:
            score *= 1.5
            insights.append("FULL_SPECTRUM")
        elif sides_count == 2:
            score *= 1.2

        # Blind Spot Logic
        if event.article_count >= self.MIN_ARTICLES_FOR_BLIND_SPOT:
            is_blind_spot = False
            if has_left and not has_right:
                insights.append("BS_LEFT")
                is_blind_spot = True
            elif has_right and not has_left:
                insights.append("BS_RIGHT")
                is_blind_spot = True
            elif has_center and not has_left and not has_right:
                insights.append("BS_CENTER")
                is_blind_spot = True
            
            if is_blind_spot:
                score *= 1.2
                insights.append("BLIND_SPOT")
        
        elif sides_count == 1:
                score *= 0.5
                insights.append("NICHE")

        return round(score, 2), insights, {"bias_counts": bias_counts}

    def _execute_publish(
        self, session: Session, event: NewsEventModel, job, score, insights, metadata
    ):
        session.refresh(event)
        if not event.is_active:
            return

        event.status = EventStatus.PUBLISHED
        event.hot_score = score

        event.publisher_isights = insights

        event.is_blind_spot = "BLIND_SPOT" in insights
        event.blind_spot_side = None
        if event.is_blind_spot:
            for tag in insights:
                if tag.startswith("BS_"):
                    event.blind_spot_side = tag.replace("BS_", "").lower()
                    break

        if not event.published_at:
            event.published_at = datetime.now(timezone.utc)

        job.status = JobStatus.COMPLETED
        job.msg = f"Published (Score: {score:.1f}) | Tags: {insights}"

        logger.success(f"🚀 {event.title[:40]}... | ID: {event.id} | Score: {score} | {insights}")

    def publish_event_direct(
        self, session: Session, event: NewsEventModel, commit: bool = True
    ):
        if not event.is_active:
            return
        if not event.title:
            logger.warning(
                f"⚠️ Direct Publish Aborted: Incomplete Metadata for {event.id}"
            )
            return
        topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []
        hot_score, insights, metadata = self.calculate_spectrum_score(event, topics)

        event.status = EventStatus.PUBLISHED
        event.hot_score = hot_score
        event.publisher_isights = insights

        event.is_blind_spot = "BLIND_SPOT" in insights
        event.blind_spot_side = None
        if event.is_blind_spot:
            for tag in insights:
                if tag.startswith("BS_"):
                    event.blind_spot_side = tag.replace("BS_", "").lower()
                    break

        event.published_at = datetime.now(timezone.utc)

        session.add(event)
        if commit:
            session.commit()
        else:
            session.flush()

        logger.success(
            f"🚀 Direct Publish: {event.title} | Score: {hot_score} | {insights}"
        )
