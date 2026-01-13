import math
from datetime import datetime, timezone
from typing import Tuple, List, Dict
from loguru import logger
from sqlalchemy.orm import Session
from news_events_lib.models import NewsEventModel, EventStatus, JobStatus
from config import Settings

class NewsPublisherDomain:
    def __init__(self):
        self.settings = Settings()
        
        # --- CONFIGURATION ---
        self.WEIGHT_EDITORIAL = 2.0
        self.WEIGHT_VOLUME = 4.0
        
        # FIX 1: RECENCY DECAY CONFIG
        self.RECENCY_MAX_BONUS = 400.0
        self.RECENCY_HALFLIFE_HOURS = 6.0 
        
        # FIX 2: FEED BASELINE (Approximate your feeds.json distribution)
        # This prevents "Center" from dominating just because you have more Center feeds.
        # Check your feeds.json to tune this.
        self.BASELINE_BIAS = {
            "left": 0.3,   # 30% of my feeds
            "center": 0.4, # 40% of my feeds
            "right": 0.3   # 30% of my feeds
        }
        
        self.TOPIC_MULTIPLIERS = {
            "Politics": 1.5, "Economy": 1.4, "World": 1.3, "Crime": 1.2,
            "Science": 0.8, "Technology": 0.9, "Entertainment": 0.6,
            "Sports": 0.5, "Lifestyle": 0.4, "Nature": 0.4, "Oddities": 0.3, "Mercado da Bola": 0.1, "Futebol Brasileiro": 0.1
        }

        # Thresholds
        self.MIN_ARTICLES_FOR_BLIND_SPOT = 3
        self.CLICKBAIT_PENALTY_THRESHOLD = 0.7 

    def publish_event_job(self, session: Session, job) -> bool:
        event = job.event
        
        if not event or not event.is_active:
            logger.warning(f"👻 Publisher ignored dead event: {job.event_id}")
            job.status = JobStatus.COMPLETED 
            return False

        # 1. Extract Topics safely from main_topic_counts (Dict)
        # keys are topics, values are frequency. We just need existence for now.
        event_topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []

        # 2. Calculate Score
        hot_score, insights, metadata = self.calculate_spectrum_score(event, event_topics)
        
        # 3. Quality Gates
        is_breaking = "BREAKING" in insights
        is_blind_spot = "BLIND_SPOT" in insights
        is_high_impact = "HIGH_IMPACT" in insights
        # Gate: "Soft News" requires more volume
        is_soft_news = any(t in event_topics for t in ["Lifestyle", "Entertainment", "Nature", "Sports"])
        
        if is_soft_news and not (is_breaking or is_high_impact) and event.article_count < 5:
             job.status = JobStatus.WAITING
             job.msg = "Soft News: Low Volume"
             return False

        # Standard Volume Gate
        if not (is_breaking or is_blind_spot or is_high_impact) and event.article_count < 2:
            job.status = JobStatus.WAITING
            job.msg = "Low Volume: Waiting"
            return False

        # 4. Publish
        self._execute_publish(session, event, job, hot_score, insights, metadata)
        return True

    def calculate_spectrum_score(self, event: NewsEventModel, topics: List[str]) -> Tuple[float, List[str], Dict]:
        score = 0.0
        insights = []
        
        # --- A. BASE METRICS ---
        score += (event.editorial_score * self.WEIGHT_EDITORIAL)
        score += (math.log1p(event.article_count) * self.WEIGHT_VOLUME)

        # --- FIX 1: SMOOTH DECAY CURVE ---
        ref_date = event.first_article_date or event.created_at
        if ref_date.tzinfo is None: ref_date = ref_date.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - ref_date).total_seconds() / 3600.0
        
        # Decay Formula: 40 * (0.5 ^ (age / 12))
        # 0h = 40pts | 12h = 20pts | 24h = 10pts
        if age_hours < 48:
            recency_score = self.RECENCY_MAX_BONUS * (0.5 ** (age_hours / self.RECENCY_HALFLIFE_HOURS))
            score += recency_score
            if recency_score > 200: # Arbitrary threshold for "Breaking" tag
                insights.append("BREAKING")
        else:
             # Archive penalty for very old stuff
            score -= (age_hours * 0.5)

        # --- B. SEMANTIC LAYER (Topics & Clickbait) ---
        
        # [Same Topic Logic as before] ...
        if topics:
            best_multiplier = 1.0
            for topic in topics:
                norm_topic = topic.capitalize() 
                mult = self.TOPIC_MULTIPLIERS.get(norm_topic, 1.0)
                best_multiplier*=mult
                # # Logic: Boost if good topic exists, Downgrade only if ALL are bad
                # if mult > best_multiplier: 
                #     best_multiplier = mult
                # elif mult < 1.0 and best_multiplier == 1.0:
                #      best_multiplier = mult
            
            score *= best_multiplier
        impact = event.ai_impact_score or 50 # Default to neutral if missing
        
        # Normalize impact (0-100) to a multiplier (e.g., 0.5x to 2.0x)
        # Impact 20 (Gossip) -> 0.6x multiplier
        # Impact 90 (War) -> 1.8x multiplier
        semantic_multiplier = 0.4 + (impact / 100 * 1.6)
        
        score *= semantic_multiplier
        # 2. Clickbait Penalty (From clickbait_distribution)
        # clickbait_distribution is {"left": 0.8, "center": 0.2}
        if event.clickbait_distribution:
            values = list(event.clickbait_distribution.values())
            if values:
                avg_clickbait = sum(values) / len(values)
                if avg_clickbait > self.CLICKBAIT_PENALTY_THRESHOLD:
                    score *= 0.7 
                    insights.append("CLICKBAIT_RISK")
        if impact >= 85:
            insights.append("HIGH_IMPACT")
        elif impact <= 30:
            insights.append("LOW_IMPACT")
        # --- C. SPECTRUM ANALYSIS ---
        bias_counts = event.article_counts_by_bias or {}
        sides = {k: v for k, v in bias_counts.items() if k in ['left', 'center', 'right'] and v > 0}
        total_sides = len(sides)
        
        if total_sides == 3:
            score *= 1.8 
            insights.append("FULL_SPECTRUM")
        elif total_sides == 2:
            score *= 1.2
        elif total_sides == 1:
            dominant_side = list(sides.keys())[0]
            
            # --- FIX: USE BASELINE ---
            baseline_prob = self.BASELINE_BIAS.get(dominant_side, 0.33)
            
            # Boost logic: If baseline is LOW (e.g. 0.1), multiplier is HIGH (1.9)
            # If baseline is HIGH (e.g. 0.6), multiplier is LOW (1.4)
            blind_spot_multiplier = 1.0 + (1.0 - baseline_prob)

            if event.article_count >= self.MIN_ARTICLES_FOR_BLIND_SPOT:
                score *= (1.2 * blind_spot_multiplier) # Scale boost by rarity
                insights.append(f"ONLY_{dominant_side.upper()}")
                insights.append("BLIND_SPOT")
            else:
                score *= 0.8
                insights.append("NICHE")

        return round(score, 2), insights, {"bias_counts": bias_counts}

    def _execute_publish(self, session: Session, event: NewsEventModel, job, score, insights, metadata):
        session.refresh(event)
        if not event.is_active: return

        event.status = EventStatus.PUBLISHED
        event.hot_score = score
        
        if event.summary and isinstance(event.summary, dict):
            summary_update = dict(event.summary)
            summary_update["insights"] = insights
            event.summary = summary_update
        
        if not event.published_at:
             event.published_at = datetime.now(timezone.utc)

        job.status = JobStatus.COMPLETED
        job.msg = f"Published (Score: {score:.1f}) | Tags: {insights}"
        
        logger.success(f"🚀 {event.title[:40]}... | Score: {score} | {insights}")