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
        
        # --- CONFIGURATION (EQUILIBRADA) ---
        
        # FIX 1: PESOS LOGARÍTMICOS
        # Achata a curva. Editorial Score 300 vira ~230 pontos, não 600.
        # Permite que eventos novos compitam sem precisarmos de bônus absurdos.
        self.WEIGHT_EDITORIAL_LOG_FACTOR = 40.0 
        self.WEIGHT_VOLUME_LOG_FACTOR = 10.0
        
        # FIX 2: RECÊNCIA MODERADA
        # Bônus máximo de 180 (suficiente para destaque, mas não para vencer eventos gigantes sozinhos)
        # Meia-vida de 24h: A notícia permanece "fresca" por um dia inteiro, caindo suavemente.
        self.RECENCY_MAX_BONUS = 180.0
        self.RECENCY_HALFLIFE_HOURS = 24.0
        
        # Baseline de Feed (Ajuste conforme seu feeds.json)
        self.BASELINE_BIAS = {
            "left": 0.3,
            "center": 0.4,
            "right": 0.3
        }
        
        self.TOPIC_MULTIPLIERS = {
            "Politics": 1.5, "Economy": 1.4, "World": 1.3, "Crime": 1.2,
            "Science": 0.8, "Technology": 0.9, "Entertainment": 0.6,
            "Sports": 0.5, "Lifestyle": 0.4, "Nature": 0.4, "Oddities": 0.3, 
            "Mercado da Bola": 0.1, "Futebol Brasileiro": 0.1
        }

        self.MIN_ARTICLES_FOR_BLIND_SPOT = 3
        self.CLICKBAIT_PENALTY_THRESHOLD = 0.7 

    def publish_event_job(self, session: Session, job) -> bool:
        event = job.event
        
        if not event or not event.is_active:
            logger.warning(f"👻 Publisher ignored dead event: {job.event_id}")
            job.status = JobStatus.COMPLETED 
            return False

        event_topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []

        # Calculate Score
        hot_score, insights, metadata = self.calculate_spectrum_score(event, event_topics)
        
        # Quality Gates
        is_breaking = "BREAKING" in insights
        is_blind_spot = "BLIND_SPOT" in insights
        is_high_impact = "HIGH_IMPACT" in insights
        is_soft_news = any(t in event_topics for t in ["Lifestyle", "Entertainment", "Nature", "Sports"])
        
        if is_soft_news and not (is_breaking or is_high_impact) and event.article_count < 5:
             job.status = JobStatus.WAITING
             job.msg = "Soft News: Low Volume"
             return False

        if not (is_breaking or is_blind_spot or is_high_impact) and event.article_count < 2:
            job.status = JobStatus.WAITING
            job.msg = "Low Volume: Waiting"
            return False

        self._execute_publish(session, event, job, hot_score, insights, metadata)
        return True

    def calculate_spectrum_score(self, event: NewsEventModel, topics: List[str]) -> Tuple[float, List[str], Dict]:
        score = 0.0
        insights = []
        
        # --- A. BASE METRICS (LOGARITHMIC) ---
        # Importante: Substituímos a multiplicação linear por Logaritmo
        if event.editorial_score > 0:
            score += (math.log1p(event.editorial_score) * self.WEIGHT_EDITORIAL_LOG_FACTOR)
        
        score += (math.log1p(event.article_count) * self.WEIGHT_VOLUME_LOG_FACTOR)

        # --- B. RECENCY CURVE (Suavizada) ---
        ref_date = event.first_article_date or event.created_at
        if ref_date.tzinfo is None: ref_date = ref_date.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - ref_date).total_seconds() / 3600.0
        
        # Decay: 
        # 0h  = +180 pts
        # 12h = +127 pts
        # 24h = +90 pts
        # 48h = +45 pts
        if age_hours < 72:
            recency_score = self.RECENCY_MAX_BONUS * (0.5 ** (age_hours / self.RECENCY_HALFLIFE_HOURS))
            score += recency_score
            
            # Tag Breaking apenas para as primeiras ~12-15 horas
            if recency_score > 120:
                insights.append("BREAKING")
        else:
            # Penalidade leve para arquivo
            score -= (age_hours * 0.1)

        # --- C. SEMANTIC & IMPACT ---
        if topics:
            best_multiplier = 1.0
            for topic in topics:
                norm_topic = topic.capitalize() 
                mult = self.TOPIC_MULTIPLIERS.get(norm_topic, 1.0)
                best_multiplier *= mult
            score *= best_multiplier
            
        impact = event.ai_impact_score or 50 
        semantic_multiplier = 0.4 + (impact / 100 * 1.6)
        score *= semantic_multiplier
        
        # Clickbait Penalty
        if event.clickbait_distribution:
            values = list(event.clickbait_distribution.values())
            if values:
                avg_clickbait = sum(values) / len(values)
                if avg_clickbait > self.CLICKBAIT_PENALTY_THRESHOLD:
                    score *= 0.7 
                    insights.append("CLICKBAIT_RISK")
                    
        if impact >= 85: insights.append("HIGH_IMPACT")
        elif impact <= 30: insights.append("LOW_IMPACT")

        # --- D. SPECTRUM BONUS ---
        bias_counts = event.article_counts_by_bias or {}
        sides = {k: v for k, v in bias_counts.items() if k in ['left', 'center', 'right'] and v > 0}
        total_sides = len(sides)
        
        if total_sides == 3:
            score *= 1.5 
            insights.append("FULL_SPECTRUM")
        elif total_sides == 2:
            score *= 1.2
        elif total_sides == 1:
            dominant_side = list(sides.keys())[0]
            baseline_prob = self.BASELINE_BIAS.get(dominant_side, 0.33)
            blind_spot_multiplier = 1.0 + (1.0 - baseline_prob)

            if event.article_count >= self.MIN_ARTICLES_FOR_BLIND_SPOT:
                score *= (1.2 * blind_spot_multiplier)
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
        
    def publish_event_direct(self, session: Session, event: NewsEventModel):
        """
        Directly scores and publishes an event. 
        Designed for the Splitter to immediately put new sub-events live.
        """
        if not event.is_active:
            return

        # 1. Calculate Score (Uses your new Balanced Logic)
        topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []
        hot_score, insights, metadata = self.calculate_spectrum_score(event, topics)

        # 2. Quality Gate (Simplified)
        # Since this came from a Mega-Event split, we trust it has volume.
        # We only check if the LLM failed to generate a title or impact score.
        if not event.title or event.ai_impact_score is None:
            logger.warning(f"⚠️ Direct Publish Aborted: Incomplete Metadata for {event.id}")
            return

        # 3. Execute Publish
        # We don't need the 'job' wrapper here.
        event.status = EventStatus.PUBLISHED
        event.hot_score = hot_score
        
        # Update insights in the summary JSON
        # if event.summary and isinstance(event.summary, dict):
        #     summary_update = dict(event.summary)
        #     summary_update["insights"] = insights
        #     event.summary = summary_update
        
        # Important: Set published_at to NOW to give it a fresh start on the feed
        event.published_at = datetime.now(timezone.utc)
        
        session.add(event)
        session.commit()
        
        logger.success(f"🚀 Direct Publish: {event.title} | Score: {hot_score} | {insights}")