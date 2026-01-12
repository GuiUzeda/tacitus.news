import math
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy.orm import Session

from news_events_lib.models import NewsEventModel, EventStatus, JobStatus

class NewsPublisherDomain:
    def __init__(self):
        # Configurable Weights
        self.WEIGHT_ARTICLE_COUNT = 10.0
        self.WEIGHT_RECENCY_BONUS = 50.0
        self.MIN_ARTICLES_AUTO = 4
        self.MIN_SUMMARY_LEN = 50

    def publish_event_job(self, session: Session, job) -> bool:
        """
        Evaluates an event for publication.
        Returns True if published, False if rejected/skipped.
        """
        event = job.event
        if not event:
            self._fail_job(job, "Event not found")
            return False

        # 1. Quality Gates (Deterministic Checks)
        reasons = []
        
        # Check A: Volume (Is this actually news or just one noise article?)
        if event.article_count < self.MIN_ARTICLES_AUTO:
            reasons.append(f"Low volume ({event.article_count} < {self.MIN_ARTICLES_AUTO})")

        # Check B: Completeness (Did the Enhancer do its job?)
        summary_text = self._get_summary_text(event)
        if len(summary_text) < self.MIN_SUMMARY_LEN:
            reasons.append("Summary missing or too short")

        # 2. Decision
        if reasons:
            # ❌ Manual Review Needed
            # We mark as FAILED so it leaves the pending loop. 
            # Ideally, you'd have a separate 'NEEDS_REVIEW' status, but FAILED works for the CLI queue manager.
            self._fail_job(job, f"Auto-Skip: {', '.join(reasons)}")
            logger.info(f"⚠️ Skipped: {event.title} -> Manual Review")
            return False

        # 3. ✅ Publish
        self._execute_publish(event, job)
        return True

    def _execute_publish(self, event: NewsEventModel, job):
        # Calculate the "Hot Score" for the front page
        hot_score = self.calculate_hot_score(event)
        
        event.status = EventStatus.PUBLISHED
        event.is_active = True
        event.hot_score = hot_score  # Ensure your DB model has this column!
        
        # If this is the first time publishing, set the date
        # (Events might be re-published if they get updated)
        if not event.published_at:
             event.published_at = datetime.now(timezone.utc)

        job.status = JobStatus.COMPLETED
        job.msg = f"Published (Score: {hot_score:.1f})"
        
        logger.success(f"🚀 Published: {event.title} [Score: {hot_score:.1f}]")

    def calculate_hot_score(self, event: NewsEventModel) -> float:
        """
        Calculates ranking score.
        Formula: (Articles * 10) + (Velocity Bonus)
        """
        score = 0.0
        
        # A. Volume Weight
        # Logarithmic scale prevents a mega-event (1000 articles) from breaking the UI forever
        # but linear (article_count * 10) is fine for smaller scale.
        score += float(event.article_count) * self.WEIGHT_ARTICLE_COUNT
        
        # B. Breaking News Bonus (Velocity)
        # If created < 4 hours ago, it gets a massive boost
        now = datetime.now(timezone.utc)
        age_hours = (now - event.created_at).total_seconds() / 3600.0
        
        if age_hours < 4.0:
            # Decays linearly from 50 down to 0 over 4 hours
            bonus = self.WEIGHT_RECENCY_BONUS * (1 - (age_hours / 4.0))
            score += max(0, bonus)

        return round(score, 2)

    def _get_summary_text(self, event):
        if event.summary and isinstance(event.summary, dict):
            return event.summary.get("center") or event.summary.get("bias") or ""
        return ""

    def _fail_job(self, job, msg):
        job.status = JobStatus.FAILED
        job.msg = msg