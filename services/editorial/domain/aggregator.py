import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional
from news_events_lib.models import NewsEventModel, ArticleModel

class EventAggregator:
    """
    Centralizes the logic for updating NewsEvent statistics 
    (Centroids, Interests, Bias, Stance, Ranks) to ensure consistency.
    """

    @staticmethod
    def aggregate_basic_stats(event: NewsEventModel, article: ArticleModel):
        """
        Updates basic event stats: Article Count, Date Range, and Editorial Rank.
        Call this FIRST when linking an article.
        """
        # 1. Update Counts
        event.article_count += 1
        
        # 2. Update Date Range
        if article.published_date:
            if not event.first_article_date or article.published_date < event.first_article_date:
                event.first_article_date = article.published_date
            
            if not event.last_article_date or article.published_date > event.last_article_date:
                event.last_article_date = article.published_date

        # 3. Update Editorial Rank & Score (Hot Score Logic)
        if article.source_rank:
            # Update Best Rank (Lower is better, e.g., 1 is top)
            if event.best_source_rank is None or article.source_rank < event.best_source_rank:
                event.best_source_rank = article.source_rank

            # Calculate Editorial Weight
            # Formula: 10 / Rank. (Rank 1 = 10 pts, Rank 10 = 1 pt)
            weight = 10.0 / float(article.source_rank)
            
            # Add to cumulative score
            event.editorial_score = (event.editorial_score or 0.0) + weight

    @staticmethod
    def update_centroid(event: NewsEventModel, new_vector: List[float]):
        """
        Updates the event centroid using weighted average.
        Assumes aggregate_basic_stats has already incremented article_count.
        """
        if new_vector is None or len(new_vector) == 0: return
        
        n = event.article_count
        
        # Initialization or Reset
        if n <= 1 or event.embedding_centroid is None or len(event.embedding_centroid) == 0:
            event.embedding_centroid = new_vector
            return
            
        if n == 0: n = 1 # Safety guard against division by zero

        # Weighted Average Calculation
        # Since n includes the current article, the 'old' weight is n-1.
        current_centroid = np.array(event.embedding_centroid)
        vec = np.array(new_vector)
        
        updated = ((current_centroid * (n - 1)) + vec) / n
        event.embedding_centroid = updated.tolist()

    @staticmethod
    def aggregate_interests(event: NewsEventModel, interests: Dict[str, List[str]]|None):
        """Aggregates entity interests into the event."""
        if not interests: return

        # Copy dict to ensure SQLAlchemy tracks the change
        current = dict(event.interest_counts or {})
        
        for category, items in interests.items():
            if category not in current:
                current[category] = {}
            for item in items:
                current[category][item] = current[category].get(item, 0) + 1
        
        event.interest_counts = current

    @staticmethod
    def aggregate_main_topics(event: NewsEventModel, topics: List[str] | None):
        """Aggregates main topics into the event."""
        if not topics: return

        current = dict(event.main_topic_counts or {})
        
        for topic in topics:
            current[topic] = current.get(topic, 0) + 1
        
        event.main_topic_counts = current

    @staticmethod
    def aggregate_metadata(event: NewsEventModel, article: ArticleModel):
        """Aggregates Newspaper Bias (Source tracking) and Ownership stats."""
        if not article.newspaper: return

        # 1. Bias Distribution
        if article.newspaper.bias:
            bias = article.newspaper.bias
            bias_dist = dict(event.bias_distribution or {})
            
            existing_ids = set(bias_dist.get(bias, []))
            existing_ids.add(str(article.newspaper.id))
            
            bias_dist[bias] = list(existing_ids)
            event.bias_distribution = bias_dist

        # 2. Ownership Stats
        if article.newspaper.ownership_type:
            otype = article.newspaper.ownership_type
            own_stats = dict(event.ownership_stats or {})
            own_stats[otype] = own_stats.get(otype, 0) + 1
            event.ownership_stats = own_stats

    @staticmethod
    def aggregate_stance(event: NewsEventModel, bias: str, stance_score: float):
        """Aggregates Stance Distribution (Called by Enhancer)."""
        if not bias or stance_score is None: return

        bucket = "neutral"
        if stance_score <= -0.35: bucket = "critical"
        elif stance_score >= 0.35: bucket = "supportive"
        
        stance_dist = dict(event.stance_distribution or {})
        if bias not in stance_dist: stance_dist[bias] = {}
        stance_dist[bias][bucket] = stance_dist[bias].get(bucket, 0) + 1
        event.stance_distribution = stance_dist

    @staticmethod
    def aggregate_clickbait(event: NewsEventModel, bias: str, score: float):
        """
        Aggregates the clickbait score into a running average per bias.
        Uses stance_distribution to determine the 'N' (number of enhanced articles).
        """
        if not bias or score is None: return

        # Get N from Stance Distribution (tracks enhanced articles)
        stance_dist = event.stance_distribution or {}
        bias_stats = stance_dist.get(bias, {})
        n = sum(bias_stats.values())
        
        if n == 0: n = 1
        
        cb_dist = dict(event.clickbait_distribution or {})
        current_avg = cb_dist.get(bias, 0.0)

        # Running Average Update
        new_avg = score if n <= 1 else ((current_avg * (n - 1)) + score) / n
        
        cb_dist[bias] = new_avg
        event.clickbait_distribution = cb_dist
    
    @staticmethod
    def recalculate_event(event: NewsEventModel, articles: List[ArticleModel]):
        """
        Full recalculation of an event's stats from a list of articles.
        Useful for Merge operations or Database Audits.
        """
        if not articles: return

        event.article_count = len(articles)
        
        # 1. Dates
        dates = [a.published_date for a in articles if a.published_date]
        if dates:
            event.first_article_date = min(dates)
            event.last_article_date = max(dates)
        
        # 2. Rank & Score
        best_rank = None
        total_score = 0.0
        
        for art in articles:
            if art.source_rank:
                if best_rank is None or art.source_rank < best_rank:
                    best_rank = art.source_rank
                total_score += (10.0 / float(art.source_rank))
        
        event.best_source_rank = best_rank
        event.editorial_score = total_score
        
        # FIX: Use 'last_updated_at' instead of 'updated_at'
        event.last_updated_at = datetime.now(timezone.utc)