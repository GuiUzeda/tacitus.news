import numpy as np
from typing import Dict, List, Optional
from news_events_lib.models import NewsEventModel, ArticleModel

class EventAggregator:
    """
    Centralizes the logic for updating NewsEvent statistics 
    (Centroids, Interests, Bias, Stance) to ensure consistency 
    between the Clusterer and the Enhancer.
    """

    @staticmethod
    def update_centroid(event: NewsEventModel, new_vector: List[float]):
        """Updates the event centroid using weighted average."""
        if new_vector is None or len(new_vector) == 0: return
        
        n = event.article_count
        # If it's a new event (0 articles), the centroid is just the vector
        if n == 0:
            event.embedding_centroid = new_vector
            return

        current_centroid = np.array(event.embedding_centroid)
        vec = np.array(new_vector)
        
        # Weighted average: (Old * N + New) / (N + 1)
        updated = ((current_centroid * n) + vec) / (n + 1)
        event.embedding_centroid = updated.tolist()

    @staticmethod
    def aggregate_interests(event: NewsEventModel, interests: Dict[str, List[str]]|None):
        """Aggregates entity interests into the event."""
        if not interests: return

        # Copy to ensure SQLAlchemy detects mutation
        current = dict(event.interest_counts or {})
        
        for category, items in interests.items():
            if category not in current:
                current[category] = {}
            for item in items:
                current[category][item] = current[category].get(item, 0) + 1
        
        event.interest_counts = current

    @staticmethod
    def aggregate_metadata(event: NewsEventModel, article: ArticleModel):
        """Aggregates Newspaper Bias (Source tracking) and Ownership stats."""
        if not article.newspaper: return

        # 1. Bias Distribution (Tracking Sources)
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
        """Aggregates Stance Distribution."""
        if not bias or stance_score is None: return

        bucket = "neutral"
        if stance_score <= -0.35: bucket = "critical"
        elif stance_score >= 0.35: bucket = "supportive"
        
        stance_dist = dict(event.stance_distribution or {})
        if bias not in stance_dist: stance_dist[bias] = {}
        stance_dist[bias][bucket] = stance_dist[bias].get(bucket, 0) + 1
        event.stance_distribution = stance_dist
        
        # Update Explicit Article Counts per Bias
        counts = dict(event.article_counts_by_bias or {})
        counts[bias] = counts.get(bias, 0) + 1
        event.article_counts_by_bias = counts

    @staticmethod
    def aggregate_clickbait(event: NewsEventModel, bias: str, score: float):
        """
        Aggregates the clickbait score into a running average per bias.
        Relies on 'article_counts_by_bias' to determine the current article count (N).
        """
        if not bias or score is None: return

        # 1. Get N (Count of articles for this bias)
        # We assume aggregate_stance has run first, so the count includes the current article.
        counts = event.article_counts_by_bias or {}
        n = counts.get(bias, 0)
        
        # 2. Update Weighted Average
        cb_dist = dict(event.clickbait_distribution or {})
        current_avg = cb_dist.get(bias, 0.0)

        # Formula: ((OldAvg * (N-1)) + NewScore) / N
        # If N=1, this simplifies to NewScore / 1
        new_avg = score if n <= 1 else ((current_avg * (n - 1)) + score) / n
        
        cb_dist[bias] = new_avg
        event.clickbait_distribution = cb_dist