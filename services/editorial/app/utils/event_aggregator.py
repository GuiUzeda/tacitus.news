from datetime import datetime, timezone
from typing import Dict, List

from news_events_lib.models import ArticleModel, NewsEventModel


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
            if (
                not event.first_article_date
                or article.published_date < event.first_article_date
            ):
                event.first_article_date = article.published_date

            if (
                not event.last_article_date
                or article.published_date > event.last_article_date
            ):
                event.last_article_date = article.published_date

        # 3. Update Editorial Rank & Score (Hot Score Logic)
        if article.source_rank:
            # Update Best Rank (Lower is better, e.g., 1 is top)
            if (
                event.best_source_rank is None
                or article.source_rank < event.best_source_rank
            ):
                event.best_source_rank = article.source_rank

            # Calculate Editorial Weight
            # Formula: 10 / Rank. (Rank 1 = 10 pts, Rank 10 = 1 pt)
            weight = 10.0 / float(article.source_rank)

            # Add to cumulative score
            event.editorial_score = (event.editorial_score or 0.0) + weight

    @staticmethod
    def update_centroid(event: NewsEventModel, new_vector: List[float]):
        """
        Updates the event centroid using incremental mean formula.
        Assumes aggregate_basic_stats has already incremented article_count.
        """
        if new_vector is None or len(new_vector) == 0:
            return

        n = event.article_count

        # Initialization or Reset
        if (
            n <= 1
            or event.embedding_centroid is None
            or len(event.embedding_centroid) == 0
        ):
            event.embedding_centroid = new_vector
            return

        # Optimization: Saturation Threshold
        # For large clusters, the centroid stabilizes. Skipping updates saves CPU/DB.
        if n > 500:
            return

        # Stable Online Update: new_avg = old_avg + (new_val - old_avg) / n
        # Pure Python is faster here than Numpy due to array creation overhead for single vectors.
        event.embedding_centroid = [
            c + (v - c) / n for c, v in zip(event.embedding_centroid, new_vector)
        ]

    @staticmethod
    def aggregate_interests(
        event: NewsEventModel, interests: Dict[str, List[str]] | None
    ):
        """Aggregates entity interests into the event."""
        if not interests:
            return

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
        if not topics:
            return

        current = dict(event.main_topic_counts or {})

        for topic in topics:
            current[topic] = current.get(topic, 0) + 1

        event.main_topic_counts = current

    @staticmethod
    def aggregate_metadata(event: NewsEventModel, article: ArticleModel):
        """Aggregates Newspaper Bias (Source tracking) and Ownership stats."""
        if not article.newspaper:
            return

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
        """
        Aggregates Stance Distribution using a balanced approach.
        Global stance is the average of per-bias averages to neutralize volume bias.
        """
        if not bias or stance_score is None:
            return

        bucket = "neutral"
        if stance_score <= -0.35:
            bucket = "critical"
        elif stance_score >= 0.35:
            bucket = "supportive"

        stance_dist = dict(event.stance_distribution or {})
        if bias not in stance_dist:
            stance_dist[bias] = {
                "critical": 0,
                "supportive": 0,
                "neutral": 0,
                "total": 0.0,
            }

        # 1. Update Bias Average (Incremental)
        stats = stance_dist[bias]
        current_n = (
            stats.get("critical", 0)
            + stats.get("supportive", 0)
            + stats.get("neutral", 0)
        )
        current_avg = stats.get("total", 0.0)

        new_n = current_n + 1
        stats[bucket] = stats.get(bucket, 0) + 1
        stats["total"] = current_avg + (stance_score - current_avg) / new_n

        event.stance_distribution = stance_dist

        # 2. Update Global Stance (Balanced)
        # We take the mean of all available bias averages
        all_bias_averages = [
            s.get("total", 0.0)
            for s in stance_dist.values()
            if (s.get("critical", 0) + s.get("supportive", 0) + s.get("neutral", 0)) > 0
        ]
        if all_bias_averages:
            event.stance = sum(all_bias_averages) / len(all_bias_averages)

    @staticmethod
    def aggregate_clickbait(event: NewsEventModel, bias: str, score: float):
        """
        Aggregates the clickbait score into a running average per bias.
        """
        if not bias or score is None:
            return

        stance_dist = event.stance_distribution or {}
        bias_stats = stance_dist.get(bias, {})
        # Total number of ENHANCED articles for this bias
        n = (
            bias_stats.get("critical", 0)
            + bias_stats.get("supportive", 0)
            + bias_stats.get("neutral", 0)
        )

        if n == 0:
            n = 1

        cb_dist = dict(event.clickbait_distribution or {})
        current_avg = cb_dist.get(bias, 0.0)

        # Running Average Update
        new_avg = score if n <= 1 else ((current_avg * (n - 1)) + score) / n

        cb_dist[bias] = new_avg
        event.clickbait_distribution = cb_dist

    @staticmethod
    def aggregate_bias_counts(event: NewsEventModel, article: ArticleModel):
        """Aggregates the count of articles per bias."""
        if not article.newspaper or not article.newspaper.bias:
            return

        bias = article.newspaper.bias
        current = dict(event.article_counts_by_bias or {})
        current[bias] = current.get(bias, 0) + 1
        event.article_counts_by_bias = current

    @staticmethod
    def aggregate_source_snapshot(event: NewsEventModel, article: ArticleModel):
        """Updates the snapshot of sources covering the event."""
        if not article.newspaper:
            return

        name = article.newspaper.name
        snapshot = dict(event.sources_snapshot or {})

        # Store metadata for the source
        snapshot[name] = {
            "icon": article.newspaper.icon_url,
            "name": name,
            "id": str(article.newspaper.id),
            "logo": article.newspaper.logo_url,
            "bias": article.newspaper.bias,
        }
        event.sources_snapshot = snapshot

    @staticmethod
    def merge_event_stats(target: NewsEventModel, source: NewsEventModel):
        """
        Merges statistics from source event into target event.
        Handles all counters, distributions, and metadata.
        """
        # 1. Scalars
        target.article_count += source.article_count
        target.editorial_score = (target.editorial_score or 0.0) + (
            source.editorial_score or 0.0
        )

        if source.best_source_rank:
            if (
                target.best_source_rank is None
                or source.best_source_rank < target.best_source_rank
            ):
                target.best_source_rank = source.best_source_rank

        # 2. Dates
        if source.first_article_date:
            if (
                not target.first_article_date
                or source.first_article_date < target.first_article_date
            ):
                target.first_article_date = source.first_article_date

        if source.last_article_date:
            if (
                not target.last_article_date
                or source.last_article_date > target.last_article_date
            ):
                target.last_article_date = source.last_article_date

        # 3. Simple Dictionaries (Sum Values)
        for field in ["main_topic_counts", "ownership_stats", "article_counts_by_bias"]:
            source_dict = getattr(source, field) or {}
            target_dict = dict(getattr(target, field) or {})
            for k, v in source_dict.items():
                target_dict[k] = target_dict.get(k, 0) + v
            setattr(target, field, target_dict)

        # 4. Nested Dictionaries (Sum Values)
        for field in ["interest_counts"]:
            source_dict = getattr(source, field) or {}
            target_dict = dict(getattr(target, field) or {})
            for cat, items in source_dict.items():
                if cat not in target_dict:
                    target_dict[cat] = {}
                for item, count in items.items():
                    target_dict[cat][item] = target_dict[cat].get(item, 0) + count
            setattr(target, field, target_dict)

        # 4b. Stance Distribution (Weighted Merge)
        if source.stance_distribution:
            s_dist = source.stance_distribution
            t_dist = dict(target.stance_distribution or {})

            # Helper to get count from a distribution entry
            get_n = (
                lambda d: d.get("critical", 0)
                + d.get("supportive", 0)
                + d.get("neutral", 0)
            )

            # Merge Global Stance
            s_total_n = sum(get_n(d) for d in s_dist.values())
            t_total_n = sum(get_n(d) for d in t_dist.values())

            if (s_total_n + t_total_n) > 0:
                target.stance = (
                    ((target.stance or 0.0) * t_total_n)
                    + ((source.stance or 0.0) * s_total_n)
                ) / (s_total_n + t_total_n)

            # Merge Per-Bias Distribution
            for bias, s_stats in s_dist.items():
                if bias not in t_dist:
                    t_dist[bias] = s_stats.copy()
                else:
                    t_stats = t_dist[bias]
                    s_n = get_n(s_stats)
                    t_n = get_n(t_stats)

                    # Weighted Average for Total
                    if (s_n + t_n) > 0:
                        t_stats["total"] = (
                            (t_stats.get("total", 0.0) * t_n)
                            + (s_stats.get("total", 0.0) * s_n)
                        ) / (s_n + t_n)

                    for k in ["critical", "supportive", "neutral"]:
                        t_stats[k] = t_stats.get(k, 0) + s_stats.get(k, 0)

            target.stance_distribution = t_dist

        # 5. Bias Distribution (Merge Lists)
        if source.bias_distribution:
            target_dist = dict(target.bias_distribution or {})
            for bias, ids in source.bias_distribution.items():
                existing = set(target_dist.get(bias, []))
                existing.update(ids)
                target_dist[bias] = list(existing)
            target.bias_distribution = target_dist

        # 6. Clickbait Distribution (Weighted Average)
        # Weighting based on stance_distribution counts (enhanced articles)
        if source.clickbait_distribution:
            target_cb = dict(target.clickbait_distribution or {})
            source_cb = source.clickbait_distribution

            def get_enhanced_count(ev, b):
                dist = ev.stance_distribution or {}
                if b in dist:
                    return sum(dist[b].values())
                return 0

            for bias, s_score in source_cb.items():
                t_score = target_cb.get(bias)
                s_count = get_enhanced_count(source, bias)

                if t_score is None:
                    target_cb[bias] = s_score
                else:
                    t_count = get_enhanced_count(target, bias)
                    total = s_count + t_count
                    if total > 0:
                        target_cb[bias] = (
                            (t_score * t_count) + (s_score * s_count)
                        ) / total

            target.clickbait_distribution = target_cb

        # 7. Sources Snapshot (Update)
        if source.sources_snapshot:
            target_snap = dict(target.sources_snapshot or {})
            target_snap.update(source.sources_snapshot)
            target.sources_snapshot = target_snap

        # 8. Publisher Insights (Union)
        if source.publisher_insights:
            current = set(target.publisher_insights or [])
            current.update(source.publisher_insights)
            target.publisher_insights = list(current)

    @staticmethod
    def recalculate_event(event: NewsEventModel, articles: List[ArticleModel]):
        """
        Full recalculation of an event's stats from a list of articles.
        Useful for Merge operations or Database Audits.
        """
        if not articles:
            return

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
                total_score += 10.0 / float(art.source_rank)

        # 3. Stance (Average)
        stance_sum = 0.0
        stance_n = 0
        for art in articles:
            if art.newspaper and art.stance is not None:
                stance_sum += art.stance
                stance_n += 1

        event.stance = (stance_sum / stance_n) if stance_n > 0 else 0.0

        event.best_source_rank = best_rank
        event.editorial_score = total_score

        # FIX: Use 'last_updated_at' instead of 'updated_at'
        event.last_updated_at = datetime.now(timezone.utc)
