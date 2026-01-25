from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.services.base_service import BaseService
from news_events_lib.models import ArticleModel, NewsEventModel, NewspaperModel
from sqlalchemy import desc, func, select


class AnalyticsService(BaseService):
    """
    Service for advanced data analysis and aggregations.
    """

    def get_article_bias_distribution(self) -> List[Dict[str, Any]]:
        """
        Calculates the global distribution of articles across different biases.
        """
        stmt = (
            select(NewspaperModel.bias, func.count(ArticleModel.id).label("count"))
            .join(ArticleModel, ArticleModel.newspaper_id == NewspaperModel.id)
            .group_by(NewspaperModel.bias)
            .order_by(func.count(ArticleModel.id).desc())
        )

        results = self.session.execute(stmt).all()
        return [{"bias": r.bias, "count": r.count} for r in results]

    def get_source_volume_stats(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Gets the top newspapers by article volume.
        """
        stmt = (
            select(NewspaperModel.name, func.count(ArticleModel.id).label("count"))
            .join(ArticleModel, ArticleModel.newspaper_id == NewspaperModel.id)
            .group_by(NewspaperModel.name)
            .order_by(func.count(ArticleModel.id).desc())
            .limit(limit)
        )

        results = self.session.execute(stmt).all()
        return [{"newspaper": r.name, "count": r.count} for r in results]

    def get_global_event_stats(self) -> Dict[str, Any]:
        """
        Returns high-level statistics about events.
        """
        total_articles = self.session.scalar(select(func.count(ArticleModel.id)))
        total_events = self.session.scalar(select(func.count(NewsEventModel.id)))
        avg_articles_per_event = (
            total_articles / total_events if total_events > 0 else 0
        )

        return {
            "total_articles": total_articles,
            "total_events": total_events,
            "avg_articles_per_event": round(avg_articles_per_event, 2),
        }

    def get_last_ingestion_time(self) -> Dict[str, Any]:
        """
        Returns the timestamp of the most recently created article.
        Used for 'Heartbeat' monitoring.
        """
        stmt = (
            select(ArticleModel.created_at)
            .order_by(desc(ArticleModel.created_at))
            .limit(1)
        )
        last_time = self.session.scalar(stmt)

        if not last_time:
            return {"last_ingest": None, "minutes_ago": None, "status": "offline"}

        now = datetime.now(timezone.utc)
        diff = now - last_time
        minutes = int(diff.total_seconds() / 60)

        status = "healthy"
        if minutes > 60:
            status = "critical"
        elif minutes > 15:
            status = "warning"

        return {
            "last_ingest": last_time.isoformat(),
            "minutes_ago": minutes,
            "status": status,
        }

    def get_blind_spot_stats(self) -> Dict[str, Any]:
        """
        Returns counts of active blind spots (events ignored by one side).
        """
        stmt = (
            select(NewsEventModel.blind_spot_side, func.count(NewsEventModel.id))
            .where(NewsEventModel.is_active, NewsEventModel.is_blind_spot.is_(True))
            .group_by(NewsEventModel.blind_spot_side)
        )

        results = self.session.execute(stmt).all()
        counts = {r.blind_spot_side: r[1] for r in results}

        total = sum(counts.values())
        return {"total_blind_spots": total, "breakdown": counts}

    def get_top_topics(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Returns the top topics by volume across all active events.
        """
        from sqlalchemy import text

        # Aggregates the JSONB 'main_topic_counts' keys and values
        stmt = text("""
            SELECT key, SUM(value::int) as count
            FROM news_events, jsonb_each_text(main_topic_counts)
            WHERE is_active = true
            GROUP BY key
            ORDER BY count DESC
            LIMIT :limit
        """)
        results = self.session.execute(stmt, {"limit": limit}).all()
        return [{"topic": r[0], "count": r[1]} for r in results]

    def get_ingestion_stats_by_source(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Returns daily ingestion counts by newspaper for the last N days.
        """
        from sqlalchemy import text

        stmt = text("""
            SELECT
                DATE(a.created_at) as day,
                n.name,
                COUNT(a.id) as count
            FROM articles a
            JOIN newspapers n ON a.newspaper_id = n.id
            WHERE a.created_at >= NOW() - make_interval(days => :days)
            GROUP BY 1, 2
            ORDER BY 1 DESC, 3 DESC
        """)
        results = self.session.execute(stmt, {"days": days}).all()

        # Serialize
        return [
            {"date": r[0].isoformat(), "source": r[1], "count": r[2]} for r in results
        ]

    def get_ingestion_grid(self, windows: int = 6) -> List[Dict[str, Any]]:
        """
        Returns ingestion counts grouped by Source and 6-hour windows.
        """
        from sqlalchemy import text

        # 21600 seconds = 6 hours
        stmt = text("""
            SELECT
                n.name as source,
                to_char(to_timestamp(floor((extract('epoch' from a.created_at) / 21600 )) * 21600), 'DD HH24:MI') as slot,
                COUNT(a.id) as count
            FROM articles a
            JOIN newspapers n ON a.newspaper_id = n.id
            WHERE a.created_at >= NOW() - make_interval(hours => :hours)
            GROUP BY 1, 2
            ORDER BY 2 DESC, 1 ASC
        """)
        # windows * 6 hours
        total_hours = windows * 6
        results = self.session.execute(stmt, {"hours": total_hours}).all()

        return [{"source": r[0], "slot": r[1], "count": r[2]} for r in results]

    def get_daily_volumes(self, days: int = 14) -> List[Dict[str, Any]]:
        """
        Returns daily volume of Articles Ingested vs Events Created.
        """
        from sqlalchemy import text

        stmt = text("""
            SELECT TO_CHAR(day, 'YYYY-MM-DD') as date, type, count FROM (
                SELECT date(created_at) as day, 'Articles' as type, count(id) as count
                FROM articles
                WHERE created_at >= NOW() - make_interval(days => :days)
                GROUP BY 1
                UNION ALL
                SELECT date(created_at) as day, 'Events' as type, count(id) as count
                FROM news_events
                WHERE created_at >= NOW() - make_interval(days => :days)
                GROUP BY 1
            ) as combined
            ORDER BY day ASC
        """)
        results = self.session.execute(stmt, {"days": days}).all()
        return [{"date": r[0], "type": r[1], "count": r[2]} for r in results]

    def get_stance_distribution(self) -> List[Dict[str, Any]]:
        """
        Returns stance distribution grouped by 0.2 buckets (-1.0 to 1.0).
        """
        from sqlalchemy import text

        stmt = text("""
            SELECT
                width_bucket(stance, -1, 1, 10) as bucket,
                count(*) as count
            FROM articles
            WHERE stance IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """)
        results = self.session.execute(stmt).all()
        # bucket 1 is -1.0 to -0.8, etc.
        # Map bucket to label
        data = []
        for r in results:
            bucket = r[0]
            # range is -1 to 1 (size 2). 10 buckets -> 0.2 width
            # start = -1 + (bucket-1)*0.2
            center = -1.0 + (bucket - 1) * 0.2 + 0.1
            data.append({"bucket": bucket, "label": f"{center:.1f}", "count": r[1]})
        return data

    def get_clickbait_distribution(self) -> List[Dict[str, Any]]:
        """
        Returns clickbait score distribution (0-100) in 10 buckets.
        """
        from sqlalchemy import text

        stmt = text("""
            SELECT
                width_bucket(clickbait_score, 0, 100, 10) as bucket,
                count(*) as count
            FROM articles
            WHERE clickbait_score IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """)
        results = self.session.execute(stmt).all()
        data = []
        for r in results:
            bucket = r[0]
            # 0-100, 10 buckets -> width 10.
            # start = (bucket-1)*10
            label = f"{(bucket - 1) * 10}-{(bucket) * 10}"
            data.append({"bucket": bucket, "label": label, "count": r[1]})
        return data

    def get_queue_volume_history(
        self, queue_type: str, hours: int = 24, queue_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Returns volume history (CREATED vs terminal statuses) grouped by hour.
        Optionally filtered by queue_name (e.g. 'ENRICHER').
        """
        table_map = {
            "articles": "articles_queue",
            "events": "events_queue",
            "proposals": "merge_proposals",
        }

        if queue_type not in table_map:
            return []

        table = table_map[queue_type]

        # Determine terminal statuses
        if queue_type == "proposals":
            statuses = "('APPROVED', 'REJECTED', 'FAILED')"
        else:
            statuses = "('COMPLETED', 'FAILED')"

        from sqlalchemy import text

        # Build Where Clause
        where_clause = ""
        params = {"hours": hours}

        if queue_name and queue_type != "proposals":
            where_clause = "AND lower(queue_name::text) = lower(:qname)"
            params["qname"] = queue_name

        stmt = text(f"""
            SELECT hour, status, COUNT(*) as count FROM (
                -- Processed
                SELECT to_char(updated_at, 'YYYY-MM-DD HH24:00') as hour, status::text
                FROM {table}
                WHERE updated_at >= NOW() - make_interval(hours => :hours)
                  AND status IN {statuses}
                  {where_clause}
                UNION ALL
                -- Created
                SELECT to_char(created_at, 'YYYY-MM-DD HH24:00') as hour, 'CREATED' as status
                FROM {table}
                WHERE created_at >= NOW() - make_interval(hours => :hours)
                  {where_clause}
            ) as combined
            GROUP BY 1, 2
            ORDER BY 1 ASC
        """)

        results = self.session.execute(stmt, params).all()
        return [{"hour": r[0], "status": r[1], "count": r[2]} for r in results]
