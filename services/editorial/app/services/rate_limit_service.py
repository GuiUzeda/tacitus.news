from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session


class RateLimitService:
    def __init__(self, session: Session):
        self.session = session

    def get_status(self) -> List[Dict[str, Any]]:
        """
        Returns the current status of all rate limits.
        Joins Config and Usage to show utilization percentages.
        """
        stmt = text("""
            SELECT
                c.model_id,
                c.metric,
                c.limit_value,
                c.window_seconds,
                COALESCE(u.current_value, 0) as current_value,
                u.window_start,
                -- Calculated fields
                (COALESCE(u.current_value, 0)::float / NULLIF(c.limit_value, 0)) * 100 as percent_used,
                u.window_start + (c.window_seconds || ' seconds')::interval as reset_at
            FROM rate_limit_config c
            LEFT JOIN rate_limit_usage u ON
                u.key = c.model_id || ':' || (c.metric::text)
                OR u.key = c.model_id || ':' || lower(c.metric::text)
            ORDER BY percent_used DESC, c.model_id
        """)

        results = self.session.execute(stmt).mappings().all()

        status_list = []
        for row in results:
            status_list.append(
                {
                    "model": row.model_id,
                    "metric": row.metric,
                    "used": row.current_value,
                    "limit": row.limit_value,
                    "percent": round(row.percent_used or 0.0, 2),
                    "reset_at": row.reset_at.isoformat() if row.reset_at else None,
                    "window_sec": row.window_seconds,
                }
            )

        return status_list

    def reset_limit(self, model_id: str, metric: str):
        """
        Manually resets a limit (e.g. for debugging or admin override).
        """
        key = f"{model_id}:{metric.upper()}"
        stmt = text("DELETE FROM rate_limit_usage WHERE key = :key")
        result = self.session.execute(stmt, {"key": key})
        self.session.commit()
        return result.rowcount > 0
