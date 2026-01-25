from datetime import datetime, timezone

# Models
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    JobStatus,
    NewsEventModel,
    NewspaperModel,
)
from rich.console import Console
from sqlalchemy import desc, func, or_, select, text
from sqlalchemy.orm import Session


class HealthService:
    def __init__(self, session: Session):
        self.session = session
        self.console = Console()

    def get_db_stats(self) -> dict:
        """Returns row counts for core tables."""
        return {
            "articles": self.session.scalar(select(func.count(ArticleModel.id))),
            "events": self.session.scalar(select(func.count(NewsEventModel.id))),
            "newspapers": self.session.scalar(select(func.count(NewspaperModel.id))),
            "queue_articles": self.session.scalar(
                select(func.count(ArticlesQueueModel.id))
            ),
        }

    def get_source_health(self) -> list[dict]:
        """
        Returns list of newspapers with their last ingestion time and status.
        """
        # Subquery for last article per newspaper
        stmt = text("""
            SELECT
                n.name,
                MAX(a.created_at) as last_ingest,
                COUNT(a.id) FILTER (WHERE a.created_at > NOW() - INTERVAL '24 hours') as count_24h
            FROM newspapers n
            LEFT JOIN articles a ON n.id = a.newspaper_id
            GROUP BY n.name
            ORDER BY last_ingest ASC NULLS FIRST
        """)
        results = self.session.execute(stmt).all()

        data = []
        now = datetime.now(timezone.utc)

        for row in results:
            name, last, count_24h = row
            status = "OK"
            minutes_ago = 999999

            if last:
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                diff = now - last
                minutes_ago = int(diff.total_seconds() / 60)

                if minutes_ago > 60 * 24:  # > 24h
                    status = "SILENT"
                elif minutes_ago > 60 * 3:  # > 3h
                    status = "WARNING"
            else:
                status = "DEAD"

            data.append(
                {
                    "source": name,
                    "last_ingest": last.strftime("%Y-%m-%d %H:%M") if last else "Never",
                    "minutes_ago": minutes_ago,
                    "count_24h": count_24h or 0,
                    "status": status,
                }
            )

        return data

    def check_blocks(self) -> str:
        """Checks the Enricher Queue for HTTP 403/429 errors indicating blocking."""

        # Define blocking signatures
        blocking_conditions = or_(
            ArticlesQueueModel.msg.ilike("%HTTP 403%"),
            ArticlesQueueModel.msg.ilike("%HTTP 429%"),
            ArticlesQueueModel.msg.ilike("%HTTP 503%"),
            ArticlesQueueModel.msg.ilike("%Access Denied%"),
            ArticlesQueueModel.msg.ilike("%Cloudflare%"),
            ArticlesQueueModel.msg.ilike("%Captcha%"),
            ArticlesQueueModel.msg.ilike("%Forbidden%"),
            ArticlesQueueModel.msg.ilike("%Too Many Requests%"),
            ArticlesQueueModel.msg.ilike("%Blocked%"),
        )

        # Query grouped by Newspaper
        query = (
            self.session.query(
                NewspaperModel.name,
                ArticlesQueueModel.msg,
                func.count(ArticlesQueueModel.id),
            )
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .join(NewspaperModel, ArticleModel.newspaper_id == NewspaperModel.id)
            .filter(
                ArticlesQueueModel.queue_name == ArticlesQueueName.ENRICHER,
                ArticlesQueueModel.status == JobStatus.FAILED,
                blocking_conditions,
            )
            .group_by(NewspaperModel.name, ArticlesQueueModel.msg)
            .order_by(NewspaperModel.name, desc(func.count(ArticlesQueueModel.id)))
        )

        results = query.all()

        if not results:
            return "✅ No obvious blocking detected in the Enrichment Queue."

        # Organize by Newspaper
        grouped = {}
        for name, msg, count in results:
            if name not in grouped:
                grouped[name] = []
            grouped[name].append((msg, count))

        # Format Output
        output = []
        for newspaper, errors in grouped.items():
            total_blocks = sum(count for _, count in errors)
            output.append(f"🚨 {newspaper} (Total: {total_blocks})")
            for msg, count in errors:
                output.append(f"  - {count}x: {msg}")
            output.append("")

        return "\n".join(output)
