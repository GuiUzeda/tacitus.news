from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    EventStatus,
    JobStatus,
    NewsEventModel,
    NewspaperModel,
)
from sqlalchemy import desc, func, or_, select


class ReportingService:
    def __init__(self, session):
        self.session = session

    def get_daily_report(self) -> str:
        report = ["# 📊 Daily System Report"]

        # 1. Processing Volume
        proc_stats = self._query_processed_volume()
        report.append(
            f"**Processed (Last 24h):** {sum(c for _, c in proc_stats)} articles"
        )

        # 2. Errors
        errs = self._query_errors()
        if errs:
            report.append("\n### 🚨 Active Blocks (Enricher)")
            for name, msg, count in errs[:5]:
                report.append(f"- **{name}**: {count}x `{msg[:40]}...`")
        else:
            report.append("\n✅ No recent blocks detected.")

        return "\n".join(report)

    def get_system_health(self) -> str:
        """Detailed system health report including data integrity and source status."""
        report = ["# 🏥 System Health & Integrity"]

        # 1. Overview
        total_active = self.session.scalar(
            select(func.count(NewsEventModel.id)).where(
                NewsEventModel.is_active.is_(True)
            )
        )
        published = self.session.scalar(
            select(func.count(NewsEventModel.id)).where(
                NewsEventModel.status == EventStatus.PUBLISHED,
                NewsEventModel.is_active.is_(True),
            )
        )
        report.append(f"- **Total Active Events:** {total_active}")
        report.append(f"- **Published:** {published}")

        # 2. Integrity Checks
        events = self.session.scalars(
            select(NewsEventModel).where(NewsEventModel.is_active.is_(True))
        ).all()
        missing_bias = 0
        zero_score = 0

        for e in events:
            bias_sum = (
                sum(e.article_counts_by_bias.values())
                if e.article_counts_by_bias
                else 0
            )
            if e.article_count > bias_sum:
                missing_bias += 1
            if e.article_count > 0 and e.editorial_score == 0:
                zero_score += 1

        if missing_bias > 0:
            report.append(
                f"\n⚠️ **Warning:** {missing_bias} events have articles with unknown Bias/Newspaper."
            )
        if zero_score > 0:
            report.append(
                f"\n⚠️ **Warning:** {zero_score} events have 0 Editorial Score despite having articles."
            )

        # 3. Blind Spots
        blind_spots = [e for e in events if e.is_blind_spot]
        if blind_spots:
            bs_left = len([e for e in blind_spots if e.blind_spot_side == "left"])
            bs_right = len([e for e in blind_spots if e.blind_spot_side == "right"])
            report.append(f"\n### 🙈 Blind Spots ({len(blind_spots)})")
            report.append(f"- Ignored by Left: {bs_left}")
            report.append(f"- Ignored by Right: {bs_right}")
        else:
            report.append("\n✅ No active blind spots.")

        return "\n".join(report)

    def analyze_blocking(self) -> str:
        """Analyzes enrichment queue for blocking signatures (403, 429, etc)."""
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

        results = (
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
            .all()
        )

        if not results:
            return "✅ No obvious blocking detected in the Enrichment Queue."

        report = ["# 🕵️ Blocking Analysis"]

        grouped = {}
        for name, msg, count in results:
            if name not in grouped:
                grouped[name] = []
            grouped[name].append((msg, count))

        for newspaper, errors in grouped.items():
            report.append(f"\n### {newspaper}")
            total = 0
            for msg, count in errors:
                report.append(f"- `{msg}`: {count}x")
                total += count
            report.append(f"**Total:** {total}")

        return "\n".join(report)

    def _query_processed_volume(self):
        return (
            self.session.query(
                func.date(ArticleModel.summary_date), func.count(ArticleModel.id)
            )
            .filter(ArticleModel.summary_status == JobStatus.COMPLETED)
            .group_by(func.date(ArticleModel.summary_date))
            .limit(5)
            .all()
        )

    def _query_errors(self):
        return (
            self.session.query(
                NewspaperModel.name,
                ArticlesQueueModel.msg,
                func.count(ArticlesQueueModel.id),
            )
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .join(NewspaperModel, ArticleModel.newspaper_id == NewspaperModel.id)
            .filter(
                ArticlesQueueModel.status == JobStatus.FAILED,
                ArticlesQueueModel.queue_name == ArticlesQueueName.ENRICHER,
            )
            .group_by(NewspaperModel.name, ArticlesQueueModel.msg)
            .order_by(desc(func.count(ArticlesQueueModel.id)))
            .limit(10)
            .all()
        )
