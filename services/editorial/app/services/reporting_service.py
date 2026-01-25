from datetime import datetime, timedelta, timezone

from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    EventStatus,
    JobStatus,
    NewsEventModel,
    NewspaperModel,
)
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session


class ReportingService:
    def __init__(self, session: Session):
        self.session = session
        self.console = Console()

    # --- METRICS & HEALTH ---

    def analyze_metrics(self) -> None:
        """
        Prints a comprehensive health report:
        - Active/Published counts
        - Missing Data Integrity
        - Blind Spot Distribution
        - Source Health (Last 24h)
        - Recent Errors
        """
        self.console.print(
            Panel.fit("📊 Editorial Metrics Analysis & Health Check", style="bold blue")
        )

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
        drafts = self.session.scalar(
            select(func.count(NewsEventModel.id)).where(
                NewsEventModel.status == EventStatus.DRAFT,
                NewsEventModel.is_active.is_(True),
            )
        )
        archived = self.session.scalar(
            select(func.count(NewsEventModel.id)).where(
                NewsEventModel.status == EventStatus.ARCHIVED
            )
        )

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")
        grid.add_row("Total Active Events:", f"[bold]{total_active}[/bold]")
        grid.add_row("Published:", f"[green]{published}[/green]")
        grid.add_row("Drafts:", f"[yellow]{drafts}[/yellow]")
        grid.add_row("Archived:", f"[dim]{archived}[/dim]")
        self.console.print(Panel(grid, title="Overview", border_style="blue"))

        # 2. Integrity
        self._check_integrity()

        # 3. Blind Spots
        self._check_blind_spots()

        # 4. Source Health
        self._check_source_health()

        # 5. Recent Errors
        self._check_recent_errors()

    def _check_integrity(self):
        self.console.print("\n[bold]🔍 Data Integrity Checks[/bold]")
        events = self.session.scalars(
            select(NewsEventModel).where(NewsEventModel.is_active.is_(True))
        ).all()

        missing_bias_events = []
        zero_score_events = []

        for e in events:
            bias_sum = (
                sum(e.article_counts_by_bias.values())
                if e.article_counts_by_bias
                else 0
            )
            if e.article_count > bias_sum:
                missing_bias_events.append((e, e.article_count - bias_sum))

            if e.article_count > 0 and e.editorial_score == 0:
                zero_score_events.append(e)

        if missing_bias_events:
            self.console.print(
                f"\n[bold red]⚠️  {len(missing_bias_events)} Events have articles with unknown Bias/Newspaper[/bold red]"
            )
        else:
            self.console.print("[green]✅ All articles have bias mappings.[/green]")

    def _check_blind_spots(self):
        self.console.print("\n[bold]⚖️  Stance & Blind Spots[/bold]")
        events = self.session.scalars(
            select(NewsEventModel).where(NewsEventModel.is_active.is_(True))
        ).all()
        blind_spots = [e for e in events if e.is_blind_spot]

        bs_left = len([e for e in blind_spots if e.blind_spot_side == "left"])
        bs_right = len([e for e in blind_spots if e.blind_spot_side == "right"])
        bs_center = len([e for e in blind_spots if e.blind_spot_side == "center"])

        self.console.print(f"Total Blind Spots: [bold]{len(blind_spots)}[/bold]")
        self.console.print(f" • Ignored by Left:  [blue]{bs_left}[/blue]")
        self.console.print(f" • Ignored by Right: [red]{bs_right}[/red]")
        self.console.print(f" • Ignored by Center:[white]{bs_center}[/white]")

    def _check_source_health(self):
        self.console.print("\n[bold]📡 Source Health (Last 24h)[/bold]")
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        stmt = (
            select(NewspaperModel.name, func.count(ArticleModel.id))
            .outerjoin(
                ArticleModel,
                (NewspaperModel.id == ArticleModel.newspaper_id)
                & (ArticleModel.published_date >= cutoff),
            )
            .group_by(NewspaperModel.name)
            .order_by(desc(func.count(ArticleModel.id)))
        )
        results = self.session.execute(stmt).all()

        t = Table(show_header=True)
        t.add_column("Source")
        t.add_column("Arts (24h)", justify="right")
        t.add_column("Status")

        for name, count in results:
            status = "[green]OK[/green]"
            if count == 0:
                status = "[bold red]SILENT[/bold red]"
            elif count < 5:
                status = "[yellow]LOW[/yellow]"
            t.add_row(name, str(count), status)
        self.console.print(t)

    def _check_recent_errors(self):
        self.console.print("\n[bold]❌ Recent Errors by Source (Last 24h)[/bold]")
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        stmt = (
            select(
                NewspaperModel.name,
                ArticlesQueueModel.msg,
                func.count(ArticlesQueueModel.id),
            )
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .join(NewspaperModel, ArticleModel.newspaper_id == NewspaperModel.id)
            .where(
                ArticlesQueueModel.status == JobStatus.FAILED,
                ArticlesQueueModel.updated_at >= cutoff,
            )
            .group_by(NewspaperModel.name, ArticlesQueueModel.msg)
            .order_by(NewspaperModel.name, desc(func.count(ArticlesQueueModel.id)))
        )
        results = self.session.execute(stmt).all()

        if results:
            t = Table(show_header=True)
            t.add_column("Source")
            t.add_column("Error")
            t.add_column("Count")
            for name, msg, count in results:
                t.add_row(name, (msg or "")[:80], str(count))
            self.console.print(t)
        else:
            self.console.print("[green]No errors recorded in the last 24h.[/green]")

    # --- STATS REPORT (8h Slots) ---

    def stats_report(self):
        """Prints processing volume and error stats bucketed by 8h slots."""
        self._report_articles_processed()
        self._report_errors_by_source()

    def _get_8h_slot(self, col):
        return func.to_timestamp(
            func.floor(func.extract("epoch", col) / 28800) * 28800
        ).label("time_slot")

    def _report_articles_processed(self):
        self.console.print(
            Panel("[bold blue]1. Articles Processed (8h Slots)[/bold blue]")
        )
        slot = self._get_8h_slot(ArticleModel.summary_date)

        query = (
            self.session.query(slot, func.count(ArticleModel.id))
            .filter(ArticleModel.summary_status == JobStatus.COMPLETED)
            .group_by(slot)
            .order_by(desc(slot))
            .limit(20)
        )
        results = query.all()

        t = Table(show_header=True, header_style="bold magenta")
        t.add_column("Time Slot")
        t.add_column("Count")
        for ts, count in results:
            t.add_row(str(ts), str(count))
        self.console.print(t)

    def _report_errors_by_source(self):
        self.console.print(
            Panel("[bold red]2. Enrich/Fetch Errors by Source (8h Slots)[/bold red]")
        )
        slot = self._get_8h_slot(ArticlesQueueModel.updated_at)

        query = (
            self.session.query(
                slot,
                NewspaperModel.name,
                ArticlesQueueModel.msg,
                func.count(ArticlesQueueModel.id),
            )
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .join(NewspaperModel, ArticleModel.newspaper_id == NewspaperModel.id)
            .filter(ArticlesQueueModel.status == JobStatus.FAILED)
            .group_by(slot, NewspaperModel.name, ArticlesQueueModel.msg)
            .order_by(desc(slot), desc(func.count(ArticlesQueueModel.id)))
            .limit(50)
        )
        results = query.all()

        if not results:
            self.console.print("[green]No errors found.[/green]\n")
            return

        t = Table(show_header=True, header_style="bold red")
        t.add_column("Time Slot")
        t.add_column("Source")
        t.add_column("Error")
        t.add_column("Count")
        for ts, src, msg, count in results:
            t.add_row(str(ts), src, (msg or "")[:60], str(count))
        self.console.print(t)
