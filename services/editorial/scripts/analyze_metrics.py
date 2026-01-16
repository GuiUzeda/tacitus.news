import sys
import os
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, select, func, desc
from sqlalchemy.orm import sessionmaker
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Settings
from news_events_lib.models import NewsEventModel, EventStatus, NewspaperModel, ArticleModel, JobStatus
from core.models import ArticlesQueueModel

def analyze_metrics():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)
    console = Console()

    with SessionLocal() as session:
        console.print(Panel.fit("📊 Editorial Metrics Analysis & Health Check", style="bold blue"))

        # --- 1. General Overview ---
        total_active = session.scalar(select(func.count(NewsEventModel.id)).where(NewsEventModel.is_active == True))
        published = session.scalar(select(func.count(NewsEventModel.id)).where(NewsEventModel.status == EventStatus.PUBLISHED, NewsEventModel.is_active == True))
        drafts = session.scalar(select(func.count(NewsEventModel.id)).where(NewsEventModel.status == EventStatus.DRAFT, NewsEventModel.is_active == True))
        archived = session.scalar(select(func.count(NewsEventModel.id)).where(NewsEventModel.status == EventStatus.ARCHIVED))

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")
        grid.add_row("Total Active Events:", f"[bold]{total_active}[/bold]")
        grid.add_row("Published:", f"[green]{published}[/green]")
        grid.add_row("Drafts:", f"[yellow]{drafts}[/yellow]")
        grid.add_row("Archived:", f"[dim]{archived}[/dim]")
        console.print(Panel(grid, title="Overview", border_style="blue"))

        # --- 2. Data Integrity Checks ---
        console.print("\n[bold]🔍 Data Integrity Checks[/bold]")
        
        # Fetch all active events for analysis
        events = session.scalars(select(NewsEventModel).where(NewsEventModel.is_active == True)).all()
        
        missing_bias_events = []
        zero_score_events = []
        empty_published_events = []

        for e in events:
            # Check 1: Articles vs Bias Counts (Detects missing newspaper/bias mapping)
            bias_sum = sum(e.article_counts_by_bias.values()) if e.article_counts_by_bias else 0
            if e.article_count > bias_sum:
                missing_bias_events.append((e, e.article_count - bias_sum))
            
            # Check 2: Published but 0 articles
            if e.status == EventStatus.PUBLISHED and e.article_count == 0:
                empty_published_events.append(e)

            # Check 3: Articles exist but Editorial Score is 0 (Rank parsing fail?)
            if e.article_count > 0 and e.editorial_score == 0:
                zero_score_events.append(e)

        # Report Missing Bias
        if missing_bias_events:
            console.print(f"\n[bold red]⚠️  {len(missing_bias_events)} Events have articles with unknown Bias/Newspaper[/bold red]")
            t = Table(show_header=True, header_style="bold red")
            t.add_column("ID", width=8)
            t.add_column("Title")
            t.add_column("Total Arts")
            t.add_column("Missing Bias")
            for e, diff in missing_bias_events[:10]:
                t.add_row(str(e.id)[:8], e.title[:50], str(e.article_count), str(diff))
            console.print(t)
            if len(missing_bias_events) > 10:
                console.print(f"[dim]...and {len(missing_bias_events) - 10} more[/dim]")
        else:
            console.print("[green]✅ All articles have bias mappings.[/green]")

        # Report Zero Score
        if zero_score_events:
            console.print(f"\n[bold yellow]⚠️  {len(zero_score_events)} Events have articles but 0 Editorial Score[/bold yellow]")
            t = Table(show_header=True, header_style="bold yellow")
            t.add_column("ID", width=8)
            t.add_column("Title")
            t.add_column("Arts")
            for e in zero_score_events[:5]:
                t.add_row(str(e.id)[:8], e.title[:50], str(e.article_count))
            console.print(t)
        else:
            console.print("[green]✅ Editorial scoring looks healthy.[/green]")

        # --- 3. Stance & Blind Spots ---
        console.print("\n[bold]⚖️  Stance & Blind Spots[/bold]")
        
        blind_spots = [e for e in events if e.is_blind_spot]
        bs_left = len([e for e in blind_spots if e.blind_spot_side == 'left'])
        bs_right = len([e for e in blind_spots if e.blind_spot_side == 'right'])
        bs_center = len([e for e in blind_spots if e.blind_spot_side == 'center'])

        console.print(f"Total Blind Spots: [bold]{len(blind_spots)}[/bold]")
        console.print(f" • Ignored by Left:  [blue]{bs_left}[/blue]")
        console.print(f" • Ignored by Right: [red]{bs_right}[/red]")
        console.print(f" • Ignored by Center:[white]{bs_center}[/white]")

        # Stance Distribution Check (Flat stance?)
        flat_stance = [e for e in events if e.article_count > 3 and e.stance == 0.0]
        if flat_stance:
            console.print(f"\n[bold magenta]❓ {len(flat_stance)} Events have >3 articles but 0.0 Stance (Perfectly Neutral or Error?)[/bold magenta]")
            t = Table(show_header=True)
            t.add_column("Title")
            t.add_column("Arts")
            t.add_column("Bias Dist")
            for e in flat_stance[:5]:
                bias_str = ", ".join([f"{k}:{v}" for k,v in (e.article_counts_by_bias or {}).items()])
                t.add_row(e.title[:50], str(e.article_count), bias_str)
            console.print(t)

        # --- 4. Source Health ---
        console.print("\n[bold]📡 Source Health (Last 24h)[/bold]")
        
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        
        stmt = (
            select(NewspaperModel.name, func.count(ArticleModel.id))
            .outerjoin(ArticleModel, (NewspaperModel.id == ArticleModel.newspaper_id) & (ArticleModel.published_date >= cutoff))
            .group_by(NewspaperModel.name)
            .order_by(desc(func.count(ArticleModel.id)))
        )
        
        results = session.execute(stmt).all()
        
        t_sources = Table(show_header=True)
        t_sources.add_column("Source")
        t_sources.add_column("Arts (24h)", justify="right")
        t_sources.add_column("Status")
        
        for name, count in results:
            status = "[green]OK[/green]"
            if count == 0:
                status = "[bold red]SILENT[/bold red]"
            elif count < 5:
                status = "[yellow]LOW[/yellow]"
            
            t_sources.add_row(name, str(count), status)
            
        console.print(t_sources)

        # --- 5. Recent Errors (Last 24h) ---
        console.print("\n[bold]❌ Recent Errors by Source (Last 24h)[/bold]")
        
        error_stmt = (
            select(
                NewspaperModel.name, 
                ArticlesQueueModel.msg, 
                func.count(ArticlesQueueModel.id)
            )
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .join(NewspaperModel, ArticleModel.newspaper_id == NewspaperModel.id)
            .where(
                ArticlesQueueModel.status == JobStatus.FAILED,
                ArticlesQueueModel.updated_at >= cutoff
            )
            .group_by(NewspaperModel.name, ArticlesQueueModel.msg)
            .order_by(NewspaperModel.name, desc(func.count(ArticlesQueueModel.id)))
        )
        
        error_results = session.execute(error_stmt).all()
        
        if error_results:
            t_errors = Table(show_header=True)
            t_errors.add_column("Source")
            t_errors.add_column("Error Message")
            t_errors.add_column("Count", justify="right")
            
            for name, msg, count in error_results:
                clean_msg = (msg or "Unknown")[:80].replace("\n", " ")
                t_errors.add_row(name, clean_msg, str(count))
            
            console.print(t_errors)
        else:
            console.print("[green]No errors recorded in the last 24h.[/green]")

if __name__ == "__main__":
    analyze_metrics()