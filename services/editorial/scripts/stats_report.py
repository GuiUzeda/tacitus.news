import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
from sqlalchemy import create_engine, func, case, desc, and_
from sqlalchemy.orm import sessionmaker
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Project Imports
from config import Settings
from news_events_lib.models import ArticleModel, NewspaperModel, JobStatus
from core.models import ArticlesQueueModel, ArticlesQueueName

console = Console()
settings = Settings()
engine = create_engine(str(settings.pg_dsn))
SessionLocal = sessionmaker(bind=engine)

def get_8h_slot(timestamp_col):
    """SQLAlchemy expression to bucket timestamps into 8-hour slots."""
    # 28800 seconds = 8 hours
    return func.to_timestamp(
        func.floor(func.extract('epoch', timestamp_col) / 28800) * 28800
    ).label('time_slot')

def report_articles_processed(session):
    console.print(Panel("[bold blue]1. Articles Processed (8h Slots)[/bold blue]"))
    
    # We use summary_date as the proxy for "Processed/Enriched"
    slot = get_8h_slot(ArticleModel.summary_date)
    
    query = (
        session.query(
            slot,
            func.count(ArticleModel.id)
        )
        .filter(ArticleModel.summary_status == JobStatus.COMPLETED)
        .group_by(slot)
        .order_by(desc(slot))
        .limit(20)
    )
    
    results = query.all()
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Time Slot (UTC)", style="dim")
    table.add_column("Count", justify="right")
    
    for ts, count in results:
        table.add_row(str(ts), str(count))
        
    console.print(table)
    console.print()

def report_articles_by_source(session):
    console.print(Panel("[bold blue]2. Articles by Source (8h Slots)[/bold blue]"))
    
    slot = get_8h_slot(ArticleModel.summary_date)
    
    query = (
        session.query(
            slot,
            NewspaperModel.name,
            func.count(ArticleModel.id)
        )
        .join(NewspaperModel, ArticleModel.newspaper_id == NewspaperModel.id)
        .filter(ArticleModel.summary_status == JobStatus.COMPLETED)
        .group_by(slot, NewspaperModel.name)
        .order_by(desc(slot), desc(func.count(ArticleModel.id)))
        .limit(50)
    )
    
    results = query.all()
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Time Slot (UTC)", style="dim")
    table.add_column("Source")
    table.add_column("Count", justify="right")
    
    for ts, name, count in results:
        table.add_row(str(ts), name, str(count))
        
    console.print(table)
    console.print()

def report_errors(session, error_type_label, filter_condition):
    console.print(Panel(f"[bold red]3/4. {error_type_label} (8h Slots)[/bold red]"))
    
    slot = get_8h_slot(ArticlesQueueModel.updated_at)
    
    # Group by Slot + Message
    query = (
        session.query(
            slot,
            ArticlesQueueModel.msg,
            func.count(ArticlesQueueModel.id)
        )
        .filter(
            ArticlesQueueModel.status == JobStatus.FAILED,
            filter_condition
        )
        .group_by(slot, ArticlesQueueModel.msg)
        .order_by(desc(slot), desc(func.count(ArticlesQueueModel.id)))
        .limit(30)
    )
    
    results = query.all()
    
    if not results:
        console.print("[green]No errors found.[/green]\n")
        return

    table = Table(show_header=True, header_style="bold red")
    table.add_column("Time Slot (UTC)", style="dim")
    table.add_column("Error Message")
    table.add_column("Count", justify="right")
    
    for ts, msg, count in results:
        # Truncate long messages
        clean_msg = (msg or "Unknown")[:80].replace("\n", " ")
        table.add_row(str(ts), clean_msg, str(count))
        
    console.print(table)
    console.print()

def report_errors_by_source(session, error_type_label, filter_condition):
    console.print(Panel(f"[bold red]5. {error_type_label} by Source (8h Slots)[/bold red]"))
    
    slot = get_8h_slot(ArticlesQueueModel.updated_at)
    
    query = (
        session.query(
            slot,
            NewspaperModel.name,
            ArticlesQueueModel.msg,
            func.count(ArticlesQueueModel.id)
        )
        .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
        .join(NewspaperModel, ArticleModel.newspaper_id == NewspaperModel.id)
        .filter(
            ArticlesQueueModel.status == JobStatus.FAILED,
            filter_condition
        )
        .group_by(slot, NewspaperModel.name, ArticlesQueueModel.msg)
        .order_by(desc(slot), desc(func.count(ArticlesQueueModel.id)))
        .limit(50)
    )
    
    results = query.all()
    
    if not results:
        console.print("[green]No errors found.[/green]\n")
        return

    table = Table(show_header=True, header_style="bold red")
    table.add_column("Time Slot (UTC)", style="dim")
    table.add_column("Source")
    table.add_column("Error Message")
    table.add_column("Count", justify="right")
    
    for ts, source, msg, count in results:
        clean_msg = (msg or "Unknown")[:60].replace("\n", " ")
        table.add_row(str(ts), source, clean_msg, str(count))
        
    console.print(table)
    console.print()

def main():
    with SessionLocal() as session:
        # 1. Articles Processed
        report_articles_processed(session)
        
        # 2. Articles by Source
        report_articles_by_source(session)
        
        # Define Filters for "Harvester" vs "Enrich" errors
        # Harvester = Fetch/Network issues
        # Enrich = NLP/Extraction issues
        
        harvester_filter = and_(
            ArticlesQueueModel.queue_name == ArticlesQueueName.ENRICH,
            (
                ArticlesQueueModel.msg.ilike('%Fetch%') | 
                ArticlesQueueModel.msg.ilike('%HTTP%') | 
                ArticlesQueueModel.msg.ilike('%Timeout%') |
                ArticlesQueueModel.msg.ilike('%Empty HTML%')
            )
        )
        
        enrich_filter = and_(
            ArticlesQueueModel.queue_name == ArticlesQueueName.ENRICH,
            ~harvester_filter
        )

        # 3. Enrich Errors (General)
        report_errors(session, "Enrichment Logic Errors", enrich_filter)
        
        # 4. Enrich Errors by Source
        report_errors_by_source(session, "Enrichment Logic Errors", enrich_filter)
        
        # 5. Harvester Errors (Fetch/Network) by Source
        # Note: This captures fetch errors that happen during the Enrichment phase.
        # Initial harvesting errors (getting the feed itself) are logged to file, not DB.
        report_errors_by_source(session, "Harvester/Fetch Errors", harvester_filter)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting...")