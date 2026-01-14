import sys
import os
from sqlalchemy import create_engine, func, desc, or_
from sqlalchemy.orm import sessionmaker
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Path setup
current_dir = os.path.dirname(os.path.abspath(__file__))
editorial_dir = os.path.dirname(current_dir)
sys.path.append(editorial_dir)

# Try to add common to path
project_root = os.path.abspath(os.path.join(editorial_dir, "../../"))
common_path = os.path.join(project_root, "common")
if common_path not in sys.path:
    sys.path.append(common_path)

from config import Settings
from news_events_lib.models import ArticleModel, NewspaperModel, JobStatus
from core.models import ArticlesQueueModel, ArticlesQueueName

console = Console()
settings = Settings()
engine = create_engine(str(settings.pg_dsn))
SessionLocal = sessionmaker(bind=engine)

def check_blocks():
    with SessionLocal() as session:
        console.print(Panel("[bold red]🕵️  Blocking Inspector[/bold red]", subtitle="Checking Enricher Queue for HTTP 403/429"))
        
        # Define blocking signatures
        blocking_conditions = or_(
            ArticlesQueueModel.msg.ilike('%HTTP 403%'),
            ArticlesQueueModel.msg.ilike('%HTTP 429%'),
            ArticlesQueueModel.msg.ilike('%HTTP 503%'),
            ArticlesQueueModel.msg.ilike('%Access Denied%'),
            ArticlesQueueModel.msg.ilike('%Cloudflare%'),
            ArticlesQueueModel.msg.ilike('%Captcha%'),
            ArticlesQueueModel.msg.ilike('%Forbidden%'),
            ArticlesQueueModel.msg.ilike('%Too Many Requests%'),
            ArticlesQueueModel.msg.ilike('%Blocked%')
        )

        # Query grouped by Newspaper
        query = (
            session.query(
                NewspaperModel.name,
                ArticlesQueueModel.msg,
                func.count(ArticlesQueueModel.id)
            )
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .join(NewspaperModel, ArticleModel.newspaper_id == NewspaperModel.id)
            .filter(
                ArticlesQueueModel.queue_name == ArticlesQueueName.ENRICH,
                ArticlesQueueModel.status == JobStatus.FAILED,
                blocking_conditions
            )
            .group_by(NewspaperModel.name, ArticlesQueueModel.msg)
            .order_by(NewspaperModel.name, desc(func.count(ArticlesQueueModel.id)))
        )

        results = query.all()

        if not results:
            console.print("[green]✅ No obvious blocking detected in the Enrichment Queue.[/green]")
            return

        # Organize by Newspaper
        grouped = {}
        for name, msg, count in results:
            if name not in grouped:
                grouped[name] = []
            grouped[name].append((msg, count))

        # Display
        for newspaper, errors in grouped.items():
            table = Table(title=f"🚨 {newspaper}", show_header=True, header_style="bold red", expand=True)
            table.add_column("Error Message")
            table.add_column("Count", justify="right")
            
            total_blocks = 0
            for msg, count in errors:
                table.add_row(msg, str(count))
                total_blocks += count
            
            console.print(table)
            console.print(f"[bold]Total Potential Blocks for {newspaper}: {total_blocks}[/bold]\n")

if __name__ == "__main__":
    try:
        check_blocks()
    except KeyboardInterrupt:
        print("\nExiting...")