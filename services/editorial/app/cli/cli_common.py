import sys
import time
from datetime import datetime, timezone

from app.config import Settings
from loguru import logger
from news_events_lib.audit import receive_after_flush  # noqa: F401
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    EventsQueueModel,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from sqlalchemy import create_engine, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import joinedload, sessionmaker

# --- LOGGING CONFIG ---
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
    backtrace=False,
    diagnose=False,
)


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.opt(exception=(exc_type, exc_value, exc_traceback)).critical(
        "Uncaught exception"
    )


sys.excepthook = handle_exception
# ----------------------


console = Console()
settings = Settings()
engine = create_engine(str(settings.pg_dsn), pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


def render_summary(summary) -> str:
    """Helper to safely render summary text from dict or string."""
    if not summary:
        return ""

    if isinstance(summary, dict):
        val = summary.get("center") or summary.get("bias")
        if isinstance(val, list):
            return "\n".join(str(x) for x in val)
        return str(val) if val else ""

    return str(summary)


def recalc_event_score(session, event, publisher_domain):
    """Helper to recalc score and save immediately"""
    topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []
    new_score, insights, _ = publisher_domain.calculate_spectrum_score(event, topics)
    event.hot_score = new_score
    event.publisher_insights = insights

    # Sync Blind Spot Flags
    event.is_blind_spot = "BLIND_SPOT" in insights
    event.blind_spot_side = None
    if event.is_blind_spot:
        for tag in insights:
            if tag.startswith("BS_"):
                event.blind_spot_side = tag.replace("BS_", "").lower()
                break

    if event.summary and isinstance(event.summary, dict):
        s = dict(event.summary)
        s["insights"] = insights
        event.summary = s
    session.commit()


def dissolve_event(session, event):
    """Unlinks articles, resets them for enrichment, and deletes the event."""
    # 1. Unlink Articles & Re-queue
    articles = event.articles
    if articles:
        console.print(f"Unlinking {len(articles)} articles...")

        # Explicitly delete contents to ensure re-scrape
        # article_ids = [a.id for a in articles]

        queue_data = []
        now = datetime.now(timezone.utc)

        for art in articles:
            art.event_id = None  # Unlink

            # Strip Article Info (Reset for Re-Enrichment)
            art.title = "Unknown"
            art.subtitle = None
            art.summary = None
            art.published_date = None
            art.stance = None
            art.stance_reasoning = None
            art.clickbait_score = None
            art.clickbait_reasoning = None
            art.main_topics = None
            art.key_points = None
            art.entities = []
            art.interests = {}
            art.embedding = None
            art.summary_status = JobStatus.PENDING

            queue_data.append(
                {
                    "article_id": art.id,
                    "status": JobStatus.PENDING,
                    "queue_name": ArticlesQueueName.ENRICHER,
                    "created_at": now,
                    "updated_at": now,
                    "attempts": 0,
                    "msg": f"Dissolved from Event {event.id}",
                }
            )

        session.add_all(articles)

        if queue_data:
            stmt = insert(ArticlesQueueModel).values(queue_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=["article_id"],
                set_={
                    "status": JobStatus.PENDING,
                    "queue_name": ArticlesQueueName.ENRICHER,
                    "updated_at": now,
                    "msg": stmt.excluded.msg,
                },
            )
            session.execute(stmt)

    # 2. Cleanup Dependencies
    session.execute(
        delete(MergeProposalModel).where(
            (MergeProposalModel.source_event_id == event.id)
            | (MergeProposalModel.target_event_id == event.id)
        )
    )
    session.execute(
        delete(EventsQueueModel).where(EventsQueueModel.event_id == event.id)
    )

    # 3. Delete Event
    session.delete(event)
    session.commit()
    console.print("[green]Event Dissolved![/green]")
    time.sleep(1)


def read_article_content(article):
    """Displays article content and summary."""
    console.clear()
    console.rule(f"Reading: {article.title}")

    console.print(
        f"[link={article.original_url}]{article.original_url}[/link]",
        style="blue underline",
        no_wrap=True,
        overflow="ignore",
    )
    print()

    content_text = "No content found."
    with SessionLocal() as session:
        fresh_article = session.get(ArticleModel, article.id)
        if fresh_article and fresh_article.content:
            content_text = fresh_article.content

        if fresh_article and fresh_article.summary:
            console.print(Panel(render_summary(fresh_article.summary), title="Summary"))

        console.print(Markdown(content_text))

    Prompt.ask("\nPress Enter...")


def inspect_event_deep_dive(event_id):
    """Interactive loop to inspect an event and its articles."""
    while True:
        console.clear()
        with SessionLocal() as session:
            event = (
                session.query(NewsEventModel)
                .options(joinedload(NewsEventModel.articles))
                .get(event_id)
            )
            if not event:
                return

            console.rule("[bold blue]EVENT INSPECTOR[/bold blue]")
            console.print(f"[bold size=14]{event.title}[/bold size=14]")
            console.print(f"ID: {event.id} | Articles: {len(event.articles)}")

            if event.summary:
                sum_text = render_summary(event.summary)
                console.print(Panel(sum_text, title="Summary", border_style="green"))

            table = Table(title="Linked Articles", show_header=True)
            table.add_column("#", width=4)
            table.add_column("Title")
            table.add_column("Date")

            sorted_articles = sorted(
                event.articles,
                key=lambda x: x.published_date or datetime.min,
                reverse=True,
            )
            for i, art in enumerate(sorted_articles):
                table.add_row(
                    str(i + 1),
                    art.title,
                    str(art.published_date.date() if art.published_date else "?"),
                )
            console.print(table)

            console.print(
                "\n[blue]#[/blue] Read Article - #   [green]b[/green] Back - b "
            )
            choice = Prompt.ask("Action").lower()
            if choice == "b":
                return
            if choice.isdigit() and 1 <= int(choice) <= len(sorted_articles):
                read_article_content(sorted_articles[int(choice) - 1])
