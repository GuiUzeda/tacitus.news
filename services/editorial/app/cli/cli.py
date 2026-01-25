import asyncio
import time

import typer

# Models
from app.services.article_service import ArticleService

# Services (Standardized)
from app.services.audit_service import AuditService
from app.services.backfill import BackfillService
from app.services.event_service import EventService
from app.services.health_service import HealthService
from app.services.maintenance import MaintenanceService
from app.services.queue_service import QueueService
from app.services.review_service import ReviewService
from app.workers.cluster.domain import NewsCluster
from app.workers.publisher.domain import NewsPublisherDomain

# --- IMPORTS ---
from cli_common import SessionLocal, console, inspect_event_deep_dive

# Dashboard Render
from dashboard_render import render_dashboard
from rich.live import Live
from rich.prompt import Confirm, Prompt

app = typer.Typer(help="Tacitus Editorial CLI")
events_app = typer.Typer(help="Manage News Events")
merges_app = typer.Typer(help="Review & Merge Proposals")
queue_app = typer.Typer(help="Manage Processing Queues")
system_app = typer.Typer(help="System Maintenance & Tools")

app.add_typer(events_app, name="events")
app.add_typer(merges_app, name="merges")
app.add_typer(queue_app, name="queue")
app.add_typer(system_app, name="system")

# --- LAZY DEPENDENCIES ---
_publisher = None
_cluster = None
_nlp = None


def get_publisher():
    global _publisher
    if not _publisher:
        with console.status("Loading Publisher Domain..."):
            _publisher = NewsPublisherDomain()
    return _publisher


def get_cluster():
    global _cluster
    if not _cluster:
        with console.status("Loading Cluster Domain..."):
            _cluster = NewsCluster()
    return _cluster


# --- DASHBOARD ---
@app.command()
def dashboard(
    watch: bool = typer.Option(False, "--watch", "-w", help="Refresh every 5 seconds")
):
    """
    Show the Editorial Dashboard (Stats, Queues, Live Feed).
    """
    publisher = get_publisher()
    cluster = get_cluster()

    with SessionLocal() as session:
        if not watch:
            layout = render_dashboard(session, cluster, publisher)
            console.print(layout)
        else:
            with Live(
                render_dashboard(session, cluster, publisher), refresh_per_second=0.2
            ) as live:
                while True:
                    time.sleep(5)
                    live.update(render_dashboard(session, cluster, publisher))


# --- EVENTS ---
@events_app.command("list")
def list_events(limit: int = 50):
    """List top published events."""
    with SessionLocal() as session:
        service = EventService(session)
        events = service.get_top_events(limit)

        table = helper_create_table(["#", "Score", "Title", "Created"])
        for e in events:
            table.add_row(
                e["id"][:8], f"{e['hot_score']:.1f}", e["title"], e["created_at"]
            )
        console.print(table)


@events_app.command("show")
def show_event(event_id: str):
    """Show details for an event."""
    with SessionLocal() as session:
        service = EventService(session)
        details = service.get_event_details(event_id)
        if not details:
            console.print("[red]Event not found.[/red]")
            return

        console.print(f"[bold]{details['title']}[/bold]")
        console.print(
            f"Score: {details['hot_score']} (Editorial: {details['editorial_score']})"
        )
        console.print(f"Status: {details['status']}")
        console.print(f"Insights: {details['insights']}")


@events_app.command("inspect")
def inspect_event(event_id: str):
    """Deep dive inspector for an event."""
    inspect_event_deep_dive(event_id)


@events_app.command("boost")
def boost_event(event_id: str, amount: int = 10):
    """Boost event score."""
    with SessionLocal() as session:
        service = EventService(session)
        msg = service.boost_event(event_id, amount)
        console.print(msg)


@events_app.command("kill")
def kill_event(event_id: str):
    """Archive an event."""
    if not Confirm.ask(f"Are you sure you want to KILL (archive) event {event_id}?"):
        return
    with SessionLocal() as session:
        service = EventService(session)
        msg = service.archive_event(event_id)
        console.print(msg)


@events_app.command("dissolve")
def dissolve_event_cmd(event_id: str):
    """Dissolve event and re-queue articles."""
    if not Confirm.ask(f"Dissolve event {event_id}? Articles will be re-queued."):
        return
    with SessionLocal() as session:
        service = EventService(session)
        msg = service.dissolve_event(event_id)
        console.print(msg)


@events_app.command("approve-waiting")
def approve_waiting():
    """List and approve waiting events."""
    with SessionLocal() as session:
        service = EventService(session)
        events = service.get_waiting_events()

        if not events:
            console.print("[green]No events waiting.[/green]")
            return

        table = helper_create_table(["ID", "Title", "Reason"])
        for e in events:
            table.add_row(e["id"][:8], e["title"], e["reason"])
        console.print(table)

        target = Prompt.ask("Enter ID to approve (or 'q')")
        if target.lower() == "q":
            return

        # Find full ID if partial
        full_id = next((e["id"] for e in events if e["id"].startswith(target)), target)

        msg = service.approve_event(full_id)
        console.print(msg)


@events_app.command("recalculate")
def recalculate_event(event_id: str):
    """Recalculate aggregate metrics for a single event."""
    with SessionLocal() as session:
        service = EventService(session)
        msg = service.recalculate_event_metrics(event_id)
        console.print(msg)


# --- MERGES ---
@merges_app.command("review")
def review_merges():
    """Interactive review of merge proposals."""
    # We invoke the menu helper which uses ReviewService/Cluster logic
    from review_station import ReviewMergesMenu

    # Ensure dependencies
    cluster = get_cluster()
    menu = ReviewMergesMenu(cluster)

    choice = Prompt.ask(
        "Review [1] Article Merges or [2] Event Merges?", choices=["1", "2"]
    )
    if choice == "1":
        menu._review_article_merges()
    else:
        menu._review_event_merges()


@merges_app.command("auto-process")
def auto_process_proposal(proposal_id: str, action: str):
    """Process a specific proposal non-interactively."""
    with SessionLocal() as session:
        # We need ReviewService to have a cluster service if it's going to merge
        # But our ReviewService init handles lazy loading if None is passed?
        # Let's pass the one we have to be safe/fast
        service = ReviewService(session, get_cluster())
        msg = service.process_merge_proposal(proposal_id, action)
        console.print(msg)


# --- QUEUE ---
@queue_app.command("list")
def list_queues():
    """Show queue stats."""
    with SessionLocal() as session:
        service = AuditService(session)
        # Reuse dashboard logic or simple dump
        stats = service.get_queue_overview("all")
        console.print(stats)


@queue_app.command("retry")
def retry_failed(queue_name: str = typer.Option(None, help="Filter by queue name")):
    """Retry FAILED jobs."""
    with SessionLocal() as session:
        # We have QueueService, ArticleService, EventService all inheriting Retry logic
        # Ideally we use a unified way. EngineRoomService used to do batch.
        # Let's use QueueService for general if possible, but QueueService is abstract-ish base?
        # No, QueueService in queue_service.py has _retry_failed but it takes model_class.
        # EngineRoomService did specific updates on multiple tables.
        # Let's use EngineRoomService logic but implemented via the specialized services?
        # Or just use raw QueueService logic if I exposed it.
        # Actually, `mcp_server.py` doesn't expose a "retry all". It has `requeue_stale_items`.
        # I should probably stick to `requeue_stale` for "Reset Stuck".
        # For "Retry Failed", I might need to implement it in QueueService or keep it manual?
        # Let's use the individual services to retry their queues.

        console.print("Retrying Articles...")
        from app.services.article_service import ArticleService

        a_svc = ArticleService(session)
        c1 = a_svc.retry_queue(queue_name)

        console.print("Retrying Events...")
        from app.services.event_service import EventService

        e_svc = EventService(session)
        c2 = e_svc.retry_queue(queue_name)

        console.print(f"Retried {c1} Articles, {c2} Events.")


@queue_app.command("reset-stuck")
def reset_stuck(minutes: int = 10):
    """Reset processing jobs that are stuck."""
    with SessionLocal() as session:
        service = QueueService(session)
        res = service.requeue_stale_items(minutes)
        console.print(res)


@queue_app.command("clear")
def clear_completed():
    """Delete COMPLETED jobs."""
    with SessionLocal() as session:
        service = MaintenanceService(session)
        res = service.clear_completed_queues()
        console.print(res)


# --- SYSTEM ---
@system_app.command("check-blocks")
def check_blocks():
    """Check for HTTP 403/429 blocks."""
    with SessionLocal() as session:
        service = HealthService(session)
        console.print(service.check_blocks())


@system_app.command("recalc-scores")
def recalc_scores():
    """Recalculate all event scores."""
    if not Confirm.ask("This is heavy. Proceed?"):
        return
    with SessionLocal() as session:
        service = EventService(session)
        # This implementation in EventService uses EventManager, so it doesn't need publisher passed in
        res = service.recalculate_all_events()
        console.print(res)


@system_app.command("refetch-article")
def refetch_article_cmd(article_id: str):
    """Refetch article HTML and date."""
    with SessionLocal() as session:
        service = ArticleService(session)
        res = asyncio.run(service.refetch_article(article_id))
        console.print(res)


@system_app.command("backfill")
def backfill(type: str = typer.Argument(..., help="interests, topics, bias, stance")):
    """Run backfill jobs."""
    with SessionLocal() as session:
        service = BackfillService(session)
        if type == "interests":
            res = service.backfill_interests_batch()
        elif type == "topics":
            res = service.backfill_missing_topics_batch()
        elif type == "bias":
            res = service.backfill_bias_distribution_batch()
        elif type == "stance":
            res = service.backfill_stance_batch()
        else:
            console.print("[red]Unknown backfill type.[/red]")
            return
        console.print(res)


@system_app.command("sql")
def sql_console():
    """Run SQL console."""
    from engine_room import EngineRoomActions

    # EngineRoomActions is a helper, we can keep it or port it.
    # For now, let's reuse it as it's just a shell wrapper.
    actions = EngineRoomActions(
        None, None
    )  # Params not needed for SQL/ORM console usually?
    # Wait, EngineRoomActions init requires services.
    # Let's just instantiate it properly.
    actions = EngineRoomActions(get_cluster(), get_publisher())
    actions.run_sql_console()


@system_app.command("shell")
def shell():
    """Run ORM Shell."""
    from engine_room import EngineRoomActions

    actions = EngineRoomActions(get_cluster(), get_publisher())
    actions.run_orm_console()


# --- HELPERS ---
def helper_create_table(columns):
    from rich.table import Table

    t = Table(show_header=True, header_style="bold magenta")
    for c in columns:
        t.add_column(c)
    return t


if __name__ == "__main__":
    app()
