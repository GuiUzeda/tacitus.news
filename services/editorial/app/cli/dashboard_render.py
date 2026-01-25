from app.services.audit_service import AuditService
from app.services.event_service import EventService
from rich.align import Align
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table


def render_dashboard(session, cluster_service, publisher_domain) -> Layout:
    """
    Renders the main dashboard layout.
    """
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=3),
    )

    # 1. Header (Stats)
    audit = AuditService(session)
    q_stats = audit.get_queue_overview("all")

    # Live Desk for top events
    # Note: EventService doesn't need publisher_domain for get_top_events
    event_service = EventService(session)
    top_events = event_service.get_top_events(limit=10)

    # Custom quick stats for header
    from news_events_lib.models import (
        EventsQueueModel,
        EventStatus,
        JobStatus,
        NewsEventModel,
    )

    # Quick counts
    active_count = session.query(NewsEventModel).filter_by(is_active=True).count()
    published_count = (
        session.query(NewsEventModel).filter_by(status=EventStatus.PUBLISHED).count()
    )
    waiting_count = (
        session.query(EventsQueueModel).filter_by(status=JobStatus.WAITING).count()
    )

    failed_jobs = 0
    # q_stats structure: {'articles': [{'queue':..., 'status':..., 'count':...}], 'events': ..., 'proposals': ...}
    for cat in q_stats.values():
        for item in cat:
            if item.get("status") == "FAILED":
                failed_jobs += item["count"]

    header_content = (
        f"[bold green]Live: {published_count}[/bold green]   |   "
        f"[bold yellow]Waiting: {waiting_count}[/bold yellow]   |   "
        f"[bold blue]Total Active: {active_count}[/bold blue]   |   "
        f"[bold red]System Failures: {failed_jobs}[/bold red]"
    )
    layout["header"].update(Panel(Align.center(header_content), style="white on black"))

    # 2. Body - Split into Queues and Events
    layout["body"].split_row(
        Layout(name="left", ratio=1), Layout(name="right", ratio=2)
    )

    # Left: Queues
    q_table = Table(title="Queues", expand=True, box=None)
    q_table.add_column("Queue")
    q_table.add_column("Status")
    q_table.add_column("Cnt")

    all_queues = []
    # Normalize structure for display
    # AuditService returns: {'articles': [{'queue': 'ENRICHER', ...}], ...}
    for cat_name, items in q_stats.items():
        for item in items:
            q_name = item.get(
                "queue", cat_name
            )  # Proposals don't have queue name, use category
            all_queues.append((cat_name, q_name, item.get("status"), item.get("count")))

    # Sort by fail first
    all_queues.sort(key=lambda x: (0 if x[2] == "FAILED" else 1, x[0]))

    for cat, q, s, c in all_queues:
        color = "red" if s == "FAILED" else "green" if s == "COMPLETED" else "yellow"
        # Cleanup Enum string if present
        q_label = (
            str(q).replace("ArticlesQueueName.", "").replace("EventsQueueName.", "")
        )
        q_table.add_row(q_label, f"[{color}]{s}[/{color}]", str(c))

    layout["left"].update(Panel(q_table, title="System Status"))

    # Right: Live Feed
    e_table = Table(title="Top Live Events", expand=True, box=None)
    e_table.add_column("Score", justify="right", style="cyan")
    e_table.add_column("Title")
    e_table.add_column("Last Upd", style="dim")

    for e in top_events:
        e_table.add_row(
            f"{e['hot_score']:.1f}",
            e["title"][:50],
            e["created_at"][:10] if e["created_at"] else "-",
        )

    layout["right"].update(Panel(e_table, title="Live Desk (Top 10)"))

    # Footer
    layout["footer"].update(
        Align.center("[dim]Press Ctrl+C to exit dashboard loop[/dim]")
    )

    return layout
