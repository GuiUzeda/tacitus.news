import json
import time

from cli_common import (
    SessionLocal,
    console,
    dissolve_event,
    inspect_event_deep_dive,
    recalc_event_score,
    render_summary,
)
from menu import Menu
from news_events_lib.models import EventStatus, NewsEventModel
from rich.console import Group
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.syntax import Syntax
from rich.table import Table
from sqlalchemy import desc, select


class InspectEventJsonMenu(Menu):
    def __init__(self, event_id):
        self.event_id = event_id

    def render(self):
        with SessionLocal() as session:
            event = session.get(NewsEventModel, self.event_id)
            if not event:
                return Panel("[red]Event not found.[/red]")

            # Serialize model to dict
            data = {c.name: getattr(event, c.name) for c in event.__table__.columns}

            # Add articles for context
            data["articles"] = [
                {
                    "id": a.id,
                    "title": a.title,
                    "source": a.newspaper.name if a.newspaper else "Unknown",
                    "url": a.original_url,
                }
                for a in event.articles
            ]

            json_str = json.dumps(data, indent=2, default=str)

            # Use pager for large content
            with console.pager(styles=True):
                console.print(
                    Syntax(
                        json_str,
                        "json",
                        theme="monokai",
                        line_numbers=True,
                        word_wrap=True,
                    )
                )

            return Panel(
                "[green]JSON data displayed in pager.[/green]\n(Press Enter to return)",
                title=f"JSON Inspector: {event.title}",
                expand=True,
            )

    def handle_input(self, choice):
        return "back"


class EventManageMenu(Menu):
    def __init__(self, event_id, publisher_domain):
        self.event_id = event_id
        self.publisher_domain = publisher_domain

    def render(self):
        with SessionLocal() as session:
            event = session.get(NewsEventModel, self.event_id)
            if not event:
                return Panel("[red]Event not found.[/red]")

            # --- HEADER ---
            info_table = Table.grid(padding=(0, 2))
            info_table.add_row(f"Title: {event.title}")
            info_table.add_row(f"Subtitle: {event.subtitle}")
            info_table.add_row(f"Summary: {render_summary(event.summary)}")

            info_table.add_row(
                f"ID: {event.id}",
                f"Status: [{event.status.value}]",
                f"Active: {event.is_active}",
            )
            info_table.add_row(
                f"Articles: {event.article_count}",
                f"Published: {event.published_at}",
                "",
            )
            info_table.add_row(
                f"Last Updated: {event.last_updated_at}",
                f"Last Article: {event.last_article_date}",
                "",
            )

            # --- SCORE BREAKDOWN (The 'Why') ---
            # topics = (
            #     list(event.main_topic_counts.keys()) if event.main_topic_counts else []
            # )
            # Dry run calculation
            score = event.ai_impact_score
            insights = event.publisher_insights

            score_panel = Panel(
                Group(
                    f"Current Hot Score: [bold cyan]{event.hot_score:.2f}[/bold cyan] (Calc: {score:.2f})",
                    f"Editorial Boost: [bold yellow]{event.editorial_score}[/bold yellow]",
                    f"Impact Score: [bold yellow]{event.ai_impact_score}[/bold yellow] - {event.ai_impact_reasoning}",
                    f"Insights: {insights}",
                ),
                title="Score Analysis",
            )

            # --- MENU ---
            actions = (
                "\n[bold green]b[/bold green] : Boost (+10)    [bold red]d[/bold red] : Demote (-10)\n"
                "[bold red]k[/bold red] : KILL (Archive)  [bold cyan]e[/bold cyan] : Edit Title\n"
                "i : Inspect Content    j : JSON Data\n"
                "[bold red]x[/bold red] : Dissolve (Reset Articles)\n"
                "q : Back"
            )

            return Panel(
                Group(info_table, score_panel, actions),
                title=f"Managing: {event.title}",
            )

    def handle_input(self, choice):
        action = choice.lower()
        if action == "q":
            return "back"

        with SessionLocal() as session:
            event = session.get(NewsEventModel, self.event_id)
            if not event:
                return "back"

            if action == "b":
                event.editorial_score = (event.editorial_score or 0) + 10
                recalc_event_score(session, event, self.publisher_domain)
                console.print("[green]Boosted![/green]")
                time.sleep(0.5)

            elif action == "d":
                event.editorial_score = (event.editorial_score or 0) - 10
                recalc_event_score(session, event, self.publisher_domain)
                console.print("[red]Demoted![/red]")
                time.sleep(0.5)

            elif action == "k":
                if Confirm.ask("Are you sure you want to KILL this event?"):
                    event.status = EventStatus.ARCHIVED
                    event.is_active = False
                    session.commit()
                    return "back"

            elif action == "e":
                new_t = Prompt.ask("New Title", default=event.title)
                event.title = new_t
                session.commit()

            elif action == "i":
                inspect_event_deep_dive(event.id)

            elif action == "j":
                return InspectEventJsonMenu(event.id)

            elif action == "x":
                if Confirm.ask(
                    "⚠️ DISSOLVE Event? This will unlink articles and re-queue them for Enrichment."
                ):
                    dissolve_event(session, event)
                    return "back"
        return None


class LiveDeskMenu(Menu):
    def __init__(self, publisher_domain):
        self.publisher_domain = publisher_domain
        self.events = []

    def render(self):
        with SessionLocal() as session:
            self.events = session.scalars(
                select(NewsEventModel)
                .where(NewsEventModel.status == EventStatus.PUBLISHED)
                .order_by(desc(NewsEventModel.hot_score))
                .limit(50)
            ).all()

            if not self.events:
                return Panel(
                    "[yellow]No published events.[/yellow]", title="Live Feed Control"
                )

            table = Table(show_header=True, header_style="bold blue", expand=True)
            table.add_column("#", width=3, style="dim")
            table.add_column("Score", width=6, justify="right")
            table.add_column("Edit.", width=5, justify="right", style="cyan")
            table.add_column("Imp.", width=5, justify="right", style="cyan")
            table.add_column("Title")
            table.add_column("Tags", style="magenta italic")
            table.add_column("Published")
            table.add_column("Last Artc.")

            for i, e in enumerate(self.events):
                tags = []
                if e.summary and isinstance(e.summary, dict):
                    tags = e.summary.get("insights", [])[:2]  # Show top 2 tags

                table.add_row(
                    str(i + 1),
                    f"{e.hot_score:.1f}",
                    f"{e.editorial_score:+.0f}" if e.editorial_score else "0",
                    f"{e.ai_impact_score}" if e.ai_impact_score else "50",
                    e.title[:60],
                    ", ".join(tags),
                    e.created_at.strftime("%Y-%m-%d") if e.created_at else "-",
                    (
                        e.last_article_date.strftime("%Y-%m-%d")
                        if e.last_article_date
                        else "-"
                    ),
                )
            return Panel(table, title="📰 Live Feed Control (Top 50)")

    def handle_input(self, choice):
        if choice == "q":
            return "back"
        if choice.isdigit() and 1 <= int(choice) <= len(self.events):
            event = self.events[int(choice) - 1]
            return EventManageMenu(event.id, self.publisher_domain)
        return None
