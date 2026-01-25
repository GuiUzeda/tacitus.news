import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from cli_common import SessionLocal, console, recalc_event_score
from menu import Menu
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    EventsQueueModel,
    EventStatus,
    FeedModel,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
    NewspaperModel,
)
from rich.console import Group
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table
from sqlalchemy import delete, func, select, text, update


class EngineRoomMenu(Menu):
    def __init__(self, cluster_service, publisher_domain):
        self.cluster_service = cluster_service
        self.publisher_domain = publisher_domain

    def render(self):
        with SessionLocal() as session:
            # 1. Gather Stats
            a_stats = (
                session.query(
                    ArticlesQueueModel.queue_name,
                    ArticlesQueueModel.status,
                    func.count(ArticlesQueueModel.id),
                )
                .group_by(ArticlesQueueModel.queue_name, ArticlesQueueModel.status)
                .all()
            )

            e_stats = (
                session.query(
                    EventsQueueModel.queue_name,
                    EventsQueueModel.status,
                    func.count(EventsQueueModel.id),
                )
                .group_by(EventsQueueModel.queue_name, EventsQueueModel.status)
                .all()
            )

            p_stats = (
                session.query(
                    MergeProposalModel.status, func.count(MergeProposalModel.id)
                )
                .group_by(MergeProposalModel.status)
                .all()
            )

        # 2. Render Tables
        t1 = self._render_queue_table("Articles", a_stats)
        t2 = self._render_queue_table("Events", e_stats)

        # Proposal Table
        ptable = Table(title="Proposals", show_header=False, box=None, expand=True)
        ptable.add_column("Status")
        ptable.add_column("Count")
        for s, c in p_stats:
            color = "red" if s == JobStatus.FAILED else "green"
            ptable.add_row(f"[{color}]{s.value}[/{color}]", str(c))

        # 3. Actions
        actions = (
            "\n[1] Retry ALL Failed Jobs\n"
            "[2] Reset ALL 'Processing' (Stuck) Jobs\n"
            "[3] Clear ALL Completed Jobs\n"
            "r Refresh  |  q Back"
        )
        return Panel(Group(t1, t2, ptable, actions), title="🚑 Queue Manager")

    def handle_input(self, choice):
        if choice == "q":
            return "back"
        if choice == "r":
            return None

        if choice == "1":
            self._batch_update_queue(JobStatus.FAILED, JobStatus.PENDING, "Retry")
        elif choice == "2":
            self._batch_update_queue(
                JobStatus.PROCESSING, JobStatus.PENDING, "Reset Stuck"
            )
        elif choice == "3":
            self._batch_delete_completed()
        return None

    def _render_queue_table(self, title, stats):
        table = Table(title=title, expand=True)
        table.add_column("Queue")
        table.add_column("Status")
        table.add_column("Count")
        for q, s, c in stats:
            color = (
                "red"
                if s == JobStatus.FAILED
                else "green" if s == JobStatus.COMPLETED else "yellow"
            )
            q_name = q.value if hasattr(q, "value") else str(q)
            s_name = s.value if hasattr(s, "value") else str(s)
            table.add_row(q_name, f"[{color}]{s_name}[/{color}]", str(c))
        return table

    def _batch_update_queue(self, from_status, to_status, action_name):
        with SessionLocal() as session:
            r1 = session.execute(
                update(ArticlesQueueModel)
                .where(ArticlesQueueModel.status == from_status)
                .values(
                    status=to_status,
                    msg=f"CLI {action_name}",
                    updated_at=datetime.now(timezone.utc),
                )
            )
            r2 = session.execute(
                update(EventsQueueModel)
                .where(EventsQueueModel.status == from_status)
                .values(
                    status=to_status,
                    msg=f"CLI {action_name}",
                    updated_at=datetime.now(timezone.utc),
                )
            )
            r3 = session.execute(
                update(MergeProposalModel)
                .where(MergeProposalModel.status == from_status)
                .values(
                    status=to_status,
                    reasoning=f"CLI {action_name}",
                    updated_at=datetime.now(timezone.utc),
                )
            )
            session.commit()
            console.print(
                f"[green]{action_name}: {r1.rowcount} Arts, {r2.rowcount} Evts, {r3.rowcount} Props[/green]"
            )
            time.sleep(1)

    def _batch_delete_completed(self):
        with SessionLocal() as session:
            r1 = session.execute(
                delete(ArticlesQueueModel).where(
                    ArticlesQueueModel.status == JobStatus.COMPLETED
                )
            )
            r2 = session.execute(
                delete(EventsQueueModel).where(
                    EventsQueueModel.status == JobStatus.COMPLETED
                )
            )
            r3 = session.execute(
                delete(MergeProposalModel).where(
                    MergeProposalModel.status.in_(
                        [JobStatus.APPROVED, JobStatus.REJECTED]
                    )
                )
            )
            session.commit()
            console.print(
                f"[green]Cleared: {r1.rowcount} Arts, {r2.rowcount} Evts, {r3.rowcount} Props[/green]"
            )
            time.sleep(1)


class EngineRoomActions:
    """Helper class for actions that don't need a full menu loop but are part of Engine Room."""

    def __init__(self, cluster_service, publisher_domain):
        self.cluster_service = cluster_service
        self.publisher_domain = publisher_domain

    def recalc_all_scores(self):
        if not Confirm.ask("Recalculate scores for ALL active events? (Heavy DB load)"):
            return

        with SessionLocal() as session:
            events = session.scalars(
                select(NewsEventModel).where(
                    NewsEventModel.is_active.is_(True),
                    NewsEventModel.status == EventStatus.PUBLISHED,
                )
            ).all()

            console.print(f"Recalculating {len(events)} events...")
            with console.status("Working..."):
                for e in events:
                    recalc_event_score(session, e, self.publisher_domain)

            console.print("[green]Done![/green]")
            time.sleep(1)

    def merge_duplicate_events(self):
        console.clear()
        console.rule("🔗 Find & Merge Duplicate Events")
        target_query = Prompt.ask("Search for the MAIN Event (Title)")
        with SessionLocal() as session:
            targets = (
                session.query(NewsEventModel)
                .filter(
                    NewsEventModel.title.ilike(f"%{target_query}%"),
                    NewsEventModel.is_active.is_(True),
                )
                .limit(10)
                .all()
            )

            if not targets:
                console.print("[red]No events found.[/red]")
                time.sleep(2)
                return

            table = Table(title="Select Target Event")
            table.add_column("#")
            table.add_column("Title")
            for i, e in enumerate(targets):
                table.add_row(str(i + 1), e.title)
            console.print(table)
            sel = IntPrompt.ask("Select #", default=1)
            target_event = targets[sel - 1]

            console.print(
                f"\n🔎 Searching duplicates for: [bold]{target_event.title}[/bold]..."
            )
            results = self.cluster_service.search_news_events_hybrid(
                session,
                target_event.search_text or target_event.title,
                target_event.embedding_centroid,
                target_date=target_event.created_at,
                limit=20,
            )
            candidates = [r[0] for r in results if r[0].id != target_event.id]
            if not candidates:
                console.print("[green]No duplicates![/green]")
                time.sleep(2)
                return

            table = Table(title="Potential Duplicates")
            table.add_column("#")
            table.add_column("Title")
            for i, ev in enumerate(candidates):
                table.add_row(str(i + 1), ev.title)
            console.print(table)

            choice = Prompt.ask("Select to merge (e.g. '1,3') or 'all'")
            indices = []
            if choice == "all":
                indices = range(len(candidates))
            else:
                try:
                    indices = [
                        int(x.strip()) - 1 for x in choice.split(",") if x.strip()
                    ]
                except ValueError:
                    return

            if Confirm.ask(f"⚠️ Merge {len(indices)} events?"):
                for idx in indices:
                    if 0 <= idx < len(candidates):
                        self.cluster_service.execute_event_merge(
                            session, candidates[idx], target_event
                        )
                session.commit()
                console.print("[green]Merged![/green]")
                time.sleep(2)

    def check_blocks(self):
        from scripts.check_blocks import check_blocks

        check_blocks()
        Prompt.ask("\nPress Enter...")

    def run_backfill(self):
        # Run as subprocess to ensure clean environment and memory management for heavy scripts
        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "scripts",
            "backfill_interests.py",
        )
        try:
            subprocess.run([sys.executable, script_path], check=True)
        except Exception as e:
            console.print(f"[bold red]Error running script:[/bold red] {e}")
        Prompt.ask("\nPress Enter...")

    def run_sql_console(self):
        console.clear()
        console.rule("💻 SQL Console")
        console.print("[dim]Type your query (SQL). Type 'exit' or 'q' to quit.[/dim]")
        console.print(
            "[dim]Shortcuts: 'tables' to list tables, 'desc <table>' to show columns.[/dim]"
        )

        while True:
            query = Prompt.ask("\nSQL")
            if query.lower() in ["q", "exit", "quit"]:
                break

            if not query.strip():
                continue

            if query.lower() == "tables":
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            elif query.lower().startswith("desc "):
                table_name = query.split(" ")[1]
                query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"

            try:
                with SessionLocal() as session:
                    stmt = text(query)
                    result = session.execute(stmt)

                    if result.returns_rows:
                        keys = result.keys()
                        rows = result.fetchall()

                        if not rows:
                            console.print("[yellow]No results found.[/yellow]")
                        else:
                            table = Table(show_header=True, header_style="bold magenta")
                            for k in keys:
                                table.add_column(k)
                            for row in rows:
                                table.add_row(*[str(c) for c in row])
                            console.print(table)
                            console.print(f"[dim]{len(rows)} rows returned.[/dim]")
                    else:
                        session.commit()
                        console.print(
                            f"[green]Executed. Rows affected: {result.rowcount}[/green]"
                        )

            except Exception as e:
                console.print(f"[bold red]Error:[/bold red] {e}")

    def run_orm_console(self):
        import code

        try:
            import readline
            import rlcompleter
        except ImportError:
            readline = None
            console.print(
                "[yellow]Readline not available. Autocomplete disabled.[/yellow]"
            )

        console.clear()
        console.rule("🐍 ORM Console (Python/SQLAlchemy)")

        session = SessionLocal()

        # Context for the shell
        ctx = {
            "session": session,
            "select": select,
            "func": func,
            "update": update,
            "delete": delete,
            "text": text,
            "NewsEventModel": NewsEventModel,
            "ArticleModel": ArticleModel,
            "MergeProposalModel": MergeProposalModel,
            "EventsQueueModel": EventsQueueModel,
            "ArticlesQueueModel": ArticlesQueueModel,
            "NewspaperModel": NewspaperModel,
            "FeedModel": FeedModel,
            "JobStatus": JobStatus,
            "EventStatus": EventStatus,
            "console": console,
            "cluster_service": self.cluster_service,
            "publisher_domain": self.publisher_domain,
        }

        if readline:
            readline.set_completer(rlcompleter.Completer(ctx).complete)
            readline.parse_and_bind("tab: complete")

        console.print(
            "[dim]Interactive Shell. Objects: session, select, NewsEventModel, ArticleModel...[/dim]"
        )
        console.print(
            "[dim]Example: session.scalars(select(NewsEventModel).limit(5)).all()[/dim]"
        )
        console.print("[dim]Ctrl+D to exit.[/dim]\n")

        try:
            code.interact(banner="", local=ctx)
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
        finally:
            session.close()
