import time
from datetime import datetime, timezone

from cli_common import SessionLocal, console, recalc_event_score
from menu import Menu
from news_events_lib.models import (
    ArticleModel,
    EventsQueueModel,
    EventStatus,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from rich.align import Align
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table
from sqlalchemy import desc, func, select
from sqlalchemy.orm import joinedload


class ReviewWaitingMenu(Menu):
    def __init__(self, publisher_domain):
        self.publisher_domain = publisher_domain
        self.events = []

    def render(self):
        with SessionLocal() as session:
            self.events = session.scalars(
                select(NewsEventModel)
                .join(EventsQueueModel, NewsEventModel.id == EventsQueueModel.event_id)
                .where(EventsQueueModel.status == JobStatus.WAITING)
                .order_by(desc(NewsEventModel.created_at))
            ).all()

            if not self.events:
                return Panel(
                    Align.center("[green]No events waiting for approval.[/green]"),
                    title="Waiting Room",
                )

            table = Table(show_header=True, expand=True)
            table.add_column("#", width=3)
            table.add_column("Title")
            table.add_column("Reason (Est.)")

            for i, e in enumerate(self.events):
                reason = "Low Volume"
                if e.article_count > 5:
                    reason = "Topic/Niche"
                table.add_row(str(i + 1), e.title[:60], reason)

            return Panel(table, title="⏳ Waiting Room (Approvals)")

    def handle_input(self, choice):
        if choice == "q":
            return "back"
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(self.events):
                e = self.events[idx]
                if Confirm.ask(f"Publish '{e.title}'?"):
                    with SessionLocal() as session:
                        e = session.get(NewsEventModel, e.id)
                        e.status = EventStatus.PUBLISHED
                        e.published_at = datetime.now(timezone.utc)
                        recalc_event_score(session, e, self.publisher_domain)
                        session.commit()
                return None
        return None


class ReviewMergesMenu(Menu):
    def __init__(self, cluster_service):
        self.cluster_service = cluster_service

    def render(self):
        with SessionLocal() as session:
            a_count = session.scalar(
                select(func.count(MergeProposalModel.id)).where(
                    MergeProposalModel.status == JobStatus.PENDING,
                    MergeProposalModel.source_article_id.is_not(None),
                )
            )
            e_count = session.scalar(
                select(func.count(MergeProposalModel.id)).where(
                    MergeProposalModel.status == JobStatus.PENDING,
                    MergeProposalModel.source_event_id.is_not(None),
                )
            )

            return Panel(
                f"[1] Article Merges ({a_count})\n[2] Event Merges ({e_count})\nq Back",
                title="🕵️ Review Merges",
            )

    def handle_input(self, choice):
        if choice == "q":
            return "back"
        if choice == "1":
            self._review_article_merges()
        if choice == "2":
            self._review_event_merges()
        return None

    def _review_article_merges(self):
        while True:
            console.clear()
            with SessionLocal() as session:
                proposal = session.scalars(
                    select(MergeProposalModel)
                    .options(
                        joinedload(MergeProposalModel.source_article),
                        joinedload(MergeProposalModel.target_event),
                    )
                    .where(
                        MergeProposalModel.status == JobStatus.PENDING,
                        MergeProposalModel.source_article_id.is_not(None),
                    )
                    .order_by(MergeProposalModel.distance_score)
                    .limit(1)
                ).first()

                if not proposal:
                    console.print("[green]No more article merges.[/green]")
                    time.sleep(1)
                    return

                article = proposal.source_article
                event = proposal.target_event

                if not article or not event:
                    proposal.status = JobStatus.FAILED
                    session.commit()
                    continue

                console.rule(f"Article Merge Proposal ({proposal.distance_score:.3f})")
                console.print(f"Reason: {proposal.reasoning}")
                console.print(f"\n[bold]Article:[/bold] {article.title}")
                console.print(f"[bold]Target Event:[/bold] {event.title}")

                console.print(
                    "\n[y] Yes (Merge)  [n] No (Reject/New Event)  [s] Skip  [q] Quit"
                )
                choice = Prompt.ask("Action").lower()

                if choice == "q":
                    return
                if choice == "s":
                    return

                if choice == "y":
                    self.cluster_service.execute_merge_action(session, article, event)
                    session.commit()
                    console.print("[green]Merged![/green]")

                elif choice == "n":
                    if Confirm.ask("Create NEW EVENT from this article?"):
                        new_event = self.cluster_service.execute_new_event_action(
                            session, article, reason="Manual Review: Rejected Merge"
                        )
                        session.add(new_event)
                        session.commit()
                        console.print("[yellow]New Event Created![/yellow]")
                    else:
                        proposal.status = JobStatus.REJECTED
                        session.commit()
                        console.print("[red]Rejected![/red]")

                time.sleep(0.5)

    def _review_event_merges(self):
        while True:
            console.clear()
            with SessionLocal() as session:
                proposal = session.scalars(
                    select(MergeProposalModel)
                    .options(
                        joinedload(MergeProposalModel.source_event),
                        joinedload(MergeProposalModel.target_event),
                    )
                    .where(
                        MergeProposalModel.status == JobStatus.PENDING,
                        MergeProposalModel.source_event_id.is_not(None),
                    )
                    .order_by(MergeProposalModel.distance_score)
                    .limit(1)
                ).first()

                if not proposal:
                    console.print("[green]No more event merges.[/green]")
                    time.sleep(1)
                    return

                source = proposal.source_event
                target = proposal.target_event

                if not source or not target:
                    proposal.status = JobStatus.FAILED
                    session.commit()
                    continue

                console.rule(f"Event Merge Proposal ({proposal.distance_score:.3f})")
                console.print(f"Reason: {proposal.reasoning}")
                console.print(
                    f"\n[bold red]SOURCE (Will Die):[/bold red] {source.title} ({source.article_count} arts)"
                )
                console.print(
                    f"[bold green]TARGET (Survivor):[/bold green] {target.title} ({target.article_count} arts)"
                )

                console.print("\n[y] Yes (Merge)  [n] No (Reject)  [q] Quit")
                choice = Prompt.ask("Action").lower()

                if choice == "q":
                    return

                if choice == "y":
                    self.cluster_service.execute_event_merge(session, source, target)
                    session.commit()
                    console.print("[green]Merged![/green]")

                elif choice == "n":
                    proposal.status = JobStatus.REJECTED
                    session.commit()
                    console.print("[red]Rejected![/red]")

                time.sleep(0.5)


class ManualSearchMenu(Menu):
    def __init__(self, cluster_service, nlp_service, article=None):
        self.cluster_service = cluster_service
        self.nlp_service = nlp_service
        self.article = article

    def render(self):
        return Panel("Enter search query to find events.", title="🔎 Manual Search")

    def handle_input(self, choice):
        if choice == "q":
            return "back"
        # This is a bit hybrid, we use the input as the query
        self._run_search(choice)
        return None

    def _run_search(self, query):
        console.clear()
        console.rule("🔎 Manual Search & Link")

        with SessionLocal() as session:
            target_date = None
            if not self.article:
                vector = self.nlp_service.calculate_vector(query)
            else:
                article_db = session.get(ArticleModel, self.article.id)
                if not article_db:
                    return False
                console.print(f"Searching for: [cyan]{article_db.title}[/cyan]")
                query = Prompt.ask(
                    "Refine Query",
                    default=self.cluster_service.derive_search_query(article_db),
                )
                vector = article_db.embedding
                # Use article date to scope search
                target_date = article_db.published_date

            # Updated call to match Domain Service signature (passing date if available)
            results = self.cluster_service.search_news_events_hybrid(
                session,
                query,
                vector,
                target_date=target_date if target_date else datetime.now(timezone.utc),
                limit=10,
            )

            table = Table(title=f"Results for '{query}'")
            table.add_column("#")
            table.add_column("Event")
            table.add_column("Score")
            candidates = []
            for i, (event, rrf, _) in enumerate(results):
                candidates.append(event)
                table.add_row(str(i + 1), event.title, f"{rrf:.3f}")
            console.print(table)

            if self.article:
                choice = Prompt.ask("Link to Event # (or Enter to cancel)")
                if not choice.isdigit():
                    return False
                idx = int(choice) - 1
                if 0 <= idx < len(candidates):
                    target = candidates[idx]

                    art_db = session.get(ArticleModel, self.article.id)
                    ev_db = session.get(NewsEventModel, target.id)

                    self.cluster_service.execute_merge_action(session, art_db, ev_db)
                    session.commit()
                    console.print(f"[green]Linked to {target.title}![/green]")
                    time.sleep(1)
                    return True
            else:
                Prompt.ask("Press Enter to continue...")
        return False
