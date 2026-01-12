import sys
import os
import time
import textwrap
import uuid
from datetime import datetime, timezone

from sqlalchemy import create_engine, select, func, or_, update, delete, desc
from sqlalchemy.orm import sessionmaker, joinedload
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, Confirm
from rich.markdown import Markdown

# --- REFACTORED IMPORTS ---
from config import Settings

# Core (Shared Models & Utils)
from core.nlp_service import NLPService
from core.models import (
    ArticlesQueueModel,
    EventsQueueModel, 
    JobStatus,
    EventsQueueName,
)

# Domain Services (The "Brains")
from domain.clustering import NewsCluster

# Shared Lib
from news_events_lib.models import (
    ArticleModel, 
    NewsEventModel, 
    MergeProposalModel, 
    EventStatus
)

console = Console()
settings = Settings()
engine = create_engine(str(settings.pg_dsn))
SessionLocal = sessionmaker(bind=engine)


class EditorialCLI:
    def __init__(self):
        # Use the Domain Service
        self.cluster_service = NewsCluster()
        # Use the Core Service
        self.nlp_service = NLPService()

    def start(self):
        while True:
            console.clear()
            console.print(
                Panel.fit("📰 Tacitus Editorial Control Room", style="bold blue")
            )

            with SessionLocal() as session:
                pending_props = session.query(MergeProposalModel).filter_by(status="pending").count()
                failed_arts = session.query(ArticlesQueueModel).filter_by(status=JobStatus.FAILED).count()
                failed_evts = session.query(EventsQueueModel).filter_by(status=JobStatus.FAILED).count()
                active_events = session.query(NewsEventModel).filter_by(is_active=True).count()
                ready_to_publish = session.query(EventsQueueModel).filter_by(
                    queue_name=EventsQueueName.PUBLISHER, 
                    status=JobStatus.PENDING
                ).count()

            console.print(f"[1] 🕵️  Review Merges ({pending_props} pending)")
            console.print(f"[2] 🚑 Queue Manager (Arts: {failed_arts}, Evts: {failed_evts} failed)")
            console.print(f"[3] 🔎 Manual Search & Link")
            console.print(f"[4] 📖 Inspect Event/Article (ID Search)")
            console.print(f"[5] 🔗 Find & Merge Duplicate Events (Total: {active_events})")
            console.print(f"[6] 🚀 Publishing Review ({ready_to_publish} ready)")
            console.print(f"[q] Quit")

            choice = Prompt.ask("Select Mode", choices=["1", "2", "3", "4", "5", "6", "q"])

            if choice == "1":
                self.review_merges()
            elif choice == "2":
                self.queue_manager()
            elif choice == "3":
                self.manual_search()
            elif choice == "4":
                self.inspect_tool()
            elif choice == "5":
                self.merge_duplicate_events()
            elif choice == "6":
                self.publishing_review()
            elif choice == "q":
                console.print("Bye! 👋")
                sys.exit(0)

    # ... [Rest of the methods remain mostly the same, but use self.cluster_service which is now domain.NewsCluster] ...

    def review_merges(self):
        with SessionLocal() as session:
            stmt = (
                select(MergeProposalModel)
                .options(
                    joinedload(MergeProposalModel.source_article),
                    joinedload(MergeProposalModel.source_event),
                    joinedload(MergeProposalModel.target_event),
                )
                .where(MergeProposalModel.status == "pending")
                .order_by(
                    MergeProposalModel.distance_score,
                )
            )
            all_proposals = session.execute(stmt).scalars().all()

        if not all_proposals:
            console.print(Panel("✅ No pending merges found.", style="green"))
            Prompt.ask("Press Enter to return")
            return

        # Split proposals
        article_props = [p for p in all_proposals if p.source_article_id]
        event_props = [p for p in all_proposals if p.source_event_id]

        while True:
            console.clear()
            console.rule("🕵️  Review Merges Dashboard")
            
            grid = Table.grid(expand=True, padding=(0, 2))
            grid.add_column(justify="center", ratio=1)
            grid.add_column(justify="center", ratio=1)
            
            p1 = Panel(f"[bold cyan]{len(article_props)}[/bold cyan]\nPending Article Links", title="📄 Articles", border_style="cyan")
            p2 = Panel(f"[bold magenta]{len(event_props)}[/bold magenta]\nPending Event Merges", title="🔗 Events", border_style="magenta")
            grid.add_row(p1, p2)
            console.print(grid)
            console.print()

            choices = []
            if article_props: choices.append("1")
            if event_props: choices.append("2")
            choices.append("q")

            console.print("[1] Review Articles" if article_props else "[dim][1] Review Articles (Empty)[/dim]")
            console.print("[2] Review Events" if event_props else "[dim][2] Review Events (Empty)[/dim]")
            console.print("[q] Back")

            choice = Prompt.ask("Select", choices=choices, default="1" if article_props else "q")

            if choice == "1" and article_props:
                self._review_article_merges(article_props)
                return 
            elif choice == "2" and event_props:
                self._review_event_merges(event_props)
                return
            elif choice == "q":
                return

    def _review_article_merges(self, proposals):
        from itertools import groupby
        proposals.sort(key=lambda x: str(x.source_article_id))
        grouped_props = {
            k: list(v)
            for k, v in groupby(proposals, key=lambda x: x.source_article)
            if k is not None
        }

        total = len(grouped_props)
        idx = 0
        for article, props in grouped_props.items():
            idx += 1
            if self._handle_single_review(article, props, idx, total) == "quit":
                break

    def _handle_single_review(self, article, props, idx, total):
        while True:
            console.clear()
            console.rule(f"Review {idx}/{total}")

            console.print(f"[bold cyan]ARTICLE:[/bold cyan] {article.title}")
            console.print(
                f"[link={article.original_url}]{article.original_url}[/link]", 
                style="blue underline", no_wrap=True, overflow="ignore"
            )
            console.print(article.published_date.strftime("%Y-%m-%d") if article.published_date else "No date")
            console.print(Panel(textwrap.shorten(article.summary or "No summary", width=200)))
            if article.entities:
                console.print(f"[yellow]Entities:[/yellow] {', '.join(article.entities)}")

            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("#", style="dim", width=4)
            table.add_column("Event Title")
            table.add_column("Score", justify="right")
            table.add_column("Reason")
            table.add_column("Date")

            for i, p in enumerate(props):
                score_color = "green" if p.distance_score < 0.15 else "yellow"
                table.add_row(
                    str(i + 1),
                    p.target_event.title if p.target_event else "Unknown",
                    f"[{score_color}]{p.distance_score:.3f}[/{score_color}]",
                    p.reasoning,
                    p.target_event.created_at.strftime("%Y-%m-%d") if p.target_event else "-",
                )
            console.print(table)
            
            menu_text = (
                f"[bold green]1-{len(props)}[/bold green] : Merge with Candidate\n"
                "[bold red]n[/bold red]   : New Event (Reject All)\n"
                "[bold magenta]f[/bold magenta]   : Find Alternative Event (Search DB)\n"
                "[bold yellow]r[/bold yellow]   : Read Source Article Content\n"
                "[bold blue]i #[/bold blue] : Inspect Candidate Event\n"
                "[dim]skip[/dim]: Skip\n"
                "[dim]q[/dim]   : Quit"
            )
            console.print(Panel(menu_text, title="Actions", expand=False))

            choice = Prompt.ask("Decision").lower().strip()

            if choice == "q": return "quit"
            if choice == "skip": return "next"
            if choice == "r":
                self._read_article_content(article)
                continue
            if choice.startswith("i "):
                try:
                    idx_to_inspect = int(choice.split(" ")[1]) - 1
                    if 0 <= idx_to_inspect < len(props):
                        self._inspect_event_deep_dive(props[idx_to_inspect].target_event.id)
                        continue
                except: pass

            elif choice.isdigit() and 1 <= int(choice) <= len(props):
                self._execute_merge(props[int(choice) - 1])
                return "next"

            elif choice == "n":
                self._execute_new_event(article)
                return "next"

            elif choice == "f" or choice == "s":
                if self.manual_search(article=article):
                    return "next"
                continue

    def _review_event_merges(self, proposals):
        from itertools import groupby
        proposals.sort(key=lambda x: str(x.source_event_id))
        
        grouped = {
            k: list(v)
            for k, v in groupby(proposals, key=lambda x: x.source_event)
            if k is not None
        }
        
        total = len(grouped)
        idx = 0
        for source_event, props in grouped.items():
            idx += 1
            if self._handle_single_event_review(source_event, props, idx, total) == "quit":
                break

    def _handle_single_event_review(self, source, props, idx, total):
        while True:
            console.clear()
            console.rule(f"🔗 Event Merge Review {idx}/{total}", style="magenta")

            source_text = f"[bold]{source.title}[/bold]\n"
            source_text += f"[dim]ID: {source.id}[/dim]\n"
            source_text += f"📅 {source.created_at.strftime('%Y-%m-%d')} | 📰 {source.article_count} Articles"
            if source.summary and isinstance(source.summary, dict):
                s = source.summary.get('center') or source.summary.get('bias') or ""
                source_text += f"\n\n[italic]{textwrap.shorten(s, width=150)}[/italic]"
            
            console.print(Panel(source_text, title="🔻 SOURCE EVENT (To be dissolved)", border_style="red"))

            table = Table(show_header=True, header_style="bold magenta", title="Target Candidates (Master)")
            table.add_column("#", style="dim", width=4)
            table.add_column("Target Event")
            table.add_column("Score", justify="right")
            table.add_column("Reason")
            table.add_column("Stats")

            for i, p in enumerate(props):
                target = p.target_event
                score_color = "green" if p.distance_score < 0.15 else "yellow"
                stats = f"{target.article_count} arts"
                table.add_row(
                    str(i + 1),
                    target.title,
                    f"[{score_color}]{p.distance_score:.3f}[/{score_color}]",
                    p.reasoning or "",
                    stats
                )
            console.print(table)

            menu_text = (
                f"[bold green]1-{len(props)}[/bold green] : Merge Source into Target #\n"
                "[bold red]r[/bold red]   : Reject All (Keep Separate)\n"
                "[bold blue]i #[/bold blue] : Inspect Target Event\n"
                "[dim]skip[/dim]: Skip\n"
                "[dim]q[/dim]   : Quit"
            )
            console.print(Panel(menu_text, title="Actions", expand=False))

            choice = Prompt.ask("Decision").lower().strip()

            if choice == "q": return "quit"
            if choice == "skip": return "next"
            
            if choice == "r":
                self._reject_event_merge(props)
                return "next"

            if choice.startswith("i "):
                try:
                    idx_to_inspect = int(choice.split(" ")[1]) - 1
                    if 0 <= idx_to_inspect < len(props):
                        self._inspect_event_deep_dive(props[idx_to_inspect].target_event.id)
                        continue
                except: pass

            if choice.isdigit() and 1 <= int(choice) <= len(props):
                self._execute_event_merge_cli(props[int(choice) - 1])
                return "next"

    def _execute_merge(self, proposal):
        with SessionLocal() as session:
            article = session.get(ArticleModel, proposal.source_article_id)
            event = session.get(NewsEventModel, proposal.target_event_id)
            
            if not article or not event:
                console.print("[red]Object not found in DB[/red]")
                return

            # Call Domain Logic
            self.cluster_service.execute_merge_action(session, article, event)
            
            session.commit()
            console.print("[green]✅ Merged and Queue Updated![/green]")
            time.sleep(1)

    def _execute_new_event(self, article_ref):
        with SessionLocal() as session:
            article = session.get(ArticleModel, article_ref.id)
            if not article:
                console.print("[red]Article not found[/red]")
                return

            # Call Domain Logic
            self.cluster_service.execute_new_event_action(session, article, reason="Manual CLI")
            
            session.commit()
            console.print("[blue]🆕 New Event Created and Queued![/blue]")
            time.sleep(1)

    def _execute_event_merge_cli(self, proposal):
        with SessionLocal() as session:
            prop = session.get(MergeProposalModel, proposal.id)
            source = session.get(NewsEventModel, prop.source_event_id)
            target = session.get(NewsEventModel, prop.target_event_id)

            if not source or not target:
                console.print("[red]Objects missing.[/red]")
                return

            # Call Domain Logic
            self.cluster_service.execute_event_merge(session, source, target)
            
            prop.status = "approved"
            prop.reasoning = "Manual CLI Approval"
            session.add(prop)
            
            session.commit()
            console.print(f"[green]✅ Merged '{source.title}' into '{target.title}'[/green]")
            time.sleep(1.5)

    def _reject_event_merge(self, proposals):
        with SessionLocal() as session:
            for p in proposals:
                db_p = session.get(MergeProposalModel, p.id)
                if db_p:
                    db_p.status = "rejected"
                    db_p.reasoning = "Manual CLI Rejection"
            session.commit()
            console.print("[yellow]🚫 Proposals rejected. Events kept separate.[/yellow]")
            time.sleep(1)

    def manual_search(self, article=None):
        console.clear()
        console.rule("🔎 Manual Search & Link")

        with SessionLocal() as session:
            target_date = None
            if not article:
                query = Prompt.ask("Enter search query")
                vector = self.nlp_service.calculate_vector(query)
            else:
                article_db = session.get(ArticleModel, article.id)
                if not article_db: return False
                console.print(f"Searching for: [cyan]{article_db.title}[/cyan]")
                query = Prompt.ask("Refine Query", default=self.cluster_service.derive_search_query(article_db))
                vector = article_db.embedding
                # Use article date to scope search
                target_date = article_db.published_date

            # Updated call to match Domain Service signature (passing date if available)
            results = self.cluster_service.search_news_events_hybrid(
                session, 
                query, 
                vector, 
                target_date=target_date if target_date else datetime.now(timezone.utc),
                limit=10
            )
            
            table = Table(title=f"Results for '{query}'")
            table.add_column("#"); table.add_column("Event"); table.add_column("Score")
            candidates = []
            for i, (event, rrf, _) in enumerate(results):
                candidates.append(event)
                table.add_row(str(i + 1), event.title, f"{rrf:.3f}")
            console.print(table)

            if article:
                choice = Prompt.ask("Link to Event # (or Enter to cancel)")
                if not choice.isdigit(): return False
                idx = int(choice) - 1
                if 0 <= idx < len(candidates):
                    target = candidates[idx]
                    
                    art_db = session.get(ArticleModel, article.id)
                    ev_db = session.get(NewsEventModel, target.id)
                    
                    self.cluster_service.execute_merge_action(session, art_db, ev_db)
                    session.commit()
                    console.print(f"[green]Linked to {target.title}![/green]")
                    time.sleep(1)
                    return True
        return False

    def merge_duplicate_events(self):
        console.clear()
        console.rule("🔗 Find & Merge Duplicate Events")
        target_query = Prompt.ask("Search for the MAIN Event (Title)")
        with SessionLocal() as session:
            targets = session.query(NewsEventModel).filter(
                NewsEventModel.title.ilike(f"%{target_query}%"),
                NewsEventModel.is_active == True
            ).limit(10).all()
            
            if not targets:
                console.print("[red]No events found.[/red]")
                time.sleep(2); return

            table = Table(title="Select Target Event"); table.add_column("#"); table.add_column("Title")
            for i, e in enumerate(targets): table.add_row(str(i + 1), e.title)
            console.print(table)
            sel = IntPrompt.ask("Select #", default=1)
            target_event = targets[sel - 1]

            console.print(f"\n🔎 Searching duplicates for: [bold]{target_event.title}[/bold]...")
            results = self.cluster_service.search_news_events_hybrid(
                session, 
                target_event.search_text or target_event.title, 
                target_event.embedding_centroid, 
                target_date=target_event.created_at,
                limit=20
            )
            candidates = [r[0] for r in results if r[0].id != target_event.id]
            if not candidates:
                console.print("[green]No duplicates![/green]"); time.sleep(2); return

            table = Table(title="Potential Duplicates"); table.add_column("#"); table.add_column("Title")
            for i, ev in enumerate(candidates): table.add_row(str(i + 1), ev.title)
            console.print(table)

            choice = Prompt.ask("Select to merge (e.g. '1,3') or 'all'")
            indices = []
            if choice == "all": indices = range(len(candidates))
            else: 
                try: indices = [int(x.strip()) - 1 for x in choice.split(",") if x.strip()]
                except: return

            if Confirm.ask(f"⚠️ Merge {len(indices)} events?"):
                for idx in indices:
                    if 0 <= idx < len(candidates):
                        self.cluster_service.execute_event_merge(session, candidates[idx], target_event)
                session.commit()
                console.print("[green]Merged![/green]")
                time.sleep(2)

    def queue_manager(self):
        while True:
            console.clear()
            console.rule("🚑 Queue Manager")

            with SessionLocal() as session:
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

            table = Table(title="Articles Queue")
            table.add_column("Queue")
            table.add_column("Status")
            table.add_column("Count")
            for q, s, c in a_stats:
                status_str = s.value if hasattr(s, 'value') else str(s)
                queue_str = q.value if hasattr(q, 'value') else str(q)
                color = "red" if s == JobStatus.FAILED else "green"
                table.add_row(queue_str, f"[{color}]{status_str}[/{color}]", str(c))
            console.print(table)

            table2 = Table(title="Events Queue")
            table2.add_column("Queue")
            table2.add_column("Status")
            table2.add_column("Count")
            for q, s, c in e_stats:
                status_str = s.value if hasattr(s, 'value') else str(s)
                queue_str = q.value if hasattr(q, 'value') else str(q)
                color = "red" if s == JobStatus.FAILED else "green"
                table2.add_row(queue_str, f"[{color}]{status_str}[/{color}]", str(c))
            console.print(table2)

            p_stats = (
                session.query(
                    MergeProposalModel.status,
                    func.count(MergeProposalModel.id),
                )
                .group_by(MergeProposalModel.status)
                .all()
            )
            table3 = Table(title="Proposals Queue")
            table3.add_column("Status"); table3.add_column("Count")
            for s, c in p_stats:
                color = "red" if s == "failed" else "green"
                table3.add_row(f"[{color}]{s}[/{color}]", str(c))
            console.print(table3)

            console.print("\n[1] Manage Articles Queue   [2] Manage Events Queue   [3] Manage Proposals Queue")
            console.print("\\[rt] Retry ALL Failed     \\[c] Clear ALL Completed  \\[r] Refresh Totals")
            console.print("\\[b] Back")
            
            choice = Prompt.ask("Action")

            if choice == "b":
                return
            elif choice == "r":
                continue
            elif choice == "1":
                self._manage_specific_queue(ArticlesQueueModel, "Articles Queue")
            elif choice == "2":
                self._manage_specific_queue(EventsQueueModel, "Events Queue")
            elif choice == "3":
                self._manage_proposals_queue()
            elif choice == "rt":
                with SessionLocal() as session:
                    res1 = session.execute(
                        update(ArticlesQueueModel)
                        .where(ArticlesQueueModel.status == JobStatus.FAILED)
                        .values(status=JobStatus.PENDING, msg=None)
                    )
                    res2 = session.execute(
                        update(EventsQueueModel)
                        .where(EventsQueueModel.status == JobStatus.FAILED)
                        .values(status=JobStatus.PENDING, msg=None)
                    )
                    res3 = session.execute(
                        update(MergeProposalModel)
                        .where(MergeProposalModel.status == "failed")
                        .values(status="pending", reasoning=None)
                    )
                    session.commit()
                    console.print(
                        f"[green]Retried {res1.rowcount} Arts, {res2.rowcount} Evts, {res3.rowcount} Props.[/green]"
                    )
                    time.sleep(1)
            elif choice == "c":
                with SessionLocal() as session:
                    session.execute(
                        delete(ArticlesQueueModel).where(
                            ArticlesQueueModel.status == JobStatus.COMPLETED
                        )
                    )
                    session.execute(
                        delete(EventsQueueModel).where(
                            EventsQueueModel.status == JobStatus.COMPLETED
                        )
                    )
                    session.execute(
                        delete(MergeProposalModel).where(
                            MergeProposalModel.status.in_(["approved", "rejected"])
                        )
                    )
                    session.commit()
                    console.print(f"[green]Cleared completed jobs.[/green]")
                    time.sleep(1)

    def _manage_specific_queue(self, model_class, title):
        while True:
            console.clear()
            console.rule(f"🔧 Managing: {title}")
            
            with SessionLocal() as session:
                stats = (
                    session.query(
                        model_class.queue_name,
                        model_class.status,
                        func.count(model_class.id),
                    )
                    .group_by(model_class.queue_name, model_class.status)
                    .all()
                )
                
                table = Table(title="Statistics")
                table.add_column("Queue")
                table.add_column("Status")
                table.add_column("Count")
                for q, s, c in stats:
                    status_str = s.value if hasattr(s, 'value') else str(s)
                    queue_str = q.value if hasattr(q, 'value') else str(q)
                    color = "red" if s == JobStatus.FAILED else "green"
                    table.add_row(queue_str, f"[{color}]{status_str}[/{color}]", str(c))
                console.print(table)
                
                failures = (
                    session.query(model_class)
                    .filter(model_class.status == JobStatus.FAILED)
                    .order_by(desc(model_class.updated_at))
                    .limit(5)
                    .all()
                )
                
                if failures:
                    console.print("\n[bold red]Recent Failures:[/bold red]")
                    ftable = Table(show_header=True)
                    ftable.add_column("ID", width=4)
                    ftable.add_column("Msg")
                    ftable.add_column("Updated")
                    for f in failures:
                        ftable.add_row(str(f.id), str(f.msg or "No error"), f.updated_at.strftime("%H:%M:%S"))
                    console.print(ftable)

            console.print("\n\\[r] Retry Failed (This Queue)")
            console.print("\\[c] Clear Completed (This Queue)")
            console.print("\\[v] View Pending Jobs")   
            console.print("\\[b] Back")
            
            choice = Prompt.ask("Action")
            
            if choice == "b": return
            
            if choice == "r":
                with SessionLocal() as session:
                    res = session.execute(
                        update(model_class)
                        .where(model_class.status == JobStatus.FAILED)
                        .values(status=JobStatus.PENDING, msg=None, attempts=0)
                    )
                    session.commit()
                    console.print(f"[green]Retried {res.rowcount} jobs.[/green]")
                    time.sleep(1)
            
            if choice == "c":
                with SessionLocal() as session:
                    res = session.execute(
                        delete(model_class).where(model_class.status == JobStatus.COMPLETED)
                    )
                    session.commit()
                    console.print(f"[green]Deleted {res.rowcount} completed jobs.[/green]")
                    time.sleep(1)

            if choice == "v":
                self._view_pending_jobs(model_class)

    def _manage_proposals_queue(self):
        while True:
            console.clear()
            console.rule("🔧 Managing: Proposals Queue")
            
            with SessionLocal() as session:
                stats = (
                    session.query(MergeProposalModel.status, func.count(MergeProposalModel.id))
                    .group_by(MergeProposalModel.status)
                    .all()
                )
                
                table = Table(title="Statistics")
                table.add_column("Status"); table.add_column("Count")
                for s, c in stats:
                    color = "red" if s == "failed" else "green"
                    table.add_row(f"[{color}]{s}[/{color}]", str(c))
                console.print(table)
                
                failures = (
                    session.query(MergeProposalModel)
                    .filter(MergeProposalModel.status == "failed")
                    .limit(5).all()
                )
                if failures:
                    console.print("\n[bold red]Recent Failures:[/bold red]")
                    ftable = Table(show_header=True)
                    ftable.add_column("ID", width=8); ftable.add_column("Reason")
                    for f in failures:
                        ftable.add_row(str(f.id)[:8], str(f.reasoning or "No error"))
                    console.print(ftable)

            console.print("\n\\[r] Retry Failed   \\[c] Clear Completed/Rejected   \\[v] View Pending   \\[b] Back")
            choice = Prompt.ask("Action")
            
            if choice == "b": return
            if choice == "r":
                with SessionLocal() as session:
                    res = session.execute(
                        update(MergeProposalModel)
                        .where(MergeProposalModel.status == "failed")
                        .values(status="pending", reasoning=None)
                    )
                    session.commit()
                    console.print(f"[green]Retried {res.rowcount} proposals.[/green]")
                    time.sleep(1)
            if choice == "c":
                with SessionLocal() as session:
                    res = session.execute(
                        delete(MergeProposalModel).where(MergeProposalModel.status.in_(["approved", "rejected"]))
                    )
                    session.commit()
                    console.print(f"[green]Deleted {res.rowcount} finished proposals.[/green]")
                    time.sleep(1)
            if choice == "v":
                self._view_pending_jobs(MergeProposalModel)

    def _view_pending_jobs(self, model_class):
        console.clear()
        console.rule("Pending Jobs")
        with SessionLocal() as session:
            if model_class == MergeProposalModel:
                jobs = session.query(model_class).filter(model_class.status == "pending").limit(20).all()
                if not jobs:
                    console.print("[yellow]No pending jobs.[/yellow]")
                    Prompt.ask("Press Enter to return")
                    return
                
                table = Table(show_header=True)
                table.add_column("ID", width=8); table.add_column("Type"); table.add_column("Score")
                for job in jobs:
                    j_type = "Event Merge" if job.source_event_id else "Article Merge"
                    table.add_row(str(job.id)[:8], j_type, f"{job.distance_score:.3f}")
                console.print(table)
                Prompt.ask("Press Enter to return")
                return

            query = session.query(model_class).filter(model_class.status == JobStatus.PENDING).limit(20)
            
            if model_class == ArticlesQueueModel:
                query = query.options(joinedload(ArticlesQueueModel.article))
            elif model_class == EventsQueueModel:
                query = query.options(joinedload(EventsQueueModel.event))
                
            jobs = query.all()
            
            if not jobs:
                console.print("[yellow]No pending jobs.[/yellow]")
                Prompt.ask("Press Enter to return")
                return

            table = Table(show_header=True)
            table.add_column("ID", width=4)
            table.add_column("Queue")
            table.add_column("Entity")
            table.add_column("Created")
            
            for job in jobs:
                entity_title = "Unknown"
                if model_class == ArticlesQueueModel and job.article:
                    entity_title = job.article.title[:50]
                elif model_class == EventsQueueModel and job.event:
                    entity_title = job.event.title[:50]
                
                q_name = job.queue_name.value if hasattr(job.queue_name, 'value') else str(job.queue_name)
                table.add_row(str(job.id), q_name, entity_title, job.created_at.strftime("%Y-%m-%d %H:%M"))
            
            console.print(table)
            Prompt.ask("Press Enter to return")

    def publishing_review(self):
        with SessionLocal() as session:
            stmt = (
                select(EventsQueueModel)
                .options(joinedload(EventsQueueModel.event).joinedload(NewsEventModel.articles))
                .where(
                    EventsQueueModel.queue_name == EventsQueueName.PUBLISHER,
                    EventsQueueModel.status == JobStatus.PENDING
                )
                .order_by(EventsQueueModel.created_at.asc())
            )
            jobs = session.execute(stmt).unique().scalars().all()

        if not jobs:
            console.print(Panel("✅ No events waiting for publication.", style="green"))
            Prompt.ask("Press Enter to return")
            return

        total = len(jobs)
        for i, job in enumerate(jobs):
            if self._handle_single_publish_review(job, i + 1, total) == "quit":
                break

    def _handle_single_publish_review(self, job, idx, total):
        event = job.event
        while True:
            console.clear()
            console.rule(f"🚀 Publishing Review {idx}/{total}")
            
            console.print(f"[bold size=16]{event.title}[/bold size=16]")
            console.print(f"ID: {event.id} | Articles: {event.article_count}")
            
            if event.summary and isinstance(event.summary, dict):
                summary_text = event.summary.get("center") or event.summary.get("bias") or str(event.summary)
                console.print(Panel(Markdown(summary_text), title="Generated Briefing", border_style="blue"))
            
            if event.stance_distribution:
                console.print(f"[yellow]Stance Dist:[/yellow] {event.stance_distribution}")
            
            console.print("\n[bold green]p[/bold green] : Publish (Live)")
            console.print("[bold red]a[/bold red] : Archive (Reject)")
            console.print("[bold yellow]e[/bold yellow] : Edit Title")
            console.print("[dim]skip[/dim]: Skip")
            console.print("[dim]q[/dim]   : Quit")
            
            choice = Prompt.ask("Action").lower().strip()
            
            if choice == "q": return "quit"
            if choice == "skip": return "next"
            
            if choice == "p":
                self._execute_publish_action(job.id, event.id, EventStatus.PUBLISHED)
                return "next"
            
            if choice == "a":
                self._execute_publish_action(job.id, event.id, EventStatus.ARCHIVED)
                return "next"
                
            if choice == "e":
                new_title = Prompt.ask("New Title", default=event.title)
                with SessionLocal() as session:
                    e = session.get(NewsEventModel, event.id)
                    e.title = new_title
                    session.commit()
                    event.title = new_title 

    def _execute_publish_action(self, job_id, event_id, new_status):
        with SessionLocal() as session:
            job = session.get(EventsQueueModel, job_id)
            event = session.get(NewsEventModel, event_id)
            if job and event:
                event.status = new_status
                event.is_active = (new_status == EventStatus.PUBLISHED)
                job.status = JobStatus.COMPLETED
                session.commit()
                console.print(f"[green]✅ Set to {new_status.value}![/green]")
                time.sleep(1)

    def inspect_tool(self):
        event_id_str = Prompt.ask("Enter Event UUID (or partial title search)")
        with SessionLocal() as session:
            try:
                event_id = uuid.UUID(event_id_str)
                self._inspect_event_deep_dive(event_id)
            except:
                events = (
                    session.query(NewsEventModel)
                    .filter(NewsEventModel.title.ilike(f"%{event_id_str}%"))
                    .limit(5)
                    .all()
                )
                if not events:
                    console.print("[red]No events found.[/red]")
                    time.sleep(2)
                    return

                table = Table(title="Search Results")
                table.add_column("#")
                table.add_column("Title")
                table.add_column("ID")
                for i, e in enumerate(events):
                    table.add_row(str(i + 1), e.title, str(e.id))
                console.print(table)
                sel = IntPrompt.ask("Select #", default=1)
                self._inspect_event_deep_dive(events[sel - 1].id)

    def _inspect_event_deep_dive(self, event_id):
        while True:
            console.clear()
            with SessionLocal() as session:
                event = (
                    session.query(NewsEventModel)
                    .options(
                        joinedload(NewsEventModel.articles).joinedload(
                            ArticleModel.contents
                        )
                    )
                    .get(event_id)
                )
                if not event:
                    return

                console.rule(f"[bold blue]EVENT INSPECTOR[/bold blue]")
                console.print(f"[bold size=14]{event.title}[/bold size=14]")
                console.print(f"ID: {event.id} | Articles: {len(event.articles)}")

                if event.summary:
                    sum_text = str(event.summary)
                    if isinstance(event.summary, dict):
                        sum_text = event.summary.get("center", "") or event.summary.get(
                            "bias", ""
                        )
                    console.print(
                        Panel(sum_text, title="Summary", border_style="green")
                    )

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
                    "\n[blue][#][/blue] Read Article - #   [green][b][/green] Back - b "
                )
                choice = Prompt.ask("Action").lower()
                if choice == "b":
                    return
                if choice.isdigit() and 1 <= int(choice) <= len(sorted_articles):
                    self._read_article_content(sorted_articles[int(choice) - 1])

    def _read_article_content(self, article):
        console.clear()
        console.rule(f"Reading: {article.title}")
        
        console.print(
            f"[link={article.original_url}]{article.original_url}[/link]", 
            style="blue underline", 
            no_wrap=True, 
            overflow="ignore"
        )
        print() 

        content_text = "No content found."
        with SessionLocal() as session:
            fresh_article = session.get(ArticleModel, article.id)
            if fresh_article and fresh_article.contents:
                content_text = fresh_article.contents[0].content
            
            if fresh_article and fresh_article.summary:
                console.print(Panel(fresh_article.summary, title="Summary"))
            
            console.print(Markdown(content_text))
            
        Prompt.ask("\nPress Enter...")


if __name__ == "__main__":
    cli = EditorialCLI()
    try: cli.start()
    except KeyboardInterrupt: print("\nExiting...")