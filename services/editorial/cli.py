import sys
import os
import time
import textwrap
import uuid
from datetime import datetime, timezone

from sqlalchemy import create_engine, select, func, or_, update, delete
from sqlalchemy.orm import sessionmaker, joinedload
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, Confirm
from rich.markdown import Markdown

from config import Settings
from models import (
    ArticlesQueueModel,
    EventsQueueModel, 
    JobStatus,
    EventsQueueName,
)
from news_events_lib.models import ArticleModel, NewsEventModel, MergeProposalModel
from news_cluster import NewsCluster

console = Console()
settings = Settings()
engine = create_engine(str(settings.pg_dsn))
SessionLocal = sessionmaker(bind=engine)


class EditorialCLI:
    def __init__(self):
        self.cluster_service = NewsCluster()

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

            console.print(f"[1] 🕵️  Review Merges ({pending_props} pending)")
            console.print(f"[2] 🚑 Queue Manager (Arts: {failed_arts}, Evts: {failed_evts} failed)")
            console.print(f"[3] 🔎 Manual Search & Link")
            console.print(f"[4] 📖 Inspect Event/Article (ID Search)")
            console.print(f"[5] 🔗 Find & Merge Duplicate Events (Total: {active_events})")
            console.print(f"[q] Quit")

            choice = Prompt.ask("Select Mode", choices=["1", "2", "3", "4", "5", "q"])

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
            elif choice == "q":
                console.print("Bye! 👋")
                sys.exit(0)

    def review_merges(self):
        with SessionLocal() as session:
            stmt = (
                select(MergeProposalModel)
                .options(
                    joinedload(MergeProposalModel.source_article),
                    joinedload(MergeProposalModel.target_event),
                )
                .where(MergeProposalModel.status == "pending")
                .order_by(
                    MergeProposalModel.source_article_id,
                    MergeProposalModel.similarity_score,
                )
            )
            all_proposals = session.execute(stmt).scalars().all()

        if not all_proposals:
            console.print("[green]No pending merges![/green]")
            Prompt.ask("Press Enter to return")
            return

        from itertools import groupby
        grouped_props = {
            k: list(v)
            for k, v in groupby(all_proposals, key=lambda x: x.source_article)
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
                score_color = "green" if p.similarity_score < 0.15 else "yellow"
                table.add_row(
                    str(i + 1),
                    p.target_event.title if p.target_event else "Unknown",
                    f"[{score_color}]{p.similarity_score:.3f}[/{score_color}]",
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

    # --- EXECUTION (DELEGATED TO CLUSTER SERVICE) ---

    def _execute_merge(self, proposal):
        with SessionLocal() as session:
            # Re-fetch active objects
            article = session.get(ArticleModel, proposal.source_article_id)
            event = session.get(NewsEventModel, proposal.target_event_id)
            
            if not article or not event:
                console.print("[red]Object not found in DB[/red]")
                return

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

            self.cluster_service.execute_new_event_action(session, article, reason="Manual CLI")
            
            session.commit()
            console.print("[blue]🆕 New Event Created and Queued![/blue]")
            time.sleep(1)

    def manual_search(self, article=None):
        console.clear()
        console.rule("🔎 Manual Search & Link")

        with SessionLocal() as session:
            if not article:
                query = Prompt.ask("Enter search query")
                vector = [0.0] * 768
            else:
                article_db = session.get(ArticleModel, article.id)
                if not article_db: return False
                console.print(f"Searching for: [cyan]{article_db.title}[/cyan]")
                query = Prompt.ask("Refine Query", default=self.cluster_service.derive_search_query(article_db))
                vector = article_db.embedding

            results = self.cluster_service.search_news_events_hybrid(session, query, vector, limit=10)
            
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
                    
                    # Re-fetch for safety
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
                session, target_event.search_text or target_event.title, target_event.embedding_centroid, limit=20
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
                # Articles Stats
                a_stats = (
                    session.query(
                        ArticlesQueueModel.queue_name,
                        ArticlesQueueModel.status,
                        func.count(ArticlesQueueModel.id),
                    )
                    .group_by(ArticlesQueueModel.queue_name, ArticlesQueueModel.status)
                    .all()
                )

                # Events Stats
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
                color = "red" if s == JobStatus.FAILED else "green"
                table.add_row(q, f"[{color}]{s}[/{color}]", str(c))
            console.print(table)

            table2 = Table(title="Events Queue")
            table2.add_column("Queue")
            table2.add_column("Status")
            table2.add_column("Count")
            for q, s, c in e_stats:
                color = "red" if s == JobStatus.FAILED else "green"
                table2.add_row(q, f"[{color}]{s}[/{color}]", str(c))
            console.print(table2)

            console.print("\n[r] Retry Failed   [c] Clear Completed   [b] Back")
            choice = Prompt.ask("Action")

            if choice == "b":
                return
            if choice == "r":
                with SessionLocal() as session:
                    # Retry both queues
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
                    session.commit()
                    console.print(
                        f"[green]Retried {res1.rowcount} Articles, {res2.rowcount} Events.[/green]"
                    )
                    time.sleep(1)
            if choice == "c":
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
                    session.commit()
                    console.print(f"[green]Cleared completed jobs.[/green]")
                    time.sleep(1)

    # --- TOOLS ---

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
        """
        Safely reads article content by opening a new session and re-fetching.
        This handles both detached and attached objects.
        """
        console.clear()
        console.rule(f"Reading: {article.title}")
        
        console.print(
            f"[link={article.original_url}]{article.original_url}[/link]", 
            style="blue underline", 
            no_wrap=True, 
            overflow="ignore"
        )
        print() 

        # FIX: Fetch fresh content using a dedicated session
        content_text = "No content found."
        with SessionLocal() as session:
            # Re-fetch article to load lazy relationships
            fresh_article = session.get(ArticleModel, article.id)
            if fresh_article and fresh_article.contents:
                content_text = fresh_article.contents[0].content
            
            # Print logic inside the session to ensure access
            if fresh_article and fresh_article.summary:
                console.print(Panel(fresh_article.summary, title="Summary"))
            
            console.print(Markdown(content_text))
            
        Prompt.ask("\nPress Enter...")


if __name__ == "__main__":
    cli = EditorialCLI()
    try: cli.start()
    except KeyboardInterrupt: print("\nExiting...")