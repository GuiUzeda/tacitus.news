import sys
import os
import typer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.align import Align
from rich.prompt import Prompt

# --- IMPORTS ---
from core.nlp_service import NLPService
from core.models import (
    ArticlesQueueModel,
    EventsQueueModel, 
    JobStatus,
)
from domain.clustering import NewsCluster
from domain.publisher import NewsPublisherDomain # New import for scoring logic
from news_events_lib.models import (
    NewsEventModel, 
    EventStatus,
)

from cli_common import SessionLocal
from live_desk import LiveDeskMenu
from review_station import ReviewWaitingMenu, ReviewMergesMenu, ManualSearchMenu
from engine_room import EngineRoomMenu, EngineRoomActions
from menu import Menu, MenuManager, LegacyMenuWrapper

app = typer.Typer()

class MainMenu(Menu):
    def __init__(self, cli):
        self.cli = cli

    def render(self):
        layout = Layout()
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3)
        )
        
        # Header
        stats_panel = self.cli._get_dashboard_stats()
        layout["header"].update(stats_panel)
        
        # Body
        table = Table(box=None, show_header=False, padding=(0, 2), expand=True)
        table.add_column("Section", style="bold cyan", ratio=1)
        table.add_column("Options", ratio=3)
        
        table.add_row("📰 LIVE DESK", "[1] Live Feed Control (Boost, Kill, Explain)\n[2] Review 'Waiting' Events (Approvals)")
        table.add_row("", "")
        table.add_row("🕵️ REVIEW STATION", "[3] Review Merge Proposals\n[4] Manual Search & Link")
        table.add_row("", "")
        table.add_row("🔧 ENGINE ROOM", "[5] Queue Manager (Retry, Reset, Monitor)\n[6] Recalculate ALL Scores\n[7] Find & Merge Duplicates\n[8] Check for Blocking (403/429)\n[9] Backfill Interests (Entity Fixer)\n[10] SQL Console\n[11] ORM Console (Python)")
        
        body_panel = Panel(
            Align.center(table, vertical="middle"),
            title="[bold]Tacitus Editorial Control[/bold]",
            border_style="blue",
            padding=(1, 2)
        )
        layout["body"].update(body_panel)
        
        # Footer
        layout["footer"].update(Align.center("[dim]q: Quit | Enter number to select[/dim]"))
        
        return layout

    def handle_input(self, choice):
        if choice == "1": return LiveDeskMenu(self.cli.publisher_domain)
        elif choice == "2": return ReviewWaitingMenu(self.cli.publisher_domain)
        elif choice == "3": return ReviewMergesMenu(self.cli.cluster_service)
        elif choice == "4": return ManualSearchMenu(self.cli.cluster_service, self.cli.nlp_service)
        elif choice == "5": return EngineRoomMenu(self.cli.cluster_service, self.cli.publisher_domain)
        elif choice == "6": return LegacyMenuWrapper(self.cli.engine_actions.recalc_all_scores, "Recalculating Scores")
        elif choice == "7": return LegacyMenuWrapper(self.cli.engine_actions.merge_duplicate_events, "Merging Duplicates")
        elif choice == "8": return LegacyMenuWrapper(self.cli.engine_actions.check_blocks, "Checking Blocks")
        elif choice == "9": return LegacyMenuWrapper(self.cli.engine_actions.run_backfill, "Backfilling Interests")
        elif choice == "10": return LegacyMenuWrapper(self.cli.engine_actions.run_sql_console, "SQL Console")
        elif choice == "11": return LegacyMenuWrapper(self.cli.engine_actions.run_orm_console, "ORM Console")
        elif choice == "q": return 'quit'
        return None

class EditorialCLI:
    def __init__(self):
        self._cluster_service = None
        self._nlp_service = None
        self._publisher_domain = None
        self._engine_actions = None

    @property
    def cluster_service(self):
        if self._cluster_service is None:
            self._cluster_service = NewsCluster()
        return self._cluster_service

    @property
    def nlp_service(self):
        if self._nlp_service is None:
            self._nlp_service = NLPService()
        return self._nlp_service

    @property
    def publisher_domain(self):
        if self._publisher_domain is None:
            self._publisher_domain = NewsPublisherDomain()
        return self._publisher_domain

    @property
    def engine_actions(self):
        if self._engine_actions is None:
            self._engine_actions = EngineRoomActions(self.cluster_service, self.publisher_domain)
        return self._engine_actions

    def start(self):
        main_menu = MainMenu(self)
        manager = MenuManager(main_menu)
        manager.run()

    def _get_dashboard_stats(self):
        with SessionLocal() as session:
            # Quick Stats
            active = session.query(NewsEventModel).filter_by(is_active=True).count()
            published = session.query(NewsEventModel).filter_by(status=EventStatus.PUBLISHED).count()
            waiting = session.query(EventsQueueModel).filter_by(status=JobStatus.WAITING).count()
            failed_jobs = (
                session.query(ArticlesQueueModel).filter_by(status=JobStatus.FAILED).count() + 
                session.query(EventsQueueModel).filter_by(status=JobStatus.FAILED).count()
            )
            
        return Panel(
            Align.center(
                f"[bold green]Live: {published}[/bold green]   |   "
                f"[bold yellow]Waiting: {waiting}[/bold yellow]   |   "
                f"[bold blue]Total Active: {active}[/bold blue]   |   "
                f"[bold red]System Failures: {failed_jobs}[/bold red]"
            ),
            style="white on black"
        )

@app.command()
def main():
    """Tacitus Editorial CLI"""
    cli = EditorialCLI()
    try: cli.start()
    except KeyboardInterrupt: print("\nExiting...")

if __name__ == "__main__":
    app()