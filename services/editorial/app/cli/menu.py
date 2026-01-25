"""
Modular Menu System for CLI.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Union

from cli_common import console
from prompt_toolkit import PromptSession
from rich.align import Align
from rich.panel import Panel


class Menu(ABC):
    """Abstract base class for CLI Menus."""

    @abstractmethod
    def render(self) -> any:
        """Return a rich renderable (Layout, Panel, Table, etc.) to display."""
        pass

    @abstractmethod
    def handle_input(self, choice: str) -> Union[Optional["Menu"], str]:
        """Handle user input."""
        pass

    def get_prompt_text(self) -> str:
        return "❯ "


class LegacyMenuWrapper(Menu):
    """Wrapper for legacy functions that contain their own loops."""

    def __init__(self, func, title="Processing"):
        self.func = func
        self.title = title

    def render(self):
        return Panel(
            Align.center(
                f"[bold yellow]Running {self.title}...[/bold yellow]\n(Check terminal output)"
            ),
            title=self.title,
        )

    def handle_input(self, choice):
        return "back"

    def run_direct(self):
        """Execute directly without the render loop."""
        self.func()
        return "back"


class MenuManager:
    def __init__(self, root_menu: Menu):
        self.stack: List[Menu] = [root_menu]
        self.console = console
        self.session = PromptSession()

    def run(self):
        # Removed console.screen() to allow terminal scrolling for large content
        while self.stack:
            current = self.stack[-1]

            # Special handling for legacy wrappers
            if isinstance(current, LegacyMenuWrapper):
                self.console.clear()
                try:
                    current.run_direct()
                except Exception as e:
                    self.console.print(f"[red]Error:[/red] {e}")
                    self.session.prompt("Press Enter to continue...")
                self.stack.pop()
                continue

            self.console.clear()
            renderable = current.render()
            self.console.print(renderable)

            try:
                choice = self.session.prompt(current.get_prompt_text())
                result = current.handle_input(choice)

                if result == "back":
                    self.stack.pop()
                elif result == "quit":
                    self.stack.clear()
                    break
                elif isinstance(result, Menu):
                    self.stack.append(result)
                # If None, loop continues (refresh)

            except KeyboardInterrupt:
                if len(self.stack) > 1:
                    self.stack.pop()
                else:
                    break
