import logging
import os
from logging.handlers import RotatingFileHandler
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich import print as rprint

console = Console()

BANNER = """
[bold cyan]
  ███╗   ██╗███████╗██████╗ ███████╗███╗   ██╗ █████╗ ███╗   ██╗
  ████╗  ██║██╔════╝██╔══██╗██╔════╝████╗  ██║██╔══██╗████╗  ██║
  ██╔██╗ ██║█████╗  ██████╔╝█████╗  ██╔██╗ ██║███████║██╔██╗ ██║
  ██║╚██╗██║██╔══╝  ██╔══██╗██╔══╝  ██║╚██╗██║██╔══██║██║╚██╗██║
  ██║ ╚████║███████╗██████╔╝███████╗██║ ╚████║██║  ██║██║ ╚████║
  ╚═╝  ╚═══╝╚══════╝╚═════╝ ╚══════╝╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝  ╚═══╝
[/bold cyan]
[dim]nebenan.de Parser + Sender[/dim]
"""


def setup_logging() -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("nebena")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger
    file_handler = RotatingFileHandler(
        "logs/nebena.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    rich_handler = RichHandler(console=console, rich_tracebacks=True, show_path=False)
    rich_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(rich_handler)
    return logger


def show_menu() -> str:
    console.print(BANNER)
    console.print(Panel(
        "[1] Run Parser\n"
        "[2] Run Sender\n"
        "[3] Run Both (Parser → Sender)\n"
        "[4] Settings\n"
        "[5] Statistics\n"
        "[6] Exit",
        title="[bold yellow]Main Menu[/bold yellow]",
        border_style="yellow",
    ))
    return Prompt.ask("[bold]Choose option[/bold]", choices=["1","2","3","4","5","6"])


def show_settings_menu(current: dict) -> dict:
    console.print(Panel(
        f"Current settings:\n"
        f"  Hours lookback : [cyan]{current['hours']}[/cyan]\n"
        f"  Message delay  : [cyan]{current['delay']}s[/cyan]\n"
        f"  Max accounts   : [cyan]{current['max_accounts']} (auto = all cookies)[/cyan]",
        title="[bold yellow]Settings[/bold yellow]",
        border_style="yellow",
    ))
    hours = Prompt.ask("Hours lookback", default=str(current["hours"]))
    delay = Prompt.ask("Delay between messages (seconds)", default=str(current["delay"]))
    return {"hours": int(hours), "delay": float(delay), "max_accounts": current["max_accounts"]}


def make_progress(description: str, total: int) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )


def show_stats(stats: dict, accounts_total: int, templates_count: int) -> None:
    table = Table(title="Statistics", border_style="cyan", show_header=True)
    table.add_column("Metric", style="bold")
    table.add_column("Value", style="cyan")
    table.add_row("Total sellers found", str(stats["total_sellers"]))
    table.add_row("Sellers messaged", str(stats["messaged"]))
    table.add_row("Total listings parsed", str(stats["total_listings"]))
    table.add_row("Accounts loaded", str(accounts_total))
    table.add_row("Templates loaded", str(templates_count))
    console.print(table)
