"""Consolidated utilities for the data pipeline."""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.logging import RichHandler

# Configure rich logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)
logger = logging.getLogger("data_pipeline")
console = Console()


def read_sql_file(path: Path) -> str:
    """Read SQL file content."""
    with open(path, "r") as f:
        return f.read()


def safe_fetch_value(result: Optional[Tuple[Any, ...]], index: int = 0) -> float:
    """Safely fetch single value from database result."""
    if result is None or len(result) <= index:
        return 0.0
    return float(result[index]) if result[index] is not None else 0.0


def log_step(step_name: str) -> None:
    """Log a pipeline step with rich formatting."""
    console.print(f"\n[bold blue]Running {step_name}...[/bold blue]")


def log_success(message: str) -> None:
    """Log a success message with rich formatting."""
    console.print(f"[bold green]✓ {message}[/bold green]")


def log_error(message: str) -> None:
    """Log an error message with rich formatting."""
    console.print(f"[bold red]✗ {message}[/bold red]")
