import logging
import time
from pathlib import Path
from typing import Any, List, Optional, Tuple

import duckdb
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.style import Style
from rich.table import Table

from data_pipeline.data_storage import ensure_database_setup, verify_table_exists
from data_pipeline.utils.metrics import PROCESSING_TIME, start_metrics_server
from data_pipeline.utils.quality import check_data_quality
from data_pipeline.utils.read_sql import read_sql_file

logger = logging.getLogger(__name__)

DB_PATH = "persons.duckdb"


def _safe_fetch_value(row: Optional[Tuple[Any, ...]]) -> float:
    """Safely extract value from database row or return 0."""
    return row[0] if row else 0


def generate_report(db_path: str = DB_PATH) -> None:
    """Generate reports from the stored data."""
    console = Console()

    try:
        with Progress() as progress:
            task = progress.add_task("[cyan]Generating report...", total=100)

            start_metrics_server()
            conn = duckdb.connect(db_path)
            ensure_database_setup(conn)
            progress.update(task, advance=20)

            quality_metrics = check_data_quality(conn)
            progress.update(task, advance=30)

            # Data Quality Section
            console.print(
                "\n[bold cyan]Data Quality Dashboard[/bold cyan]", justify="center"
            )
            console.print("=" * 80, justify="center")

            # Summary Table
            summary_table = Table(show_header=True, header_style="bold magenta")
            summary_table.add_column("Metric", style="cyan")
            summary_table.add_column("Value", justify="right")

            summary_table.add_row("Total Records", str(quality_metrics.total_records))
            summary_table.add_row("Valid Records", str(quality_metrics.valid_records))
            summary_table.add_row(
                "Overall Quality Score", f"{quality_metrics.overall_score:.2%}"
            )
            console.print(Panel(summary_table, title="Summary", border_style="cyan"))

            # Detailed Metrics Table
            metrics_table = Table(show_header=True, header_style="bold magenta")
            metrics_table.add_column("Field")
            metrics_table.add_column("Completeness", justify="right")
            metrics_table.add_column("Uniqueness", justify="right")
            metrics_table.add_column("Format Validity", justify="right")

            for field in quality_metrics.completeness.keys():
                metrics_table.add_row(
                    field,
                    f"{quality_metrics.completeness.get(field, 0):.2%}",
                    f"{quality_metrics.uniqueness.get(field, 0):.2%}",
                    f"{quality_metrics.format_validity.get(field, 0):.2%}",
                )

            console.print(
                Panel(metrics_table, title="Detailed Metrics", border_style="green")
            )

            # PII Masking Score
            pii_panel = Panel(
                f"[bold]{quality_metrics.pii_masking:.2%}[/bold]",
                title="PII Masking Score",
                border_style="red" if quality_metrics.pii_masking < 1 else "green",
            )
            console.print(pii_panel)

            # Analytics Section
            console.print(
                "\n[bold cyan]Analytics Dashboard[/bold cyan]", justify="center"
            )
            console.print("=" * 80, justify="center")

            # Gmail Usage Analytics
            analytics_table = Table(show_header=True, header_style="bold magenta")
            analytics_table.add_column("Metric", style="cyan")
            analytics_table.add_column("Value", justify="right")

            # Execute analytics queries
            gmail_germany = conn.execute(
                read_sql_file("query_percentage_germany_gmail.sql")
            ).fetchone()
            gmail_over_60 = conn.execute(
                read_sql_file("query_over_60_gmail.sql")
            ).fetchone()

            analytics_table.add_row(
                "Gmail Users in Germany 🇩🇪", f"{_safe_fetch_value(gmail_germany):.2%}"
            )
            analytics_table.add_row(
                "Gmail Users Over 60 👴", str(_safe_fetch_value(gmail_over_60))
            )

            console.print(
                Panel(
                    analytics_table,
                    title="\nGmail Usage Stats",
                    border_style="turquoise2",
                )
            )

            # Top Countries Table
            top_countries = conn.execute(
                read_sql_file("query_top_countries_gmail.sql")
            ).fetchall()
            if top_countries:
                countries_table = Table(show_header=True, header_style="bold magenta")
                countries_table.add_column("Country", style="cyan")
                countries_table.add_column("Gmail Users", justify="right")

                for country, count in top_countries:
                    countries_table.add_row(country, str(count))

                console.print(
                    Panel(
                        countries_table,
                        title="Top Countries by Gmail Usage 🔝",
                        border_style="turquoise2",
                    )
                )

            progress.update(task, advance=100)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_report()
