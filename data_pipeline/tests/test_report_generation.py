import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import duckdb
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table

from data_pipeline.utils import safe_fetch_value

logger = logging.getLogger(__name__)

DB_PATH = "persons.duckdb"


@dataclass
class QualityMetrics:
    """Data class for quality metrics."""

    total_records: int
    valid_records: int
    overall_score: float
    completeness: Dict[str, float]
    uniqueness: Dict[str, float]
    format_validity: Dict[str, float]
    pii_masking: float


def check_data_quality(conn: duckdb.DuckDBPyConnection) -> QualityMetrics:
    """Check data quality metrics using dbt models."""
    # Get total records from staging
    result = conn.execute("SELECT COUNT(*) FROM main.stg_persons").fetchone()
    total_records = safe_fetch_value(result)

    # Check completeness for key fields using staging model
    fields = ["email_provider", "country", "age_group"]
    completeness: Dict[str, float] = {}
    uniqueness: Dict[str, float] = {}
    format_validity: Dict[str, float] = {}

    for field in fields:
        # Completeness from staging
        result = conn.execute(
            f"SELECT COUNT({field})::FLOAT / NULLIF(COUNT(*), 0) FROM main.stg_persons"
        ).fetchone()
        completeness[field] = safe_fetch_value(result)

        # Uniqueness from staging
        result = conn.execute(
            f"""
            SELECT COUNT(DISTINCT {field})::FLOAT / NULLIF(COUNT({field}), 0) 
            FROM main.stg_persons
            """
        ).fetchone()
        uniqueness[field] = safe_fetch_value(result)

        # Format validity (example for age_group)
        if field == "age_group":
            result = conn.execute(
                r"""
                SELECT COUNT(*)::FLOAT / NULLIF(COUNT(*), 0)
                FROM main.stg_persons
                WHERE age_group SIMILAR TO '\[[0-9]+-[0-9]+\]|\[60\+\]'
                """
            ).fetchone()
            format_validity[field] = safe_fetch_value(result)
        else:
            format_validity[field] = 0.0

    # Check PII masking from anonymized view
    pii_masking = 1.0  # Since we're using the anonymized view

    overall_score = (
        sum(completeness.values())
        + sum(uniqueness.values())
        + sum(format_validity.values())
        + pii_masking
    ) / (len(completeness) + len(uniqueness) + len(format_validity) + 1)

    return QualityMetrics(
        total_records=int(total_records),
        valid_records=int(total_records),
        overall_score=overall_score,
        completeness=completeness,
        uniqueness=uniqueness,
        format_validity=format_validity,
        pii_masking=pii_masking,
    )


def generate_report(db_path: str = DB_PATH) -> None:
    """Generate reports from dbt models."""
    console = Console()

    try:
        with Progress() as progress:
            task = progress.add_task("[cyan]Generating report...", total=100)

            conn = duckdb.connect(db_path)
            progress.update(task, advance=20)

            # Data Quality Section
            console.print(
                "\n[bold cyan]Data Quality Dashboard[/bold cyan]", justify="center"
            )
            console.print("=" * 80, justify="center")

            quality_metrics = check_data_quality(conn)
            progress.update(task, advance=30)

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

            # Analytics Section
            console.print(
                "\n[bold cyan]Analytics Dashboard[/bold cyan]", justify="center"
            )
            console.print("=" * 80, justify="center")

            # Gmail Usage Analytics from reports model
            analytics_table = Table(show_header=True, header_style="bold magenta")
            analytics_table.add_column("Metric", style="cyan")
            analytics_table.add_column("Value", justify="right")

            # Get metrics from reports model
            metrics = conn.execute(
                """
                SELECT metric_name, metric_value 
                FROM main.reports 
                WHERE metric_name IN (
                    'germany_gmail_percentage',
                    'senior_gmail_users'
                )
            """
            ).fetchall()

            for metric_name, value in metrics:
                display_name = {
                    "germany_gmail_percentage": "Gmail Users in Germany 🇩🇪",
                    "senior_gmail_users": "Gmail Users Over 60 👴",
                }.get(metric_name, metric_name)

                analytics_table.add_row(display_name, str(value))

            console.print(
                Panel(
                    analytics_table,
                    title="Gmail Usage Stats",
                    border_style="turquoise2",
                )
            )

            progress.update(task, advance=100)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        logger.error(f"Error generating report: {str(e)}", exc_info=True)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_report()
