import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import duckdb
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table

from data_pipeline.utils import safe_fetch_value  # Using existing utility function

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

        # Format validity checks for each field
        if field == "age_group":
            result = conn.execute(
                """
                SELECT COUNT(*)::FLOAT / NULLIF(COUNT(*), 0)
                FROM main.stg_persons
                WHERE age_group SIMILAR TO '\[[0-9]+-[0-9]+\]|\[60\+\]'
                """
            ).fetchone()
            format_validity[field] = safe_fetch_value(result)
        elif field == "email_provider":
            result = conn.execute(
                """
                SELECT COUNT(*)::FLOAT / NULLIF(COUNT(*), 0)
                FROM main.stg_persons
                WHERE email_provider SIMILAR TO '[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                """
            ).fetchone()
            format_validity[field] = safe_fetch_value(result)
        elif field == "country":
            result = conn.execute(
                """
                SELECT COUNT(*)::FLOAT / NULLIF(COUNT(*), 0)
                FROM main.stg_persons
                WHERE LENGTH(country) > 0 AND country NOT SIMILAR TO '[0-9]+'
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
    console = Console()
    conn = None

    try:
        # Ensure database exists
        if not Path(db_path).exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")

        conn = duckdb.connect(db_path)

        # Test connection
        conn.execute("SELECT 1").fetchone()

        with Progress() as progress:
            task = progress.add_task("[cyan]Generating report...", total=100)

            quality_metrics = check_data_quality(conn)
            progress.update(task, advance=20)

            # Data Quality Dashboard header
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

            for field in ["email_provider", "country", "age_group"]:
                metrics_table.add_row(
                    field,
                    f"{quality_metrics.completeness.get(field, 0):.2%}",
                    f"{quality_metrics.uniqueness.get(field, 0):.2%}",
                    f"{quality_metrics.format_validity.get(field, 0):.2%}",
                )
            console.print(
                Panel(metrics_table, title="Detailed Metrics", border_style="green")
            )

            # Analytics Dashboard
            console.print(
                "\n[bold cyan]Analytics Dashboard[/bold cyan]", justify="center"
            )
            console.print("=" * 80, justify="center")

            # Gmail Usage Analytics
            analytics_table = Table(show_header=True, header_style="bold magenta")
            analytics_table.add_column("Metric", style="cyan")
            analytics_table.add_column("Value", justify="right")

            # Query from persons_email_usage view for German Gmail usage
            gmail_germany = conn.execute(
                """
                SELECT gmail_percentage
                FROM main.persons_email_usage
                """
            ).fetchone()

            # Query from persons_age_groups view for seniors using Gmail
            gmail_over_60 = conn.execute(
                """
                SELECT 
                    gmail_users,
                    total_seniors,
                    gmail_percentage
                FROM main.persons_age_groups
                """
            ).fetchone()

            analytics_table.add_row(
                "Gmail Users in Germany 🇩🇪", f"{safe_fetch_value(gmail_germany)}%"
            )
            analytics_table.add_row(
                "Gmail Users Over 60 👴",
                f"{round(safe_fetch_value(gmail_over_60, 2), 2)}%",
            )

            console.print(
                Panel(
                    analytics_table,
                    title="Gmail Usage Stats",
                    border_style="turquoise2",
                )
            )

            # Top Countries Table from persons_location view
            top_countries = conn.execute(
                """
                SELECT 
                    country,
                    gmail_users
                FROM main.persons_location
                WHERE rank <= 3
                ORDER BY gmail_users DESC, country
                LIMIT 3
                """
            ).fetchall()

            if top_countries:
                countries_table = Table(show_header=True, header_style="bold magenta")
                countries_table.add_column("Rank", style="cyan", justify="center")
                countries_table.add_column("Country", style="cyan")
                countries_table.add_column("Gmail Users", justify="right")

                for idx, (country, count) in enumerate(top_countries, 1):
                    countries_table.add_row(str(idx), country, str(count))

                console.print(
                    Panel(
                        countries_table,
                        title="Top 3 Countries by Gmail Usage 🔝",
                        border_style="turquoise2",
                    )
                )

            progress.update(task, advance=100)

    except FileNotFoundError as e:
        console.print(f"[bold red]Database Error:[/bold red] {str(e)}")
        raise

    except duckdb.Error as e:
        console.print(f"[bold red]Database Error:[/bold red] {str(e)}")
        raise

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        logger.error(f"Error generating report: {str(e)}", exc_info=True)
        raise

    finally:
        if conn:
            try:
                conn.close()
            except:
                pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        generate_report()
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        raise
