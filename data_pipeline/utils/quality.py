"""Data quality monitoring and checks."""
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, cast

import duckdb
import great_expectations as ge
import pandas as pd

from data_pipeline.utils.expectations import create_expectation_suite
from data_pipeline.utils.metrics import (
    PII_MASKING_SCORE,
    PROCESSING_TIME,
    QUALITY_METRICS,
    QUALITY_SCORE,
    RECORDS_PROCESSED,
    VALIDATION_METRICS,
)

logger = logging.getLogger(__name__)


@dataclass
class QualityMetrics:
    """Data quality metrics container."""

    completeness: Dict[str, float]
    uniqueness: Dict[str, float]
    pii_masking: float
    format_validity: Dict[str, float]
    total_records: int
    valid_records: int

    @property
    def overall_score(self) -> float:
        """Calculate overall quality score."""
        if not self.completeness.values() or not self.uniqueness.values():
            logger.warning("Empty completeness or uniqueness values")
            return 0.0

        weights = {"completeness": 0.4, "uniqueness": 0.3, "pii": 0.2, "format": 0.1}

        completeness_score = sum(self.completeness.values()) / len(self.completeness)
        uniqueness_score = sum(self.uniqueness.values()) / len(self.uniqueness)
        format_score = sum(self.format_validity.values()) / len(self.format_validity)

        return (
            completeness_score * weights["completeness"]
            + uniqueness_score * weights["uniqueness"]
            + self.pii_masking * weights["pii"]
            + format_score * weights["format"]
        )

    def update_prometheus_metrics(self) -> None:
        """Update Prometheus metrics."""
        try:
            # Reset existing metrics
            QUALITY_SCORE._value.set(0)
            PII_MASKING_SCORE._value.set(0)

            # Set scores
            QUALITY_SCORE._value.set(self.overall_score)
            PII_MASKING_SCORE._value.set(self.pii_masking)

            # Set individual metrics
            for field, value in self.completeness.items():
                QUALITY_METRICS.labels(
                    metric_name=field, check_type="completeness"
                ).set(value)

            for field, value in self.uniqueness.items():
                QUALITY_METRICS.labels(metric_name=field, check_type="uniqueness").set(
                    value
                )

            for field, value in self.format_validity.items():
                QUALITY_METRICS.labels(metric_name=field, check_type="format").set(
                    value
                )

            # Set PII masking as a quality metric
            QUALITY_METRICS.labels(metric_name="pii", check_type="masking").set(
                self.pii_masking
            )
        except Exception as e:
            logger.error(f"Failed to update Prometheus metrics: {str(e)}")
            raise


def _safe_fetch_value(result: Optional[Tuple[Any, ...]], index: int = 0) -> float:
    """Safely fetch single value from database result."""
    if result is None or len(result) <= index:
        return 0.0
    return float(result[index]) if result[index] is not None else 0.0


def _safe_fetch_result(
    result: Optional[Tuple[Any, ...]], expected_length: int
) -> List[float]:
    """Safely convert database result to list of floats."""
    if result is None:
        return [0.0] * expected_length
    return [float(x) if x is not None else 0.0 for x in result[:expected_length]]


def _check_completeness(
    conn: duckdb.DuckDBPyConnection, fields: List[str]
) -> Dict[str, float]:
    """Check completeness of required fields."""
    query = f"""
    SELECT {','.join(
        f"SUM(CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) as {field}_completeness" 
        for field in fields
    )} FROM persons
    """
    result = conn.execute(query).fetchone()
    values = _safe_fetch_result(result, len(fields))
    return dict(zip(fields, values))


def _check_uniqueness(
    conn: duckdb.DuckDBPyConnection, fields: List[str]
) -> Dict[str, float]:
    """Check uniqueness of fields."""
    query = f"""
    SELECT {','.join(
        f"COUNT(DISTINCT {field})::FLOAT / NULLIF(COUNT(*), 0) as {field}_uniqueness" 
        for field in fields
    )} FROM persons
    """
    result = conn.execute(query).fetchone()
    values = _safe_fetch_result(result, len(fields))
    return dict(zip(fields, values))


def _check_pii_masking(conn: duckdb.DuckDBPyConnection) -> float:
    """Check PII masking compliance."""
    query = """
    SELECT COALESCE(
        SUM(CASE 
            WHEN firstname LIKE '****%' 
            AND lastname LIKE '****%'
            AND phone LIKE '****%'
            AND city LIKE '****%'
            AND street LIKE '****%'
            AND zipcode LIKE '****%'
            AND location_masked = true
            THEN 1 ELSE 0 
        END)::FLOAT / NULLIF(COUNT(*), 0),
        0.0
    ) FROM persons
    """
    result = conn.execute(query).fetchone()
    return _safe_fetch_value(result)


def _check_format_validity(conn: duckdb.DuckDBPyConnection) -> Dict[str, float]:
    """Check format validity of fields."""
    query = r"""
    SELECT COALESCE(
        SUM(CASE 
            WHEN age_group SIMILAR TO '\[\d+-\d+\]' 
            AND CAST(REGEXP_EXTRACT(age_group, '\[(\d+)-\d+\]', 1) AS INTEGER) <= 
                CAST(REGEXP_EXTRACT(age_group, '\[\d+-(\d+)\]', 1) AS INTEGER)
            THEN 1 
            ELSE 0 
        END)::FLOAT / NULLIF(COUNT(*), 0),
        0.0
    ) FROM persons
    """
    result = conn.execute(query).fetchone()
    return {"age_group": _safe_fetch_value(result)}


def check_data_quality(conn: duckdb.DuckDBPyConnection) -> QualityMetrics:
    """Run comprehensive data quality checks."""
    try:
        # Verify we have enough data for meaningful metrics
        count_result = conn.execute("SELECT COUNT(*) FROM persons").fetchone()
        if count_result is None or count_result[0] < 2:
            logger.warning("Insufficient data for quality metrics")
            raise ValueError("Need at least 2 records for meaningful metrics")

        fields = ["email_provider", "country", "age_group", "gender"]
        total_records = count_result[0]

        metrics = QualityMetrics(
            completeness=_check_completeness(conn, fields),
            uniqueness=_check_uniqueness(conn, fields),
            pii_masking=_check_pii_masking(conn),
            format_validity=_check_format_validity(conn),
            total_records=int(total_records),
            valid_records=_count_valid_records(conn, fields),
        )

        metrics.update_prometheus_metrics()
        return metrics

    except Exception as e:
        logger.error(f"Error in quality check: {e}")
        raise


def _count_valid_records(conn: duckdb.DuckDBPyConnection, fields: List[str]) -> int:
    """Count records that pass all quality checks."""
    query = f"""
    SELECT COUNT(*) 
    FROM persons 
    WHERE {' AND '.join(f"{field} IS NOT NULL" for field in fields)}
    AND age_group SIMILAR TO '\[\d+-\d+\]'
    AND location_masked = true
    """
    result = conn.execute(query).fetchone()
    return int(_safe_fetch_value(result))


def validate_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate DataFrame using Great Expectations."""
    ge_df = ge.from_pandas(df)
    suite = create_expectation_suite()
    results = ge_df.validate(expectation_suite=suite, result_format="COMPLETE")

    validation_results: Dict[str, Any] = {"success": results.success, "results": []}

    for result in results.results:
        check_name = result.expectation_config.expectation_type
        status = "success" if result.success else "failure"
        VALIDATION_METRICS.labels(check_name=check_name, status=status).inc(
            len(df)
        )  # Increment by number of records checked
        validation_results["results"].append(
            {
                "expectation": check_name,
                "success": result.success,
                "result": result.result,
            }
        )

    return validation_results
