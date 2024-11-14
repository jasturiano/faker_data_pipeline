"""Prometheus metrics for pipeline monitoring."""
import logging
import socket
from typing import Any

from prometheus_client import (
    GC_COLLECTOR,
    PLATFORM_COLLECTOR,
    PROCESS_COLLECTOR,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

logger = logging.getLogger(__name__)

# Unregister default collectors at the top of the file
REGISTRY.unregister(GC_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)
REGISTRY.unregister(PROCESS_COLLECTOR)

# Add at the top with other imports
DEFAULT_METRICS_PORT = 9090

# Data Quality Metrics
QUALITY_METRICS = Gauge(
    "pipeline_data_quality", "Data quality metrics", ["metric_name", "check_type"]
)

# Processing Metrics
PROCESSING_TIME = Histogram(
    "pipeline_processing_seconds",
    "Time spent processing data",
    ["operation"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0],
)

RECORDS_PROCESSED = Counter(
    "pipeline_records_total", "Total records processed", ["status"]
)

# Validation Metrics
VALIDATION_METRICS = Counter(
    "pipeline_validation_checks", "Data validation results", ["check_name", "status"]
)

# Overall Quality Score
QUALITY_SCORE = Gauge("pipeline_quality_score", "Overall data quality score")

# PII Masking Score
PII_MASKING_SCORE = Gauge("pipeline_pii_masking_score", "PII masking compliance score")


def start_metrics_server(port: int = DEFAULT_METRICS_PORT) -> None:
    """Start Prometheus metrics server."""

    def is_port_in_use() -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("localhost", port)) == 0

    try:
        if is_port_in_use():
            logger.info(f"Metrics server already running on port {port}")
            return

        logger.info(f"Starting metrics server on port {port}...")
        start_http_server(port)
        logger.info("Metrics server started successfully")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
        raise
