"""Great Expectations integration for data validation."""
from typing import Any, Dict

import great_expectations as ge
import pandas as pd

from data_pipeline.utils.metrics import VALIDATION_METRICS


def create_expectation_suite() -> ge.core.ExpectationSuite:
    """Create expectation suite for person data."""
    suite = ge.core.ExpectationSuite(expectation_suite_name="person_data_suite")

    # Add expectations
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "email_provider"},
        )
    )

    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={"column": "age_group", "regex": r"\[\d+-\d+\]"},
        )
    )

    return suite


def validate_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate DataFrame using Great Expectations."""
    ge_df = ge.from_pandas(df)
    suite = create_expectation_suite()
    results = ge_df.validate(expectation_suite=suite, result_format="COMPLETE")

    validation_results: Dict[str, Any] = {
        "success": results.success,
        "results": [
            {
                "expectation": result.expectation_config.expectation_type,
                "success": result.success,
                "result": result.result,
            }
            for result in results.results
        ],
    }

    # Update Prometheus metrics
    for result in results.results:
        status = "success" if result.success else "failure"
        VALIDATION_METRICS.labels(
            check_name=result.expectation_config.expectation_type, status=status
        ).inc()

    return validation_results
