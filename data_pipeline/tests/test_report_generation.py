import unittest
from unittest.mock import MagicMock, patch

import duckdb

from data_pipeline.report_generation import (
    QualityMetrics,
    check_data_quality,
    generate_report,
)


class TestReportGeneration(unittest.TestCase):
    @patch("data_pipeline.report_generation.duckdb.connect")
    def test_check_data_quality(self, mock_connect: MagicMock) -> None:
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor

        # Mock the fetchone results for each query
        mock_cursor.fetchone.side_effect = [
            (100,),  # Total records
            (0.9,),  # Completeness for email_provider
            (0.8,),  # Uniqueness for email_provider
            (0.95,),  # Format validity for email_provider
            (0.85,),  # Completeness for country
            (0.75,),  # Uniqueness for country
            (0.9,),  # Format validity for country
            (0.8,),  # Completeness for age_group
            (0.7,),  # Uniqueness for age_group
            (0.85,),  # Format validity for age_group
        ]

        # Call the function
        metrics = check_data_quality(mock_conn)

        # Assertions
        self.assertEqual(metrics.total_records, 100)
        self.assertAlmostEqual(metrics.completeness["email_provider"], 0.9)
        self.assertAlmostEqual(metrics.uniqueness["email_provider"], 0.8)
        self.assertAlmostEqual(metrics.format_validity["email_provider"], 0.95)

    @patch("data_pipeline.report_generation.Path.exists", return_value=True)
    @patch("data_pipeline.report_generation.Console")
    @patch("data_pipeline.report_generation.duckdb.connect")
    def test_generate_report(
        self,
        mock_connect: MagicMock,
        mock_console: MagicMock,
        mock_path_exists: MagicMock,
    ) -> None:
        # Mock the database connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Mock the console
        mock_console_instance = mock_console.return_value

        # Mock the database queries
        mock_conn.execute.return_value.fetchone.side_effect = [
            (1,),  # Test connection
            (100,),  # Total records
            (0.9,),  # Completeness for email_provider
            (0.8,),  # Uniqueness for email_provider
            (0.95,),  # Format validity for email_provider
            (0.85,),  # Completeness for country
            (0.75,),  # Uniqueness for country
            (0.9,),  # Format validity for country
            (0.8,),  # Completeness for age_group
            (0.7,),  # Uniqueness for age_group
            (0.85,),  # Format validity for age_group
            (50.0,),  # Gmail percentage in Germany
            (10, 100, 10.0),  # Gmail users over 60
        ]

        # Mock the fetchall for top countries
        mock_conn.execute.return_value.fetchall.return_value = [
            ("Country1", 1000),
            ("Country2", 800),
            ("Country3", 600),
        ]

        # Call the function
        generate_report()

        # Assertions
        mock_console_instance.print.assert_called()  # Check if console print was called


if __name__ == "__main__":
    unittest.main()
