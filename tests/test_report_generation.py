from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from data_pipeline.data_storage import ensure_database_setup, store_data
from data_pipeline.models import PersonAnonymized
from data_pipeline.report_generation import generate_report, start_metrics_server


@pytest.fixture
def setup_test_db(tmp_path):
    db_path = str(tmp_path / "test_data.duckdb")
    conn = duckdb.connect(db_path)

    # Create and populate test table
    conn.execute(
        """
        CREATE TABLE persons (
            id INTEGER PRIMARY KEY,
            firstname VARCHAR,
            lastname VARCHAR,
            email_provider VARCHAR,
            phone VARCHAR,
            age_group VARCHAR,
            gender VARCHAR,
            country VARCHAR,
            city VARCHAR,
            street VARCHAR,
            zipcode VARCHAR,
            location_masked BOOLEAN
        )
    """
    )

    conn.close()
    return db_path


@pytest.fixture
def mock_sql_files(monkeypatch):
    def mock_read_sql(filename: str) -> str:
        queries = {
            "create_tables.sql": """
                CREATE TABLE IF NOT EXISTS persons (
                    id INTEGER PRIMARY KEY,
                    firstname VARCHAR,
                    lastname VARCHAR,
                    email_provider VARCHAR,
                    phone VARCHAR,
                    age_group VARCHAR,
                    gender VARCHAR,
                    country VARCHAR,
                    city VARCHAR,
                    street VARCHAR,
                    zipcode VARCHAR,
                    location_masked BOOLEAN
                );
            """,
            "query_percentage_germany_gmail.sql": "SELECT 1 as percentage",
            "query_top_countries_gmail.sql": "SELECT 'Germany' as country, 1 as count",
            "query_over_60_gmail.sql": "SELECT 1",
        }
        return queries[filename]

    monkeypatch.setattr("data_pipeline.utils.read_sql.read_sql_file", mock_read_sql)


def test_generate_report(setup_test_db, mock_sql_files):
    # Initialize metrics server for test
    start_metrics_server(port=9091)  # Use different port for tests

    test_data = [
        PersonAnonymized(
            firstname="****",
            lastname="****",
            email_provider="gmail.com",
            phone="****",
            age_group="[30-40]",
            gender="M",
            country="Germany",
            city="****",
            street="****",
            zipcode="****",
            location_masked=True,
        ),
        PersonAnonymized(
            firstname="****",
            lastname="****",
            email_provider="yahoo.com",
            phone="****",
            age_group="[20-30]",
            gender="F",
            country="France",
            city="****",
            street="****",
            zipcode="****",
            location_masked=True,
        ),
        PersonAnonymized(
            firstname="****",
            lastname="****",
            email_provider="gmail.com",
            phone="****",
            age_group="[40-50]",
            gender="M",
            country="Mexico",
            city="****",
            street="****",
            zipcode="****",
            location_masked=True,
        ),
    ]

    with duckdb.connect(setup_test_db) as conn:
        ensure_database_setup(conn)
        store_data(test_data, db_path=setup_test_db)

    generate_report(setup_test_db)
