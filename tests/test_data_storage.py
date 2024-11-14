from datetime import date
from pathlib import Path

import duckdb
import pytest

from data_pipeline.data_storage import ensure_database_setup, store_data, verify_data
from data_pipeline.models import PersonAnonymized


@pytest.fixture
def sample_data():
    return [
        {"email_provider": "gmail.com", "country": "Germany", "age_group": "[30-40]"},
        {"email_provider": "yahoo.com", "country": "France", "age_group": "[40-50]"},
    ]


@pytest.fixture
def mock_sql_file(monkeypatch):
    def mock_read_sql(filename: str) -> str:
        return """
        CREATE TABLE IF NOT EXISTS persons (
            id BIGINT PRIMARY KEY,
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
        """

    monkeypatch.setattr("data_pipeline.utils.read_sql.read_sql_file", mock_read_sql)


@pytest.fixture
def test_db(tmp_path, mock_sql_file):
    """Create a test database with the correct schema."""
    db_path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_path)
    ensure_database_setup(conn)
    conn.close()
    return db_path


@pytest.fixture
def test_persons():
    return [
        PersonAnonymized(
            firstname="****",
            lastname="****",
            phone="****",
            email_provider="gmail.com",
            gender="M",
            country="Germany",
            city="****",
            street="****",
            zipcode="****",
            age_group="[30-40]",
            location_masked=True,
        ),
        PersonAnonymized(
            firstname="****",
            lastname="****",
            phone="****",
            email_provider="yahoo.com",
            gender="F",
            country="France",
            city="****",
            street="****",
            zipcode="****",
            age_group="[40-50]",
            location_masked=True,
        ),
    ]


def test_store_data(test_db, test_persons):
    store_data(test_persons, db_path=test_db)

    # Verify the data was stored correctly
    conn = duckdb.connect(test_db)
    result = conn.execute(
        """
        SELECT email_provider, country, age_group 
        FROM persons 
        ORDER BY email_provider
    """
    ).fetchall()
    conn.close()

    # Basic data verification
    assert len(result) == 2
    assert result[0][0] == "gmail.com"
    assert result[0][1] == "Germany"
    assert result[0][2] == "[30-40]"
    assert result[1][0] == "yahoo.com"
    assert result[1][1] == "France"
    assert result[1][2] == "[40-50]"

    # Verify metrics were updated
    from prometheus_client import REGISTRY

    quality_score = REGISTRY.get_sample_value("pipeline_quality_score")
    pii_score = REGISTRY.get_sample_value("pipeline_pii_masking_score")

    assert quality_score is not None and quality_score > 0
    assert pii_score is not None and pii_score > 0


def test_verify_data(test_db, test_persons):
    store_data(test_persons, db_path=test_db)

    # Verify the data
    assert verify_data(test_db) is True


def test_verify_data_empty_table(test_db):
    # Should return False for empty table
    assert verify_data(test_db) is False


def test_verify_data_invalid_age_group(test_db):
    conn = duckdb.connect(test_db)
    conn.execute(
        """
        INSERT INTO persons (
            id, firstname, lastname, email_provider, phone, age_group, 
            gender, country, city, street, zipcode, location_masked
        )
        VALUES (
            1, '****John', '****Doe', 'gmail.com', '****1234', 'invalid-format',
            'M', 'Germany', '****City', '****Street', '****123', true
        )
        """
    )

    # Should return False for invalid age group format
    assert verify_data(test_db) is False


def test_verify_data_valid_age_group(test_db):
    conn = duckdb.connect(test_db)
    conn.execute(
        """
        INSERT INTO persons (id, firstname, lastname, email_provider, phone, age_group, 
                           gender, country, city, street, zipcode, location_masked)
        VALUES
            (1, '****John', '****Doe', 'gmail.com', '****1234', '[30-40]', 
             'M', 'Germany', '****City', '****Street', '****123', true),
            (2, '****Jane', '****Smith', 'yahoo.com', '****5678', '[40-50]', 
             'F', 'France', '****City', '****Street', '****456', true)
        """
    )
    conn.close()

    # Should return True for valid data
    assert verify_data(test_db) is True
