import logging
from pathlib import Path
from typing import List

import duckdb
import pandas as pd

from data_pipeline.models import PersonAnonymized
from data_pipeline.utils.metrics import PROCESSING_TIME, RECORDS_PROCESSED
from data_pipeline.utils.quality import check_data_quality, validate_dataframe
from data_pipeline.utils.read_sql import read_sql_file

logger = logging.getLogger(__name__)


def ensure_database_setup(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure database tables and indexes exist."""
    try:
        ddl = read_sql_file("create_tables.sql")
        conn.execute(ddl)
        logger.info("Database schema verified/created successfully")
    except Exception as e:
        logger.error(f"Error setting up database: {str(e)}")
        raise


def verify_table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Verify if a table exists in the database."""
    result = conn.execute(
        """
        SELECT count(*) 
        FROM information_schema.tables 
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()

    return result is not None and result[0] > 0


def store_data(
    persons: List[PersonAnonymized],
    batch_size: int = 1000,
    db_path: str = "persons.duckdb",
) -> None:
    """Store anonymized person data in DuckDB."""
    try:
        logger.info(f"Storing {len(persons)} records to {db_path}")
        conn = duckdb.connect(db_path)
        ensure_database_setup(conn)

        # Convert dataclass objects to dictionaries
        records = [person.to_dict() for person in persons]
        df = pd.DataFrame.from_records(records)
        df.insert(0, "id", range(len(df)))

        with PROCESSING_TIME.labels(operation="store").time():
            try:
                validate_dataframe(df)
                conn.execute("BEGIN")
                conn.execute("INSERT INTO persons SELECT * FROM df")
                conn.execute("COMMIT")

                # Update quality metrics after successful insertion
                quality_metrics = check_data_quality(conn)
                quality_metrics.update_prometheus_metrics()

                RECORDS_PROCESSED.labels(status="success").inc(len(persons))
            except Exception as e:
                conn.execute("ROLLBACK")
                RECORDS_PROCESSED.labels(status="failed").inc(len(persons))
                raise
            finally:
                conn.close()

    except Exception as e:
        logger.error(f"Error storing data: {str(e)}")
        raise


def verify_data(db_path: str = "persons.duckdb") -> bool:
    """Verify that data was stored correctly and meets basic quality checks."""
    try:
        conn = duckdb.connect(db_path)

        # Check if table exists
        if not verify_table_exists(conn, "persons"):
            logger.error("Table 'persons' does not exist")
            return False

        # Check if table has data
        result = conn.execute("SELECT COUNT(*) FROM persons").fetchone()
        if result is None or result[0] == 0:
            logger.error("No data found in persons table")
            return False

        # Check for null values in required fields
        null_check = conn.execute(
            """
            SELECT COUNT(*) 
            FROM persons 
            WHERE email_provider IS NULL 
               OR country IS NULL 
               OR age_group IS NULL
               OR gender IS NULL
        """
        ).fetchone()

        if null_check is not None and null_check[0] > 0:
            logger.error(
                f"Found {null_check[0]} rows with NULL values in required fields"
            )
            return False

        # Check age group format
        format_check = conn.execute(
            r"""
            SELECT COUNT(*) 
            FROM persons 
            WHERE NOT age_group SIMILAR TO '\[\d+-\d+\]'
            """
        ).fetchone()

        if format_check is not None and format_check[0] > 0:
            logger.error(f"Found {format_check[0]} rows with invalid age group format")
            return False

        # Check PII masking
        pii_check = conn.execute(
            """
            SELECT COUNT(*) 
            FROM persons 
            WHERE (firstname NOT LIKE '****%'
               OR lastname NOT LIKE '****%'
               OR phone NOT LIKE '****%'
               OR city NOT LIKE '****%'
               OR street NOT LIKE '****%'
               OR zipcode NOT LIKE '****%')
               AND location_masked = true
        """
        ).fetchone()

        if pii_check is not None and pii_check[0] > 0:
            logger.error(f"Found {pii_check[0]} rows with unmasked PII")
            return False

        final_count = conn.execute("SELECT COUNT(*) FROM persons").fetchone()
        if final_count is not None:
            logger.info(f"Verified {final_count[0]} records in the database")
            return True

        logger.error("Could not verify final record count")
        return False

    except Exception as e:
        logger.error(f"Error verifying data: {str(e)}")
        return False
    finally:
        conn.close()
