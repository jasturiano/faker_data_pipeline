"""Data ingestion module for fetching and processing person data."""

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, TypedDict, cast

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console
from rich.progress import Progress
from tqdm import tqdm

from .utils import console, log_step, log_success, read_sql_file

# Configure logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DB_PATH = "persons.duckdb"
TABLE_NAME = "persons"
API_URL = "https://fakerapi.it/api/v2/persons"
BATCH_SIZE = 1000


class PersonData(TypedDict):
    id: int
    firstname: str
    lastname: str
    email_provider: str
    phone: str
    birthday: str
    gender: str
    address: Dict[str, str]


class DataFetcher:
    """Fetches data from Faker API."""

    def __init__(
        self,
        total: int,
        gender: str,
        batch_size: int,
    ) -> None:
        """Initialize DataFetcher with parameters."""
        if total <= 0:
            raise ValueError("Total must be greater than 0")
        if gender not in ["male", "female"]:
            raise ValueError("Gender must be either 'male' or 'female'")
        if batch_size <= 0:
            raise ValueError("Batch size must be greater than 0")

        self.total = total
        self.gender = gender
        self.batch_size = batch_size
        self.base_url = API_URL
        self.progress_bar = tqdm(
            total=total,
            desc="Fetching records",
            unit="records",
            unit_scale=True,
            ncols=80,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        )

    async def _fetch_batch(
        self, client: httpx.AsyncClient, batch_id: int
    ) -> List[Dict[str, Any]]:
        """Fetch a batch of fake data with retries."""
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                response = await client.get(
                    self.base_url,
                    params={
                        "gender": self.gender,
                        "_quantity": self.batch_size,
                        "_seed": batch_id,
                    },
                    timeout=20.0,
                )
                response.raise_for_status()
                data = response.json()
                return cast(List[Dict[str, Any]], data["data"])
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(retry_delay * (attempt + 1))
        return []  # Return empty list if all retries fail

    async def fetch_persons(self) -> List[Dict[str, Any]]:
        """Fetch all persons data in batches."""
        num_batches = (self.total + self.batch_size - 1) // self.batch_size
        results: List[Dict[str, Any]] = []

        async with httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        ) as client:
            with Progress() as progress:
                task = progress.add_task("[cyan]Fetching data...", total=num_batches)

                tasks = [
                    self._fetch_batch(client=client, batch_id=batch_id)
                    for batch_id in range(num_batches)
                ]

                for result in asyncio.as_completed(tasks):
                    try:
                        batch_data = await result
                        results.extend(batch_data)
                        progress.advance(task)
                    except Exception as e:
                        logger.error(f"Batch failed: {str(e)}", exc_info=True)

        return results[: self.total] if results else []


def setup_database() -> None:
    """Set up the DuckDB database and create the table if it doesn't exist."""
    log_step(f"Setting up database at {DB_PATH}")

    # Read and execute create tables SQL
    create_tables_sql = read_sql_file(Path("data_pipeline/sql/create_tables.sql"))

    conn = duckdb.connect(DB_PATH)
    conn.execute(create_tables_sql)
    conn.close()

    log_success("Database setup complete")


async def main() -> None:
    """Main function to run the data ingestion pipeline."""
    parser = argparse.ArgumentParser(description="Fetch data from Faker API")
    parser.add_argument(
        "--gender", type=str, default="male", help="Gender of persons to fetch"
    )
    parser.add_argument(
        "--batch_size", type=int, default=1000, help="Batch size for processing"
    )
    parser.add_argument(
        "--total", type=int, default=30000, help="Total number of records to fetch"
    )

    args = parser.parse_args()
    log_step(
        f"Starting data ingestion with: Gender={args.gender}, "
        f"Batch size={args.batch_size}, Total records={args.total}"
    )

    try:
        # Set up database
        setup_database()

        # Fetch data
        fetcher = DataFetcher(
            total=args.total, gender=args.gender, batch_size=args.batch_size
        )
        persons = await fetcher.fetch_persons()

        if not persons:
            raise ValueError("No valid data fetched from API")

        # Save raw data to parquet
        log_step("Saving raw data to parquet...")
        raw_data_path = Path("raw_data")
        raw_data_path.mkdir(exist_ok=True)

        table = pa.Table.from_pylist(persons)
        pq.write_table(table, raw_data_path / "persons.parquet")
        log_success("Raw data saved to parquet")

        # Insert masked data into DuckDB from parquet
        log_step("Loading and masking data into DuckDB...")
        conn = duckdb.connect(DB_PATH)

        insert_sql = read_sql_file(Path("data_pipeline/sql/insert_raw_data.sql"))
        conn.execute(insert_sql)
        conn.close()

        log_success(f"Successfully processed {len(persons)} records")

    except Exception as e:
        logger.error(f"Error during data ingestion: {str(e)}", exc_info=True)
        raise


def run_async_main() -> None:
    """Run the async main function."""
    asyncio.run(main())


if __name__ == "__main__":
    run_async_main()
