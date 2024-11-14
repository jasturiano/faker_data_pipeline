"""Data ingestion module for fetching and processing person data."""

import argparse
import asyncio
import json
import logging
import random
import sys
import traceback
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Union

import duckdb
import httpx
from pydantic import BaseModel
from tqdm import tqdm

from data_pipeline.data_storage import ensure_database_setup, store_data
from data_pipeline.models import Person, PersonAnonymized
from data_pipeline.utils.metrics import DEFAULT_METRICS_PORT, start_metrics_server

logger = logging.getLogger(__name__)

DB_PATH = "persons.duckdb"


class DateEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling dates."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


@dataclass
class FakerAPIConfig:
    """Configuration for Faker API."""

    total: int = 30000
    gender: str = "male"
    batch_size: int = 1000


class DataFetcher:
    """Handles data fetching and processing from Faker API."""

    BASE_URL = "https://fakerapi.it/api/v2/persons"
    MAX_BATCH_SIZE = 1000
    MAX_RETRIES = 5
    BASE_DELAY = 1.0
    MAX_DELAY = 30.0

    def __init__(
        self,
        config: Union[FakerAPIConfig, int],
        gender: str = "male",
        batch_size: int = 1000,
    ):
        if isinstance(config, FakerAPIConfig):
            self.total = config.total
            self.gender = config.gender
            self.batch_size = min(config.batch_size, self.MAX_BATCH_SIZE)
            self.config = config
        else:
            self.total = config
            self.gender = gender
            self.batch_size = min(batch_size, self.MAX_BATCH_SIZE)
            self.config = FakerAPIConfig(
                total=self.total, gender=self.gender, batch_size=self.batch_size
            )

    async def get_persons(self) -> List[PersonAnonymized]:
        """Fetch persons data from the API asynchronously."""
        async with httpx.AsyncClient() as client:
            return await self._fetch_batches(client)

    async def _fetch_batches(self, client: httpx.AsyncClient) -> List[PersonAnonymized]:
        persons = []
        remaining = self.total

        # Create progress bar
        pbar = tqdm(
            total=self.total,
            desc="Fetching records",
            unit="records",
            unit_scale=True,
            ncols=80,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        )

        while remaining > 0:
            batch = await self._fetch_batch(client, min(self.MAX_BATCH_SIZE, remaining))
            persons.extend(batch)
            remaining -= len(batch)
            pbar.update(len(batch))  # Update progress bar

        pbar.close()
        return persons

    async def _fetch_batch(
        self, client: httpx.AsyncClient, size: int
    ) -> List[PersonAnonymized]:
        params = {"_quantity": str(size), "_gender": self.gender}

        for retry in range(self.MAX_RETRIES):
            try:
                response = await client.get(self.BASE_URL, params=params, timeout=30.0)
                response.raise_for_status()
                data = response.json()

                if data.get("status") != "OK":
                    raise ValueError("API returned non-OK status")

                return [
                    PersonAnonymized(
                        **Person(**self._transform_api_data(p)).anonymize()
                    )
                    for p in data["data"]
                ]
            except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as e:
                delay = min(self.BASE_DELAY * (2**retry), self.MAX_DELAY)
                delay *= 0.5 + random.random()  # Add jitter

                if retry == self.MAX_RETRIES - 1:
                    logger.error(
                        f"Max retries ({self.MAX_RETRIES}) exceeded. Last error: {str(e)}"
                    )
                    raise

                logger.warning(
                    f"Attempt {retry + 1}/{self.MAX_RETRIES} failed. "
                    f"Retrying in {delay:.2f} seconds. Error: {str(e)}"
                )
                await asyncio.sleep(delay)

        raise ValueError("Failed to fetch data after all retries")

    def _transform_api_data(self, person_data: dict) -> dict:
        """Transform API data to match our model structure."""
        return {
            "firstname": person_data["firstname"],
            "lastname": person_data["lastname"],
            "email": person_data["email"],
            "email_provider": person_data["email"].split("@")[1],
            "phone": person_data["phone"],
            "gender": person_data["gender"],
            "birthday": person_data["birthday"],
            "address": person_data["address"],
        }

    async def fetch_and_save(self) -> None:
        """Main method to fetch and save data."""
        try:
            logger.info(f"Starting data fetch with config: {self.config}")
            persons = await self.get_persons()

            if persons:
                logger.info("Storing data...")
                conn = duckdb.connect(DB_PATH)
                try:
                    ensure_database_setup(conn)
                    store_data(persons, batch_size=self.config.batch_size)
                    logger.info("Data stored successfully")
                finally:
                    conn.close()
            else:
                logger.error("No data to store!")
                raise ValueError("No valid data fetched from API")
            return None  # Explicit return None
        except Exception as e:
            logger.error(f"Pipeline Error: {str(e)}")
            raise


async def main() -> None:
    """Main function to run the data ingestion pipeline."""
    # Configure logging first
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    # Set httpx logging to WARNING to suppress request logs
    logging.getLogger("httpx").setLevel(logging.WARNING)

    try:
        # Start metrics server with retries
        logger.info("Starting metrics server...")
        start_metrics_server(DEFAULT_METRICS_PORT)
        await asyncio.sleep(3)  # Give server time to start

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
        logger.info(
            f"Starting data ingestion with: Gender={args.gender}, Batch size={args.batch_size}, Total records={args.total}"
        )

        config = FakerAPIConfig(
            total=args.total, gender=args.gender, batch_size=args.batch_size
        )

        fetcher = DataFetcher(config)
        persons = await fetcher.get_persons()

        if not persons:
            logger.error("No data was fetched!")
            sys.exit(1)

        logger.info(f"Fetched {len(persons)} records")
        conn = duckdb.connect(DB_PATH)
        try:
            ensure_database_setup(conn)
            store_data(persons, batch_size=args.batch_size)
            logger.info("Data stored successfully")
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error during data ingestion: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
