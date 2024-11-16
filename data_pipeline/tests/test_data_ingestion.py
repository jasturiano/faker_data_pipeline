from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pytest import FixtureRequest, MonkeyPatch

from data_pipeline.data_ingestion import DataFetcher


@pytest.fixture
def mock_api_response() -> Dict[str, Any]:
    """Fixture providing mock API response data."""
    return {
        "status": "OK",
        "code": 200,
        "data": [
            {
                "id": 1,
                "firstname": "John",
                "lastname": "Doe",
                "email": "john.doe@example.com",
                "phone": "1234567890",
                "birthday": "1990-01-01",
                "gender": "male",
                "address": {
                    "street": "Main St",
                    "city": "Metropolis",
                    "country": "USA",
                },
            }
        ],
    }


@pytest.fixture
def fetcher() -> DataFetcher:
    """Fixture providing configured DataFetcher instance."""
    return DataFetcher(total=10, gender="male", batch_size=5)


@pytest.mark.asyncio
async def test_fetch_batch() -> None:
    """Test fetching a single batch of data."""
    fetcher = DataFetcher(total=10, gender="male", batch_size=10)
    async with httpx.AsyncClient() as client:
        batch_data = await fetcher._fetch_batch(client=client, batch_id=1)
        assert len(batch_data) == 10


@pytest.mark.asyncio
async def test_fetch_persons() -> None:
    """Test fetching all persons."""
    fetcher = DataFetcher(total=20, gender="male", batch_size=10)
    result = await fetcher.fetch_persons()
    assert isinstance(result, list)
    assert len(result) > 0
    assert isinstance(result[0], dict)


def test_data_fetcher_init() -> None:
    """Test DataFetcher initialization."""
    fetcher = DataFetcher(
        total=100,
        gender="female",
        batch_size=50,
    )
    assert fetcher.total == 100
    assert fetcher.gender == "female"
    assert fetcher.batch_size == 50


@pytest.mark.asyncio
async def test_data_fetcher_with_invalid_params() -> None:
    """Test DataFetcher with invalid parameters."""
    with pytest.raises(ValueError):
        DataFetcher(total=-1, gender="invalid", batch_size=10)
