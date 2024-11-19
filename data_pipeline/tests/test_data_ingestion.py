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
                "firstname": "Clark",
                "lastname": "Kent",
                "email": "clark.kent@example.com",
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
    # Create mock response data
    mock_response_data = {
        "status": "OK",
        "code": 200,
        "data": [
            {
                "id": 1,
                "firstname": "Clark",
                "lastname": "Kent",
                "email": "clark.kent@example.com",
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

    # Create mock response
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = lambda: mock_response_data

    # Create mock client
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response

    # Create fetcher instance
    fetcher = DataFetcher(total=10, gender="male", batch_size=10)

    # Execute the fetch
    batch_data = await fetcher._fetch_batch(client=mock_client, batch_id=1)

    # Assertions
    assert isinstance(batch_data, list)
    assert len(batch_data) == 1
    assert batch_data[0]["firstname"] == "Clark"

    # Verify the API was called with correct parameters
    mock_client.get.assert_called_once_with(
        fetcher.base_url,
        params={
            "gender": "male",
            "_quantity": 10,
            "_seed": 1,
        },
        timeout=20.0,
    )


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
