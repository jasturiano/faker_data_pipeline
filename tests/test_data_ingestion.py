from dataclasses import dataclass, field
from datetime import date
from typing import Dict
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from data_pipeline.data_ingestion import DataFetcher, FakerAPIConfig
from data_pipeline.models import Person, PersonAnonymized


@dataclass
class MockResponse:
    status_code: int = 200
    data: Dict = field(
        default_factory=lambda: {
            "status": "OK",
            "data": [
                {
                    "firstname": "John",
                    "lastname": "Doe",
                    "email": "test@gmail.com",
                    "phone": "1234567890",
                    "gender": "M",
                    "birthday": "1990-01-01",
                    "address": {
                        "country": "Germany",
                        "city": "Berlin",
                        "street": "Test St",
                        "zipcode": "12345",
                        "country_code": "DE",
                        "latitude": 52.520008,
                        "longitude": 13.404954,
                    },
                }
            ],
        }
    )

    def json(self):
        return self.data

    def raise_for_status(self):
        if self.status_code != 200:
            raise httpx.HTTPError("Test error")


@pytest.fixture
def config():
    return FakerAPIConfig(total=2, gender="male", batch_size=2)


@pytest.fixture
def mock_person_data():
    return {
        "firstname": "John",
        "lastname": "Doe",
        "email": "test@gmail.com",
        "email_provider": "gmail.com",
        "phone": "1234567890",
        "gender": "M",
        "birthday": "1990-01-01",
        "address": {
            "country": "Germany",
            "city": "Berlin",
            "street": "Test St",
            "zipcode": "12345",
            "country_code": "DE",
            "latitude": 52.520008,
            "longitude": 13.404954,
        },
    }


def test_process_record(mock_person_data):
    # Create a Person object from mock data
    person = Person(**mock_person_data)

    # Transform and anonymize the person data
    anonymized = PersonAnonymized(**person.anonymize())

    current_year = date.today().year
    age = current_year - 1990  # Based on mock birth year
    expected_age_group = f"[{(age // 10) * 10}-{(age // 10) * 10 + 9}]"

    # Assert the expected anonymized data
    assert anonymized.email_provider == "gmail.com"
    assert anonymized.country == "Germany"
    assert anonymized.age_group == expected_age_group


@pytest.mark.asyncio
async def test_get_persons_retry():
    config = FakerAPIConfig(total=1, gender="male", batch_size=1000)

    success_response = {
        "status": "OK",
        "code": 200,
        "total": 1,
        "data": [
            {
                "firstname": "John",
                "lastname": "Doe",
                "email": "test@gmail.com",
                "phone": "1234567890",
                "gender": "M",
                "birthday": "1990-01-01",
                "address": {
                    "country": "Germany",
                    "city": "Berlin",
                    "street": "Test St",
                    "zipcode": "12345",
                    "country_code": "DE",
                    "latitude": 52.520008,
                    "longitude": 13.404954,
                },
            }
        ],
    }

    call_count = 0

    async def mock_get(*args, **kwargs):
        nonlocal call_count
        call_count += 1

        if call_count == 1:  # First call fails
            raise httpx.RequestError("Connection error")

        response = AsyncMock()
        response.status_code = 200
        response.raise_for_status = lambda: None
        response.json = lambda: success_response
        response.headers = {
            "content-type": "application/json",
            "server": "nginx/1.18.0",
            "content-length": "500",
        }
        return response

    with patch("httpx.AsyncClient") as mock_async_client:
        mock_client = AsyncMock()
        mock_client.get = mock_get
        mock_async_client.return_value.__aenter__.return_value = mock_client

        fetcher = DataFetcher(config)
        result = await fetcher.get_persons()

        assert len(result) == 1
        assert result[0].firstname == "****"
        assert result[0].lastname == "****"
        assert result[0].email_provider == "gmail.com"
        assert result[0].country == "Germany"
        assert call_count == 2  # One failed attempt + one successful


@pytest.mark.asyncio
async def test_fetch_and_save(config):
    # Create test data
    persons = [
        PersonAnonymized(
            firstname="****",
            lastname="****",
            email_provider="gmail.com",
            phone="****",
            gender="M",
            country="Germany",
            city="****",
            street="****",
            zipcode="****",
            age_group="[30-40]",
            location_masked=True,
        )
    ]

    async def mock_get_persons():
        return persons

    with patch("data_pipeline.data_ingestion.store_data") as mock_store:
        fetcher = DataFetcher(config)
        fetcher.get_persons = mock_get_persons
        await fetcher.fetch_and_save()

        mock_store.assert_called_once_with(persons, batch_size=config.batch_size)


@pytest.mark.asyncio
async def test_fetch_and_save_no_data(config):
    async def mock_get_persons():
        return []

    with patch(
        "data_pipeline.data_ingestion.store_data"
    ) as mock_store:  # Updated mock path
        fetcher = DataFetcher(config)
        fetcher.get_persons = mock_get_persons

        with pytest.raises(ValueError, match="No valid data fetched from API"):
            await fetcher.fetch_and_save()

        mock_store.assert_not_called()
