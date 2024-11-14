from unittest.mock import Mock

import pytest


@pytest.fixture
def mock_response():
    mock = Mock()
    mock.json.return_value = {
        "data": [
            {
                "email": "test@gmail.com",
                "birthday": "1980-01-01",
                "address": {"country": "Germany"},
            },
            {
                "email": "example@yahoo.com",
                "birthday": "1990-01-01",
                "address": {"country": "France"},
            },
        ]
    }
    mock.raise_for_status.return_value = None
    return mock
