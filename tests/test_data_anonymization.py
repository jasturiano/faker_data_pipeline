from datetime import datetime

import pytest

from data_pipeline.data_anonymization import anonymize_data


def test_anonymize_data():
    input_data = {
        "email": "test@gmail.com",
        "birthday": "1991-04-12",
        "address": {"country": "Germany"},
    }

    current_year = datetime.now().year
    age = current_year - 1991
    decade = (age // 10) * 10
    expected_age_group = f"[{decade}-{decade + 9}]"

    expected = {
        "email_provider": "gmail.com",
        "country": "Germany",
        "age_group": expected_age_group,  # Should be [30-39] in 2024
    }

    result = anonymize_data(input_data)
    assert result == expected
