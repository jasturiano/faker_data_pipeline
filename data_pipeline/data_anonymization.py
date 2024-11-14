from datetime import datetime
from typing import Any, Dict


def anonymize_data(data: Dict[str, Any]) -> Dict[str, str]:
    """
    Anonymize sensitive data fields.

    Args:
        data: Dictionary containing person data

    Returns:
        Dictionary with anonymized data
    """
    birth_year = int(data["birthday"].split("-")[0])
    current_year = datetime.now().year
    age = current_year - birth_year
    decade = (age // 10) * 10
    return {
        "email_provider": data["email"].split("@")[1],
        "country": data["address"]["country"],
        "age_group": f"[{decade}-{decade + 9}]",
    }
