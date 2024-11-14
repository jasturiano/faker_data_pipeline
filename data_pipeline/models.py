"""Data models for the pipeline."""
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Dict


@dataclass
class Person:
    """Raw person data."""

    firstname: str
    lastname: str
    email: str
    email_provider: str
    phone: str
    gender: str
    birthday: str
    address: Dict[str, Any]

    def anonymize(self) -> Dict[str, Any]:
        """Create an anonymized version of the person data."""
        return {
            "firstname": "****",
            "lastname": "****",
            "email_provider": self.email_provider,
            "phone": "****",
            "gender": self.gender,
            "country": self.address["country"],
            "city": "****",
            "street": "****",
            "zipcode": "****",
            "age_group": self._calculate_age_group(),
            "location_masked": True,
        }

    def _calculate_age_group(self) -> str:
        today = date.today()
        birth_date = datetime.strptime(self.birthday, "%Y-%m-%d").date()
        age = (
            today.year
            - birth_date.year
            - ((today.month, today.day) < (birth_date.month, birth_date.day))
        )
        decade = (age // 10) * 10
        return f"[{decade}-{decade + 9}]"


@dataclass
class PersonAnonymized:
    """Anonymized person data."""

    firstname: str
    lastname: str
    email_provider: str
    phone: str
    age_group: str
    gender: str
    country: str
    city: str
    street: str
    zipcode: str
    location_masked: bool

    def to_dict(self) -> Dict[str, Any]:
        """Convert the object to a dictionary."""
        return asdict(self)
