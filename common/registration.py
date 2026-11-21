"""Registration numbers, normalised the same way by the consumer and the API."""

import re
from typing import Any, Final

REG_NO_PATTERN: Final = re.compile(r"SYN-\d{6}")


class InvalidRegistrationNumber(ValueError):
    pass


def parse_registration_number(value: Any) -> str:
    """Strip and upper-case, then require `SYN-` and six digits. Never guesses a missing or non-string value."""
    if value is None:
        raise InvalidRegistrationNumber("missing registrationNumber")
    if not isinstance(value, str):
        raise InvalidRegistrationNumber(f"registrationNumber must be a string, got {value!r}")
    normalised = value.strip().upper()
    if not REG_NO_PATTERN.fullmatch(normalised):
        raise InvalidRegistrationNumber(f"invalid registrationNumber: {value!r}")
    return normalised
