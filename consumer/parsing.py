"""Turn a raw queue message into a clean `CompanyEvent`, or reject it with a reason.

The registry feed merges several upstream sources and nothing is validated, so everything
here is defensive. Rejected messages are not dropped: the consumer stores them in the
`dead_letters` table so they can be inspected or replayed.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Final, Literal

from common.registration import InvalidRegistrationNumber, parse_registration_number

# Timestamps without an offset: a guess. The feed is Malaysian and 2,428 of its timestamps say +08:00 (28 say Z).
ASSUMED_TZ: Final = timezone(timedelta(hours=8))

# Reject timestamps from the future: one bogus `updatedAt` would otherwise win every later comparison.
MAX_CLOCK_SKEW: Final = timedelta(days=1)

STATES: Final = (
    "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan", "Pahang", "Perak", "Perlis", "Pulau Pinang",
    "Sabah", "Sarawak", "Selangor", "Terengganu", "Kuala Lumpur", "Labuan", "Putrajaya",
)  # fmt: skip
_STATE_BY_KEY: Final = {state.lower(): state for state in STATES}

EventKind = Literal["upserted", "struck_off"]
_KIND_BY_TYPE: Final[dict[str, EventKind]] = {
    "company-upserted": "upserted",
    "company-struck-off": "struck_off",
}


class InvalidMessage(ValueError):
    """The message cannot be applied; the text is the reason stored with the dead letter."""


@dataclass(frozen=True, slots=True)
class CompanyEvent:
    registration_number: str
    kind: EventKind
    updated_at: datetime  # timezone-aware, UTC
    # Optional fields are None when absent or blank. Only meaningful for "upserted".
    name: str | None = None
    description: str | None = None
    industry: str | None = None
    state: str | None = None


def parse_message(body: bytes, now: datetime | None = None) -> CompanyEvent:
    """Parse and normalise one message body. Raises `InvalidMessage` if it cannot be used."""
    try:
        raw = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidMessage(f"malformed JSON: {error}") from error
    if not isinstance(raw, dict):
        raise InvalidMessage(f"expected a JSON object, got {type(raw).__name__}")

    kind = _KIND_BY_TYPE.get(raw.get("type"))  # type: ignore[arg-type]  # unhashable values are caught below
    if kind is None:
        raise InvalidMessage(f"unknown event type: {raw.get('type')!r}")

    registration_number = _registration_number(raw.get("registrationNumber"))
    updated_at = _parse_timestamp(raw.get("updatedAt"), now or datetime.now(UTC))

    if kind == "struck_off":
        return CompanyEvent(registration_number, kind, updated_at)

    data = raw.get("data")
    if not isinstance(data, dict):
        raise InvalidMessage(f"'data' must be an object, got {type(data).__name__}")
    name = _clean_text(data.get("companyName"))
    if name is None:
        raise InvalidMessage("missing companyName")
    return CompanyEvent(
        registration_number,
        kind,
        updated_at,
        name=name,
        description=_collapse_repeats(_clean_text(data.get("natureOfBusiness"))),
        industry=_clean_text(data.get("industry")),
        state=_normalise_state(_clean_text(data.get("state"))),
    )


def _registration_number(value: Any) -> str:
    try:
        return parse_registration_number(value)
    except InvalidRegistrationNumber as error:
        raise InvalidMessage(str(error)) from error


def _parse_timestamp(value: Any, now: datetime) -> datetime:
    if isinstance(value, bool) or value is None:
        raise InvalidMessage("missing updatedAt")
    try:
        if isinstance(value, int | float):
            # Epoch milliseconds in this feed; accept seconds too.
            seconds = value / 1000 if abs(value) > 1e11 else value
            parsed = datetime.fromtimestamp(seconds, UTC)
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value.strip())
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ASSUMED_TZ)
        else:
            raise InvalidMessage(f"invalid updatedAt: {value!r}")
    except (ValueError, OverflowError, OSError) as error:
        raise InvalidMessage(f"invalid updatedAt: {value!r}") from error
    parsed = parsed.astimezone(UTC)
    if parsed > now + MAX_CLOCK_SKEW:
        raise InvalidMessage(f"updatedAt is in the future: {value!r}")
    return parsed


def _clean_text(value: Any) -> str | None:
    """Collapse whitespace; blank or non-string values become None."""
    if not isinstance(value, str):
        return None
    return " ".join(value.split()) or None


def _normalise_state(state: str | None) -> str | None:
    if state is None:
        return None
    return _STATE_BY_KEY.get(state.lower(), state.title())


def _collapse_repeats(text: str | None) -> str | None:
    """'A B A B A B' -> 'A B'. A few descriptions in the feed are one sentence repeated dozens of times."""
    if text is None:
        return None
    words = text.split()
    for period in range(1, len(words) // 2 + 1):
        if len(words) % period == 0 and words == words[:period] * (len(words) // period):
            return " ".join(words[:period])
    return text
