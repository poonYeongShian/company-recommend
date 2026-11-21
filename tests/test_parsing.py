import json
from datetime import UTC, datetime

import pytest

from consumer.parsing import CompanyEvent, InvalidMessage, parse_message

NOW = datetime(2026, 11, 21, tzinfo=UTC)


def message(**overrides: object) -> bytes:
    body: dict[str, object] = {
        "type": "company-upserted",
        "registrationNumber": "SYN-000001",
        "updatedAt": "2026-08-14T09:00:00+08:00",
        "data": {"companyName": "Acme Sdn. Bhd.", "natureOfBusiness": "Coffee shop", "state": "Selangor", "industry": "Food & Beverage"},
    }
    body.update(overrides)
    return json.dumps({k: v for k, v in body.items() if v is not ...}).encode()


def parse(body: bytes) -> CompanyEvent:
    return parse_message(body, now=NOW)


def test_parses_a_well_formed_upsert() -> None:
    event = parse(message())
    assert event.registration_number == "SYN-000001"
    assert event.kind == "upserted"
    assert event.updated_at == datetime(2026, 8, 14, 1, 0, tzinfo=UTC)
    assert (event.name, event.description, event.industry, event.state) == ("Acme Sdn. Bhd.", "Coffee shop", "Food & Beverage", "Selangor")


@pytest.mark.parametrize("raw", [" SYN-000001 ", "syn-000001", " syn-000001"])
def test_normalises_registration_number(raw: str) -> None:
    assert parse(message(registrationNumber=raw)).registration_number == "SYN-000001"


@pytest.mark.parametrize("raw", ["KUALA LUMPUR", "kuala lumpur", " Kuala  Lumpur "])
def test_normalises_state_case_and_spacing(raw: str) -> None:
    data = {"companyName": "A", "state": raw}
    assert parse(message(data=data)).state == "Kuala Lumpur"


def test_epoch_milliseconds_and_naive_timestamps() -> None:
    assert parse(message(updatedAt=1786671038000)).updated_at == datetime(2026, 8, 14, 1, 30, 38, tzinfo=UTC)
    # No offset: assumed to be Malaysian time (+08:00).
    assert parse(message(updatedAt="2026-08-14T09:00:00")).updated_at == datetime(2026, 8, 14, 1, 0, tzinfo=UTC)


def test_blank_optional_fields_become_none() -> None:
    event = parse(message(data={"companyName": "A", "natureOfBusiness": "  ", "industry": None}))
    assert (event.description, event.industry, event.state) == (None, None, None)


def test_partial_data_with_only_a_name_is_accepted() -> None:
    event = parse(message(data={"companyName": "A"}))
    assert event.name == "A" and event.description is None


def test_collapses_a_description_repeated_many_times() -> None:
    text = " ".join(["Software development for small business accounting"] * 40)
    data = {"companyName": "A", "natureOfBusiness": text}
    assert parse(message(data=data)).description == "Software development for small business accounting"


def test_does_not_collapse_legitimate_text() -> None:
    data = {"companyName": "A", "natureOfBusiness": "Trading and trading of goods"}
    assert parse(message(data=data)).description == "Trading and trading of goods"


def test_struck_off_needs_no_data() -> None:
    event = parse(message(type="company-struck-off", data=...))
    assert event.kind == "struck_off" and event.name is None


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b'{"type": "company-upserted", "registrationNumber": "SYN-000509", "updatedAt": "2026-08-14T09:15:13+08:00", "data": {"com', "malformed JSON"),
        (b"null", "expected a JSON object"),
        (b"\xff\xfe", "malformed JSON"),
        (message(type="company-deleted"), "unknown event type"),
        (message(registrationNumber=...), "missing registrationNumber"),
        (message(registrationNumber=119), "must be a string"),
        (message(registrationNumber="ABC-1"), "invalid registrationNumber"),
        (message(updatedAt=...), "missing updatedAt"),
        (message(updatedAt="yesterday"), "invalid updatedAt"),
        (message(updatedAt="2099-01-01T00:00:00+08:00"), "in the future"),
        (message(data="companyName=Foo; state=Perak"), "'data' must be an object"),
        (message(data=...), "'data' must be an object"),
        (message(data={"natureOfBusiness": "x"}), "missing companyName"),
    ],
)
def test_rejects_unusable_messages(body: bytes, reason: str) -> None:
    with pytest.raises(InvalidMessage, match=reason):
        parse(body)
