import itertools
from datetime import UTC, datetime, timedelta
from functools import reduce

from recsys.company import Company, embedding_text, merge
from recsys.parsing import CompanyEvent

T0 = datetime(2026, 8, 14, 1, 0, tzinfo=UTC)
REG = "SYN-000001"


def upsert(minutes: int, **fields: str | None) -> CompanyEvent:
    return CompanyEvent(REG, "upserted", T0 + timedelta(minutes=minutes), **{"name": "Acme", **fields})  # type: ignore[arg-type]


def struck_off(minutes: int) -> CompanyEvent:
    return CompanyEvent(REG, "struck_off", T0 + timedelta(minutes=minutes))


def apply(*events: CompanyEvent) -> Company:
    return reduce(merge, events, None)  # type: ignore[arg-type]


def test_first_event_creates_the_company() -> None:
    company = apply(upsert(0, description="Coffee shop", industry="Food & Beverage", state="Johor"))
    assert (company.name, company.description, company.industry, company.state, company.status) == ("Acme", "Coffee shop", "Food & Beverage", "Johor", "active")


def test_latest_event_wins_even_when_it_arrives_first() -> None:
    assert apply(upsert(10, description="new"), upsert(0, description="old")).description == "new"
    assert apply(upsert(0, description="old"), upsert(10, description="new")).description == "new"


def test_duplicate_event_changes_nothing() -> None:
    event = upsert(0, description="Coffee shop")
    assert apply(event, event) == apply(event)


def test_blank_value_does_not_erase_a_known_one() -> None:
    assert apply(upsert(0, description="Coffee shop"), upsert(10, description=None)).description == "Coffee shop"
    assert apply(upsert(10, description=None), upsert(0, description="Coffee shop")).description == "Coffee shop"


def test_older_event_fills_a_gap_but_not_a_known_value() -> None:
    company = apply(upsert(10, description="new", state=None), upsert(0, description="old", state="Perak"))
    assert (company.description, company.state) == ("new", "Perak")


def test_struck_off_marks_the_company_but_keeps_its_data() -> None:
    company = apply(upsert(0, description="Coffee shop"), struck_off(10))
    assert company.status == "struck_off" and company.description == "Coffee shop"


def test_struck_off_arriving_before_the_upsert_still_ends_struck_off_with_data() -> None:
    company = apply(struck_off(10), upsert(0, description="Coffee shop"))
    assert company.status == "struck_off" and company.description == "Coffee shop"


def test_a_later_upsert_reactivates_a_struck_off_company() -> None:
    assert apply(upsert(0), struck_off(10), upsert(20)).status == "active"


def test_result_does_not_depend_on_arrival_order() -> None:
    events = [
        upsert(0, description="a", industry="Retail", state="Johor"),
        upsert(5, description=None, industry="Education"),
        upsert(9, description="c", state=None),
        struck_off(7),
        upsert(12, name="Acme 2"),
    ]
    results = [apply(*order) for order in itertools.permutations(events)]
    assert all(result == results[0] for result in results)


def test_older_event_filling_a_gap_does_not_hide_a_middle_aged_one() -> None:
    # Regression: with one timestamp per company, the t=0 event filled `industry` and claimed t=9, so t=5 was ignored.
    company = apply(upsert(9, industry=None), upsert(0, industry="Retail"), upsert(5, industry="Education"))
    assert company.industry == "Education"


def test_same_timestamp_conflict_resolves_the_same_way_in_any_order() -> None:
    a, b = upsert(0, description="a"), upsert(0, description="b")
    assert apply(a, b) == apply(b, a)


def test_embedding_text_prefers_description_then_falls_back() -> None:
    assert embedding_text(Company(REG, name="Acme", description="Coffee shop", industry="Retail")) == "Coffee shop"
    assert embedding_text(Company(REG, name="Acme", industry="Retail")) == "Acme. Retail"
    assert embedding_text(Company(REG, name="Acme")) == "Acme"
    assert embedding_text(Company(REG)) is None
