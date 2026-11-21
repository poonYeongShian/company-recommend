"""Consumer integration tests: real Postgres (from `just up`), a fake embedder, no RabbitMQ.

They run in a throwaway `companies_test` database, never in the `companies` database the service uses.
"""

import random
from pathlib import Path
from typing import Any

import psycopg
import pytest

from recsys.consumer import Incoming
from tests.helpers import FakeEmbedder, consume, event, incoming, run

FEED = Path(__file__).parent.parent / "data" / "feed.jsonl"


def feed() -> list[Incoming]:
    return [incoming(line.encode()) for line in FEED.read_text(encoding="utf-8").splitlines()]


async def snapshot(url: str, table: str = "companies") -> list[tuple[Any, ...]]:
    columns = (
        "registration_number, name, description, industry, state, status, field_updated_at, status_updated_at, embedded_text, embedding::text"
        if table == "companies"
        else "message_id, reason, body"
    )
    async with await psycopg.AsyncConnection.connect(url) as connection:
        cursor = await connection.execute(f"SELECT {columns} FROM {table} ORDER BY 1")  # type: ignore[arg-type]
        return await cursor.fetchall()


def test_stores_company_with_embedding(url: str) -> None:
    run(consume(url, [event("SYN-000001", "2026-08-14T09:00:00+08:00", natureOfBusiness="Coffee shop", state="JOHOR")]))
    [row] = run(snapshot(url))
    assert row[:6] == ("SYN-000001", "Acme", "Coffee shop", None, "Johor", "active")
    assert row[8] == "Coffee shop" and row[9] is not None


def test_stale_event_does_not_overwrite_and_is_not_re_embedded(url: str) -> None:
    embedder = FakeEmbedder()
    newer = event("SYN-000001", "2026-08-14T10:00:00+08:00", natureOfBusiness="Boutique hotel")
    older = event("SYN-000001", "2026-08-14T09:00:00+08:00", natureOfBusiness="Coffee shop")
    run(consume(url, [newer], embedder))
    run(consume(url, [older], embedder))
    [row] = run(snapshot(url))
    assert row[2] == "Boutique hotel"
    assert embedder.calls == [["Boutique hotel"]]  # the stale event never reached the model


def test_embedding_follows_the_description_when_it_changes(url: str) -> None:
    run(consume(url, [event("SYN-000001", "2026-08-14T09:00:00+08:00", natureOfBusiness="Coffee shop")]))
    [before] = run(snapshot(url))
    run(consume(url, [event("SYN-000001", "2026-08-14T10:00:00+08:00", natureOfBusiness="Boutique hotel")]))
    [after] = run(snapshot(url))
    assert after[8] == "Boutique hotel" and after[9] != before[9]


def test_bad_messages_are_dead_lettered_not_lost_or_blocking(url: str) -> None:
    good = event("SYN-000001", "2026-08-14T09:00:00+08:00", natureOfBusiness="Coffee shop")
    messages = [incoming(b'{"type": "company-upserted", "regis'), good, incoming(b"null")]
    run(consume(url, messages))
    assert len(run(snapshot(url))) == 1
    reasons = {row[1].split(":")[0] for row in run(snapshot(url, "dead_letters"))}
    assert reasons == {"malformed JSON", "expected a JSON object, got NoneType"}


def test_a_poison_message_is_isolated_and_the_rest_of_the_batch_is_applied(url: str) -> None:
    messages = [
        event("SYN-000001", "2026-08-14T09:00:00+08:00", natureOfBusiness="Coffee shop"),
        event("SYN-000002", "2026-08-14T09:00:00+08:00", natureOfBusiness="POISON"),
        event("SYN-000003", "2026-08-14T09:00:00+08:00", natureOfBusiness="Boutique hotel"),
    ]
    run(consume(url, messages))
    assert [row[0] for row in run(snapshot(url))] == ["SYN-000001", "SYN-000003"]
    [(_, reason, _)] = run(snapshot(url, "dead_letters"))
    assert reason.startswith("processing error")


def test_whole_feed_is_stored_with_no_lost_companies(url: str) -> None:
    run(consume(url, feed()))
    rows = run(snapshot(url))
    assert len(rows) == 2000
    assert all(row[9] is not None for row in rows), "every company, even without a description, has an embedding"
    assert sum(row[5] == "struck_off" for row in rows) == 8


def test_replaying_the_feed_changes_nothing(url: str) -> None:
    messages = feed()
    run(consume(url, messages))
    first = run(snapshot(url)), run(snapshot(url, "dead_letters"))
    run(consume(url, messages))
    assert (run(snapshot(url)), run(snapshot(url, "dead_letters"))) == first


@pytest.mark.parametrize("order", ["reversed", "shuffled"])
def test_final_state_does_not_depend_on_arrival_order(url: str, order: str) -> None:
    messages = feed()
    run(consume(url, messages))
    expected = run(snapshot(url))

    run(consume_after_reset(url, order, messages))
    assert run(snapshot(url)) == expected


async def consume_after_reset(url: str, order: str, messages: list[Incoming]) -> None:
    async with await psycopg.AsyncConnection.connect(url) as connection:
        await connection.execute("TRUNCATE companies, dead_letters")
        await connection.commit()
    shuffled = list(messages)
    if order == "reversed":
        shuffled.reverse()
    else:
        random.Random(42).shuffle(shuffled)
    await consume(url, shuffled)
