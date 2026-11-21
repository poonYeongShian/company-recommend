"""Consumes `company-events` and keeps the `companies` table (with embeddings) up to date.

    uv run python -m consumer                 # run until stopped
    uv run python -m consumer --idle-exit 5   # stop once the queue has been empty for 5 seconds

Messages are handled in batches. A batch is one database transaction, and its messages are acked only after
that transaction commits, so a crash redelivers them (safe, because applying an event twice changes nothing).
Messages that can never be applied go to the `dead_letters` table and are acked, so they cannot block the queue.
"""

import argparse
import asyncio
import hashlib
import logging
from dataclasses import dataclass, replace
from typing import Final

import aio_pika
import psycopg
from aio_pika.abc import AbstractIncomingMessage

from common import config, db
from consumer import store
from consumer.company import embedding_text, merge
from consumer.embedding import Embedder, FastEmbedder
from consumer.parsing import CompanyEvent, InvalidMessage, parse_message

QUEUE: Final = "company-events"
BATCH_SIZE: Final = 100
BATCH_LINGER_SECONDS: Final = 0.2  # how long to wait for a batch to fill once the first message has arrived
RETRY_DELAY_SECONDS: Final = 3.0

log = logging.getLogger("consumer")


@dataclass(frozen=True, slots=True)
class Incoming:
    message_id: str
    body: bytes


@dataclass(slots=True)
class BatchResult:
    applied: int = 0
    dead_lettered: int = 0

    def add(self, other: "BatchResult") -> None:
        self.applied += other.applied
        self.dead_lettered += other.dead_lettered


async def process_batch(connection: psycopg.AsyncConnection, embedder: Embedder, batch: list[Incoming]) -> BatchResult:
    """Apply a batch in one transaction. Raises if the database fails; bad messages are dead-lettered instead."""
    events: list[CompanyEvent] = []
    rejected: list[tuple[Incoming, str]] = []
    for incoming in batch:
        try:
            events.append(parse_message(incoming.body))
        except InvalidMessage as error:
            rejected.append((incoming, str(error)))

    async with connection.transaction():
        for incoming, reason in rejected:
            await store.save_dead_letter(connection, incoming.message_id, reason, incoming.body)
        if events:
            await _apply_events(connection, embedder, events)
    return BatchResult(applied=len(events), dead_lettered=len(rejected))


async def _apply_events(connection: psycopg.AsyncConnection, embedder: Embedder, events: list[CompanyEvent]) -> None:
    current = await store.load_companies(connection, sorted({e.registration_number for e in events}))
    for event in events:  # arrival order; `merge` makes the result independent of it
        current[event.registration_number] = merge(current.get(event.registration_number), event)

    touched = {e.registration_number for e in events}
    companies = [current[registration_number] for registration_number in touched]
    stale = [(c, text) for c in companies if (text := embedding_text(c)) is not None and text != c.embedded_text]
    if stale:
        # CPU-bound: run off the event loop so RabbitMQ heartbeats keep flowing.
        vectors = await asyncio.to_thread(embedder.embed, [text for _, text in stale])
        embedded = {c.registration_number: replace(c, embedded_text=text, embedding=vector) for (c, text), vector in zip(stale, vectors, strict=True)}
        companies = [embedded.get(c.registration_number, c) for c in companies]
    await store.save_companies(connection, companies)


async def process_with_isolation(connection: psycopg.AsyncConnection, embedder: Embedder, batch: list[Incoming]) -> BatchResult:
    """Like `process_batch`, but if something unexpected breaks the batch, retry one message at a time so a
    single poison message is dead-lettered instead of blocking the others. Database outages still raise."""
    try:
        return await process_batch(connection, embedder, batch)
    except psycopg.OperationalError:
        raise
    except Exception:
        log.exception("batch of %d failed; retrying one message at a time", len(batch))
    total = BatchResult()
    for incoming in batch:
        try:
            total.add(await process_batch(connection, embedder, [incoming]))
        except psycopg.OperationalError:
            raise
        except Exception as error:
            log.exception("message %s failed", incoming.message_id)
            async with connection.transaction():
                await store.save_dead_letter(connection, incoming.message_id, f"processing error: {error!r}", incoming.body)
            total.dead_lettered += 1
    return total


async def _next_batch(inbox: "asyncio.Queue[AbstractIncomingMessage]", idle_exit: float | None) -> list[AbstractIncomingMessage] | None:
    """Wait for a message, then gather up to BATCH_SIZE more that arrive within the linger window.
    Returns None if nothing arrived for `idle_exit` seconds."""
    try:
        batch = [await asyncio.wait_for(inbox.get(), timeout=idle_exit)]
    except TimeoutError:
        return None
    loop = asyncio.get_running_loop()
    deadline = loop.time() + BATCH_LINGER_SECONDS
    while len(batch) < BATCH_SIZE and (remaining := deadline - loop.time()) > 0:
        try:
            batch.append(await asyncio.wait_for(inbox.get(), timeout=remaining))
        except TimeoutError:
            break
    return batch


def _to_incoming(message: AbstractIncomingMessage) -> Incoming:
    return Incoming(message.message_id or hashlib.sha256(message.body).hexdigest(), message.body)


async def run(idle_exit: float | None = None, embedder: Embedder | None = None) -> BatchResult:
    embedder = embedder or FastEmbedder()
    if isinstance(embedder, FastEmbedder):
        await asyncio.to_thread(embedder.warm_up)  # download and load the model before accepting messages

    amqp = await aio_pika.connect_robust(config.rabbitmq_url())
    async with amqp:
        channel = await amqp.channel()
        await channel.set_qos(prefetch_count=BATCH_SIZE * 2)
        # The registry team owns this queue: durable, no extra arguments. Declaring it differently would fail.
        queue = await channel.declare_queue(QUEUE, durable=True)
        inbox: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
        await queue.consume(inbox.put)

        total = BatchResult()
        connection = await db.connect(config.database_url())
        log.info("consuming %s", QUEUE)
        try:
            while (batch := await _next_batch(inbox, idle_exit)) is not None:
                try:
                    result = await process_with_isolation(connection, embedder, [_to_incoming(m) for m in batch])
                except psycopg.OperationalError:
                    log.exception("database unavailable; requeueing %d messages and reconnecting", len(batch))
                    for message in batch:
                        await message.nack(requeue=True)
                    await _close_quietly(connection)
                    connection = await _reconnect()
                    continue
                for message in batch:
                    await message.ack()
                total.add(result)
                log.info("batch of %d: %d applied, %d dead-lettered", len(batch), result.applied, result.dead_lettered)
        finally:
            await _close_quietly(connection)
    log.info("stopped: %d applied, %d dead-lettered", total.applied, total.dead_lettered)
    return total


async def _reconnect() -> psycopg.AsyncConnection:
    while True:
        await asyncio.sleep(RETRY_DELAY_SECONDS)
        try:
            return await db.connect(config.database_url())
        except psycopg.OperationalError:
            log.warning("database still unavailable; retrying")


async def _close_quietly(connection: psycopg.AsyncConnection) -> None:
    try:
        await connection.close()
    except psycopg.Error:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--idle-exit", type=float, default=None, metavar="SECONDS", help="stop after the queue has been empty this long")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        # psycopg's async mode needs a selector event loop, which is not the default on Windows.
        asyncio.run(run(idle_exit=args.idle_exit), loop_factory=asyncio.SelectorEventLoop)
    except KeyboardInterrupt:
        log.info("interrupted")


if __name__ == "__main__":
    main()
