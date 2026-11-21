"""Test helpers shared by the integration tests."""

import asyncio
import hashlib
import json
from collections.abc import Coroutine
from typing import Any

import numpy as np
import psycopg

from common import db
from consumer.worker import BATCH_SIZE, Incoming, process_with_isolation
from consumer.embedding import DIMENSIONS


class FakeEmbedder:
    """Deterministic stand-in for the model: same text, same vector. Records what it was asked to embed."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def embed(self, texts: list[str]) -> list[np.ndarray]:
        self.calls.append(list(texts))
        if any("POISON" in text for text in texts):
            raise RuntimeError("embedding failed")
        return [np.random.default_rng(int(hashlib.sha256(t.encode()).hexdigest()[:8], 16)).random(DIMENSIONS, dtype=np.float32) for t in texts]


def run[T](coroutine: Coroutine[Any, Any, T]) -> T:
    # psycopg's async mode does not work with Windows' default event loop.
    return asyncio.run(coroutine, loop_factory=asyncio.SelectorEventLoop)


def incoming(body: bytes) -> Incoming:
    return Incoming(hashlib.sha256(body).hexdigest(), body)


def event(reg: str, updated_at: str, **data: str) -> Incoming:
    body = {"type": "company-upserted", "registrationNumber": reg, "updatedAt": updated_at, "data": {"companyName": "Acme", **data}}
    return incoming(json.dumps(body).encode())


async def consume(url: str, messages: list[Incoming], embedder: Any = None) -> None:
    connection = await db.connect(url)
    async with connection:
        for start in range(0, len(messages), BATCH_SIZE):
            await process_with_isolation(connection, embedder or FakeEmbedder(), messages[start : start + BATCH_SIZE])
