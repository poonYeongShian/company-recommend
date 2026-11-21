"""Postgres access shared by the consumer and the API: connection and schema."""

from pathlib import Path
from typing import Final

import psycopg
from pgvector.psycopg import register_vector_async

_SCHEMA: Final = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")
CONNECT_TIMEOUT_SECONDS: Final = 10
_SCHEMA_LOCK_ID: Final = 7_205_001  # arbitrary; serialises concurrent schema creation (consumer and API start together)


async def connect(url: str) -> psycopg.AsyncConnection:
    """Open a connection, creating the schema (and the vector extension) first if needed."""
    await ensure_schema(url)
    connection = await psycopg.AsyncConnection.connect(url, connect_timeout=CONNECT_TIMEOUT_SECONDS)
    await register_vector_async(connection)
    return connection


async def ensure_schema(url: str) -> None:
    async with await psycopg.AsyncConnection.connect(url, autocommit=True, connect_timeout=CONNECT_TIMEOUT_SECONDS) as setup:
        await init_schema(setup)


async def init_schema(connection: psycopg.AsyncConnection) -> None:
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_LOCK_ID,))
        await connection.execute(_SCHEMA)  # type: ignore[arg-type]  # one multi-statement script, no parameters
