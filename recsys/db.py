"""Postgres access: connection, schema, and reading and writing companies."""

from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Final

import psycopg
from psycopg.types.json import Jsonb
from pgvector.psycopg import register_vector_async

from recsys.company import Company

_SCHEMA: Final = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")
CONNECT_TIMEOUT_SECONDS: Final = 10
_SCHEMA_LOCK_ID: Final = 7_205_001  # arbitrary; serialises concurrent schema creation (consumer and API start together)

_COLUMNS: Final = "registration_number, name, description, industry, state, status, field_updated_at, status_updated_at, embedded_text"

_UPSERT: Final = """
INSERT INTO companies (registration_number, name, description, industry, state, status,
                       field_updated_at, status_updated_at, embedded_text, embedding)
VALUES (%(registration_number)s, %(name)s, %(description)s, %(industry)s, %(state)s, %(status)s,
        %(field_updated_at)s, %(status_updated_at)s, %(embedded_text)s, %(embedding)s::vector)
ON CONFLICT (registration_number) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    industry = EXCLUDED.industry,
    state = EXCLUDED.state,
    status = EXCLUDED.status,
    field_updated_at = EXCLUDED.field_updated_at,
    status_updated_at = EXCLUDED.status_updated_at,
    embedded_text = EXCLUDED.embedded_text,
    embedding = COALESCE(EXCLUDED.embedding, companies.embedding),
    updated_at = now()
"""


async def connect(url: str) -> psycopg.AsyncConnection:
    """Open a connection, creating the schema (and the vector extension) first if needed."""
    async with await psycopg.AsyncConnection.connect(url, autocommit=True, connect_timeout=CONNECT_TIMEOUT_SECONDS) as setup:
        await init_schema(setup)
    connection = await psycopg.AsyncConnection.connect(url, connect_timeout=CONNECT_TIMEOUT_SECONDS)
    await register_vector_async(connection)
    return connection


async def init_schema(connection: psycopg.AsyncConnection) -> None:
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_LOCK_ID,))
        await connection.execute(_SCHEMA)  # type: ignore[arg-type]  # one multi-statement script, no parameters


async def load_companies(connection: psycopg.AsyncConnection, registration_numbers: Sequence[str]) -> dict[str, Company]:
    """Fetch and row-lock the given companies, so a concurrent writer cannot interleave with our merge."""
    cursor = await connection.execute(
        f"SELECT {_COLUMNS} FROM companies WHERE registration_number = ANY(%s) FOR UPDATE",  # type: ignore[arg-type]
        (list(registration_numbers),),
    )
    return {
        row[0]: Company(*row[:6], {k: datetime.fromisoformat(v) for k, v in row[6].items()}, *row[7:])
        async for row in cursor
    }


async def save_companies(connection: psycopg.AsyncConnection, companies: Iterable[Company]) -> None:
    async with connection.cursor() as cursor:
        await cursor.executemany(
            _UPSERT,
            [
                {
                    "registration_number": c.registration_number,
                    "name": c.name,
                    "description": c.description,
                    "industry": c.industry,
                    "state": c.state,
                    "status": c.status,
                    "field_updated_at": Jsonb({k: v.isoformat() for k, v in c.field_updated_at.items()}),
                    "status_updated_at": c.status_updated_at,
                    "embedded_text": c.embedded_text,
                    "embedding": c.embedding,
                }
                for c in companies
            ],
        )


async def save_dead_letter(connection: psycopg.AsyncConnection, message_id: str, reason: str, body: bytes) -> None:
    await connection.execute(
        "INSERT INTO dead_letters (message_id, reason, body) VALUES (%s, %s, %s) ON CONFLICT (message_id) DO NOTHING",
        (message_id, reason, body.decode("utf-8", errors="replace")),
    )
