"""Nearest-neighbour search over the stored embeddings, done in Postgres with pgvector."""

from dataclasses import dataclass
from typing import Final

import psycopg


class CompanyNotFound(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class Neighbour:
    registration_number: str
    name: str
    industry: str | None
    state: str | None
    similarity: float  # cosine similarity: 1.0 is identical meaning, lower is less alike


# Cosine distance (`<=>`), matching the index operator class. Excluded from the results: the company itself,
# struck-off companies (not worth recommending), and placeholder rows that have no name to show on a card.
# Ties are common (many companies share an identical description), so break them on the registration number
# to give the same answer every time.
#
# This is an exact scan, which is right for thousands of rows. The HNSW index cannot serve a two-key ORDER BY, so
# at much larger scale this becomes an indexed top-N followed by a re-sort (see DESIGN.md).
_SIMILAR: Final = """
SELECT registration_number, name, industry, state, 1 - (embedding <=> %(vector)s::vector) AS similarity
FROM companies
WHERE embedding IS NOT NULL
  AND name IS NOT NULL
  AND status = 'active'
  AND registration_number <> %(registration_number)s
ORDER BY embedding <=> %(vector)s::vector, registration_number
LIMIT %(limit)s
"""


async def find_similar(connection: psycopg.AsyncConnection, registration_number: str, limit: int) -> list[Neighbour]:
    """The `limit` companies most similar to `registration_number`, most similar first.

    Raises `CompanyNotFound` if the company is unknown. A known company with nothing to compare returns [].
    """
    cursor = await connection.execute(
        "SELECT embedding FROM companies WHERE registration_number = %s AND name IS NOT NULL", (registration_number,)
    )
    row = await cursor.fetchone()
    if row is None:
        raise CompanyNotFound(registration_number)
    vector = row[0]
    if vector is None:
        return []

    cursor = await connection.execute(_SIMILAR, {"vector": vector, "registration_number": registration_number, "limit": limit})
    return [
        Neighbour(reg, name, industry, state, min(1.0, round(similarity, 4)))
        async for reg, name, industry, state, similarity in cursor
    ]
