"""The "Similar companies" service the portal calls.

    uv run python -m service            # http://127.0.0.1:8000, docs at /docs

    GET /companies/{registrationNumber}/similar?limit=5
    GET /health
"""

import argparse
import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Final

import psycopg
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pgvector.psycopg import register_vector_async
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from common import config, db
from common.registration import InvalidRegistrationNumber, parse_registration_number
from service.search import CompanyNotFound, find_similar

DEFAULT_LIMIT: Final = 5
MAX_LIMIT: Final = 50
POOL_SIZE: Final = 10
POOL_TIMEOUT_SECONDS: Final = 2.0  # fail fast with a 503 rather than make the portal page wait
SCHEMA_RETRY_SECONDS: Final = 5.0

log = logging.getLogger("api")


class CamelModel(BaseModel):
    """JSON in camelCase, like the registry feed."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class SimilarCompany(CamelModel):
    registration_number: str
    company_name: str
    industry: str | None
    state: str | None
    similarity: float


class SimilarCompanies(CamelModel):
    registration_number: str
    similar: list[SimilarCompany]


def create_app(database_url: str | None = None, pool_timeout: float = POOL_TIMEOUT_SECONDS) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        url = database_url or config.database_url()
        # Never block startup on the database: create the schema in the background, retrying until it is reachable.
        # (Lets the service start before the consumer has ever run, and before Postgres is up.)
        schema_task = asyncio.create_task(_ensure_schema_eventually(url))
        pool = AsyncConnectionPool(
            url,
            min_size=1,
            max_size=POOL_SIZE,
            timeout=pool_timeout,
            kwargs={"connect_timeout": db.CONNECT_TIMEOUT_SECONDS},
            configure=_register_vector,
            open=False,
        )
        await pool.open(wait=False)
        app.state.pool = pool
        try:
            yield
        finally:
            schema_task.cancel()
            await pool.close(timeout=1.0)

    app = FastAPI(title="Similar companies", lifespan=lifespan)

    @app.exception_handler(psycopg.OperationalError)  # includes the pool's acquire timeout
    async def database_unavailable(request: Request, error: psycopg.OperationalError) -> JSONResponse:
        log.error("database error on %s: %s", request.url.path, error)
        return JSONResponse(
            {"detail": "Recommendations are temporarily unavailable."}, status_code=503, headers={"Retry-After": "5"}
        )

    @app.get("/companies/{registration_number}/similar", response_model=SimilarCompanies)
    async def similar_companies(
        registration_number: str,
        request: Request,
        limit: Annotated[int, Query(ge=1, le=MAX_LIMIT, description="How many companies to return.")] = DEFAULT_LIMIT,
    ) -> SimilarCompanies:
        try:
            registration_number = parse_registration_number(registration_number)
        except InvalidRegistrationNumber as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

        pool: AsyncConnectionPool = request.app.state.pool
        async with pool.connection() as connection:
            try:
                neighbours = await find_similar(connection, registration_number, limit)
            except CompanyNotFound:
                raise HTTPException(status_code=404, detail=f"Unknown company {registration_number}") from None
        return SimilarCompanies(
            registration_number=registration_number,
            similar=[
                SimilarCompany(
                    registration_number=n.registration_number,
                    company_name=n.name,
                    industry=n.industry,
                    state=n.state,
                    similarity=n.similarity,
                )
                for n in neighbours
            ],
        )

    @app.get("/health")
    async def health(request: Request) -> dict[str, str]:
        """Readiness: 200 only if the database answers."""
        pool: AsyncConnectionPool = request.app.state.pool
        async with pool.connection() as connection:
            await connection.execute("SELECT 1")
        return {"status": "ok"}

    return app


async def _ensure_schema_eventually(url: str) -> None:
    while True:
        try:
            await db.ensure_schema(url)
            return
        except psycopg.OperationalError:
            log.warning("database unavailable; will retry creating the schema in %.0fs", SCHEMA_RETRY_SECONDS)
            await asyncio.sleep(SCHEMA_RETRY_SECONDS)


async def _register_vector(connection: psycopg.AsyncConnection) -> None:
    await register_vector_async(connection)
    await connection.commit()  # the pool requires connections to be idle after configuring


app = create_app()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    server = uvicorn.Server(uvicorn.Config(app, host=args.host, port=args.port, log_level="info"))
    # psycopg's async mode needs a selector event loop, which is not the default on Windows.
    asyncio.run(server.serve(), loop_factory=asyncio.SelectorEventLoop)


if __name__ == "__main__":
    main()
