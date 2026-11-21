"""Fixtures for the integration tests: a throwaway `companies_test` database on the compose Postgres."""

from collections.abc import Iterator

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from common import config, db
from tests.helpers import run

TEST_DB = "companies_test"


@pytest.fixture(scope="session")
def test_url() -> Iterator[str]:
    admin_url = config.database_url()
    try:
        with psycopg.connect(admin_url, autocommit=True, connect_timeout=3) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB)))
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DB)))
    except psycopg.OperationalError as error:
        pytest.skip(f"Postgres is not reachable (run `just up`): {error}")
    yield make_conninfo(admin_url, **{**conninfo_to_dict(admin_url), "dbname": TEST_DB})
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB)))


@pytest.fixture
def url(test_url: str) -> str:
    """The test database, emptied."""

    async def reset() -> None:
        connection = await db.connect(test_url)
        async with connection:
            await connection.execute("TRUNCATE companies, dead_letters")
            await connection.commit()

    run(reset())
    return test_url
