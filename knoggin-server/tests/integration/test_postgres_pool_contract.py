import os

import pytest

from infrastructure.postgres_client import PostgresClient

DB_URL = os.environ.get(
    "KNOGGIN_TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
)


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_async_psycopg
@pytest.mark.slow
async def test_postgres_client_async_pool_executes_real_reads():
    client = PostgresClient(
        DB_URL,
        min_size=1,
        max_size=2,
    )

    await client.connect()
    try:
        assert await client.execute_read("SELECT 1 AS ok") == [{"ok": 1}]
    finally:
        await client.close()
