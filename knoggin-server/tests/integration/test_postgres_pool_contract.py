import asyncio
import os

import pytest

from infrastructure.postgres_client import PostgresClient


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_async_psycopg
@pytest.mark.slow
@pytest.mark.skipif(
    not os.environ.get("KNOGGIN_TEST_DATABASE_URL"),
    reason="Set KNOGGIN_TEST_DATABASE_URL to run Postgres pool contract tests.",
)
async def test_postgres_client_async_and_sync_pools_execute_real_reads():
    client = PostgresClient(
        os.environ["KNOGGIN_TEST_DATABASE_URL"],
        min_size=0,
        max_size=2,
    )

    await client.connect()
    try:
        assert await client.execute_read("SELECT 1 AS ok") == [{"ok": 1}]
        sync_rows = await asyncio.to_thread(
            client.execute_read_sync,
            "SELECT 1 AS ok",
        )
        assert sync_rows == [{"ok": 1}]
    finally:
        await client.close()
