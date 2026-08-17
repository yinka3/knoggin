import asyncio
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
        assert await client.fetch_all("SELECT 1 AS ok") == [{"ok": 1}]
    finally:
        await client.close()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_async_psycopg
@pytest.mark.slow
@pytest.mark.no_network
async def test_postgres_pool_replaces_a_closed_established_connection():
    """A broken idle connection is discarded and replaced on the next query."""

    client = PostgresClient(DB_URL, min_size=1, max_size=1)
    await client.connect()
    pool = client._pool
    assert pool is not None

    try:
        async with pool.connection() as connection:
            await connection.execute("SELECT 1")
            await connection.close()

        assert await client.fetch_one("SELECT 2 AS ok") == {"ok": 2}
        assert pool.get_stats()["returns_bad"] >= 1
    finally:
        await client.close()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_async_psycopg
@pytest.mark.slow
@pytest.mark.no_network
async def test_postgres_pool_recovers_after_single_slot_exhaustion():
    """A queued query completes once the only checked-out slot is released."""

    client = PostgresClient(DB_URL, min_size=1, max_size=1)
    await client.connect()
    pool = client._pool
    assert pool is not None

    try:
        async with pool.connection():
            pending = asyncio.create_task(client.fetch_one("SELECT 3 AS ok"))
            await asyncio.sleep(0.05)
            assert not pending.done()

        assert await asyncio.wait_for(pending, timeout=2) == {"ok": 3}
        assert pool.get_stats()["requests_queued"] >= 1
    finally:
        await client.close()
