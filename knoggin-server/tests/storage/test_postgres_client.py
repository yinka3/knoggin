import os

import pytest

from infrastructure import postgres_client as postgres_client_module
from infrastructure.postgres_client import PostgresClient

DB_URL = os.environ.get(
    "KNOGGIN_TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
)


class RecordingAsyncPool:
    instances = []
    next_open_error = None
    next_close_error = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.open_calls = []
        self.close_calls = 0
        self.open_error = self.__class__.next_open_error
        self.close_error = self.__class__.next_close_error
        self.__class__.instances.append(self)

    async def open(self, *, wait, timeout):
        self.open_calls.append({"wait": wait, "timeout": timeout})
        if self.open_error:
            raise self.open_error

    async def close(self):
        self.close_calls += 1
        if self.close_error:
            raise self.close_error


@pytest.fixture
def recording_pool(monkeypatch):
    RecordingAsyncPool.instances = []
    RecordingAsyncPool.next_open_error = None
    RecordingAsyncPool.next_close_error = None
    monkeypatch.setattr(
        postgres_client_module,
        "AsyncConnectionPool",
        RecordingAsyncPool,
    )
    return RecordingAsyncPool


@pytest.mark.storage
@pytest.mark.no_network
def test_build_cypher_wraps_query_with_graph_and_return_types():
    query = PostgresClient.build_cypher(
        "MATCH (n) RETURN n.id",
        return_types="id agtype",
        graph_name="test_graph",
    )

    assert "cypher('test_graph'" in query
    assert "MATCH (n) RETURN n.id" in query
    assert "AS (id agtype)" in query


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"min_size": 0}, "min_size must be at least 1"),
        (
            {"min_size": 3, "max_size": 2},
            "max_size must be greater than or equal to min_size",
        ),
        ({"startup_timeout": 0}, "startup_timeout must be greater than 0"),
        ({"startup_timeout": -1}, "startup_timeout must be greater than 0"),
    ],
)
def test_postgres_client_rejects_invalid_pool_settings(kwargs, message):
    with pytest.raises(ValueError, match=message):
        PostgresClient("postgresql://example", **kwargs)


@pytest.mark.storage
@pytest.mark.no_network
async def test_connect_waits_for_default_minimum_pool_readiness(recording_pool):
    client = PostgresClient("postgresql://example")

    await client.connect()

    pool = recording_pool.instances[0]
    assert pool.kwargs["open"] is False
    assert pool.kwargs["min_size"] == 1
    assert pool.kwargs["max_size"] == 10
    assert pool.open_calls == [{"wait": True, "timeout": 30.0}]
    assert client.async_pool is pool


@pytest.mark.storage
@pytest.mark.no_network
async def test_connect_passes_custom_startup_timeout(recording_pool):
    client = PostgresClient("postgresql://example", startup_timeout=7.5)

    await client.connect()

    pool = recording_pool.instances[0]
    assert pool.open_calls == [{"wait": True, "timeout": 7.5}]


@pytest.mark.storage
@pytest.mark.no_network
async def test_connect_closes_and_clears_pool_when_startup_fails(recording_pool):
    startup_error = ConnectionError("database unavailable")
    recording_pool.next_open_error = startup_error
    client = PostgresClient("postgresql://example")

    with pytest.raises(ConnectionError) as exc_info:
        await client.connect()

    pool = recording_pool.instances[0]
    assert exc_info.value is startup_error
    assert pool.close_calls == 1
    assert client.async_pool is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_cleanup_failure_does_not_mask_startup_failure(
    recording_pool,
):
    startup_error = ConnectionError("database unavailable")
    recording_pool.next_open_error = startup_error
    recording_pool.next_close_error = RuntimeError("cleanup failed")
    client = PostgresClient("postgresql://example")

    with pytest.raises(ConnectionError) as exc_info:
        await client.connect()

    pool = recording_pool.instances[0]
    assert exc_info.value is startup_error
    assert pool.close_calls == 1
    assert client.async_pool is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_close_only_closes_async_pool(recording_pool):
    client = PostgresClient("postgresql://example")
    await client.connect()
    pool = recording_pool.instances[0]

    await client.close()

    assert pool.close_calls == 1


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
async def test_postgres_client_connects_when_test_database_is_configured():
    client = PostgresClient(
        DB_URL,
        min_size=1,
        max_size=2,
    )

    await client.connect()
    try:
        assert client.async_pool is not None
    finally:
        await client.close()
