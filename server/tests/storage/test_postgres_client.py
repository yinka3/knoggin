import os

import psycopg
import pytest
from psycopg.rows import dict_row

from infrastructure import postgres_client as postgres_client_module
from infrastructure.postgres_client import (
    PostgresClient,
    _AgtypeLoader,
    _configure_async_conn,
)

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
        self.connection_enters = 0
        self.connection_exits = 0
        self.transaction_enters = 0
        self.transaction_exits = []
        self.cursor_enters = 0
        self.cursor_exits = 0
        self.execute_calls = []
        self.fetch_one_results = []
        self.fetch_all_results = []
        self.rowcount = 0
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

    def connection(self):
        return RecordingConnection(self)


class RecordingCursor:
    def __init__(self, pool):
        self.pool = pool
        self.rowcount = pool.rowcount

    async def __aenter__(self):
        self.pool.cursor_enters += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.pool.cursor_exits += 1
        return False

    async def execute(self, query, params=None):
        self.pool.execute_calls.append((query, params))

    async def fetchone(self):
        if not self.pool.fetch_one_results:
            return None
        return self.pool.fetch_one_results.pop(0)

    async def fetchall(self):
        if not self.pool.fetch_all_results:
            return []
        return self.pool.fetch_all_results.pop(0)


class RecordingTransaction:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        self.pool.transaction_enters += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.pool.transaction_exits.append(exc_type)
        return False


class RecordingConnection:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        self.pool.connection_enters += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.pool.connection_exits += 1
        return False

    def transaction(self):
        return RecordingTransaction(self.pool)

    def cursor(self):
        return RecordingCursor(self.pool)


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
def test_agtype_loader_decodes_age_json_values():
    loader = _AgtypeLoader(0)

    assert loader.load(b"true") is True
    assert loader.load(b"false") is False
    assert loader.load(b'[1, "two"]') == [1, "two"]
    assert loader.load(b'{"answer": 42}') == {"answer": 42}
    assert loader.load(b'{"id": 1}::vertex') == {"id": 1}
    assert loader.load(b"unsupported::custom") == "unsupported::custom"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_age_connection_configuration_registers_the_agtype_loader():
    conn = await psycopg.AsyncConnection.connect(DB_URL, row_factory=dict_row)
    try:
        await _configure_async_conn(conn)
        cursor = await conn.execute("SELECT 'false'::agtype AS value;")
        assert (await cursor.fetchone()) == {"value": False}
    finally:
        await conn.close()


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_postgres_client_decodes_real_age_agtype_values(real_postgres_client):
    row = await real_postgres_client.fetch_one(
        real_postgres_client.build_cypher(
            """
            RETURN true AS truth,
                   false AS falsity,
                   [1, 'two'] AS values,
                   {answer: 42} AS mapping
            """,
            "truth agtype, falsity agtype, values agtype, mapping agtype",
        ),
        ("{}",),
    )

    assert row == {
        "truth": True,
        "falsity": False,
        "values": [1, "two"],
        "mapping": {"answer": 42},
    }


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
    assert client._pool is pool


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
    assert client._pool is None


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
    assert client._pool is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_close_closes_and_clears_pool(recording_pool):
    client = PostgresClient("postgresql://example")
    await client.connect()
    pool = recording_pool.instances[0]

    await client.close()

    assert pool.close_calls == 1
    assert client._pool is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_close_is_idempotent(recording_pool):
    client = PostgresClient("postgresql://example")
    await client.connect()
    pool = recording_pool.instances[0]

    await client.close()
    await client.close()

    assert pool.close_calls == 1
    assert client._pool is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_close_clears_pool_when_pool_close_fails(recording_pool):
    recording_pool.next_close_error = RuntimeError("close failed")
    client = PostgresClient("postgresql://example")
    await client.connect()
    first_pool = recording_pool.instances[0]

    with pytest.raises(RuntimeError, match="close failed"):
        await client.close()

    assert client._pool is None

    await client.connect()

    assert recording_pool.instances[1] is not first_pool
    assert client._pool is recording_pool.instances[1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_connect_rejects_already_connected_client(recording_pool):
    client = PostgresClient("postgresql://example")
    await client.connect()
    pool = recording_pool.instances[0]

    with pytest.raises(RuntimeError, match="PostgresClient is already connected"):
        await client.connect()

    assert recording_pool.instances == [pool]
    assert client._pool is pool


@pytest.mark.storage
@pytest.mark.no_network
async def test_client_can_reconnect_after_close(recording_pool):
    client = PostgresClient("postgresql://example")
    await client.connect()
    first_pool = recording_pool.instances[0]
    await client.close()

    await client.connect()
    second_pool = recording_pool.instances[1]

    assert first_pool.close_calls == 1
    assert second_pool is not first_pool
    assert client._pool is second_pool


@pytest.mark.storage
@pytest.mark.no_network
async def test_transaction_rejects_use_after_close(recording_pool):
    client = PostgresClient("postgresql://example")
    await client.connect()
    await client.close()

    with pytest.raises(RuntimeError, match="PostgresClient is not connected"):
        async with client.transaction():
            pass


@pytest.mark.storage
@pytest.mark.no_network
async def test_transaction_requires_connected_client():
    client = PostgresClient("postgresql://example")

    with pytest.raises(RuntimeError, match="PostgresClient is not connected"):
        async with client.transaction():
            pass


@pytest.mark.storage
@pytest.mark.no_network
async def test_transaction_manages_connection_transaction_and_cursor(recording_pool):
    client = PostgresClient("postgresql://example")
    await client.connect()
    pool = recording_pool.instances[0]

    async with client.transaction() as cur:
        await cur.execute("SELECT 1", None)

    assert pool.execute_calls == [("SELECT 1", None)]
    assert pool.connection_enters == 1
    assert pool.connection_exits == 1
    assert pool.transaction_enters == 1
    assert pool.transaction_exits == [None]
    assert pool.cursor_enters == 1
    assert pool.cursor_exits == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_transaction_rolls_back_when_exception_escapes(recording_pool):
    client = PostgresClient("postgresql://example")
    await client.connect()
    pool = recording_pool.instances[0]

    with pytest.raises(ValueError, match="abort"):
        async with client.transaction():
            raise ValueError("abort")

    assert pool.transaction_exits == [ValueError]
    assert pool.connection_exits == 1
    assert pool.cursor_exits == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_explicit_query_helpers_preserve_params_and_result_shapes(recording_pool):
    client = PostgresClient("postgresql://example")
    await client.connect()
    pool = recording_pool.instances[0]
    pool.fetch_all_results = [[{"id": 1}, {"id": 2}]]
    pool.fetch_one_results = [{"id": 3}]
    pool.rowcount = 99

    assert await client.fetch_all("SELECT many", None) == [{"id": 1}, {"id": 2}]
    assert await client.fetch_one("SELECT one", ("value",)) == {"id": 3}
    assert await client.fetch_one("SELECT missing") is None
    assert await client.execute("UPDATE things", {"id": 3}) == 99

    assert pool.execute_calls == [
        ("SELECT many", None),
        ("SELECT one", ("value",)),
        ("SELECT missing", None),
        ("UPDATE things", {"id": 3}),
    ]


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
        assert client._pool is not None
    finally:
        await client.close()
