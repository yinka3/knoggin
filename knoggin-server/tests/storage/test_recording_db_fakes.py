import pytest

from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_recording_postgres_client_supports_writer_context_shape():
    client = RecordingPostgresClient(
        fetchone_results=[{"created_count": 2}],
        fetchall_results=[[{"id": 1}, {"id": 2}]],
        execute_read_results=[[{"ok": 1}]],
        execute_write_results=[3],
    )

    async with client.async_pool.connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                cypher_query = client.build_cypher(
                    "MATCH (n) RETURN n.id",
                    "id agtype",
                    graph_name="test_graph",
                )
                await cur.execute(cypher_query, ("{}",))
                assert await cur.fetchone() == {"created_count": 2}

                await cur.execute("SELECT id FROM entity_search", (7,))
                assert await cur.fetchall() == [{"id": 1}, {"id": 2}]

    assert client.calls == [
        (
            "execute",
            "cypher<test_graph|id agtype>:MATCH (n) RETURN n.id",
            ("{}",),
        ),
        ("execute", "SELECT id FROM entity_search", (7,)),
    ]
    assert client.connection_enters == 1
    assert client.connection_exits == 1
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert client.cursor_enters == 1
    assert client.cursor_exits == 1

    assert await client.execute_read("SELECT 1", None) == [{"ok": 1}]
    assert await client.execute_write("UPDATE things", ("param",)) == 3

    assert client.calls[-2:] == [
        ("execute_read", "SELECT 1", None),
        ("execute_write", "UPDATE things", ("param",)),
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_recording_postgres_client_preserves_ordered_fetch_queues():
    client = RecordingPostgresClient(
        fetchone_results=[{"one": 1}, {"two": 2}],
        fetchall_results=[[{"batch": 1}], [{"batch": 2}]],
    )

    async with client.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            assert await cur.fetchone() == {"one": 1}
            assert await cur.fetchone() == {"two": 2}
            assert await cur.fetchone() is None
            assert await cur.fetchall() == [{"batch": 1}]
            assert await cur.fetchall() == [{"batch": 2}]
            assert await cur.fetchall() == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_recording_postgres_client_can_inject_cursor_failures():
    client = RecordingPostgresClient(
        execute_exceptions=[RuntimeError("execute failed")],
        fetchone_exceptions=[RuntimeError("fetchone failed")],
        fetchall_exceptions=[RuntimeError("fetchall failed")],
    )

    async with client.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            with pytest.raises(RuntimeError, match="execute failed"):
                await cur.execute("SELECT broken", ("param",))
            with pytest.raises(RuntimeError, match="fetchone failed"):
                await cur.fetchone()
            with pytest.raises(RuntimeError, match="fetchall failed"):
                await cur.fetchall()

    assert client.calls == [("execute", "SELECT broken", ("param",))]
    assert client.connection_enters == 1
    assert client.connection_exits == 1
    assert client.cursor_enters == 1
    assert client.cursor_exits == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_recording_postgres_client_can_inject_direct_query_failures():
    client = RecordingPostgresClient(
        execute_read_exceptions=[RuntimeError("read failed")],
        execute_write_exceptions=[RuntimeError("write failed")],
    )

    with pytest.raises(RuntimeError, match="read failed"):
        await client.execute_read("SELECT broken", (1,))

    with pytest.raises(RuntimeError, match="write failed"):
        await client.execute_write("UPDATE broken", (2,))

    assert client.calls == [
        ("execute_read", "SELECT broken", (1,)),
        ("execute_write", "UPDATE broken", (2,)),
    ]
