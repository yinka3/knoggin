import pytest

from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_recording_postgres_client_supports_writer_context_shape():
    client = RecordingPostgresClient(
        fetch_one_results=[{"created_count": 2}],
        fetch_all_results=[[{"id": 1}, {"id": 2}], [{"ok": 1}]],
    )

    async with client.transaction() as cur:
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
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert client.cursor_enters == 1
    assert client.cursor_exits == 1

    assert await client.fetch_all("SELECT 1", None) == [{"ok": 1}]
    assert await client.execute("UPDATE things", ("param",)) is None

    assert client.calls[-2:] == [
        ("fetch_all", "SELECT 1", None),
        ("execute_command", "UPDATE things", ("param",)),
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_recording_postgres_client_preserves_ordered_fetch_queues():
    client = RecordingPostgresClient(
        fetch_one_results=[{"one": 1}, {"two": 2}],
        fetch_all_results=[[{"batch": 1}], [{"batch": 2}]],
    )

    async with client.transaction() as cur:
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
        cursor_execute_exceptions=[RuntimeError("execute failed")],
        fetch_one_exceptions=[RuntimeError("fetchone failed")],
        fetch_all_exceptions=[RuntimeError("fetchall failed")],
    )

    async with client.transaction() as cur:
        with pytest.raises(RuntimeError, match="execute failed"):
            await cur.execute("SELECT broken", ("param",))
        with pytest.raises(RuntimeError, match="fetchone failed"):
            await cur.fetchone()
        with pytest.raises(RuntimeError, match="fetchall failed"):
            await cur.fetchall()

    assert client.calls == [("execute", "SELECT broken", ("param",))]
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert client.cursor_enters == 1
    assert client.cursor_exits == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_recording_postgres_client_can_inject_direct_query_failures():
    client = RecordingPostgresClient(
        fetch_all_exceptions=[RuntimeError("read failed")],
        execute_exceptions=[RuntimeError("write failed")],
    )

    with pytest.raises(RuntimeError, match="read failed"):
        await client.fetch_all("SELECT broken", (1,))

    with pytest.raises(RuntimeError, match="write failed"):
        await client.execute("UPDATE broken", (2,))

    assert client.calls == [
        ("fetch_all", "SELECT broken", (1,)),
        ("execute_command", "UPDATE broken", (2,)),
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_recording_postgres_client_can_inject_transaction_failure():
    client = RecordingPostgresClient(
        transaction_exceptions=[RuntimeError("transaction failed")]
    )

    with pytest.raises(RuntimeError, match="transaction failed"):
        async with client.transaction():
            pass

    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert client.cursor_enters == 0
    assert client.cursor_exits == 0
