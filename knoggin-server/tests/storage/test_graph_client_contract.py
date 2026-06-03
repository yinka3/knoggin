import pytest

from infrastructure import graph_client as graph_client_module


class RecordingComponent:
    def __init__(self, client):
        self.client = client
        self.calls = []

    def __getattr__(self, name):
        async def method(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            return f"{name}-result"

        return method


class RecordingPostgresClient:
    def __init__(self, dsn):
        self.dsn = dsn
        self.connected = False
        self.closed = False

    async def connect(self):
        self.connected = True

    async def close(self):
        self.closed = True


@pytest.fixture
def graph_client(monkeypatch):
    components = {}

    def component_factory(name):
        class Component(RecordingComponent):
            def __init__(self, client):
                super().__init__(client)
                components[name] = self

        return Component

    monkeypatch.setattr(graph_client_module, "PostgresClient", RecordingPostgresClient)
    monkeypatch.setattr(
        graph_client_module, "EntityWriter", component_factory("entity_writer")
    )
    monkeypatch.setattr(
        graph_client_module, "FactWriter", component_factory("fact_writer")
    )
    monkeypatch.setattr(
        graph_client_module, "GraphWriter", component_factory("graph_writer")
    )
    monkeypatch.setattr(
        graph_client_module, "EntityReader", component_factory("entity_reader")
    )
    monkeypatch.setattr(
        graph_client_module, "FactReader", component_factory("fact_reader")
    )
    monkeypatch.setattr(
        graph_client_module, "GraphReader", component_factory("graph_reader")
    )
    monkeypatch.setattr(graph_client_module, "ToolQueries", component_factory("tools"))
    monkeypatch.setattr(
        graph_client_module, "CommunityStore", component_factory("community")
    )

    client = graph_client_module.GraphClient("postgresql://example")
    return client, components


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_client_connect_and_close_delegate_to_postgres(graph_client):
    client, _ = graph_client

    await client.connect()
    await client.close()

    assert client._postgres_client.connected is True
    assert client._postgres_client.closed is True


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_client_facade_delegates_writes_reads_and_tools(graph_client):
    client, components = graph_client

    assert await client.save_message_logs([{"id": 1}]) == "save_message_logs-result"
    assert await client.write_batch([{"id": 1}], []) == "write_batch-result"
    assert (
        await client.create_facts_batch(
            7,
            ["fact"],
            user_name="ada",
            session_id="session-1",
            project_id="project-1",
        )
        == "create_facts_batch-result"
    )
    assert (
        await client.get_message_text(1, "ada", "session-1")
        == "get_message_text-result"
    )
    assert (
        await client.search_messages_fts("hello", 3, "ada", ["session-1"])
        == "search_messages_fts-result"
    )

    assert components["graph_writer"].calls == [
        ("save_message_logs", ([{"id": 1}],), {})
    ]
    assert components["entity_writer"].calls == [
        ("write_batch", ([{"id": 1}], []), {})
    ]
    assert components["fact_writer"].calls == [
        (
            "create_facts_batch",
            (7, ["fact"]),
            {
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
            },
        )
    ]
    assert components["graph_reader"].calls == [
        ("get_message_text", (1, "ada", "session-1"), {})
    ]
    assert components["tools"].calls == [
        ("search_messages_fts", ("hello", 3, "ada", ["session-1"]), {})
    ]
    assert client.community is components["community"]
