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
            def __init__(self, client, *args):
                super().__init__(client)
                components[name] = self

        return Component

    monkeypatch.setattr(graph_client_module, "PostgresClient", RecordingPostgresClient)
    monkeypatch.setattr(
        graph_client_module, "IdAllocator", component_factory("id_allocator")
    )
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
        graph_client_module,
        "ProjectionRebuilder",
        component_factory("projection_rebuilder"),
    )
    monkeypatch.setattr(
        graph_client_module,
        "SearchIndexRebuilder",
        component_factory("search_index_rebuilder"),
    )
    monkeypatch.setattr(
        graph_client_module, "CommunityStore", component_factory("community")
    )

    client = graph_client_module.GraphClient(
        "postgresql://example",
        embedding_service=object(),
    )
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
def test_graph_client_community_property(graph_client):
    client, components = graph_client
    assert client.community is components["community"]


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    "method_name, component_name, args, kwargs",
    [
        ("allocate_entity_id", "id_allocator", (), {}),
        ("allocate_message_id", "id_allocator", (), {}),
        # --- Graph Writer (6) ---
        ("save_message_logs", "graph_writer", ([{"id": 1}],), {}),
        ("create_hierarchy_edge", "graph_writer", (1, 2), {"project_id": "test"}),
        (
            "merge_entities",
            "graph_writer",
            (1, 2),
            {"project_id": "test", "final_topic": "Projects"},
        ),
        (
            "create_preference",
            "graph_writer",
            ("id1", "content", "kind", "session"),
            {},
        ),
        ("delete_preference", "graph_writer", ("pref1",), {}),
        ("delete_relationship", "graph_writer", (1, 2), {"project_id": "test"}),
        (
            "rebuild_project_projection",
            "projection_rebuilder",
            ("project-1",),
            {"user_name": "ada"},
        ),
        (
            "rebuild_project_search_indexes",
            "search_index_rebuilder",
            ("project-1", "ada", ["project-1", "archive-1"]),
            {},
        ),
        # --- Entity Writer (9) ---
        ("write_batch", "entity_writer", ([{"id": 1}], [{"rel": 2}]), {}),
        (
            "ensure_identity_entity",
            "entity_writer",
            ("ada", ["A. Lovelace"]),
            {},
        ),
        (
            "update_entity_profile",
            "entity_writer",
            (1, "name", [0.1], 123),
            {"project_id": "test"},
        ),
        (
            "update_entity_canonical_name",
            "entity_writer",
            (1, "name"),
            {"project_id": "test"},
        ),
        (
            "update_entity_embedding",
            "entity_writer",
            (1, [0.1]),
            {"project_id": "test"},
        ),
        ("update_entity_checkpoint", "entity_writer", (1, 123), {"project_id": "test"}),
        (
            "update_entity_aliases",
            "entity_writer",
            ({1: ["a"]},),
            {"project_id": "test"},
        ),
        ("cleanup_null_entities", "entity_writer", (), {"project_id": "test"}),
        ("delete_entity", "entity_writer", (1,), {"project_id": "test"}),
        ("bulk_delete_entities", "entity_writer", ([1, 2],), {"project_id": "test"}),
        # --- Fact Writer (3) ---
        (
            "create_facts_batch",
            "fact_writer",
            (1, ["fact"]),
            {"user_name": "ada", "session_id": "session-1", "project_id": "test"},
        ),
        (
            "invalidate_fact",
            "fact_writer",
            ("fid", "2025-01-01"),
            {"project_id": "test"},
        ),
        (
            "delete_old_invalidated_facts",
            "fact_writer",
            ("2025-01-01",),
            {"project_id": "test"},
        ),
        # --- Entity Reader (18) ---
        ("get_max_entity_id", "entity_reader", (), {}),
        ("get_entity_embedding", "entity_reader", (1,), {}),
        ("validate_existing_ids", "entity_reader", ([1, 2],), {}),
        ("get_all_entities_for_hydration", "entity_reader", (), {}),
        ("find_alias_collisions", "entity_reader", (), {}),
        ("get_orphan_entities", "entity_reader", (1, 0, 0), {"project_id": "test"}),
        ("get_entities_by_names", "entity_reader", (["name"], ["test"]), {}),
        ("search_similar_entities", "entity_reader", (1, 50, ["test"]), {}),
        (
            "search_entities_by_embedding",
            "entity_reader",
            ([0.1], 10, 0.8, ["test"]),
            {},
        ),
        ("list_entities", "entity_reader", (20, 0, "topic", "type", "search"), {}),
        ("get_entity_by_id", "entity_reader", (1, ["test"]), {}),
        ("get_entities_by_ids", "entity_reader", ([1, 2],), {}),
        ("get_entity_count_by_type", "entity_reader", (), {}),
        ("get_entity_count_by_topic", "entity_reader", (), {}),
        ("get_top_connected_entities", "entity_reader", (10,), {}),
        ("get_entity_relationships", "entity_reader", (1,), {}),
        ("get_recently_active_entities", "entity_reader", (7, 10), {}),
        ("get_notable_entities", "entity_reader", (10,), {}),
        # --- Fact Reader (5) ---
        ("get_facts_for_entity", "fact_reader", (1, True), {}),
        ("search_relevant_facts", "fact_reader", (1, [0.1], 5), {}),
        ("get_facts_for_entities", "fact_reader", ([1, 2], True), {}),
        (
            "get_facts_from_message",
            "fact_reader",
            (1,),
            {"user_name": "ada", "session_id": "session-1"},
        ),
        ("get_recent_facts", "fact_reader", (7, 20), {}),
        # --- Graph Reader (14) ---
        ("get_message_text", "graph_reader", (1, "ada", "session-1"), {}),
        (
            "get_messages_by_ids",
            "graph_reader",
            ([1, 2],),
            {"user_name": "ada", "session_ids": ["session-1"]},
        ),
        (
            "get_recent_project_messages",
            "graph_reader",
            ("ada", "project-1", 20),
            {"before_message_id": None},
        ),
        (
            "get_surrounding_messages",
            "graph_reader",
            (1, 3, 10),
            {"user_name": "ada", "session_id": "session-1"},
        ),
        ("get_neighbor_ids", "graph_reader", (1,), {}),
        ("get_parent_entities", "graph_reader", (1,), {}),
        ("get_neighbor_entities", "graph_reader", (1, 5), {}),
        ("get_child_entities", "graph_reader", (1,), {}),
        (
            "get_hierarchy_candidates",
            "graph_reader",
            ("project-1", "topic", "parent", ["child"], 2),
            {},
        ),
        (
            "get_merge_topic_strength",
            "graph_reader",
            (1, 2, "project-1"),
            {},
        ),
        ("has_direct_edge", "graph_reader", (1, 2), {}),
        ("has_hierarchy_edge", "graph_reader", (1, 2), {}),
        ("list_preferences", "graph_reader", ("session-1", "kind"), {}),
        ("get_graph_stats", "graph_reader", (), {}),
        ("get_neighbor_ids_batch", "graph_reader", ([1, 2],), {}),
        # --- Tool Queries (6) ---
        (
            "get_hot_topic_context_with_messages",
            "tools",
            (["hot"], 5, False, ["test"]),
            {},
        ),
        (
            "search_messages_fts",
            "tools",
            ("query", 50),
            {
                "user_name": "ada",
                "session_ids": ["session-1"],
                "project_ids": None,
            },
        ),
        ("search_entity", "tools", ("query", ["topic"], 5, 5, 5, ["test"]), {}),
        ("get_related_entities", "tools", (["name"], ["topic"], 50, ["test"]), {}),
        ("get_recent_activity", "tools", ("name", ["topic"], 24, ["test"]), {}),
        ("find_path_filtered", "tools", ("start", "end", ["topic"], 4, ["test"]), {}),
    ],
)
async def test_graph_client_facade_delegates_correctly(
    graph_client, method_name, component_name, args, kwargs
):
    client, components = graph_client

    # Retrieve the method to test dynamically
    method = getattr(client, method_name)

    # Call the method
    result = await method(*args, **kwargs)

    # Verify the facade routed the call to the expected storage component.
    component_method_name = {
        "rebuild_project_search_indexes": "rebuild_project_indexes",
    }.get(method_name, method_name)
    assert result == f"{component_method_name}-result"
    assert components[component_name].calls == [
        (component_method_name, args, kwargs)
    ]
