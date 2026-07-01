import pytest

from infrastructure import knowledge_store as knowledge_store_module


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
def knowledge_store(monkeypatch):
    components = {}

    def component_factory(name):
        class Component(RecordingComponent):
            def __init__(self, client, *args):
                super().__init__(client)
                components[name] = self

        return Component

    monkeypatch.setattr(
        knowledge_store_module, "PostgresClient", RecordingPostgresClient
    )
    monkeypatch.setattr(
        knowledge_store_module, "IdAllocator", component_factory("id_allocator")
    )
    monkeypatch.setattr(
        knowledge_store_module, "EntityWriter", component_factory("entity_writer")
    )
    monkeypatch.setattr(
        knowledge_store_module, "FactWriter", component_factory("fact_writer")
    )
    monkeypatch.setattr(
        knowledge_store_module, "GraphWriter", component_factory("graph_writer")
    )
    monkeypatch.setattr(
        knowledge_store_module,
        "MergeAuditWriter",
        component_factory("merge_audit_writer"),
    )
    monkeypatch.setattr(
        knowledge_store_module, "EntityReader", component_factory("entity_reader")
    )
    monkeypatch.setattr(
        knowledge_store_module, "FactReader", component_factory("fact_reader")
    )
    monkeypatch.setattr(
        knowledge_store_module, "GraphReader", component_factory("graph_reader")
    )
    monkeypatch.setattr(
        knowledge_store_module,
        "MergeAuditReader",
        component_factory("merge_audit_reader"),
    )
    monkeypatch.setattr(
        knowledge_store_module, "ToolQueries", component_factory("tools")
    )
    monkeypatch.setattr(
        knowledge_store_module,
        "ProjectionRebuilder",
        component_factory("projection_rebuilder"),
    )
    monkeypatch.setattr(
        knowledge_store_module,
        "SearchIndexRebuilder",
        component_factory("search_index_rebuilder"),
    )
    monkeypatch.setattr(
        knowledge_store_module, "CommunityStore", component_factory("community")
    )

    client = knowledge_store_module.KnowledgeStore(
        "postgresql://example",
        embedding_service=object(),
    )
    return client, components


@pytest.mark.storage
@pytest.mark.no_network
async def test_knowledge_store_connect_and_close_delegate_to_postgres(knowledge_store):
    client, _ = knowledge_store

    await client.connect()
    await client.close()

    assert client._postgres_client.connected is True
    assert client._postgres_client.closed is True


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.storage
@pytest.mark.no_network
def test_knowledge_store_community_property(knowledge_store):
    client, components = knowledge_store
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
        ("delete_relationship", "graph_writer", (1, 2), {"project_id": "test"}),
        (
            "expire_merge_rollback_states",
            "merge_audit_writer",
            ("2026-01-01T05:00:00+00:00",),
            {"user_name": "ada", "project_id": "project-1"},
        ),
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
        (
            "get_entity_embedding",
            "entity_reader",
            (1,),
            {"visible_project_ids": ["test"]},
        ),
        (
            "validate_existing_ids",
            "entity_reader",
            ([1, 2],),
            {"visible_project_ids": ["test"]},
        ),
        ("get_orphan_entities", "entity_reader", (1, 0, 0), {"project_id": "test"}),
        (
            "get_entities_by_names",
            "entity_reader",
            (["name"],),
            {"visible_project_ids": ["test"]},
        ),
        (
            "search_similar_entities",
            "entity_reader",
            (1,),
            {"visible_project_ids": ["test"], "limit": 50},
        ),
        (
            "search_entities_by_embedding",
            "entity_reader",
            ([0.1],),
            {
                "visible_project_ids": ["test"],
                "limit": 10,
                "score_threshold": 0.8,
            },
        ),
        (
            "list_entities",
            "entity_reader",
            (20, 0),
            {
                "visible_project_ids": ["test"],
                "topic": "topic",
                "entity_type": "type",
                "search": "search",
            },
        ),
        (
            "get_entity_by_id",
            "entity_reader",
            (1,),
            {"visible_project_ids": ["test"]},
        ),
        (
            "get_entities_by_ids",
            "entity_reader",
            ([1, 2],),
            {"visible_project_ids": ["test"]},
        ),
        (
            "get_entity_count_by_type",
            "entity_reader",
            (),
            {"visible_project_ids": ["test"]},
        ),
        (
            "get_entity_count_by_topic",
            "entity_reader",
            (),
            {"visible_project_ids": ["test"]},
        ),
        (
            "get_top_connected_entities",
            "entity_reader",
            (),
            {"visible_project_ids": ["test"], "limit": 10},
        ),
        (
            "get_entity_relationships",
            "entity_reader",
            (1,),
            {"visible_project_ids": ["test"]},
        ),
        (
            "get_recently_active_entities",
            "entity_reader",
            (),
            {"visible_project_ids": ["test"], "days": 7, "limit": 10},
        ),
        (
            "get_notable_entities",
            "entity_reader",
            (),
            {"visible_project_ids": ["test"], "limit": 10},
        ),
        # --- Fact Reader (5) ---
        (
            "get_facts_for_entity",
            "fact_reader",
            (1,),
            {"visible_project_ids": ["test"], "active_only": True},
        ),
        (
            "search_relevant_facts",
            "fact_reader",
            (1, [0.1]),
            {"visible_project_ids": ["test"], "limit": 5},
        ),
        (
            "get_facts_for_entities",
            "fact_reader",
            ([1, 2],),
            {"visible_project_ids": ["test"], "active_only": True},
        ),
        (
            "get_facts_from_message",
            "fact_reader",
            (1,),
            {
                "user_name": "ada",
                "session_id": "session-1",
                "visible_project_ids": ["test"],
            },
        ),
        (
            "get_recent_facts",
            "fact_reader",
            (),
            {"visible_project_ids": ["test"], "days": 7, "limit": 20},
        ),
        # --- Graph Reader (14) ---
        (
            "get_message_text",
            "graph_reader",
            (1,),
            {
                "user_name": "ada",
                "session_id": "session-1",
                "visible_project_ids": ["test"],
            },
        ),
        (
            "get_messages_by_ids",
            "graph_reader",
            ([1, 2],),
            {
                "user_name": "ada",
                "session_ids": ["session-1"],
                "visible_project_ids": ["test"],
            },
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
            (1,),
            {
                "user_name": "ada",
                "session_id": "session-1",
                "visible_project_ids": ["test"],
                "forward": 3,
                "target_total": 10,
            },
        ),
        (
            "get_neighbor_ids",
            "graph_reader",
            (1,),
            {"visible_project_ids": ["test"]},
        ),
        (
            "get_parent_entities",
            "graph_reader",
            (1,),
            {"visible_project_ids": ["test"]},
        ),
        (
            "get_neighbor_entities",
            "graph_reader",
            (1,),
            {"visible_project_ids": ["test"], "limit": 5},
        ),
        (
            "get_child_entities",
            "graph_reader",
            (1,),
            {"visible_project_ids": ["test"]},
        ),
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
        (
            "has_direct_edge",
            "graph_reader",
            (1, 2),
            {"visible_project_ids": ["test"]},
        ),
        (
            "has_hierarchy_edge",
            "graph_reader",
            (1, 2),
            {"visible_project_ids": ["test"]},
        ),
        (
            "get_graph_stats",
            "graph_reader",
            (),
            {"visible_project_ids": ["test"]},
        ),
        (
            "get_neighbor_ids_batch",
            "graph_reader",
            ([1, 2],),
            {"visible_project_ids": ["test"]},
        ),
        # --- Tool Queries (6) ---
        (
            "get_hot_topic_context_with_messages",
            "tools",
            (["hot"],),
            {"visible_project_ids": ["test"], "msg_limit": 5, "slim": False},
        ),
        (
            "search_messages_fts",
            "tools",
            ("query",),
            {
                "user_name": "ada",
                "session_ids": ["session-1"],
                "visible_project_ids": ["test"],
                "limit": 50,
            },
        ),
        (
            "search_entity",
            "tools",
            ("query",),
            {
                "visible_project_ids": ["test"],
                "active_topics": ["topic"],
                "limit": 5,
                "connections_limit": 5,
                "evidence_limit": 5,
            },
        ),
        (
            "get_related_entities",
            "tools",
            (["name"],),
            {
                "visible_project_ids": ["test"],
                "active_topics": ["topic"],
                "limit": 50,
            },
        ),
        (
            "get_recent_activity",
            "tools",
            ("name",),
            {
                "visible_project_ids": ["test"],
                "active_topics": ["topic"],
                "hours": 24,
            },
        ),
        (
            "find_path_filtered",
            "tools",
            ("start", "end"),
            {
                "visible_project_ids": ["test"],
                "active_topics": ["topic"],
                "max_depth": 4,
            },
        ),
    ],
)
async def test_knowledge_store_facade_delegates_correctly(
    knowledge_store, method_name, component_name, args, kwargs
):
    client, components = knowledge_store

    # Retrieve the method to test dynamically
    method = getattr(client, method_name)

    # Call the method
    result = await method(*args, **kwargs)

    # Verify the facade routed the call to the expected storage component.
    component_method_name = {
        "expire_merge_rollback_states": "expire_rollback_states",
        "rebuild_project_search_indexes": "rebuild_project_indexes",
    }.get(method_name, method_name)
    assert result == f"{component_method_name}-result"
    assert components[component_name].calls == [
        (component_method_name, args, kwargs)
    ]
