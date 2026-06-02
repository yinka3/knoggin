import json

import pytest

from common.scoping import IDENTITY_ENTITY_ID
from knoggin_server.knowledge.db.writers.graph_writer import GraphWriter
from tests.fixtures.fakes import RecordingPostgresClient


MESSAGE_GRAPH_FIELDS = {
    "id",
    "content",
    "role",
    "user_name",
    "session_id",
    "project_id",
    "timestamp",
}

MERGE_VALIDATION_FIELDS = {
    "primary_id",
    "secondary_id",
    "project_id",
    "identity_entity_id",
}

MERGE_UPDATE_PRIMARY_FIELDS = {
    "primary_id",
    "aliases",
    "now",
    "conf",
    "last",
}

MERGE_EDGE_FIELDS = {
    "target_id",
    "weight",
    "conf",
    "msg_ids",
    "last_seen",
}


@pytest.mark.storage
@pytest.mark.no_network
def test_graph_writer_requires_project_scope_for_scoped_operations():
    with pytest.raises(ValueError, match="create_hierarchy_edge requires project_id"):
        GraphWriter._require_project_id(None, "create_hierarchy_edge")

    assert GraphWriter._require_project_id("project-1", "op") == "project-1"


@pytest.mark.storage
@pytest.mark.no_network
def test_graph_writer_merges_evidence_refs_without_duplicates():
    existing = [{"message_id": 1, "session_id": "s"}, "legacy:2"]
    incoming = [{"session_id": "s", "message_id": 1}, {"message_id": 3}]

    merged = GraphWriter._merge_evidence_refs(existing, incoming)

    assert merged == [
        {"message_id": 1, "session_id": "s"},
        "legacy:2",
        {"message_id": 3},
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_save_message_logs_writes_graph_and_search_rows(
    monkeypatch,
):
    client = RecordingPostgresClient()
    writer = GraphWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    saved = await writer.save_message_logs(
        [
            {
                "id": 7,
                "content": "hello graph",
                "role": "user",
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
            }
        ]
    )

    assert saved is True
    assert len(client.calls) == 2
    graph_call, search_call = client.calls

    assert graph_call[0] == "execute"
    assert "MERGE (m:Message" in graph_call[1]
    graph_params = json.loads(graph_call[2][0])
    assert set(graph_params["batch"][0]) == MESSAGE_GRAPH_FIELDS
    assert graph_params == {
        "batch": [
            {
                "id": 7,
                "content": "hello graph",
                "role": "user",
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
                "timestamp": 123456,
            }
        ]
    }

    assert search_call[0] == "execute"
    assert "INSERT INTO message_search" in search_call[1]
    assert "ON CONFLICT (user_name, session_id, message_id)" in search_call[1]
    assert search_call[2] == (7, "ada", "session-1", "hello graph")
    assert client.connection_enters == 1
    assert client.connection_exits == 1
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert client.cursor_enters == 1
    assert client.cursor_exits == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_save_message_logs_empty_list_skips_db():
    client = RecordingPostgresClient()
    writer = GraphWriter(client)

    assert await writer.save_message_logs([]) is True
    assert client.calls == []
    assert client.connection_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_save_message_logs_requires_async_pool():
    client = RecordingPostgresClient()
    client.async_pool = None
    writer = GraphWriter(client)

    with pytest.raises(RuntimeError, match="async_pool is not initialized"):
        await writer.save_message_logs(
            [
                {
                    "id": 7,
                    "content": "hello graph",
                    "role": "user",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                }
            ]
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_save_message_logs_rejects_missing_scope_without_execute():
    client = RecordingPostgresClient()
    writer = GraphWriter(client)

    with pytest.raises(ValueError, match="missing required scope fields"):
        await writer.save_message_logs(
            [
                {
                    "id": 7,
                    "content": "hello graph",
                    "role": "user",
                    "user_name": "ada",
                    "session_id": "session-1",
                }
            ]
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_create_hierarchy_edge_uses_project_scope(monkeypatch):
    client = RecordingPostgresClient(execute_write_results=[1])
    writer = GraphWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    assert await writer.create_hierarchy_edge(
        parent_id=2,
        child_id=3,
        project_id="project-1",
    ) is True

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call[0] == "execute_write"
    assert "CREATE (child)-[:PART_OF" in call[1]
    assert json.loads(call[2][0]) == {
        "child_id": 3,
        "parent_id": 2,
        "project_id": "project-1",
        "now": 123456,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_create_hierarchy_edge_returns_false_on_db_failure():
    client = RecordingPostgresClient(
        execute_write_exceptions=[RuntimeError("graph down")]
    )
    writer = GraphWriter(client)

    assert await writer.create_hierarchy_edge(
        parent_id=2,
        child_id=3,
        project_id="project-1",
    ) is False
    assert len(client.calls) == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_delete_relationship_uses_project_and_identity_scope():
    client = RecordingPostgresClient(execute_write_results=[1])
    writer = GraphWriter(client)

    assert await writer.delete_relationship(
        2,
        3,
        project_id="project-1",
    ) is True

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call[0] == "execute_write"
    assert "DELETE r" in call[1]
    assert json.loads(call[2][0]) == {
        "a_id": 2,
        "b_id": 3,
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_delete_relationship_returns_false_on_zero_rows():
    client = RecordingPostgresClient(execute_write_results=[0])
    writer = GraphWriter(client)

    assert await writer.delete_relationship(
        2,
        3,
        project_id="project-1",
    ) is False


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_create_preference_writes_scoped_preference(monkeypatch):
    client = RecordingPostgresClient(execute_write_results=[1])
    writer = GraphWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    assert await writer.create_preference(
        "pref-1",
        "Use concise answers",
        "style",
        "session-1",
    ) is True

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call[0] == "execute_write"
    assert "CREATE (p:Preference" in call[1]
    assert json.loads(call[2][0]) == {
        "id": "pref-1",
        "content": "Use concise answers",
        "kind": "style",
        "session_id": "session-1",
        "now": 123456,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_delete_preference_deletes_by_id():
    client = RecordingPostgresClient(execute_write_results=[1])
    writer = GraphWriter(client)

    assert await writer.delete_preference("pref-1") is True

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call[0] == "execute_write"
    assert "MATCH (p:Preference {id: $id})" in call[1]
    assert json.loads(call[2][0]) == {"id": "pref-1"}


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("method_name", "args"),
    [
        ("create_hierarchy_edge", (2, 3)),
        ("delete_relationship", (2, 3)),
        ("merge_entities", (2, 3)),
    ],
)
async def test_graph_writer_scoped_operations_require_project_without_db_access(
    method_name,
    args,
):
    client = RecordingPostgresClient()
    writer = GraphWriter(client)

    with pytest.raises(ValueError, match=f"{method_name} requires project_id scope"):
        await getattr(writer, method_name)(*args)

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_merge_entities_rejects_guardrails_without_db_access():
    client = RecordingPostgresClient()
    writer = GraphWriter(client)

    assert await writer.merge_entities(2, 2, project_id="project-1") is False
    assert (
        await writer.merge_entities(
            IDENTITY_ENTITY_ID, 2, project_id="project-1"
        )
        is False
    )
    assert (
        await writer.merge_entities(
            2, IDENTITY_ENTITY_ID, project_id="project-1"
        )
        is False
    )
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_merge_entities_returns_false_when_validation_misses():
    client = RecordingPostgresClient(fetchone_results=[None])
    writer = GraphWriter(client)

    assert await writer.merge_entities(2, 3, project_id="project-1") is False

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call[0] == "execute"
    assert "MATCH (p:Entity {id: $primary_id})" in call[1]
    assert "MATCH (s:Entity {id: $secondary_id})" in call[1]
    assert json.loads(call[2][0]) == {
        "primary_id": 2,
        "secondary_id": 3,
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_merge_entities_happy_path_reaches_dual_write_cleanup(
    monkeypatch,
):
    client = RecordingPostgresClient(
        fetchone_results=[
            {
                "p_name": '"Ada Lovelace"',
                "p_aliases": ["Ada"],
                "p_conf": "0.4",
                "p_last": "100",
                "s_name": '"Countess Lovelace"',
                "s_aliases": ["Augusta"],
                "s_conf": "0.9",
                "s_last": "200",
            },
            None,
        ],
        fetchall_results=[
            [
                {
                    "source_id": "2",
                    "target_id": "9",
                    "weight": "2",
                    "conf": "0.4",
                    "msg_ids": [{"message_id": 1}],
                    "last_seen": "100",
                },
                {
                    "source_id": "3",
                    "target_id": "9",
                    "weight": "3",
                    "conf": "0.9",
                    "msg_ids": [{"message_id": 2}],
                    "last_seen": "200",
                },
            ]
        ],
    )
    writer = GraphWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    assert await writer.merge_entities(2, 3, project_id="project-1") is True

    validation_call = client.calls[0]
    assert "MATCH (p:Entity {id: $primary_id})" in validation_call[1]
    validation_params = json.loads(validation_call[2][0])
    assert set(validation_params) == MERGE_VALIDATION_FIELDS
    assert validation_params == {
        "primary_id": 2,
        "secondary_id": 3,
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }

    update_primary_call = client.calls[1]
    update_primary_params = json.loads(update_primary_call[2][0])
    assert set(update_primary_params) == MERGE_UPDATE_PRIMARY_FIELDS
    assert set(update_primary_params["aliases"]) == {
        "Ada",
        "Augusta",
        "Countess Lovelace",
    }
    assert update_primary_params["conf"] == 0.9
    assert update_primary_params["last"] == 200

    write_edges_call = next(
        call
        for call in client.calls
        if "UNWIND $batch AS edge" in call[1]
    )
    edge_params = json.loads(write_edges_call[2][0])
    assert edge_params["primary_id"] == 2
    assert set(edge_params["batch"][0]) == MERGE_EDGE_FIELDS
    assert edge_params["batch"] == [
        {
            "target_id": 9,
            "weight": 5,
            "conf": 0.9,
            "msg_ids": [{"message_id": 1}, {"message_id": 2}],
            "last_seen": 200,
        }
    ]

    assert (
        "execute",
        "DELETE FROM entity_search WHERE entity_id = %s",
        (3,),
    ) in client.calls
    assert (
        "execute",
        "UPDATE fact_search SET entity_id = %s WHERE entity_id = %s",
        (2, 3),
    ) in client.calls
    assert client.connection_enters == 1
    assert client.connection_exits == 1
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert client.cursor_enters == 1
    assert client.cursor_exits == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_merge_entities_requires_async_pool():
    client = RecordingPostgresClient()
    client.async_pool = None
    writer = GraphWriter(client)

    with pytest.raises(RuntimeError, match="async_pool is not initialized"):
        await writer.merge_entities(2, 3, project_id="project-1")

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_merge_entities_returns_false_on_transaction_error():
    client = RecordingPostgresClient(
        fetchone_results=[
            {
                "p_name": '"Ada Lovelace"',
                "p_aliases": ["Ada"],
                "p_conf": "0.4",
                "p_last": "100",
                "s_name": '"Countess Lovelace"',
                "s_aliases": ["Augusta"],
                "s_conf": "0.9",
                "s_last": "200",
            }
        ],
        execute_exceptions=[
            None,
            RuntimeError("update failed"),
        ],
    )
    writer = GraphWriter(client)

    assert await writer.merge_entities(2, 3, project_id="project-1") is False

    assert len(client.calls) == 2
    assert "MATCH (p:Entity {id: $primary_id})" in client.calls[0][1]
    assert "SET p.aliases = $aliases" in client.calls[1][1]
    assert client.connection_enters == 1
    assert client.connection_exits == 1
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
