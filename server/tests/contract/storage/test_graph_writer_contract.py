import json

import pytest

from common.exceptions import StorageWriteError
from common.scoping import IDENTITY_ENTITY_ID
from core.knowledge.db.writers.graph_writer import GraphWriter
from tests.fixtures.fakes import RecordingPostgresClient

MESSAGE_GRAPH_FIELDS = {
    "id",
    "content",
    "role",
    "user_name",
    "session_id",
    "project_id",
    "user_msg_id",
    "metadata",
    "timestamp",
}

MESSAGE_SQL_PARAMS = (
    "ada",
    "session-1",
    7,
    "project-1",
    "user",
    "hello graph",
    7,
    "{}",
    123456,
    "sealed",
    None,
    None,
    1,
    None,
    None,
    "excluded",
    None,
    None,
    None,
    False,
    None,
)

MERGE_RELATIONSHIP_PROJECTION_FIELDS = {
    "relationship_id",
    "project_id",
    "entity_a_id",
    "entity_b_id",
    "relationship_type",
    "weight",
    "confidence",
    "context",
    "last_seen",
    "message_ids",
}


def merge_validation_row(
    *,
    p_topic="People",
    s_topic="People",
    p_conf=0.4,
    p_last=100,
    s_conf=0.9,
    s_last=200,
):
    return {
        "p_name": "Ada Lovelace",
        "p_topic": p_topic,
        "p_aliases": ["Ada"],
        "p_conf": p_conf,
        "p_last": p_last,
        "s_name": "Countess Lovelace",
        "s_topic": s_topic,
        "s_aliases": ["Augusta"],
        "s_conf": s_conf,
        "s_last": s_last,
    }


@pytest.mark.storage
@pytest.mark.no_network
def test_graph_writer_requires_project_scope_for_scoped_operations():
    with pytest.raises(ValueError, match="operation requires project_id"):
        GraphWriter._require_project_id(None, "operation")

    assert GraphWriter._require_project_id("project-1", "op") == "project-1"


@pytest.mark.storage
@pytest.mark.no_network
def test_graph_writer_relationship_identity_normalizes_type_and_endpoint_order():
    assert GraphWriter._relationship_id(
        "project-1",
        3,
        2,
        " Works   With ",
    ) == "project-1:2:3:works with"


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
async def test_graph_writer_save_message_logs_writes_canonical_and_graph_rows(
    monkeypatch,
):
    client = RecordingPostgresClient(fetch_one_results=[{"message_id": 7}])
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
    canonical_call, graph_call = client.calls

    assert canonical_call[0] == "execute"
    assert "INSERT INTO messages" in canonical_call[1]
    assert "ON CONFLICT (user_name, session_id, message_id)" in canonical_call[1]
    assert "messages.content = EXCLUDED.content" in canonical_call[1]
    assert canonical_call[2] == MESSAGE_SQL_PARAMS

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
                "user_msg_id": 7,
                "metadata": "{}",
                "timestamp": 123456,
            }
        ]
    }

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
    assert client.transaction_enters == 0


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
async def test_graph_writer_delete_relationship_uses_project_and_identity_scope():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {"relationship_id": "project-1:2:3:works_with"},
            {"deleted": "1"},
        ],
    )
    writer = GraphWriter(client)

    assert await writer.delete_relationship(
        2,
        3,
        relationship_type="works_with",
        project_id="project-1",
    ) is True

    assert len(client.calls) == 2
    canonical_call, projection_call = client.calls
    assert canonical_call[0] == "execute"
    assert "DELETE FROM relationships" in canonical_call[1]
    assert canonical_call[2] == ("project-1:2:3:works_with",)
    assert projection_call[0] == "execute"
    assert "DELETE r" in projection_call[1]
    assert json.loads(projection_call[2][0]) == {
        "relationship_id": "project-1:2:3:works_with",
        "project_id": "project-1",
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_delete_relationship_returns_false_on_zero_rows():
    client = RecordingPostgresClient(
        fetch_one_results=[
            None,
            {"deleted": "0"},
        ],
    )
    writer = GraphWriter(client)

    assert await writer.delete_relationship(
        2,
        3,
        relationship_type="works_with",
        project_id="project-1",
    ) is False


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("method_name", "args", "kwargs"),
    [
        ("delete_relationship", (2, 3), {"relationship_type": "works_with"}),
        ("merge_entities", (2, 3), {}),
    ],
)
async def test_graph_writer_scoped_operations_require_project_without_db_access(
    method_name,
    args,
    kwargs,
):
    client = RecordingPostgresClient()
    writer = GraphWriter(client)

    with pytest.raises(ValueError, match=f"{method_name} requires project_id scope"):
        await getattr(writer, method_name)(*args, project_id="", **kwargs)

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
    client = RecordingPostgresClient(fetch_one_results=[None])
    writer = GraphWriter(client)

    assert await writer.merge_entities(2, 3, project_id="project-1") is False

    assert len(client.calls) == 2
    assert "pg_advisory_xact_lock" in client.calls[0][1]
    call = client.calls[1]
    assert call[0] == "execute"
    assert "FROM entities p" in call[1]
    assert "JOIN entities s" in call[1]
    assert call[2] == (3, "project-1", 2, "project-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_merge_entities_happy_path_reaches_dual_write_cleanup(
    monkeypatch,
):
    client = RecordingPostgresClient(
        fetch_one_results=[
            merge_validation_row(),
            {
                "message_ref_count": 0,
                "episode_entity_count": 0,
                "relationship_count": 0,
            },
            {"entity_id": 3},
        ],
        fetch_all_results=[
            [
                {
                    "relationship_id": "project-1:3:9:works with",
                    "user_name": "ada",
                    "project_id": "project-1",
                    "entity_a_id": 3,
                    "entity_b_id": 9,
                    "relationship_type": "works with",
                    "weight": 3,
                    "confidence": 0.9,
                    "context": "works with",
                    "last_seen_ms": 200,
                },
            ],
            [
                {
                    "relationship_id": "project-1:2:9:works with",
                    "user_name": "ada",
                    "project_id": "project-1",
                    "entity_a_id": 2,
                    "entity_b_id": 9,
                    "relationship_type": "works with",
                    "weight": 5,
                    "confidence": 0.9,
                    "context": "works with",
                    "last_seen_ms": 200,
                    "evidence_refs": [
                        {
                            "user_name": "ada",
                            "session_id": "session-1",
                            "message_id": 1,
                        },
                        {
                            "user_name": "ada",
                            "session_id": "session-1",
                            "message_id": 2,
                        },
                    ],
                }
            ],
        ],
    )
    writer = GraphWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    assert (
        await writer.merge_entities(
            2,
            3,
            project_id="project-1",
            final_topic="Projects",
        )
        is True
    )

    lock_call = client.calls[0]
    assert "pg_advisory_xact_lock" in lock_call[1]
    assert lock_call[2] == ("project-1",)

    validation_call = client.calls[1]
    assert "FROM entities p" in validation_call[1]
    assert validation_call[2] == (3, "project-1", 2, "project-1")

    update_primary_call = next(
        call
        for call in client.calls
        if call[0] == "execute" and "UPDATE entities" in call[1]
    )
    assert update_primary_call[2] == ("Projects", 200, 2, "project-1")

    projection_update_call = next(
        call for call in client.calls if "SET p.aliases = $aliases" in call[1]
    )
    update_primary_params = json.loads(projection_update_call[2][0])
    assert set(update_primary_params) == {
        "primary_id",
        "project_id",
        "aliases",
        "last_mentioned",
    }
    assert set(update_primary_params["aliases"]) == {
        "Ada",
        "Augusta",
        "Countess Lovelace",
    }
    assert update_primary_params["last_mentioned"] == 200

    relationship_projection_call = next(
        call
        for call in client.calls
        if "UNWIND $batch AS rel" in call[1]
        and "r.project_id = rel.project_id" in call[1]
        and "r.weight = rel.weight" in call[1]
    )
    rel_params = json.loads(relationship_projection_call[2][0])
    assert set(rel_params["batch"][0]) == MERGE_RELATIONSHIP_PROJECTION_FIELDS
    assert rel_params["batch"] == [
        {
            "relationship_id": "project-1:2:9:works with",
            "project_id": "project-1",
            "entity_a_id": 2,
            "entity_b_id": 9,
            "relationship_type": "works with",
            "weight": 5,
            "confidence": 0.9,
            "context": "works with",
            "last_seen": 200,
            "message_ids": [
                '{"message_id": 1, "session_id": "session-1", "user_name": "ada"}',
                '{"message_id": 2, "session_id": "session-1", "user_name": "ada"}',
            ],
        }
    ]
    topic_projection_call = next(
        call
        for call in client.calls
        if "OPTIONAL MATCH (e)-[old:BELONGS_TO]->(:Topic)" in call[1]
    )
    assert json.loads(topic_projection_call[2][0])["batch"] == [
        {"id": 2, "topic": "Projects"}
    ]
    assert not any(
        "MATCH (s:Entity {id: $secondary_id})-[r:BELONGS_TO]" in call[1]
        for call in client.calls
    )

    assert not any(
        call[0] == "execute" and "DELETE FROM entity_search" in call[1]
        for call in client.calls
    )
    assert any(
        call[0] == "execute"
        and "UPDATE message_entity_refs" in call[1]
        and call[2] == (2, 3)
        for call in client.calls
    )
    assert any(
        call[0] == "execute"
        and "INSERT INTO relationships" in call[1]
        and call[2][:5]
        == ("project-1:2:9:works with", "ada", "project-1", 2, 9)
        for call in client.calls
    )
    assert any(
        call[0] == "execute"
        and "INSERT INTO episode_entities" in call[1]
        and call[2] == (2, 3, "project-1")
        for call in client.calls
    )
    assert any(
        call[0] == "execute"
        and "UPDATE episode_entities" in call[1]
        and "COUNT(DISTINCT em.message_id)" in call[1]
        and call[2] == (2, "project-1", 2)
        for call in client.calls
    )
    assert any(
        call[0] == "execute"
        and "FROM relationships" in call[1]
        and call[2] == ("project-1", 3, 3)
        for call in client.calls
    )
    assert any(
        call[0] == "execute"
        and "INSERT INTO episode_relationships" in call[1]
        and call[2]
        == (
            "project-1:2:9:works with",
            "project-1:3:9:works with",
            "project-1",
        )
        for call in client.calls
    )
    assert any(
        call[0] == "execute"
        and "UPDATE episode_relationships" in call[1]
        and "COUNT(DISTINCT em.message_id)" in call[1]
        and call[2]
        == (
            "project-1:2:9:works with",
            "project-1",
            "project-1:2:9:works with",
        )
        for call in client.calls
    )
    dependency_check = next(
        call
        for call in client.calls
        if call[0] == "execute" and "AS message_ref_count" in call[1]
    )
    assert dependency_check[2] == (
        3,
        3,
        3,
        3,
    )
    assert any(
        call[0] == "execute"
        and "DELETE FROM entities" in call[1]
        and "RETURNING entity_id" in call[1]
        and call[2] == (3, "project-1")
        for call in client.calls
    )
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert client.cursor_enters == 1
    assert client.cursor_exits == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_rejects_conflicting_canonical_message_payload():
    client = RecordingPostgresClient(fetch_one_results=[None])
    writer = GraphWriter(client)

    with pytest.raises(RuntimeError, match="Canonical message ID collision"):
        await writer.save_message_logs(
            [
                {
                    "id": 7,
                    "content": "different payload",
                    "role": "assistant",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "timestamp": 123456,
                }
            ]
        )

    assert len(client.calls) == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_merge_aborts_when_secondary_dependencies_remain():
    client = RecordingPostgresClient(
        fetch_one_results=[
            merge_validation_row(
                p_conf=0.8,
                p_last=100,
                s_conf=0.7,
                s_last=90,
            ),
            {
                "message_ref_count": 1,
                "episode_entity_count": 0,
                "relationship_count": 0,
            },
        ],
        fetch_all_results=[[]],
    )
    writer = GraphWriter(client)

    with pytest.raises(StorageWriteError, match="merge_entities"):
        await writer.merge_entities(2, 3, project_id="project-1")
    assert not any(
        call[0] == "execute" and "DELETE FROM entities" in call[1]
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_writer_merge_entities_raises_on_transaction_error():
    client = RecordingPostgresClient(
        fetch_one_results=[
            merge_validation_row(
                p_conf="0.4",
                p_last="100",
                s_conf="0.9",
                s_last="200",
            ),
        ],
        cursor_execute_exceptions=[
            None,
            None,
            None,
            RuntimeError("update failed"),
        ],
    )
    writer = GraphWriter(client)

    with pytest.raises(StorageWriteError, match="merge_entities"):
        await writer.merge_entities(2, 3, project_id="project-1")

    assert len(client.calls) == 4
    assert "pg_advisory_xact_lock" in client.calls[0][1]
    assert "FROM entities p" in client.calls[1][1]
    assert "UPDATE entities" in client.calls[2][1]
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
