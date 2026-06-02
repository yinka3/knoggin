import json

import pytest

from common.scoping import IDENTITY_ENTITY_ID
from knoggin_server.knowledge.db.writers.entity_writer import EntityWriter
from tests.fixtures.fakes import RecordingPostgresClient


ENTITY_GRAPH_FIELDS = {
    "id",
    "canonical_name",
    "aliases",
    "type",
    "topic",
    "confidence",
    "user_name",
    "session_id",
    "project_id",
    "embedding",
    "now",
}

RELATIONSHIP_GRAPH_FIELDS = {
    "entity_a_id",
    "entity_b_id",
    "relationship",
    "context",
    "confidence",
    "message_id",
    "user_name",
    "session_id",
    "project_id",
    "evidence_ref",
    "now",
}


def make_entity(**overrides):
    entity = {
        "id": 2,
        "canonical_name": "Ada Lovelace",
        "aliases": ["Ada"],
        "type": "person",
        "topic": "Identity",
        "confidence": 0.9,
        "user_name": "ada",
        "session_id": "session-1",
        "project_id": "project-1",
        "embedding": [0.1, 0.2, 0.3],
    }
    entity.update(overrides)
    return entity


def make_relationship(**overrides):
    relationship = {
        "entity_a_id": 2,
        "entity_b_id": 3,
        "relationship": "knows",
        "context": "Ada knows Grace",
        "confidence": 0.8,
        "message_id": "msg_123",
        "user_name": "ada",
        "session_id": "session-1",
        "project_id": "project-1",
    }
    relationship.update(overrides)
    return relationship


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_write_batch_requires_async_pool():
    client = RecordingPostgresClient()
    client.async_pool = None
    writer = EntityWriter(client)

    with pytest.raises(RuntimeError, match="async_pool is not initialized"):
        await writer.write_batch([make_entity()], [])

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_write_batch_rejects_unscoped_entities_without_execute():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)
    entity = make_entity(project_id=None)

    with pytest.raises(ValueError, match="Entity 2 missing required scope fields"):
        await writer.write_batch([entity], [])

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_write_batch_rejects_unscoped_relationship():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)
    relationship = make_relationship(session_id=None)

    with pytest.raises(
        ValueError,
        match="Relationship 2:3 missing required scope fields",
    ):
        await writer.write_batch([], [relationship])

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_write_batch_dual_writes_entities_and_relationships(
    monkeypatch,
):
    client = RecordingPostgresClient()
    writer = EntityWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    result = await writer.write_batch(
        [make_entity()],
        [
            make_relationship(),
            make_relationship(
                entity_a_id=2,
                entity_b_id=4,
                evidence_ref={
                    "user_name": "ada",
                    "session_id": "session-2",
                    "message_id": "turn_5",
                },
                message_id="ignored",
                context=None,
            ),
        ],
    )

    assert result is True
    assert len(client.calls) == 3
    entity_graph_call, entity_search_call, relationship_graph_call = client.calls

    assert entity_graph_call[0] == "execute"
    assert "MERGE (e:Entity {id: data.id})" in entity_graph_call[1]
    entity_params = json.loads(entity_graph_call[2][0])
    entity_payload = entity_params["batch"][0]
    assert set(entity_payload) == ENTITY_GRAPH_FIELDS
    assert entity_payload == {
        "id": 2,
        "canonical_name": "Ada Lovelace",
        "aliases": ["Ada"],
        "type": "person",
        "topic": "Identity",
        "confidence": 0.9,
        "user_name": "ada",
        "session_id": "session-1",
        "project_id": "project-1",
        "embedding": [0.1, 0.2, 0.3],
        "now": 123456,
    }

    assert entity_search_call[0] == "execute"
    assert "INSERT INTO entity_search" in entity_search_call[1]
    assert entity_search_call[2] == (
        2,
        "Ada Lovelace",
        "ada",
        "project-1",
        [0.1, 0.2, 0.3],
    )

    assert relationship_graph_call[0] == "execute"
    assert "MERGE (node_a)-[r:RELATED_TO]->(node_b)" in relationship_graph_call[1]
    relationship_params = json.loads(relationship_graph_call[2][0])
    first_relationship = relationship_params["batch"][0]
    second_relationship = relationship_params["batch"][1]
    assert relationship_params["identity_entity_id"] == IDENTITY_ENTITY_ID
    assert set(first_relationship) == RELATIONSHIP_GRAPH_FIELDS
    assert set(second_relationship) == RELATIONSHIP_GRAPH_FIELDS
    assert first_relationship == {
        "entity_a_id": 2,
        "entity_b_id": 3,
        "relationship": "knows",
        "context": "Ada knows Grace",
        "confidence": 0.8,
        "message_id": "msg_123",
        "user_name": "ada",
        "session_id": "session-1",
        "project_id": "project-1",
        "evidence_ref": {
            "user_name": "ada",
            "session_id": "session-1",
            "message_id": 123,
        },
        "now": 123456,
    }
    assert second_relationship["evidence_ref"] == {
        "user_name": "ada",
        "session_id": "session-2",
        "message_id": 1_000_000_005,
    }
    assert second_relationship["context"] is None
    assert second_relationship["message_id"] == "ignored"
    assert second_relationship["confidence"] == 0.8
    assert second_relationship["now"] == 123456


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_update_entity_profile_updates_graph_and_search(
    monkeypatch,
):
    client = RecordingPostgresClient()
    writer = EntityWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    await writer.update_entity_profile(
        2,
        "Ada Byron",
        [0.3, 0.2, 0.1],
        last_msg_id=77,
        project_id="project-1",
    )

    assert len(client.calls) == 2
    graph_call, search_call = client.calls
    assert "SET e.canonical_name = $canonical_name" in graph_call[1]
    assert json.loads(graph_call[2][0]) == {
        "id": 2,
        "canonical_name": "Ada Byron",
        "now": 123456,
        "last_msg_id": 77,
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }
    assert "UPDATE entity_search" in search_call[1]
    assert search_call[2] == (
        "Ada Byron",
        [0.3, 0.2, 0.1],
        2,
        "project-1",
        IDENTITY_ENTITY_ID,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_update_entity_canonical_name_updates_graph_and_search(
    monkeypatch,
):
    client = RecordingPostgresClient()
    writer = EntityWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    await writer.update_entity_canonical_name(
        2,
        "Ada Byron",
        project_id="project-1",
    )

    assert len(client.calls) == 2
    graph_call, search_call = client.calls
    assert "SET e.canonical_name = $canonical_name" in graph_call[1]
    assert json.loads(graph_call[2][0]) == {
        "id": 2,
        "canonical_name": "Ada Byron",
        "now": 123456,
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }
    assert "UPDATE entity_search" in search_call[1]
    assert search_call[2] == (
        "Ada Byron",
        2,
        "project-1",
        IDENTITY_ENTITY_ID,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_update_entity_embedding_updates_graph_and_search(
    monkeypatch,
):
    client = RecordingPostgresClient()
    writer = EntityWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    await writer.update_entity_embedding(
        2,
        [0.3, 0.2, 0.1],
        project_id="project-1",
    )

    assert len(client.calls) == 2
    graph_call, search_call = client.calls
    assert "SET e.last_updated = $now" in graph_call[1]
    assert json.loads(graph_call[2][0]) == {
        "id": 2,
        "now": 123456,
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }
    assert "UPDATE entity_search" in search_call[1]
    assert search_call[2] == (
        [0.3, 0.2, 0.1],
        2,
        "project-1",
        IDENTITY_ENTITY_ID,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_update_entity_checkpoint_uses_execute_write_scope():
    client = RecordingPostgresClient(execute_write_results=[1])
    writer = EntityWriter(client)

    await writer.update_entity_checkpoint(2, 77, project_id="project-1")

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call[0] == "execute_write"
    assert "SET e.last_profiled_msg_id = $last_msg_id" in call[1]
    assert json.loads(call[2][0]) == {
        "id": 2,
        "last_msg_id": 77,
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_update_entity_aliases_merges_existing_aliases(
    monkeypatch,
):
    client = RecordingPostgresClient(fetchone_results=[{"aliases": ["Ada", "A."]}])
    writer = EntityWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    await writer.update_entity_aliases(
        {2: ["Ada", "Byron"]},
        project_id="project-1",
    )

    assert len(client.calls) == 2
    read_call, write_call = client.calls
    assert "RETURN e.aliases" in read_call[1]
    assert json.loads(read_call[2][0]) == {
        "id": 2,
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }

    assert "SET e.aliases = $aliases" in write_call[1]
    write_params = json.loads(write_call[2][0])
    assert write_params["id"] == 2
    assert set(write_params["aliases"]) == {"Ada", "A.", "Byron"}
    assert write_params["now"] == 123456
    assert write_params["project_id"] == "project-1"
    assert write_params["identity_entity_id"] == IDENTITY_ENTITY_ID


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_update_entity_aliases_empty_input_skips_db():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)

    await writer.update_entity_aliases({}, project_id=None)

    assert client.calls == []
    assert client.connection_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("method_name", "args"),
    [
        ("update_entity_profile", (2, "Ada Byron", [0.1], 7)),
        ("update_entity_canonical_name", (2, "Ada Byron")),
        ("update_entity_embedding", (2, [0.1])),
        ("update_entity_checkpoint", (2, 7)),
        ("update_entity_aliases", ({2: ["Ada"]},)),
        ("cleanup_null_entities", ()),
        ("delete_entity", (2,)),
        ("bulk_delete_entities", ([2],)),
    ],
)
async def test_entity_writer_scoped_operations_require_project_without_db_access(
    method_name,
    args,
):
    client = RecordingPostgresClient()
    writer = EntityWriter(client)

    with pytest.raises(ValueError, match=f"{method_name} requires project_id scope"):
        await getattr(writer, method_name)(*args)

    assert client.calls == []
    assert client.connection_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_delete_entity_deletes_graph_and_search_row():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)

    assert await writer.delete_entity(2, project_id="project-1") is True

    assert len(client.calls) == 2
    graph_call, search_call = client.calls
    assert "DETACH DELETE e, f" in graph_call[1]
    assert json.loads(graph_call[2][0]) == {
        "id": 2,
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }
    assert search_call == (
        "execute",
        "DELETE FROM entity_search WHERE entity_id = %s AND project_id = %s",
        (2, "project-1"),
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_bulk_delete_entities_empty_list_skips_db():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)

    assert await writer.bulk_delete_entities([], project_id="project-1") == 0
    assert client.calls == []
    assert client.connection_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("message_id", "expected"),
    [
        (7, 7),
        ("msg_123", 123),
        ("turn_5", 1_000_000_005),
    ],
)
def test_entity_writer_build_evidence_ref_normalizes_message_ids(
    message_id,
    expected,
):
    assert EntityWriter._build_evidence_ref(
        make_relationship(message_id=message_id)
    ) == {
        "user_name": "ada",
        "session_id": "session-1",
        "message_id": expected,
    }


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_writer_build_evidence_ref_prefers_explicit_scoped_ref():
    assert EntityWriter._build_evidence_ref(
        make_relationship(
            evidence_ref={
                "user_name": "ada",
                "session_id": "session-2",
                "message_id": "turn_9",
            },
            user_name="ignored",
            session_id="ignored",
            message_id="ignored",
        )
    ) == {
        "user_name": "ada",
        "session_id": "session-2",
        "message_id": 1_000_000_009,
    }


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_writer_build_evidence_ref_requires_relationship_scope():
    relationship = make_relationship(user_name=None)

    with pytest.raises(
        ValueError,
        match="Relationship evidence requires user_name and session_id scope",
    ):
        EntityWriter._build_evidence_ref(relationship)


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize("message_id", ["msg_nope", "turn_nope", "not-an-int"])
def test_entity_writer_build_evidence_ref_rejects_malformed_message_ids(message_id):
    with pytest.raises(ValueError):
        EntityWriter._build_evidence_ref(make_relationship(message_id=message_id))
