import json

import pytest

from common.scoping import IDENTITY_ENTITY_ID
from knoggin_server.knowledge.db.writers.entity_writer import EntityWriter
from knoggin_server.knowledge.db.readers.entity_reader import EntityReader
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
        "embedding": [0.1] * 1024,
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
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_entity_writer_write_batch_dual_writes_entities_and_relationships(
    real_postgres_client, monkeypatch
):
    writer = EntityWriter(real_postgres_client)
    reader = EntityReader(real_postgres_client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    result = await writer.write_batch(
        [
            make_entity(id=2),
            make_entity(id=3, canonical_name="Grace Hopper"),
            make_entity(id=4, canonical_name="Alan Turing"),
        ],
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

    # 1. Verify entity_search table
    entity = await reader.get_entity_by_id(2, visible_project_ids=["project-1"])
    embedding = await reader.get_entity_embedding(2)
    assert entity is not None
    assert entity["canonical_name"] == "Ada Lovelace"
    assert embedding == [0.1] * 1024

    # 2. Verify graph node
    node_query = real_postgres_client.build_cypher("MATCH (e:Entity {id: 2}) RETURN e.canonical_name", "name agtype")
    res = await real_postgres_client.execute_read(node_query, ('{}',))
    assert len(res) == 1
    assert "Ada Lovelace" in str(res[0]["name"])

    # 3. Verify graph relationships
    edge_query = real_postgres_client.build_cypher(
        "MATCH (a)-[r:RELATED_TO]->(b) RETURN r.context, r.message_ids", 
        "ctx agtype, msg_ids agtype"
    )
    edges = await real_postgres_client.execute_read(edge_query, ('{}',))
    assert len(edges) == 2
    edges_str = str(edges)
    assert "1000000005" in edges_str
    assert "123" in edges_str



@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_entity_writer_update_entity_profile_updates_graph_and_search(
    real_postgres_client, monkeypatch
):
    writer = EntityWriter(real_postgres_client)
    reader = EntityReader(real_postgres_client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    # Seed
    await writer.write_batch([make_entity()], [])

    await writer.update_entity_profile(
        2,
        "Ada Byron",
        [0.3] * 1024,
        last_msg_id=77,
        project_id="project-1",
    )

    # Verify search table
    entity = await reader.get_entity_by_id(2, visible_project_ids=["project-1"])
    embedding = await reader.get_entity_embedding(2)
    assert entity["canonical_name"] == "Ada Byron"
    assert embedding == [0.3] * 1024

    # Verify graph node
    node_query = real_postgres_client.build_cypher("MATCH (e:Entity {id: 2}) RETURN e.canonical_name, e.last_profiled_msg_id", "name agtype, msg_id agtype")
    res = await real_postgres_client.execute_read(node_query, ('{}',))
    assert "Ada Byron" in str(res[0]["name"])
    assert "77" in str(res[0]["msg_id"])


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_entity_writer_update_entity_canonical_name_updates_graph_and_search(
    real_postgres_client, monkeypatch
):
    writer = EntityWriter(real_postgres_client)
    reader = EntityReader(real_postgres_client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    # Seed
    await writer.write_batch([make_entity()], [])

    await writer.update_entity_canonical_name(
        2,
        "Ada Byron",
        project_id="project-1",
    )

    entity = await reader.get_entity_by_id(2, visible_project_ids=["project-1"])
    assert entity["canonical_name"] == "Ada Byron"

    node_query = real_postgres_client.build_cypher("MATCH (e:Entity {id: 2}) RETURN e.canonical_name", "name agtype")
    res = await real_postgres_client.execute_read(node_query, ('{}',))
    assert "Ada Byron" in str(res[0]["name"])


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_entity_writer_update_entity_embedding_updates_graph_and_search(
    real_postgres_client, monkeypatch
):
    writer = EntityWriter(real_postgres_client)
    reader = EntityReader(real_postgres_client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    # Seed
    await writer.write_batch([make_entity()], [])

    await writer.update_entity_embedding(
        2,
        [0.3] * 1024,
        project_id="project-1",
    )

    entity = await reader.get_entity_by_id(2, visible_project_ids=["project-1"])
    assert entity["embedding"] == [0.3] * 1024

    node_query = real_postgres_client.build_cypher("MATCH (e:Entity {id: 2}) RETURN e.last_updated", "now agtype")
    res = await real_postgres_client.execute_read(node_query, ('{}',))
    assert "123456" in str(res[0]["now"])


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_entity_writer_update_entity_checkpoint_uses_execute_write_scope(
    real_postgres_client
):
    writer = EntityWriter(real_postgres_client)

    # Seed
    await writer.write_batch([make_entity()], [])

    await writer.update_entity_checkpoint(2, 77, project_id="project-1")

    node_query = real_postgres_client.build_cypher("MATCH (e:Entity {id: 2}) RETURN e.last_profiled_msg_id", "msg_id agtype")
    res = await real_postgres_client.execute_read(node_query, ('{}',))
    assert "77" in str(res[0]["msg_id"])



@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("method_name", "args"),
    [
        ("update_entity_profile", (2, "Ada Byron", [0.1], 7)),
        ("update_entity_canonical_name", (2, "Ada Byron")),
        ("update_entity_embedding", (2, [0.1])),
        ("update_entity_checkpoint", (2, 7)),

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
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_entity_writer_delete_entity_deletes_graph_and_search_row(
    real_postgres_client
):
    writer = EntityWriter(real_postgres_client)
    reader = EntityReader(real_postgres_client)

    # Seed
    await writer.write_batch([make_entity()], [])

    assert await writer.delete_entity(2, project_id="project-1") is True

    # Search table should be empty
    entity = await reader.get_entity_by_id(2, visible_project_ids=["project-1"])
    assert entity is None

    # Graph node should be deleted
    node_query = real_postgres_client.build_cypher("MATCH (e:Entity {id: 2}) RETURN count(e) as c", "c agtype")
    res = await real_postgres_client.execute_read(node_query, ('{}',))
    assert int(res[0]["c"]) == 0


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
