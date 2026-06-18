import json
from datetime import datetime, timezone

import pytest

from common.schema.primitives import FactRecord
from common.scoping import IDENTITY_ENTITY_ID, IDENTITY_SCOPE
from knoggin_server.knowledge.db.readers.entity_reader import EntityReader
from knoggin_server.knowledge.db.writers.entity_writer import EntityWriter
from knoggin_server.knowledge.db.writers.fact_writer import FactWriter
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
        "is_new": True,
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
@pytest.mark.no_network
async def test_entity_writer_write_batch_requires_explicit_write_intent():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)
    entity = make_entity()
    entity.pop("is_new")

    with pytest.raises(ValueError, match="missing is_new write intent"):
        await writer.write_batch([entity], [])

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_new_entity_uses_strict_insert():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)

    await writer.write_batch([make_entity(aliases=[], embedding=None)], [])

    canonical_sql = client.calls[0][1]
    assert "INSERT INTO entities" in canonical_sql
    assert "ON CONFLICT (entity_id)" not in canonical_sql


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_existing_entity_must_exist_in_scope():
    client = RecordingPostgresClient(fetchone_results=[None])
    writer = EntityWriter(client)

    with pytest.raises(RuntimeError, match="Existing entity 2 was not found"):
        await writer.write_batch(
            [make_entity(is_new=False, aliases=[], embedding=None)],
            [],
        )


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_existing_entity_replaces_age_topic():
    client = RecordingPostgresClient(
        fetchone_results=[{"entity_id": 2}],
    )
    writer = EntityWriter(client)

    await writer.write_batch(
        [
            make_entity(
                is_new=False,
                aliases=[],
                embedding=None,
                topic="Projects",
            )
        ],
        [],
    )

    topic_call = next(
        call
        for call in client.calls
        if "MERGE (e)-[:BELONGS_TO]->(t)" in call[1]
    )
    assert "OPTIONAL MATCH (e)-[old:BELONGS_TO]->(:Topic)" in topic_call[1]
    assert "DELETE old" in topic_call[1]
    assert json.loads(topic_call[2][0])["batch"] == [
        {"id": 2, "topic": "Projects"}
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_ensures_reserved_identity_in_sql_and_age(monkeypatch):
    client = RecordingPostgresClient(fetchone_results=[None])
    writer = EntityWriter(client)
    monkeypatch.setattr(writer, "_current_time_ms", lambda: 123456)

    identity = await writer.ensure_identity_entity(
        "Ada Lovelace",
        ["Ada", " ada ", "Ada Lovelace", ""],
    )

    assert identity == {
        "id": IDENTITY_ENTITY_ID,
        "user_name": "Ada Lovelace",
        "session_id": None,
        "project_id": IDENTITY_SCOPE,
        "canonical_name": "Ada Lovelace",
        "aliases": ["Ada"],
        "type": "person",
        "topic": "Identity",
        "confidence": 1.0,
        "now": 123456,
    }
    sql = "\n".join(call[1] for call in client.calls)
    assert "pg_advisory_xact_lock" in sql
    assert "INSERT INTO entities" in sql
    assert "DELETE FROM entity_aliases" in sql
    assert "INSERT INTO entity_aliases" in sql
    assert "INSERT INTO entity_search" in sql
    assert "MERGE (e:Entity {id: $id})" in sql
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_rejects_non_identity_occupant_at_reserved_id():
    client = RecordingPostgresClient(
        fetchone_results=[
            {
                "entity_id": IDENTITY_ENTITY_ID,
                "user_name": "grace",
                "project_id": "project-1",
                "canonical_name": "Grace Hopper",
            }
        ]
    )
    writer = EntityWriter(client)

    with pytest.raises(RuntimeError, match="ID 1 is occupied"):
        await writer.ensure_identity_entity("Ada Lovelace")

    assert not any("INSERT INTO entities" in call[1] for call in client.calls)


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
                    "message_id": "msg_125",
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
    node_query = real_postgres_client.build_cypher(
        "MATCH (e:Entity {id: 2}) RETURN e.canonical_name",
        "name agtype",
    )
    res = await real_postgres_client.execute_read(node_query, ("{}",))
    assert len(res) == 1
    assert "Ada Lovelace" in str(res[0]["name"])

    # 3. Verify graph relationships
    edge_query = real_postgres_client.build_cypher(
        "MATCH (a)-[r:RELATED_TO]->(b) RETURN r.context, r.message_ids",
        "ctx agtype, msg_ids agtype",
    )
    edges = await real_postgres_client.execute_read(edge_query, ("{}",))
    assert len(edges) == 2
    edges_str = str(edges)
    assert "125" in edges_str
    assert "123" in edges_str

    rel_rows = await real_postgres_client.execute_read(
        """
        SELECT relationship_id, entity_a_id, entity_b_id, weight, confidence, context
        FROM relationships
        WHERE project_id = %s
        ORDER BY entity_b_id
        """,
        ("project-1",),
    )
    assert rel_rows == [
        {
            "relationship_id": "project-1:2:3",
            "entity_a_id": 2,
            "entity_b_id": 3,
            "weight": 1,
            "confidence": 0.8,
            "context": "Ada knows Grace",
        },
        {
            "relationship_id": "project-1:2:4",
            "entity_a_id": 2,
            "entity_b_id": 4,
            "weight": 1,
            "confidence": 0.8,
            "context": None,
        },
    ]

    evidence_rows = await real_postgres_client.execute_read(
        """
        SELECT relationship_id, user_name, session_id, message_id
        FROM relationship_evidence_refs
        ORDER BY relationship_id
        """,
    )
    assert evidence_rows == [
        {
            "relationship_id": "project-1:2:3",
            "user_name": "ada",
            "session_id": "session-1",
            "message_id": 123,
        },
        {
            "relationship_id": "project-1:2:4",
            "user_name": "ada",
            "session_id": "session-2",
            "message_id": 1_000_000_005,
        },
    ]



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
    node_query = real_postgres_client.build_cypher(
        "MATCH (e:Entity {id: 2}) "
        "RETURN e.canonical_name, e.last_profiled_msg_id",
        "name agtype, msg_id agtype",
    )
    res = await real_postgres_client.execute_read(node_query, ("{}",))
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

    node_query = real_postgres_client.build_cypher(
        "MATCH (e:Entity {id: 2}) RETURN e.canonical_name",
        "name agtype",
    )
    res = await real_postgres_client.execute_read(node_query, ("{}",))
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

    node_query = real_postgres_client.build_cypher(
        "MATCH (e:Entity {id: 2}) RETURN e.last_updated",
        "now agtype",
    )
    res = await real_postgres_client.execute_read(node_query, ("{}",))
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

    node_query = real_postgres_client.build_cypher(
        "MATCH (e:Entity {id: 2}) RETURN e.last_profiled_msg_id",
        "msg_id agtype",
    )
    res = await real_postgres_client.execute_read(node_query, ("{}",))
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
    fact_writer = FactWriter(real_postgres_client)
    reader = EntityReader(real_postgres_client)

    # Seed
    await writer.write_batch([make_entity()], [])
    await fact_writer.create_facts_batch(
        2,
        [
            FactRecord(
                id="delete-fact",
                content="Temporary fact",
                source_entity_id=2,
                valid_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                embedding=[0.1] * 1024,
            )
        ],
        user_name="ada",
        session_id="session-1",
        project_id="project-1",
    )

    assert await writer.delete_entity(2, project_id="project-1") is True

    entity = await reader.get_entity_by_id(2, visible_project_ids=["project-1"])
    assert entity is None
    sql_counts = await real_postgres_client.execute_read(
        """
        SELECT
            (SELECT count(*) FROM facts WHERE fact_id = 'delete-fact') AS facts,
            (
                SELECT count(*)
                FROM fact_search
                WHERE fact_id = 'delete-fact'
            ) AS fact_search
        """
    )
    assert sql_counts == [{"facts": 0, "fact_search": 0}]

    node_query = real_postgres_client.build_cypher(
        """
        OPTIONAL MATCH (e:Entity {id: 2})
        OPTIONAL MATCH (f:Fact {id: 'delete-fact'})
        RETURN count(e), count(f)
        """,
        "entity_count agtype, fact_count agtype",
    )
    res = await real_postgres_client.execute_read(node_query, ("{}",))
    assert int(res[0]["entity_count"]) == 0
    assert int(res[0]["fact_count"]) == 0


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_bulk_delete_entities_empty_list_skips_db():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)

    assert await writer.bulk_delete_entities([], project_id="project-1") == []
    assert client.calls == []
    assert client.connection_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_bulk_delete_returns_exact_scoped_ids():
    client = RecordingPostgresClient(
        fetchall_results=[[{"entity_id": 2}, {"entity_id": 4}]]
    )
    writer = EntityWriter(client)

    deleted_ids = await writer.bulk_delete_entities(
        [4, 2, 4, IDENTITY_ENTITY_ID, 99],
        project_id="project-1",
    )

    assert deleted_ids == [2, 4]
    delete_call = next(
        call
        for call in client.calls
        if call[0] == "execute" and "DELETE FROM entities" in call[1]
    )
    assert delete_call[2] == ([2, 4, 99], "project-1", IDENTITY_ENTITY_ID)
    projection_call = next(
        call for call in client.calls if "e.id IN $entity_ids" in call[1]
    )
    assert json.loads(projection_call[2][0]) == {
        "entity_ids": [2, 4],
        "project_id": "project-1",
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_null_cleanup_uses_aggregate_deletion():
    client = RecordingPostgresClient(
        fetchall_results=[
            [{"entity_id": 4}, {"entity_id": 2}],
            [{"entity_id": 2}, {"entity_id": 4}],
        ]
    )
    writer = EntityWriter(client)

    deleted_ids = await writer.cleanup_null_entities(project_id="project-1")

    assert deleted_ids == [2, 4]
    assert any(
        call[0] == "execute"
        and "WHERE type IS NULL" in call[1]
        and call[2] == ("project-1",)
        for call in client.calls
    )
    assert any(
        call[0] == "execute"
        and "DELETE FROM entities" in call[1]
        and call[2] == ([4, 2], "project-1", IDENTITY_ENTITY_ID)
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_delete_returns_false_when_scope_does_not_match():
    client = RecordingPostgresClient(fetchall_results=[[]])
    writer = EntityWriter(client)

    assert await writer.delete_entity(2, project_id="wrong-project") is False
    assert not any("MATCH (e:Entity)" in call[1] for call in client.calls)


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_writer_rejects_identity_deletion_without_db_access():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)

    assert (
        await writer.delete_entity(
            IDENTITY_ENTITY_ID,
            project_id=IDENTITY_SCOPE,
        )
        is False
    )
    assert (
        await writer.bulk_delete_entities(
            [IDENTITY_ENTITY_ID],
            project_id=IDENTITY_SCOPE,
        )
        == []
    )
    assert client.calls == []
    assert client.connection_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("message_id", "expected"),
    [
        (7, 7),
        ("msg_123", 123),
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
                "message_id": "msg_9",
            },
            user_name="ignored",
            session_id="ignored",
            message_id="ignored",
        )
    ) == {
        "user_name": "ada",
        "session_id": "session-2",
        "message_id": 9,
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
