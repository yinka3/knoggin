import pytest

from common.scoping import IDENTITY_ENTITY_ID
from knoggin_server.knowledge.db.readers.entity_reader import EntityReader
from tests.fixtures.fakes import RecordingPostgresClient


class VectorLike:
    def __init__(self, values):
        self.values = values

    def tolist(self):
        return list(self.values)


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_entity_embedding_converts_pgvector_values():
    client = RecordingPostgresClient(
        execute_read_results=[[{"embedding": VectorLike([0.1, 0.2, 0.3])}]]
    )
    reader = EntityReader(client)

    assert await reader.get_entity_embedding(2) == [0.1, 0.2, 0.3]
    assert client.calls == [
        (
            "execute_read",
            "SELECT embedding FROM entity_search WHERE entity_id = %s",
            (2,),
        )
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_search_entities_by_embedding_adds_project_scope():
    client = RecordingPostgresClient(
        execute_read_results=[[{"entity_id": 3, "similarity": 0.91}]]
    )
    reader = EntityReader(client)

    results = await reader.search_entities_by_embedding(
        [0.1, 0.2, 0.3],
        limit=4,
        score_threshold=0.8,
        visible_project_ids=["project-1"],
    )

    assert results == [(3, 0.91)]
    sql, params = client.calls[0][1], client.calls[0][2]
    assert "AND (project_id = ANY(%s) OR entity_id = %s)" in sql
    assert params == (
        [0.1, 0.2, 0.3],
        [0.1, 0.2, 0.3],
        0.8,
        ["project-1"],
        IDENTITY_ENTITY_ID,
        [0.1, 0.2, 0.3],
        4,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_entities_by_ids_empty_list_skips_db():
    client = RecordingPostgresClient()
    reader = EntityReader(client)

    assert await reader.get_entities_by_ids([]) == []
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_entity_by_id_applies_visible_project_scope():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "id": "2",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "canonical_name": "Ada Lovelace",
                    "aliases": ["Ada"],
                    "type": "person",
                    "topic": "Identity",
                    "last_mentioned": 123000,
                    "last_updated": 456000,
                    "last_profiled_msg_id": 77,
                }
            ],
            [{"embedding": VectorLike([0.1, 0.2, 0.3])}],
        ]
    )
    reader = EntityReader(client)

    entity = await reader.get_entity_by_id(2, visible_project_ids=["project-1"])

    assert entity == {
        "id": 2,
        "session_id": "session-1",
        "project_id": "project-1",
        "canonical_name": "Ada Lovelace",
        "aliases": ["Ada"],
        "type": "person",
        "topic": "Identity",
        "last_mentioned": 123.0,
        "last_updated": 456.0,
        "last_profiled_msg_id": 77,
        "embedding": [0.1, 0.2, 0.3],
    }
    sql, params = client.calls[0][1], client.calls[0][2]
    assert "FROM entities e" in sql
    assert "LEFT JOIN entity_aliases a ON a.entity_id = e.entity_id" in sql
    assert "AND (e.project_id = ANY(%s) OR e.entity_id = %s)" in sql
    assert params == (2, ["project-1"], IDENTITY_ENTITY_ID)
    assert client.calls[1] == (
        "execute_read",
        "SELECT embedding FROM entity_search WHERE entity_id = %s",
        (2,),
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_all_entities_for_hydration_converts_vectors():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "id": "2",
                    "canonical_name": "Ada Lovelace",
                    "aliases": ["Ada"],
                    "type": "person",
                    "topic": "Identity",
                    "session_id": "session-1",
                },
                {
                    "id": "3",
                    "canonical_name": "Grace Hopper",
                    "aliases": None,
                    "type": "person",
                    "topic": "Identity",
                    "session_id": "session-1",
                },
            ],
            [{"entity_id": 2, "embedding": VectorLike([0.1, 0.2, 0.3])}],
        ]
    )
    reader = EntityReader(client)

    entities = await reader.get_all_entities_for_hydration()

    assert entities == [
        {
            "id": 2,
            "canonical_name": "Ada Lovelace",
            "aliases": ["Ada"],
            "type": "person",
            "topic": "Identity",
            "session_id": "session-1",
            "embedding": [0.1, 0.2, 0.3],
        },
        {
            "id": 3,
            "canonical_name": "Grace Hopper",
            "aliases": [],
            "type": "person",
            "topic": "Identity",
            "session_id": "session-1",
            "embedding": [],
        },
    ]
    assert "SELECT entity_id, embedding FROM entity_search" == client.calls[1][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_search_similar_entities_skips_without_embedding(
    monkeypatch,
):
    client = RecordingPostgresClient()
    reader = EntityReader(client)

    async def no_embedding(entity_id):
        return []

    monkeypatch.setattr(reader, "get_entity_embedding", no_embedding)

    assert await reader.search_similar_entities(2) == []
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_find_alias_collisions_uses_canonical_alias_tables():
    client = RecordingPostgresClient(
        execute_read_results=[[{"id_a": "2", "id_b": "3"}]]
    )
    reader = EntityReader(client)

    assert await reader.find_alias_collisions() == [(2, 3)]

    sql, params = client.calls[0][1], client.calls[0][2]
    assert "FROM entities" in sql
    assert "FROM entity_aliases" in sql
    assert params is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_entities_by_names_uses_canonical_alias_tables():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "id": "2",
                    "project_id": "project-1",
                    "canonical_name": "Ada Lovelace",
                    "aliases": ['"Ada"'],
                    "type": "person",
                    "facts": ["built notes"],
                }
            ]
        ]
    )
    reader = EntityReader(client)

    entities = await reader.get_entities_by_names(
        ["Ada"],
        visible_project_ids=["project-1"],
    )

    assert entities == [
        {
            "id": 2,
            "project_id": "project-1",
            "canonical_name": "Ada Lovelace",
            "type": "person",
            "aliases": ["Ada"],
            "facts": ["built notes"],
        }
    ]
    sql, params = client.calls[0][1], client.calls[0][2]
    assert "FROM entities e" in sql
    assert "FROM entity_aliases ea" in sql
    assert "AND (e.project_id = ANY(%s) OR e.entity_id = %s)" in sql
    assert params == (
        ["ada"],
        ["ada"],
        ["project-1"],
        IDENTITY_ENTITY_ID,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_entity_count_by_type_uses_canonical_entities():
    client = RecordingPostgresClient(
        execute_read_results=[[{"type": '"person"', "count": "3"}]]
    )
    reader = EntityReader(client)

    assert await reader.get_entity_count_by_type() == [
        {"type": "person", "count": 3}
    ]
    assert "FROM entities" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_entity_count_by_topic_uses_canonical_entities():
    client = RecordingPostgresClient(
        execute_read_results=[[{"topic": '"Identity"', "count": "2"}]]
    )
    reader = EntityReader(client)

    assert await reader.get_entity_count_by_topic() == [
        {"topic": "Identity", "count": 2}
    ]
    assert "FROM entities" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_entity_relationships_hydrates_quoted_values():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "neighbor_id": "3",
                    "neighbor_name": '"Grace Hopper"',
                    "weight": "2",
                    "message_refs": [{"message_id": 7}],
                    "context": '"worked with"',
                    "confidence": "0.75",
                }
            ]
        ]
    )
    reader = EntityReader(client)

    relationships = await reader.get_entity_relationships(2)

    assert relationships == [
        {
            "neighbor_id": 3,
            "neighbor_name": "Grace Hopper",
            "weight": 2.0,
            "message_refs": [{"message_id": 7}],
            "context": "worked with",
            "confidence": 0.75,
        }
    ]
    sql, params = client.calls[0][1], client.calls[0][2]
    assert "FROM relationships r" in sql
    assert "LEFT JOIN relationship_evidence_refs ref" in sql
    assert params == (2, 2, 2)


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_orphan_entities_refuses_missing_project_scope():
    client = RecordingPostgresClient()
    reader = EntityReader(client)

    assert await reader.get_orphan_entities(project_id=None) == []
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_orphan_entities_uses_canonical_sql():
    client = RecordingPostgresClient(
        execute_read_results=[[{"id": 2}, {"id": 7}]]
    )
    reader = EntityReader(client)

    result = await reader.get_orphan_entities(
        protected_id=IDENTITY_ENTITY_ID,
        orphan_cutoff_ms=100,
        stale_junk_cutoff_ms=50,
        project_id="project-1",
    )

    assert result == [2, 7]
    query, params = client.calls[0][1], client.calls[0][2]
    assert "FROM entities e" in query
    assert "FROM facts f" in query
    assert query.count("FROM relationships r") == 3
    assert "build_cypher" not in query
    assert params == (
        IDENTITY_ENTITY_ID,
        "project-1",
        100,
        50,
        IDENTITY_ENTITY_ID,
        IDENTITY_ENTITY_ID,
        IDENTITY_ENTITY_ID,
        IDENTITY_ENTITY_ID,
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
async def test_entity_reader_orphan_rules_use_canonical_dependencies(
    real_postgres_client,
):
    await real_postgres_client.execute_write(
        """
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name,
            type, topic, last_mentioned_ms
        )
        VALUES
            (1, 'ada', '__identity__', 'ada', 'person', 'Identity', 1000),
            (2, 'ada', 'project-1', 'orphan', 'concept', 'General', 10),
            (3, 'ada', 'project-1', 'active fact', 'concept', 'General', 10),
            (4, 'ada', 'project-1', 'connected a', 'concept', 'General', 10),
            (5, 'ada', 'project-1', 'connected b', 'concept', 'General', 10),
            (6, 'ada', 'project-1', 'identity stale', 'concept', 'General', 10),
            (7, 'ada', 'project-1', 'identity recent', 'concept', 'General', 75),
            (8, 'ada', 'project-1', 'orphan recent', 'concept', 'General', 150),
            (9, 'ada', 'project-2', 'other project', 'concept', 'General', 10),
            (10, 'ada', 'project-1', 'invalid fact', 'concept', 'General', 10)
        """
    )
    await real_postgres_client.execute_write(
        """
        INSERT INTO facts (
            fact_id, entity_id, user_name, project_id, content, invalid_at
        )
        VALUES
            ('active-fact', 3, 'ada', 'project-1', 'active', NULL),
            ('invalid-fact', 10, 'ada', 'project-1', 'old', NOW())
        """
    )
    await real_postgres_client.execute_write(
        """
        INSERT INTO relationships (
            relationship_id, user_name, project_id, entity_a_id, entity_b_id
        )
        VALUES
            ('project-1:4:5', 'ada', 'project-1', 4, 5),
            ('project-1:1:6', 'ada', 'project-1', 1, 6),
            ('project-1:1:7', 'ada', 'project-1', 1, 7)
        """
    )
    reader = EntityReader(real_postgres_client)

    orphan_ids = await reader.get_orphan_entities(
        protected_id=IDENTITY_ENTITY_ID,
        orphan_cutoff_ms=100,
        stale_junk_cutoff_ms=50,
        project_id="project-1",
    )

    assert orphan_ids == [2, 6, 10]
