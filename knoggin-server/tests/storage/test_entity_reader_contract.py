import json

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
                    "last_mentioned": "123.0",
                    "last_updated": "456.0",
                    "last_profiled_msg_id": 77,
                }
            ]
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
    }
    params = json.loads(client.calls[0][2][0])
    assert params == {
        "entity_id": 2,
        "filter_projects": True,
        "visible_project_ids": ["project-1"],
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }


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
async def test_entity_reader_search_similar_entities_skips_without_embedding(monkeypatch):
    client = RecordingPostgresClient()
    reader = EntityReader(client)

    async def no_embedding(entity_id):
        return []

    monkeypatch.setattr(reader, "get_entity_embedding", no_embedding)

    assert await reader.search_similar_entities(2) == []
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_entity_count_by_type_strips_quoted_age_strings():
    client = RecordingPostgresClient(
        execute_read_results=[[{"type": '"person"', "count": "3"}]]
    )
    reader = EntityReader(client)

    assert await reader.get_entity_count_by_type() == [
        {"type": "person", "count": 3}
    ]


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


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_reader_get_orphan_entities_refuses_missing_project_scope():
    client = RecordingPostgresClient()
    reader = EntityReader(client)

    assert await reader.get_orphan_entities(project_id=None) == []
    assert client.calls == []
