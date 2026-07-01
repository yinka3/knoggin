import json
from datetime import datetime, timezone

import pytest

from common.scoping import IDENTITY_ENTITY_ID, IDENTITY_SCOPE
from knoggin_server.knowledge.db.projection_rebuilder import GraphBuilder
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_projection_rebuilder_requires_project_scope_without_db_access():
    client = RecordingPostgresClient()
    rebuilder = GraphBuilder(client)

    with pytest.raises(ValueError, match="requires project_id scope"):
        await rebuilder.rebuild_project_projection("", user_name="ada")

    with pytest.raises(ValueError, match="requires user_name scope"):
        await rebuilder.rebuild_project_projection(
            "project-1",
            user_name="",
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_projection_rebuilder_replays_canonical_rows_into_age_projection():
    timestamp = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    client = RecordingPostgresClient(
        fetch_one_results=[{"projected_count": "1"}],
        fetch_all_results=[
            [
                {
                    "id": 7,
                    "content": "hello",
                    "role": "user",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "timestamp": 111,
                }
            ],
            [
                {
                    "id": 2,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "canonical_name": "Ada Lovelace",
                    "type": "person",
                    "topic": "People",
                    "confidence": 0.9,
                    "last_updated": 222,
                    "last_mentioned": 111,
                    "last_profiled_msg_id": 7,
                    "aliases": ["Ada"],
                },
                {
                    "id": IDENTITY_ENTITY_ID,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": IDENTITY_SCOPE,
                    "canonical_name": "ada",
                    "type": "person",
                    "topic": "Identity",
                    "confidence": 1.0,
                    "last_updated": 100,
                    "last_mentioned": 100,
                    "last_profiled_msg_id": None,
                    "aliases": [],
                },
            ],
            [
                {
                    "relationship_id": "project-1:1:2",
                    "user_name": "ada",
                    "project_id": "project-1",
                    "entity_a_id": IDENTITY_ENTITY_ID,
                    "entity_b_id": 2,
                    "weight": 3,
                    "confidence": 0.8,
                    "context": "works on",
                    "last_seen_ms": 333,
                    "evidence_refs": [
                        {
                            "user_name": "ada",
                            "session_id": "session-1",
                            "message_id": 7,
                        }
                    ],
                }
            ],
            [
                {
                    "fact_id": "fact-1",
                    "entity_id": 2,
                    "user_name": "ada",
                    "project_id": "project-1",
                    "content": "Ada works on Knoggin.",
                    "valid_at": timestamp,
                    "invalid_at": None,
                    "confidence": 0.77,
                    "source_msg_id": 7,
                    "source_user_name": "ada",
                    "source_session_id": "session-1",
                    "source": "chat",
                }
            ],
            [
                {
                    "project_id": "project-1",
                    "parent_id": 9,
                    "child_id": 2,
                    "created_at_ms": 444,
                }
            ],
        ],
    )
    rebuilder = GraphBuilder(client)

    summary = await rebuilder.rebuild_project_projection(
        "project-1",
        user_name="ada",
    )

    assert summary == {
        "messages": 1,
        "entities": 2,
        "relationships": 1,
        "facts": 1,
        "hierarchy_edges": 1,
    }

    assert "DETACH DELETE n" in client.calls[0][1]
    assert json.loads(client.calls[0][2][0]) == {
        "project_id": "project-1",
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }
    assert "FROM messages" in client.calls[2][1]
    assert client.calls[2][2] == ("project-1", "ada")
    assert "FROM entities e" in client.calls[3][1]
    assert client.calls[3][2] == (
        "project-1",
        IDENTITY_ENTITY_ID,
        "ada",
        IDENTITY_ENTITY_ID,
    )

    message_projection = next(
        call for call in client.calls if "UNWIND $batch AS msg" in call[1]
    )
    assert json.loads(message_projection[2][0])["batch"][0] == {
        "id": 7,
        "content": "hello",
        "role": "user",
        "user_name": "ada",
        "session_id": "session-1",
        "project_id": "project-1",
        "timestamp": 111,
    }

    entity_projection = next(
        call for call in client.calls if "UNWIND $batch AS data" in call[1]
    )
    entity_batch = json.loads(entity_projection[2][0])["batch"]
    assert entity_batch[0]["last_updated"] == 222
    assert entity_batch[0]["last_mentioned"] == 111
    assert entity_batch[0]["last_profiled_msg_id"] == 7

    topic_projection = next(
        call
        for call in client.calls
        if "MERGE (e)-[:BELONGS_TO]->(t)" in call[1]
    )
    assert "OPTIONAL MATCH (e)-[old:BELONGS_TO]->(:Topic)" in topic_projection[1]
    assert "DELETE old" in topic_projection[1]

    relationship_projection = next(
        call
        for call in client.calls
        if "UNWIND $batch AS rel" in call[1]
        and "r.project_id = rel.project_id" in call[1]
        and "r.weight = rel.weight" in call[1]
    )
    assert json.loads(relationship_projection[2][0])["batch"] == [
        {
            "project_id": "project-1",
            "entity_a_id": IDENTITY_ENTITY_ID,
            "entity_b_id": 2,
            "weight": 3,
            "confidence": 0.8,
            "context": "works on",
            "last_seen": 333,
            "message_ids": [
                '{"message_id": 7, "session_id": "session-1", "user_name": "ada"}'
            ],
        }
    ]

    fact_projection = next(
        call for call in client.calls if "UNWIND $batch AS item" in call[1]
    )
    fact_batch = json.loads(fact_projection[2][0])["batch"]
    assert fact_batch == [
        {
            "id": "fact-1",
            "content": "Ada works on Knoggin.",
            "valid_at": "2026-01-02T03:04:05+00:00",
            "invalid_at": None,
            "confidence": 0.77,
            "source_msg_id": 7,
            "source_user_name": "ada",
            "source_session_id": "session-1",
            "source": "chat",
        }
    ]

    hierarchy_projection = next(
        call
        for call in client.calls
        if "UNWIND $batch AS edge" in call[1]
        and "MERGE (child)-[r:PART_OF]->(parent)" in call[1]
    )
    assert json.loads(hierarchy_projection[2][0])["batch"] == [
        {
            "project_id": "project-1",
            "parent_id": 9,
            "child_id": 2,
            "created_at": 444,
        }
    ]
