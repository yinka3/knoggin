"""Current-read and historical-evidence boundaries for observations."""

import pytest

from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.evidence_traversal_reader import (
    EvidenceTraversalReader,
)
from core.knowledge.db.readers.graph_reader import GraphReader
from core.knowledge.db.readers.knowledge_query_reader import KnowledgeQueryReader
from core.knowledge.evidence_service import EvidenceService

_PROJECT_ONE_RELATIONSHIP = "project-1:2:3:works_at"
_PROJECT_TWO_RELATIONSHIP = "project-2:2:4:works_at"
_RETIRED_BLOCK_ID = "55555555-5555-4555-8555-555555555555"
_NOW_MS = 1_700_000_100_000


async def _seed_observation_matrix(client) -> None:
    await client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1');

        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms
        ) VALUES
            ('ada', 'session-1', 101, 'project-1', 'user',
             'Ade works at Acme.', 1700000000000),
            ('ada', 'session-1', 102, 'project-1', 'user',
             'Ade no longer works at Acme.', 1700000001000);

        INSERT INTO entities (entity_id, user_name, canonical_name)
        VALUES
            (2, 'ada', 'Ade'),
            (3, 'ada', 'Acme'),
            (4, 'ada', 'Other Co');

        INSERT INTO project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'Identity', 'Identity'),
            ('project-1', 3, 'ada', 'Concept', 'General'),
            ('project-2', 2, 'ada', 'Identity', 'Identity'),
            ('project-2', 4, 'ada', 'Concept', 'General');

        INSERT INTO relationships (
            relationship_id, user_name, project_id,
            entity_a_id, entity_b_id, relationship_type
        ) VALUES
            ('project-1:2:3:works_at', 'ada', 'project-1', 2, 3, 'works_at'),
            ('project-2:2:4:works_at', 'ada', 'project-2', 2, 4, 'works_at');

        INSERT INTO project_semantic_windows (
            window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, source_token_count, token_estimator,
            token_estimator_version, completed_at
        ) VALUES
            ('11111111-1111-4111-8111-111111111111', 'ada', 'project-1',
             'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now()),
            ('22222222-2222-4222-8222-222222222222', 'ada', 'project-1',
             'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now()),
            ('33333333-3333-4333-8333-333333333333', 'ada', 'project-2',
             'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now());

        INSERT INTO project_context_blocks (
            block_id, project_id, section_key, markdown, content_hash,
            assertion_kind, source_time_ms
        ) VALUES
            ('44444444-4444-4444-8444-444444444444', 'project-1',
             'current_state', 'Ade works at Acme.', repeat('a', 64),
             'source_grounded', 200),
            ('55555555-5555-4555-8555-555555555555', 'project-1',
             'current_state', 'Ade no longer works at Acme.', repeat('b', 64),
             'source_grounded', 900);

        INSERT INTO project_context_block_supports (
            block_id, project_id, message_id, session_id, support_kind
        ) VALUES
            ('44444444-4444-4444-8444-444444444444', 'project-1', 101,
             'session-1', 'user_message'),
            ('55555555-5555-4555-8555-555555555555', 'project-1', 102,
             'session-1', 'user_message');

        INSERT INTO message_entity_refs (message_id, entity_id)
        VALUES (101, 2), (102, 2);

        INSERT INTO relationship_observations (
            observation_id, relationship_id, project_id, user_name,
            semantic_window_id, source_entity_id, target_entity_id,
            observed_relationship_label, context, observed_at_ms,
            retired_at, retired_reason
        ) VALUES
            (1, 'project-1:2:3:works_at', 'project-1', 'ada',
             '11111111-1111-4111-8111-111111111111', 2, 3, 'works at',
             'Ade works at Acme.', 200, NULL, NULL),
            (2, 'project-1:2:3:works_at', 'project-1', 'ada',
             '22222222-2222-4222-8222-222222222222', 2, 3, 'works at',
             'Ade no longer works at Acme.', 900, now(), 'test_replaced'),
            (3, 'project-2:2:4:works_at', 'project-2', 'ada',
             '33333333-3333-4333-8333-333333333333', 2, 4, 'works at',
             'Ade works at Other Co.', 300, NULL, NULL);

        INSERT INTO relationship_observation_blocks (
            observation_id, project_id, block_id
        ) VALUES
            (1, 'project-1', '44444444-4444-4444-8444-444444444444'),
            (2, 'project-1', '55555555-5555-4555-8555-555555555555');
        """
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_current_readers_exclude_retired_observations_and_history_retains_them(
    real_postgres_client,
    monkeypatch,
):
    await _seed_observation_matrix(real_postgres_client)
    monkeypatch.setattr(
        "core.knowledge.db.readers.knowledge_query_reader.get_now_ms",
        lambda: _NOW_MS,
    )

    related = await EntityReader(real_postgres_client).get_related_entities(
        [2],
        visible_project_ids=["project-1"],
    )

    assert len(related) == 1
    relationship = related[0]
    assert relationship["project_id"] == "project-1"
    assert relationship["relationship_id"] == _PROJECT_ONE_RELATIONSHIP
    assert relationship["connection_strength"] == 1.0
    assert relationship["observation_count"] == 1
    assert relationship["evidence_message_count"] == 1
    assert relationship["first_observed"] == 200
    assert relationship["last_observed"] == 200
    assert relationship["evidence_refs"] == [
        {
            "project_id": "project-1",
            "user_name": "ada",
            "session_id": "session-1",
            "message_id": 101,
        }
    ]
    assert relationship["observation_refs"] == [
        {
            "observation_id": 1,
            "observed_relationship_label": "works at",
            "relationship_type": "works_at",
            "observed_at_ms": 200,
            "context": "Ade works at Acme.",
        }
    ]

    path_refs = await GraphReader(real_postgres_client)._relationship_observation_refs(
        [_PROJECT_ONE_RELATIONSHIP, _PROJECT_TWO_RELATIONSHIP],
        ["project-1"],
    )
    assert path_refs[0] == [
        {
            "kind": "relationship_observation",
            "project_id": "project-1",
            "user_name": "ada",
            "observation_id": 1,
        }
    ]
    assert path_refs[1] == []

    activity = await KnowledgeQueryReader(real_postgres_client).get_recent_activity(
        2,
        visible_project_ids=["project-1"],
        hours=1,
    )
    activity_by_message = {
        item["evidence_refs"][0]["message_id"]: item for item in activity
    }
    assert set(activity_by_message) == {101, 102}
    assert activity_by_message[101]["observation_refs"] == [
        {
            "relationship_id": _PROJECT_ONE_RELATIONSHIP,
            "project_id": "project-1",
            "observation_id": 1,
            "observed_at_ms": 200,
        }
    ]
    assert activity_by_message[102]["observation_refs"] == []

    historical = await EvidenceService(
        EvidenceTraversalReader(real_postgres_client)
    ).for_relationship_observation(
        2,
        user_name="ada",
        project_id="project-1",
    )
    observation = next(
        node
        for node in historical.nodes
        if node.pointer.kind == "relationship_observation"
    )
    assert observation.pointer.identifier == "2"
    assert observation.status == "retired"
    assert {
        (node.pointer.kind, node.pointer.identifier) for node in historical.nodes
    } == {
        ("relationship_observation", "2"),
        ("context_block", _RETIRED_BLOCK_ID),
        ("message", "102"),
    }
