import pytest

from core.knowledge.db.readers.relationship_observation_reader import (
    RelationshipObservationReader,
)
from core.knowledge.relationship_advisories import AdvisoryThresholds
from tests.fixtures.fakes import RecordingPostgresClient


def observation_row(observation_id, source_entity_id, target_entity_id):
    return {
        "observation_id": observation_id,
        "relationship_id": f"project:{source_entity_id}:{target_entity_id}:deploys to",
        "user_name": "alice",
        "project_id": "project",
        "semantic_window_id": f"window-{(observation_id + 1) // 2}",
        "source_entity_id": source_entity_id,
        "target_entity_id": target_entity_id,
        "source_type": "Project",
        "target_type": "Technology",
        "observed_relationship_label": "deploys to",
        "canonical_relationship_type": None,
        "domain_status": "unrecognized",
        "confidence": 0.8,
        "context": "deployment context",
        "observed_at_ms": observation_id * 100,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_scopes_unknown_observations_and_derives_advisories():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                observation_row(1, 101, 201),
                observation_row(2, 102, 201),
                observation_row(3, 101, 202),
            ]
        ]
    )

    advisories = await RelationshipObservationReader(client).get_advisories(
        user_name="alice",
        project_id="project",
        thresholds=AdvisoryThresholds(min_occurrences=3, min_distinct_entities=2),
    )

    assert len(advisories) == 1
    assert advisories[0].occurrence_count == 3
    assert client.calls[0][0] == "fetch_all"
    assert client.calls[0][2] == ("alice", "project")
    assert "interpretation_source = 'observed'" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_rejects_blank_scope_before_querying():
    client = RecordingPostgresClient()

    with pytest.raises(ValueError, match="user_name"):
        await RelationshipObservationReader(client).get_unrecognized_observations(
            user_name=" ",
            project_id="project",
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_reader_applies_durable_advisory_disposition_to_derived_pattern():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                observation_row(1, 101, 201),
                observation_row(2, 102, 201),
                observation_row(3, 101, 202),
            ],
            [
                {
                    "review_id": "review-1",
                    "user_name": "alice",
                    "scope": "project",
                    "project_id": "project",
                    "kind": "relationship_advisory",
                    "dedupe_key": "deploys to|project|technology",
                    "evidence_refs": [],
                    "evidence_snapshot": {},
                    "reasoning": "Repeated unrecognized wording.",
                    "proposed_plan": {
                        "kind": "relationship_advisory",
                        "pattern_key": "deploys to|project|technology",
                        "observed_label": "deploys to",
                        "proposed_relationship_type": "DEPLOYS_TO",
                        "action": "accept",
                        "note": "Reviewed",
                    },
                    "expected_state": {"revision": 2},
                    "status": "applied",
                    "created_at": None,
                    "resolved_at": None,
                }
            ],
        ]
    )

    advisories = await RelationshipObservationReader(client).get_advisories(
        user_name="alice",
        project_id="project",
        thresholds=AdvisoryThresholds(min_occurrences=3, min_distinct_entities=2),
    )

    assert advisories[0].disposition == "accepted"
    assert advisories[0].proposed_relationship_type == "DEPLOYS_TO"
    assert advisories[0].decision_revision == 2
