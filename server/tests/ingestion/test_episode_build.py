import json

import pytest

from common.schema.episode_output import LLMEpisodeDecision
from core.ingestion.episode_build import EpisodeBuild


def make_build() -> EpisodeBuild:
    return EpisodeBuild.from_window(
        project_id="project-1",
        session_id="session-1",
        messages=[
            {
                "message_id": 7,
                "role": "user",
                "content": "Ada chose episodic memory.",
                "timestamp_ms": 1700000000000,
            }
        ],
        entity_ids_by_message={7: [2]},
        relationship_ids_by_message={7: ["project-1:2:3"]},
        entity_catalog=[
            {
                "entity_id": 2,
                "canonical_name": "Ada",
                "type": "person",
                "aliases": [],
            }
        ],
        relationship_catalog=[
            {
                "relationship_id": "project-1:2:3",
                "entity_a": {"entity_id": 2, "canonical_name": "Ada", "type": "person"},
                "entity_b": {"entity_id": 3, "canonical_name": "Memory", "type": "concept"},
                "relationship_type": "adopted",
                "confidence": 0.9,
                "context": "Ada chose episodic memory.",
                "evidence_message_ids": [7],
            }
        ],
        prior_episodes=[],
    )


@pytest.mark.no_network
def test_episode_build_owns_local_reference_resolution_and_final_episode():
    build = make_build()
    build.prepare_local_references()

    payload = json.loads(build.generation_payload())
    assert payload["messages"][0]["message_id"] == "m1"
    assert payload["entity_catalog"][0]["entity_id"] == "e1"
    assert "project-1:2:3" not in build.generation_payload()

    build.apply_llm_decision(
        LLMEpisodeDecision(
            action="create",
            summary="Ada selected episodic memory for the project.",
            message_influences=[{"message_id": "m1", "influence_weight": 0.9}],
            focus_entities=[{"entity_id": "e1", "prominence_weight": 0.8}],
            central_relationships=[
                {"relationship_id": "r1", "prominence_weight": 0.7}
            ],
        )
    )
    episode = build.create_episode(max_message_count=8, max_age_hours=None)

    assert episode is not None
    assert episode.messages[0].message_id == 7
    assert episode.entities[0].entity_id == 2
    assert episode.relationships[0].relationship_id == "project-1:2:3"


@pytest.mark.no_network
def test_episode_build_rejects_incomplete_source_reference_maps():
    build = make_build()
    build.relationship_ids_by_message = {}

    with pytest.raises(ValueError, match="relationship references"):
        build.prepare_local_references()


@pytest.mark.no_network
def test_episode_build_releases_local_reference_maps_after_persistence():
    build = make_build()
    build.prepare_local_references()
    build.apply_llm_decision(
        LLMEpisodeDecision(
            action="skip",
            skip_reason="Only a short acknowledgement.",
        )
    )
    build.mark_persisted()
    build.release()

    assert build.persisted is True
    assert build.released is True
    assert build.local_message_ids == {}


@pytest.mark.no_network
def test_episode_build_represents_a_skip_without_a_persisted_episode():
    build = make_build()
    build.prepare_local_references()

    build.apply_llm_decision(
        LLMEpisodeDecision(action="skip", skip_reason="No durable development.")
    )

    assert build.create_episode(max_message_count=8, max_age_hours=None) is None
    assert build.outcome_action == "skip"


@pytest.mark.no_network
def test_episode_build_consolidates_against_a_prior_episode_and_releases():
    seed = make_build()
    seed.prepare_local_references()
    seed.apply_llm_decision(
        LLMEpisodeDecision(
            action="create",
            summary="Ada selected episodic memory.",
            message_influences=[{"message_id": "m1", "influence_weight": 0.9}],
        )
    )
    prior_episode = seed.create_episode(max_message_count=8, max_age_hours=None)
    assert prior_episode is not None

    build = make_build()
    build.prior_episodes = [prior_episode]
    build.prepare_local_references()
    build.apply_llm_decision(
        LLMEpisodeDecision(
            action="consolidate",
            target_episode_id="ep1",
            summary="Ada reaffirmed episodic memory.",
            message_influences=[{"message_id": "m1", "influence_weight": 0.9}],
        )
    )

    episode = build.create_episode(max_message_count=8, max_age_hours=None)
    assert episode is not None
    assert episode.episode_id == prior_episode.episode_id
    assert build.outcome_action == "consolidate"

    build.mark_persisted()
    build.release()
    assert build.released is True
