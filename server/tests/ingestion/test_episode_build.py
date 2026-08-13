import json

import pytest

from common.schema.episode.generation import LLMEpisodeDecision
from common.schema.settings import EpisodeSettings, IngestionSettings
from core.ingestion.episode_build import EpisodeBuild
from core.ingestion.episode_policy import EpisodeGenerationPolicy


def make_policy(*, max_message_count: int = 8) -> EpisodeGenerationPolicy:
    return EpisodeGenerationPolicy.capture(
        settings=EpisodeSettings(batch_multiple=1, max_message_count=max_message_count),
        ingestion_settings=IngestionSettings(batch_size=1),
    )


def make_build(*, max_message_count: int = 8) -> EpisodeBuild:
    return EpisodeBuild.from_window(
        project_id="project-1",
        session_id="session-1",
        policy=make_policy(max_message_count=max_message_count),
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
                "entity_b": {
                    "entity_id": 3,
                    "canonical_name": "Memory",
                    "type": "concept",
                },
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
            central_relationships=[{"relationship_id": "r1", "prominence_weight": 0.7}],
        )
    )
    episode = build.create_episode()

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

    assert build.create_episode() is None
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
    prior_episode = seed.create_episode()
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

    episode = build.create_episode()
    assert episode is not None
    assert episode.episode_id == prior_episode.episode_id
    assert build.outcome_action == "consolidate"

    build.mark_persisted()
    build.release()
    assert build.released is True


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("messages", "message_map", "match"),
    [
        ([{"role": "user"}], {7: [2]}, "message_id"),
        (
            [
                {"message_id": 7, "timestamp_ms": 2000},
                {"message_id": 8, "timestamp_ms": 1000},
            ],
            {7: [2], 8: []},
            "timestamp ordering",
        ),
    ],
)
def test_episode_build_rejects_malformed_source_messages(messages, message_map, match):
    build = EpisodeBuild.from_window(
        project_id="project-1",
        session_id="session-1",
        policy=make_policy(),
        messages=messages,
        entity_ids_by_message=message_map,
        relationship_ids_by_message={message_id: [] for message_id in message_map},
        entity_catalog=[],
        relationship_catalog=[],
        prior_episodes=[],
    )

    with pytest.raises(ValueError, match=match):
        build.validate_source_window()


@pytest.mark.no_network
def test_episode_build_rejects_relationship_evidence_outside_source_window():
    build = make_build()
    build.relationship_catalog[0]["evidence_message_ids"] = [999]

    with pytest.raises(ValueError, match="outside the source window"):
        build.prepare_local_references()


@pytest.mark.no_network
def test_episode_build_rejects_prior_episode_from_another_scope():
    seed = make_build()
    seed.prepare_local_references()
    seed.apply_llm_decision(
        LLMEpisodeDecision(
            action="create",
            summary="A prior episode belongs to the source session.",
            message_influences=[{"message_id": "m1", "influence_weight": 0.9}],
        )
    )
    prior_episode = seed.create_episode()
    assert prior_episode is not None
    prior_episode = prior_episode.model_copy(update={"session_id": "other-session"})

    build = make_build()
    build.prior_episodes = [prior_episode]

    with pytest.raises(ValueError, match="project and session scope"):
        build.prepare_local_references()


@pytest.mark.no_network
def test_episode_build_creation_identity_is_deterministic_for_same_window():
    def create_episode():
        build = make_build()
        build.prepare_local_references()
        build.apply_llm_decision(
            LLMEpisodeDecision(
                action="create",
                summary="The same source window gets the same identity.",
                message_influences=[{"message_id": "m1", "influence_weight": 0.9}],
            )
        )
        return build.create_episode()

    first = create_episode()
    second = create_episode()

    assert first is not None
    assert second is not None
    assert first.episode_id == second.episode_id


@pytest.mark.no_network
def test_episode_build_consolidation_limit_creates_new_episode_identity():
    seed = make_build()
    seed.prepare_local_references()
    seed.apply_llm_decision(
        LLMEpisodeDecision(
            action="create",
            summary="The existing episode is intentionally small.",
            message_influences=[{"message_id": "m1", "influence_weight": 0.9}],
        )
    )
    prior_episode = seed.create_episode()
    assert prior_episode is not None

    build = make_build(max_message_count=1)
    build.messages[0]["message_id"] = 8
    build.entity_ids_by_message = {8: [2]}
    build.relationship_ids_by_message = {8: ["project-1:2:3"]}
    build.relationship_catalog[0]["evidence_message_ids"] = [8]
    build.prior_episodes = [prior_episode]
    build.prepare_local_references()
    build.apply_llm_decision(
        LLMEpisodeDecision(
            action="consolidate",
            target_episode_id="ep1",
            summary="The source window exceeds the consolidation limit.",
            message_influences=[{"message_id": "m1", "influence_weight": 0.9}],
        )
    )

    episode = build.create_episode()

    assert episode is not None
    assert build.consolidation_limit_hit is True
    assert episode.generator_metadata["effective_action"] == "create"
    assert episode.episode_id != prior_episode.episode_id


@pytest.mark.no_network
def test_episode_build_rejects_mutation_after_sealing_and_release():
    build = make_build()
    build.prepare_local_references()
    build.apply_llm_decision(
        LLMEpisodeDecision(action="skip", skip_reason="No durable development.")
    )
    build.mark_persisted()

    with pytest.raises(RuntimeError, match="sealed"):
        build.prepare_local_references()

    build.release()
    with pytest.raises(RuntimeError, match="released"):
        build.create_episode()
