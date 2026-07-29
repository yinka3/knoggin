from common.schema.contracts import (
    BatchResult,
    CandidateSuggestion,
    ExtractionTrace,
    RelationshipObservation,
    ResolutionResult,
)
from core.ingestion.dlq_payload import DLQPayload


def test_batch_result_defaults_to_empty_relationship_observations():
    result = BatchResult()

    assert result.relationship_observations == []
    assert not result.has_graph_writes()


def test_batch_result_serializes_relationship_observations():
    result = BatchResult(
        relationship_observations=[
            RelationshipObservation(
                message_id=7,
                entity_a_name="Alice",
                entity_b_name="Bob",
                relationship_type="met",
                confidence=0.9,
                context="Alice met Bob.",
            )
        ]
    )

    raw = DLQPayload.from_batch(result).model_dump(mode="json")
    restored = DLQPayload.model_validate(raw).to_batch()

    assert raw["relationship_observations"][0]["message_id"] == 7
    assert restored.relationship_observations == result.relationship_observations


def test_batch_result_serializes_candidate_suggestions_without_graph_writes():
    suggestion = CandidateSuggestion(
        msg_id=7,
        mention="workspace notes tool",
        mention_type="tool",
        mention_topic="General",
        candidate_id=501,
        candidate_name="Notion",
        base_score=0.82,
        reasons=[
            "candidate_rejected",
            "below_resolution_threshold",
        ],
        created_entity_id=1001,
    )
    result = BatchResult(
        resolution=ResolutionResult(candidate_suggestions=[suggestion])
    )

    raw = DLQPayload.from_batch(result).model_dump(mode="json")
    restored = DLQPayload.model_validate(raw).to_batch()

    assert result.has_graph_writes() is False
    assert raw["resolution"]["candidate_suggestions"][0]["candidate_name"] == "Notion"
    assert restored.candidate_suggestions == [suggestion]


def test_batch_result_serializes_entity_message_provenance_for_replay():
    result = BatchResult(
        resolution=ResolutionResult(entity_msg_map={2: [7, 8], 3: [8]})
    )

    raw = DLQPayload.from_batch(result).model_dump(mode="json")
    restored = DLQPayload.model_validate(raw).to_batch()

    assert raw["resolution"]["entity_msg_map"] == {"2": [7, 8], "3": [8]}
    assert restored.entity_message_map == {2: [7, 8], 3: [8]}
    assert restored.has_graph_writes() is True


def test_batch_result_has_graph_writes_for_relationships_entities_and_aliases():
    assert BatchResult().has_graph_writes() is False
    assert BatchResult(
        relationship_observations=[
            RelationshipObservation(
                message_id=1,
                entity_a_name="Alice",
                entity_b_name="Bob",
                relationship_type="met",
            )
        ]
    ).has_graph_writes()
    assert BatchResult(resolution=ResolutionResult(new_ids={1})).has_graph_writes()
    assert BatchResult(resolution=ResolutionResult(alias_ids={1})).has_graph_writes()
    assert BatchResult(
        resolution=ResolutionResult(alias_updates={1: ["Alice"]})
    ).has_graph_writes()
    assert BatchResult(
        resolution=ResolutionResult(entity_msg_map={1: [7]})
    ).has_graph_writes()
    assert BatchResult(trace=ExtractionTrace(message_ids=[7])).has_graph_writes()
