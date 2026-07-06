from common.schema.contracts import (
    BatchResult,
    CandidateSuggestion,
    MessageConnections,
)
from common.schema.primitives import ConnectionRecord


def test_batch_result_defaults_to_empty_relationship_observations():
    result = BatchResult()

    assert result.relationship_observations == []
    assert not result.has_graph_writes()


def test_batch_result_serializes_relationship_observations():
    result = BatchResult(
        relationship_observations=[
            MessageConnections(
                message_id=7,
                entity_pairs=[
                    ConnectionRecord(
                        msg_id=7,
                        entity_a="Alice",
                        entity_b="Bob",
                        relationship="met",
                        confidence=0.9,
                        context="Alice met Bob.",
                    )
                ],
            )
        ]
    )

    raw = result.to_dict()
    restored = BatchResult.from_dict(raw)

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
    result = BatchResult(candidate_suggestions=[suggestion])

    raw = result.to_dict()
    restored = BatchResult.from_dict(raw)

    assert result.has_graph_writes() is False
    assert raw["candidate_suggestions"][0]["candidate_name"] == "Notion"
    assert restored.candidate_suggestions == [suggestion]


def test_batch_result_has_graph_writes_for_relationships_entities_and_aliases():
    assert BatchResult().has_graph_writes() is False
    assert BatchResult(
        relationship_observations=[MessageConnections(message_id=1)]
    ).has_graph_writes()
    assert BatchResult(new_entity_ids={1}).has_graph_writes()
    assert BatchResult(alias_updated_ids={1}).has_graph_writes()
    assert BatchResult(alias_updates={1: ["Alice"]}).has_graph_writes()
