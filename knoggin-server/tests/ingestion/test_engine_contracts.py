from common.schema.contracts import BatchResult, MessageConnections
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


def test_batch_result_has_graph_writes_for_relationships_entities_and_aliases():
    assert BatchResult().has_graph_writes() is False
    assert BatchResult(
        relationship_observations=[MessageConnections(message_id=1)]
    ).has_graph_writes()
    assert BatchResult(new_entity_ids={1}).has_graph_writes()
    assert BatchResult(alias_updated_ids={1}).has_graph_writes()
    assert BatchResult(alias_updates={1: ["Alice"]}).has_graph_writes()
