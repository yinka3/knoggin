import pytest
from pydantic import ValidationError

from common.schema.episode.generation import LLMEpisodeDecision
from common.schema.ingestion.extraction import (
    EntityExtraction,
    RelationshipExtraction,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_ner_output_rejects_unknown_and_blank_entity_fields():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        EntityExtraction.model_validate(
            {
                "mentions": [],
                "unexpected": True,
            }
        )

    with pytest.raises(ValidationError, match="name must not be blank"):
        EntityExtraction.model_validate(
            {
                "mentions": [
                    {
                        "name": "  ",
                        "type": "person",
                        "topic": "Identity",
                        "msg_id": "m1",
                    }
                ]
            }
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_connection_output_strips_required_text_and_rejects_blank_evidence():
    output = RelationshipExtraction.model_validate(
        {
            "connections": [
                {
                    "entity_a": " Ada ",
                    "entity_b": " Knoggin ",
                    "relationship": " uses ",
                    "context": " Ada uses Knoggin. ",
                    "msg_id": "m1",
                }
            ]
        }
    )

    assert output.connections[0].entity_a == "Ada"
    assert output.connections[0].context == "Ada uses Knoggin."

    with pytest.raises(ValidationError, match="context must not be blank"):
        RelationshipExtraction.model_validate(
            {
                "connections": [
                    {
                        "entity_a": "Ada",
                        "entity_b": "Knoggin",
                        "relationship": "uses",
                        "context": " ",
                        "msg_id": "m1",
                    }
                ]
            }
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_episode_llm_output_rejects_schema_drift_and_blank_narrative_values():
    valid = {
        "action": "create",
        "summary": "A durable decision was made.",
        "message_influences": ["message:1"],
    }
    assert LLMEpisodeDecision.model_validate(valid).summary == valid["summary"]

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        LLMEpisodeDecision.model_validate({**valid, "unexpected": "value"})

    with pytest.raises(ValidationError, match="summary must not be blank"):
        LLMEpisodeDecision.model_validate({**valid, "summary": "  "})
