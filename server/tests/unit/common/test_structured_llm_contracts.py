import pytest
from pydantic import ValidationError

from common.schema.episode.generation import LLMEpisodeDecision
from common.schema.ingestion.extraction import (
    EpisodeEntityExtraction,
    EpisodeRelationshipExtraction,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_entity_output_drops_bad_mentions_and_keeps_good_ones():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        EpisodeEntityExtraction.model_validate({"mentions": [], "unexpected": True})

    output = EpisodeEntityExtraction.model_validate({
        "mentions": [
            {"messages": ["M1"], "name": "  ", "type": "Person"},
            {"messages": ["m1"], "name": "Ada", "type": "Person"},
            {"messages": [], "name": "Ada", "type": "Person"},
            {"messages": ["M2"], "name": " Ada ", "type": "Person"},
        ]
    })

    assert [(m.messages, m.name) for m in output.mentions] == [(["M2"], "Ada")]


@pytest.mark.unit
@pytest.mark.no_network
def test_relationship_output_strips_text_and_requires_local_handles():
    output = EpisodeRelationshipExtraction.model_validate({
        "connections": [
            {
                "messages": ["M1"], "entity_a": " e1 ", "entity_b": "e999",
                "relationship": " owns ", "context": " Ada owns it. ",
            },
            *(
                {"messages": ["M1"], "entity_a": endpoint, "entity_b": "e2",
                 "relationship": "owns"}
                for endpoint in ("Alice", "10", "e0", "e01", "e-1", "e")
            ),
            {"messages": ["b1"], "entity_a": "e1", "entity_b": "e2", "relationship": "owns"},
            {"messages": ["M1"], "entity_a": "e1", "entity_b": "e2", "relationship": "owns",
             "context": " "},
        ]
    })

    assert len(output.connections) == 1
    connection = output.connections[0]
    assert (connection.entity_a, connection.entity_b) == ("e1", "e999")
    assert connection.relationship == "owns"
    assert connection.context == "Ada owns it."


@pytest.mark.unit
@pytest.mark.no_network
def test_episode_llm_output_rejects_schema_drift_and_blank_narrative_values():
    valid = {
        "summary": "A durable decision was made.",
        "message_influences": ["message:1"],
    }
    assert LLMEpisodeDecision.model_validate(valid).summary == valid["summary"]

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        LLMEpisodeDecision.model_validate({**valid, "action": "consolidate"})

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        LLMEpisodeDecision.model_validate({**valid, "unexpected": "value"})

    with pytest.raises(ValidationError, match="summary must not be blank"):
        LLMEpisodeDecision.model_validate({**valid, "summary": "  "})
