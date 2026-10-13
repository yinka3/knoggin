import math

import pytest

from common.schema.agent.tool_contracts import (
    TOOL_SCHEMAS_BY_NAME,
    validate_tool_arguments,
)


def test_entity_merge_state_hash_must_match_declared_pattern():
    schema = TOOL_SCHEMAS_BY_NAME["propose_entity_merge"]
    arguments = {
        "primary_id": 2,
        "duplicate_id": 3,
        "reasoning": "The cited evidence identifies one entity.",
        "evidence_message_ids": [7],
        "expected_state_hash": "a" * 64,
    }

    assert validate_tool_arguments(schema, arguments) == []
    for invalid_hash in ("a" * 63, "A" * 64, "a" * 64 + "extra"):
        errors = validate_tool_arguments(
            schema, {**arguments, "expected_state_hash": invalid_hash}
        )
        assert "arguments.expected_state_hash has an invalid format" in errors


def test_unknown_model_argument_name_is_not_copied_into_validation_error():
    private_key = "private stored source passage used as a field name"
    schema = TOOL_SCHEMAS_BY_NAME["load_project_topic_context"]

    errors = validate_tool_arguments(
        schema, {"topics": ["Work"], private_key: "ignored"}
    )

    assert errors == ["arguments contains 1 unknown field(s)"]
    assert private_key not in str(errors)


@pytest.mark.parametrize("confidence", [math.nan, math.inf, -math.inf])
def test_relationship_conflict_rejects_nonfinite_confidence(confidence):
    schema = TOOL_SCHEMAS_BY_NAME["report_relationship_conflict"]
    arguments = {
        "evidence_observation_ids": [101, 104],
        "kind": "possible_contradiction",
        "reasoning": "Two observations disagree.",
        "confidence": confidence,
    }

    assert validate_tool_arguments(schema, {**arguments, "confidence": 0.8}) == []
    assert validate_tool_arguments(schema, arguments) == [
        "arguments.confidence must be a finite number"
    ]
