import pytest
from pydantic import ValidationError

from common.schema.agent.stream import validate_agent_execution_event

USAGE = {
    "prompt_tokens": 1,
    "completion_tokens": 2,
    "total_tokens": 3,
    "approximate": False,
}


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    "event",
    [
        {"event": "token", "data": {"content": "Hello"}},
        {"event": "thinking", "data": {"content": "I should search."}},
        {
            "event": "tool_start",
            "data": {
                "tool": "search_knowledge_messages",
                "args": {"query": "roadmap"},
                "thinking": None,
                "call_id": "call-1",
            },
        },
        {
            "event": "tool_end",
            "data": {
                "tool": "search_knowledge_messages",
                "result": "Found 1 result",
                "call_id": "call-1",
            },
        },
        {
            "event": "tool_error",
            "data": {
                "tool": "search_knowledge_messages",
                "error": "Timed out",
                "call_id": "call-1",
            },
        },
        {
            "event": "response",
            "data": {
                "content": "The roadmap is ready.",
                "usage": USAGE,
                "sources_consulted": [],
                "artifact": {
                    "schema_version": 1,
                    "kind": "general",
                    "title": "Roadmap",
                    "blocks": [{"kind": "markdown", "content": "Ready."}],
                    "status": "complete",
                },
            },
        },
        {
            "event": "clarification",
            "data": {"question": "Which roadmap?", "usage": USAGE},
        },
        {"event": "error", "data": {"message": "The agent stopped."}},
    ],
)
def test_engine_stream_boundary_accepts_every_declared_event_shape(event):
    assert validate_agent_execution_event(event) == event


@pytest.mark.unit
@pytest.mark.no_network
def test_engine_stream_boundary_rejects_malformed_or_uncorrelated_events():
    with pytest.raises(ValidationError):
        validate_agent_execution_event(
            {
                "event": "tool_error",
                "data": {"tool": "search_knowledge_messages", "error": "Timed out"},
            }
        )

    with pytest.raises(ValidationError):
        validate_agent_execution_event(
            {
                "event": "response",
                "data": {
                    "content": "Answer",
                    "usage": USAGE,
                    "unexpected": True,
                },
            }
        )


@pytest.mark.no_network
def test_submit_answer_schema_describes_the_artifact_blocks_the_engine_accepts():
    from common.schema.agent.tool_contracts import (
        TOOL_SCHEMAS_BY_NAME,
        validate_tool_arguments,
    )
    from common.schema.artifacts import ArtifactDraft

    artifact = {
        "kind": "research_brief", "title": "SQLite vs Postgres", "status": "complete",
        "blocks": [
            {"kind": "markdown", "content": "SQLite suits local storage."},
            {"kind": "callout", "tone": "warning", "title": "Note", "content": "Check writes."},
            {"kind": "checklist", "items": [{"label": "Read both pages", "completed": True}]},
            {"kind": "table", "columns": ["Engine", "Best for"], "rows": [["SQLite", "Local"]]},
        ],
    }
    schema = TOOL_SCHEMAS_BY_NAME["submit_answer"]

    assert validate_tool_arguments(schema, {"content": "Answer", "artifact": artifact}) == []
    ArtifactDraft.model_validate(artifact)
    # The shape models guessed before the blocks were described.
    guessed = {**artifact, "blocks": [{"type": "markdown", "body": "SQLite suits local storage."}]}
    assert validate_tool_arguments(schema, {"content": "Answer", "artifact": guessed})
