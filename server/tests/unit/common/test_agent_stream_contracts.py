import pytest
from pydantic import ValidationError

from common.schema.agent_stream import validate_public_agent_stream_event

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
                "tool": "search_messages",
                "args": {"query": "roadmap"},
                "thinking": None,
                "call_id": "call-1",
            },
        },
        {
            "event": "tool_end",
            "data": {
                "tool": "search_messages",
                "result": "Found 1 result",
                "call_id": "call-1",
            },
        },
        {
            "event": "tool_error",
            "data": {
                "tool": "search_messages",
                "error": "Timed out",
                "call_id": "call-1",
            },
        },
        {
            "event": "response",
            "data": {
                "content": "The roadmap is ready.",
                "usage": USAGE,
                "sources": [],
                "sources_consulted": [],
            },
        },
        {
            "event": "clarification",
            "data": {"question": "Which roadmap?", "usage": USAGE},
        },
        {"event": "error", "data": {"message": "The agent stopped."}},
    ],
)
def test_public_stream_boundary_accepts_every_declared_event_shape(event):
    assert validate_public_agent_stream_event(event) == event


@pytest.mark.unit
@pytest.mark.no_network
def test_public_stream_boundary_rejects_malformed_or_uncorrelated_events():
    with pytest.raises(ValidationError):
        validate_public_agent_stream_event(
            {
                "event": "tool_error",
                "data": {"tool": "search_messages", "error": "Timed out"},
            }
        )

    with pytest.raises(ValidationError):
        validate_public_agent_stream_event(
            {
                "event": "response",
                "data": {
                    "content": "Answer",
                    "usage": USAGE,
                    "sources": None,
                    "unexpected": True,
                },
            }
        )
