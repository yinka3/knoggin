from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from common.exceptions import DependencyError, ToolExecutionError
from common.schema.public import (
    CreateProjectRequest,
    CreateSessionRequest,
    MessageAcceptance,
    PublicError,
    RunCompletedEvent,
    RunResult,
    StartRunRequest,
    SubmitMessageRequest,
    UpdateAgentRequest,
    Usage,
    to_public_error,
    validate_public_stream,
    validate_public_stream_event,
)
from common.schema.source.locators import PastedTextLocator
from common.schema.source.references import SourceConsulted


@pytest.fixture
def source() -> SourceConsulted:
    return SourceConsulted(
        source_kind="user_pasted_text",
        display_label="User message",
        locator=PastedTextLocator(start_char=0, end_char=8),
        excerpt="planning",
        source_message_id=12,
        source_status="available",
        contributing_message_id=13,
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_first_vertical_slice_dtos_are_separate_and_strict(source):
    project = CreateProjectRequest(name="  Research  ")
    assert project.name == "Research"

    session = CreateSessionRequest(
        project_id="project-1",
        enabled_tools=[" Search_Messages "],
    )
    assert session.enabled_tools == ["search_messages"]

    # None inherits defaults and [] deliberately disables all optional tools.
    assert CreateSessionRequest(project_id="project-1").enabled_tools is None
    assert CreateSessionRequest(project_id="project-1", enabled_tools=[]).enabled_tools == []
    assert StartRunRequest(session_id="session-1", query="hello", enabled_tools=[])

    accepted = MessageAcceptance(message_id=12, idempotent=True)
    result = RunResult(
        run_id="run-1",
        content="Done",
        sources=(source,),
        usage=Usage(
            prompt_tokens=1,
            completion_tokens=2,
            total_tokens=3,
        ),
    )
    assert accepted.model_dump()["message_id"] == 12
    assert result.model_dump(mode="json")["sources"][0]["source_kind"] == (
        "user_pasted_text"
    )

    with pytest.raises(ValidationError):
        SubmitMessageRequest(content="hello", unexpected=True)


@pytest.mark.unit
@pytest.mark.no_network
def test_enabled_tools_reject_blank_and_duplicate_names():
    with pytest.raises(ValidationError, match="non-blank"):
        CreateSessionRequest(project_id="project-1", enabled_tools=[" "])
    with pytest.raises(ValidationError, match="duplicate"):
        CreateSessionRequest(
            project_id="project-1",
            enabled_tools=["search_messages", " SEARCH_MESSAGES "],
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_agent_update_distinguishes_omitted_null_empty_and_allowlist():
    assert UpdateAgentRequest().enabled_tools_mode == "omitted"
    assert UpdateAgentRequest(enabled_tools=None).enabled_tools_mode == "inherit"
    assert UpdateAgentRequest(enabled_tools=[]).enabled_tools_mode == "disable_all"
    assert (
        UpdateAgentRequest(enabled_tools=["search_messages"]).enabled_tools_mode
        == "allowlist"
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_public_errors_use_safe_stable_projection_and_drop_internal_details():
    error = to_public_error(
        ToolExecutionError(
            "search_messages",
            "postgres://secret-host/password leaked",
            details={"raw": "model output"},
        ),
        request_id="request-1",
        run_id="run-1",
    )
    assert error == PublicError(
        code="tool_failed",
        message="A tool could not complete the request.",
        retryable=True,
        request_id="request-1",
        run_id="run-1",
    )
    assert "secret-host" not in error.model_dump_json()
    assert to_public_error(ValueError("bad input")).code == "invalid_request"
    assert to_public_error(DependencyError("redis password" )).retryable is True


@pytest.mark.unit
@pytest.mark.no_network
def test_public_stream_is_versioned_ordered_and_does_not_expose_tool_payloads(source):
    now = datetime.now(timezone.utc)
    events = [
        {
            "type": "run.started",
            "run_id": "run-1",
            "sequence": 0,
            "timestamp": now,
        },
        {
            "type": "message.delta",
            "run_id": "run-1",
            "sequence": 1,
            "timestamp": now,
            "content": "Done",
        },
        {
            "type": "run.completed",
            "run_id": "run-1",
            "sequence": 2,
            "timestamp": now,
            "result": {
                "run_id": "run-1",
                "content": "Done",
                "sources": [source.model_dump(mode="json")],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 2,
                    "total_tokens": 3,
                    "approximate": False,
                },
            },
        },
    ]
    parsed = validate_public_stream(events, require_terminal=True)
    assert [event.type for event in parsed] == [
        "run.started",
        "message.delta",
        "run.completed",
    ]
    assert parsed[0].version == "1"

    with pytest.raises(ValidationError):
        validate_public_stream_event(
            {
                "type": "tool.started",
                "run_id": "run-1",
                "sequence": 1,
                "timestamp": now,
                "tool_name": "search_messages",
                "arguments": {"query": "secret"},
            }
        )
    with pytest.raises(ValueError, match="monotonically"):
        validate_public_stream([events[0], events[0]])


@pytest.mark.unit
@pytest.mark.no_network
def test_terminal_stream_event_has_a_stable_shape(source):
    event = RunCompletedEvent(
        run_id="run-1",
        sequence=0,
        timestamp=datetime.now(timezone.utc),
        result=RunResult(run_id="run-1", content="done", sources=(source,)),
    )
    assert event.model_dump(mode="json")["version"] == "1"
