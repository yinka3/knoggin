from datetime import datetime, timezone

import pytest

from common.schema.public import (
    MessageDeltaEvent,
    PublicStreamContractError,
    PublicStreamState,
    StartRunRequest,
)


def event(kind="message.delta", sequence=0, run_id="run", **kwargs):
    return dict(type=kind, run_id=run_id, sequence=sequence,
                timestamp=datetime.now(timezone.utc), **kwargs)


def test_output_deltas_preserve_exact_text_through_json_round_trip():
    parts = [" hello ", "\n", "    code\n", "🧠 日本語 "]
    rendered = []
    for part in parts:
        value = MessageDeltaEvent(**event(content=part))
        assert value.content == part
        rendered.append(MessageDeltaEvent.model_validate_json(value.model_dump_json()).content)
    assert "".join(rendered) == "".join(parts)


@pytest.mark.parametrize("invalid", ["mixed", "sequence", "after_terminal", "missing_terminal"])
def test_incremental_stream_rejects_invalid_state(invalid):
    state = PublicStreamState()
    state.accept(event(content=" first "))
    with pytest.raises(PublicStreamContractError):
        if invalid == "mixed":
            state.accept(event(sequence=1, run_id="other", content="next"))
        elif invalid == "sequence":
            state.accept(event(content="next"))
        elif invalid == "after_terminal":
            state.accept(event("run.cancelled", sequence=1))
            state.accept(event(sequence=2, content="late"))
        else:
            state.finish()


def test_identifier_normalization_and_blank_query_validation_are_preserved():
    request = StartRunRequest(session_id=" session ", query=" hello ")
    assert request.session_id == "session"
    assert request.query == "hello"
    with pytest.raises(ValueError):
        StartRunRequest(session_id=" ", query="hello")
    with pytest.raises(ValueError):
        StartRunRequest(session_id="session", query=" \n ")


@pytest.mark.parametrize("kind", ["run.completed", "run.failed"])
def test_terminal_payload_cannot_reference_another_run(kind):
    payload = dict(result=dict(run_id="other", content="done")) if kind == "run.completed" else dict(
        error=dict(code="internal_error", message="safe", run_id="other"),
    )
    with pytest.raises(PublicStreamContractError, match="event run"):
        PublicStreamState().accept(event(kind, **payload))
