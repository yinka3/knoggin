import asyncio
from datetime import datetime, timezone

import pytest

from common.exceptions import (
    IdempotencyConflictError,
    RequestInProgressError,
    RequestInterruptedError,
    SessionBusyError,
)
from common.schema.artifacts import ArtifactDraft, MarkdownArtifactBlock
from common.schema.document import create_document_focus
from common.schema.primitives import Message
from common.schema.source.references import SourceReferenceCandidate
from common.utils.core_utils import fetch_conversation_turns
from core.knowledge.db.readers.message_reader import UserAgentExchange
from runtime.session_runtime import SessionRuntime
from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import FakeConfigValue, FakeResources


def _pasted_source_candidate():
    return SourceReferenceCandidate(
        project_id="project-1",
        session_id="session-1",
        source_kind="user_pasted_text",
        source_message_id=7,
        content_hash="a" * 64,
        locator={"kind": "character_span", "start_char": 0, "end_char": 6},
        excerpt="pasted",
        metadata={"pasted_text": True},
        encounter_kind="user_pasted_text",
        agent_run_id="run-1",
        result_position=0,
    )


def _response_event(
    content,
    *,
    sources_consulted=None,
    artifact=None,
    resolved_agent_id=None,
):
    data = {
        "content": content,
        "usage": {
            "prompt_tokens": 3,
            "completion_tokens": 5,
            "total_tokens": 8,
            "approximate": False,
        },
    }
    if sources_consulted:
        data["sources_consulted"] = sources_consulted
    if artifact is not None:
        data["artifact"] = artifact
    if resolved_agent_id is not None:
        data["resolved_agent_id"] = resolved_agent_id
    return {"event": "response", "data": data}


class _FakeTurnOrchestrator:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    async def run_stream(self, **kwargs):
        self.calls.append(kwargs)
        async for event in self.handler(kwargs):
            yield event


async def _collect_turn(ctx, message, orchestrator):
    return [
        event
        async for event in ctx.run_agent_stream(
            message,
            orchestrator=orchestrator,
        )
    ]


def _runtime(resources, *, session_id="session-1", project_id="project-1"):
    runtime = SessionRuntime(
        "ada",
        resources,
        session_id=session_id,
        project_id=project_id,
        project=make_project_state(project_id),
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    runtime.project.project_semantic_processor = type(
        "SemanticProcessor", (), {"name": "project_semantic"}
    )()
    return runtime


@pytest.fixture
def context(monkeypatch):
    resources = FakeResources()
    ctx = _runtime(resources)
    monkeypatch.setattr(
        SessionRuntime,
        "current_config",
        property(lambda self: FakeConfigValue(conversation_context_turns=100)),
    )
    return ctx, resources


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_fails_fast_when_ingestion_wiring_is_incomplete():
    resources = FakeResources()
    ctx = _runtime(resources)
    ctx.project.project_semantic_processor = None

    with pytest.raises(RuntimeError, match="not fully initialized"):
        await ctx.open_agent_run_stream(Message(content="hello"))


@pytest.mark.runtime
@pytest.mark.no_network
async def test_conversation_history_can_exclude_the_current_first_message():
    resources = FakeResources()

    await fetch_conversation_turns(
        resources.postgres,
        "ada",
        "session-1",
        num_turns=10,
        up_to_msg_id=0,
    )

    _, query, params = resources.postgres.calls[-1]
    assert "message_id <= %(up_to_msg_id)s" in query
    assert params["up_to_msg_id"] == 0


@pytest.mark.runtime
@pytest.mark.no_network
async def test_open_run_persists_editable_turn_without_waking_semantic_work(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    await ctx.open_agent_run_stream(
        Message(content="  hello world  ", timestamp=timestamp)
    )

    assert resources.knowledge_store.saved_message_logs == [
        [
            {
                "id": 1,
                "content": "hello world",
                "role": "user",
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
                "timestamp": timestamp.timestamp() * 1000,
                "metadata": {},
                "user_msg_id": 1,
                "acceptance_key": (
                    "content:4922391fe82054bfa5ad28b1e1a03bf0077f12fc578a324f37ca7263209dc0bf"
                ),
                "lifecycle_state": "editable",
                "edit_window_seconds": 600,
            }
        ]
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_open_run_uses_durable_message_acceptance(context):
    ctx, resources = context

    await ctx.open_agent_run_stream(Message(content="durable only"))



@pytest.mark.runtime
@pytest.mark.no_network
async def test_overlapping_run_is_rejected_before_second_message_persists(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    await ctx.open_agent_run_stream(Message(content="hello", timestamp=timestamp))
    with pytest.raises(SessionBusyError):
        await ctx.open_agent_run_stream(Message(content="second", timestamp=timestamp))

    assert len(resources.knowledge_store.saved_message_logs) == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_open_run_retries_after_durable_acceptance_write_failure(context, monkeypatch):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    original_persist = ctx._persist_user_turn
    attempts = 0

    async def fail_once(msg, *, acceptance_key):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("temporary Postgres failure")
        return await original_persist(msg, acceptance_key=acceptance_key)

    monkeypatch.setattr(ctx, "_persist_user_turn", fail_once)

    with pytest.raises(ConnectionError, match="temporary Postgres failure"):
        await ctx.open_agent_run_stream(Message(content="hello", timestamp=timestamp))

    await ctx.open_agent_run_stream(Message(content="hello", timestamp=timestamp))

@pytest.mark.runtime
@pytest.mark.no_network
async def test_open_run_persists_a_durable_acceptance_key(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    await ctx.open_agent_run_stream(Message(content="hello", timestamp=timestamp))

    assert resources.knowledge_store.saved_message_logs[0][0]["acceptance_key"].startswith(
        "content:"
    )
@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_assistant_turn_uses_canonical_message_sequence(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    await ctx.add_assistant_turn("hello from assistant", timestamp, user_msg_id=1)

    assert resources.knowledge_store.saved_message_logs == [
        [
            {
                "id": 1,
                "content": "hello from assistant",
                "role": "assistant",
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
                    "timestamp": timestamp.timestamp() * 1000,
                    "metadata": {},
                    "user_msg_id": 1,
                    "lifecycle_state": "sealed",
                    "sealed_at_ms": int(timestamp.timestamp() * 1000),
                }
        ]
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_exchange_closure_wakes_the_shared_project_owner(
    context,
):
    ctx, _resources = context
    project_wakes = 0

    def signal_semantic_work():
        nonlocal project_wakes
        project_wakes += 1
        return True

    ctx.project.signal_semantic_work = signal_semantic_work
    await ctx.add_assistant_turn(
        "hello from assistant",
        datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc),
        user_msg_id=1,
    )

    assert project_wakes == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_assistant_turn_persists_source_candidates_with_message(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    candidate = _pasted_source_candidate()
    calls = []

    async def save_atomically(message, candidates, *, readable_project_ids, artifact=None):
        del artifact
        calls.append((message, candidates, readable_project_ids))
        return message["id"], [], True

    resources.knowledge_store.finalize_assistant_exchange = save_atomically

    await ctx.add_assistant_turn(
        "hello from assistant",
        timestamp,
        user_msg_id=1,
        source_candidates=[candidate],
    )

    assert calls == [
        (
            {
                "id": 1,
                "content": "hello from assistant",
                "role": "assistant",
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
                    "timestamp": timestamp.timestamp() * 1000,
                    "metadata": {},
                    "user_msg_id": 1,
                    "lifecycle_state": "sealed",
                    "sealed_at_ms": int(timestamp.timestamp() * 1000),
            },
            [candidate],
            ["project-1"],
        )
    ]
    assert resources.knowledge_store.saved_message_logs == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_retries_atomic_source_handoff_with_the_same_candidates(
    context,
    monkeypatch,
):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    candidate = _pasted_source_candidate()
    calls = []

    async def fail_once_then_save(message, candidates, *, readable_project_ids, artifact=None):
        del artifact
        calls.append((message, candidates, list(readable_project_ids)))
        if len(calls) == 1:
            ctx.project.readable_project_ids.append("project-added-after-run")
            raise ConnectionError("temporary transaction failure")
        return message["id"], [], True

    async def skip_retry_delay(_delay):
        return None

    resources.knowledge_store.finalize_assistant_exchange = (
        fail_once_then_save
    )
    monkeypatch.setattr("runtime.session_runtime.asyncio.sleep", skip_retry_delay)

    await ctx.add_assistant_turn(
        "hello from assistant",
        timestamp,
        user_msg_id=1,
        source_candidates=[candidate],
    )

    assert len(calls) == 2
    assert calls[0][0]["id"] == calls[1][0]["id"] == 1
    assert calls[0][1] == calls[1][1] == [candidate]
    assert calls[0][2] == calls[1][2] == ["project-1"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_abandoned_source_handoff_leaves_no_staged_assistant_turn(
    context,
    monkeypatch,
):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    attempts = 0

    async def fail_atomically(_message, _candidates, *, readable_project_ids, artifact=None):
        del readable_project_ids
        del artifact
        nonlocal attempts
        attempts += 1
        raise ConnectionError("source reference write failed")

    async def skip_retry_delay(_delay):
        return None

    resources.knowledge_store.finalize_assistant_exchange = fail_atomically
    monkeypatch.setattr("runtime.session_runtime.asyncio.sleep", skip_retry_delay)

    with pytest.raises(ConnectionError, match="source reference write failed"):
        await ctx.add_assistant_turn(
            "failed assistant response",
            timestamp,
            user_msg_id=1,
            source_candidates=[_pasted_source_candidate()],
        )

    assert resources.knowledge_store.saved_message_logs == []
    assert attempts == 3


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_assistant_turn_failure_removes_staged_message_and_raises(
    context,
    monkeypatch,
):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    attempts = 0

    async def fail_save(_message, _candidates, *, readable_project_ids, artifact=None):
        del readable_project_ids
        del artifact
        nonlocal attempts
        attempts += 1
        raise ConnectionError("Postgres unavailable")

    async def skip_retry_delay(delay):
        return None

    monkeypatch.setattr(resources.knowledge_store, "finalize_assistant_exchange", fail_save)
    monkeypatch.setattr(
        "runtime.session_runtime.asyncio.sleep",
        skip_retry_delay,
    )

    with pytest.raises(ConnectionError, match="Postgres unavailable"):
        await ctx.add_assistant_turn(
            "failed assistant response", timestamp, user_msg_id=1
        )

    assert attempts == 3


@pytest.mark.runtime
@pytest.mark.no_network
async def test_run_agent_stream_persists_the_final_answer_and_sources_before_response(
    context,
):
    ctx, resources = context
    history_calls = []
    persisted = False
    source_handoffs = []

    async def history(limit, up_to_msg_id=None):
        history_calls.append((limit, up_to_msg_id))
        return [{"role": "assistant", "content": "A prior durable answer."}]

    async def persist_assistant(message, candidates, *, readable_project_ids, artifact=None):
        del artifact
        nonlocal persisted
        source_handoffs.append((message, candidates, readable_project_ids))
        resources.knowledge_store.saved_message_logs.append([message])
        persisted = True
        return message["id"], [], True

    async def handler(kwargs):
        candidate = _pasted_source_candidate().model_copy(
            update={"source_message_id": kwargs["user_message_id"]}
        )
        yield {"event": "token", "data": {"content": "Temporary text"}}
        yield _response_event(
            "Durable final answer",
            sources_consulted=[candidate.model_dump(mode="json")],
        )
        yield {
            "event": "error",
            "data": {"message": "must not follow a response"},
        }

    ctx.get_conversation_context = history
    resources.knowledge_store.finalize_assistant_exchange = (
        persist_assistant
    )
    orchestrator = _FakeTurnOrchestrator(handler)
    events = []

    async for event in ctx.run_agent_stream(
        Message(content="What did we decide?"),
        orchestrator=orchestrator,
    ):
        if event["event"] == "response":
            assert persisted is True
        events.append(event)

    assert [event["event"] for event in events] == ["token", "response"]
    assert history_calls == [(100, 0)]
    assert orchestrator.calls[0]["user_query"] == "What did we decide?"
    assert orchestrator.calls[0]["conversation_history"] == [
        {"role": "assistant", "content": "A prior durable answer."}
    ]
    assert orchestrator.calls[0]["user_message_id"] == 1
    assistant_message, candidates, readable_project_ids = source_handoffs[0]
    assert assistant_message["content"] == "Durable final answer"
    assert assistant_message["user_msg_id"] == 1
    assert assistant_message["metadata"] == {
        "usage": {
            "prompt_tokens": 3,
            "completion_tokens": 5,
            "total_tokens": 8,
            "approximate": False,
        }
    }
    assert candidates[0].source_message_id == 1
    assert readable_project_ids == ["project-1"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_run_agent_stream_marks_the_turn_only_after_durable_persistence(context):
    ctx, resources = context
    persisted = False
    completed_agents = []

    async def handler(_kwargs):
        yield _response_event("Durable answer", resolved_agent_id="agent-run-1")

    async def persist_assistant(message, candidates, *, readable_project_ids, artifact=None):
        nonlocal persisted
        del candidates, readable_project_ids, artifact
        resources.knowledge_store.saved_message_logs.append([message])
        persisted = True
        return message["id"], [], True

    orchestrator = _FakeTurnOrchestrator(handler)

    async def mark_turn_completed(agent_id):
        assert persisted is True
        completed_agents.append(agent_id)
        return True

    orchestrator.mark_turn_completed = mark_turn_completed
    resources.knowledge_store.finalize_assistant_exchange = persist_assistant
    events = await _collect_turn(
        ctx,
        Message(content="Save this response"),
        orchestrator,
    )

    assert completed_agents == ["agent-run-1"]
    assert "resolved_agent_id" not in events[-1]["data"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_turn_completion_bookkeeping_failure_does_not_discard_the_answer(context):
    ctx, _resources = context

    async def handler(_kwargs):
        yield _response_event("Durable answer", resolved_agent_id="agent-run-1")

    orchestrator = _FakeTurnOrchestrator(handler)

    async def fail_turn_completion(_agent_id):
        raise ConnectionError("statistics unavailable")

    orchestrator.mark_turn_completed = fail_turn_completion

    events = await _collect_turn(
        ctx,
        Message(content="Save this response"),
        orchestrator,
    )

    assert events[-1]["event"] == "response"
    assert events[-1]["data"]["content"] == "Durable answer"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_failed_assistant_persistence_does_not_mark_a_successful_turn(
    context,
    monkeypatch,
):
    ctx, resources = context
    completed_agents = []

    async def handler(_kwargs):
        yield _response_event("Answer that cannot be saved")

    async def fail_persistence(*_args, **_kwargs):
        raise ConnectionError("Postgres unavailable")

    async def skip_retry_delay(_delay):
        return None

    orchestrator = _FakeTurnOrchestrator(handler)

    async def mark_turn_completed(agent_id):
        completed_agents.append(agent_id)
        return True

    orchestrator.mark_turn_completed = mark_turn_completed
    resources.knowledge_store.finalize_assistant_exchange = fail_persistence
    monkeypatch.setattr("runtime.session_runtime.asyncio.sleep", skip_retry_delay)
    events = await _collect_turn(
        ctx,
        Message(content="Save this response"),
        orchestrator,
    )

    assert events[-1]["event"] == "error"
    assert completed_agents == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_run_agent_stream_persists_clarification_before_exposing_it(context):
    """A user-visible clarification is a durable assistant turn, not cleanup."""

    ctx, resources = context
    persisted = False

    async def handler(_kwargs):
        yield {
            "event": "clarification",
            "data": {
                "question": "Which profile should I use?",
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 0,
                    "total_tokens": 3,
                    "approximate": False,
                },
            },
        }
        yield _response_event("A response must not follow clarification")

    async def persist_assistant(
        message, candidates, *, readable_project_ids, artifact=None, outcome
    ):
        nonlocal persisted
        assert candidates == []
        assert readable_project_ids == ["project-1"]
        assert artifact is None
        assert outcome == "clarification"
        resources.knowledge_store.saved_message_logs.append([message])
        persisted = True
        return message["id"], [], True

    resources.knowledge_store.finalize_assistant_exchange = persist_assistant
    events = []
    async for event in ctx.run_agent_stream(
        Message(content="Help me choose a profile"),
        orchestrator=_FakeTurnOrchestrator(handler),
    ):
        assert persisted is True
        events.append(event)

    assert events == [
        {
            "event": "clarification",
            "data": {
                "question": "Which profile should I use?",
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 0,
                    "total_tokens": 3,
                    "approximate": False,
                },
                "assistant_message_id": 2,
                "source_ref_ids": [],
            },
        }
    ]
    assert [batch[0]["role"] for batch in resources.knowledge_store.saved_message_logs] == [
        "user",
        "assistant",
    ]
    assert resources.knowledge_store.closed_exchanges == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_run_agent_stream_finalizes_sources_with_the_admitted_read_scope(context):
    ctx, resources = context
    captured_scopes = []

    async def response_after_scope_changes(_kwargs):
        ctx.project.readable_project_ids.append("project-added-during-run")
        yield _response_event(
            "Durable final answer",
            sources_consulted=[_pasted_source_candidate().model_dump(mode="json")],
        )

    async def persist_assistant(
        _message, _candidates, *, readable_project_ids, artifact=None
    ):
        del artifact
        captured_scopes.append(list(readable_project_ids))
        return 2, [], True

    resources.knowledge_store.finalize_assistant_exchange = persist_assistant
    events = await _collect_turn(
        ctx,
        Message(content="Use the source."),
        _FakeTurnOrchestrator(response_after_scope_changes),
    )

    assert events[-1]["event"] == "response"
    assert captured_scopes == [["project-1"]]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_run_agent_stream_persists_artifact_with_assistant_completion(context):
    ctx, resources = context
    artifact = ArtifactDraft(
        kind="general",
        title="Reusable note",
        blocks=(MarkdownArtifactBlock(content="Keep this."),),
    )
    handoffs = []

    async def persist_assistant(
        message, candidates, *, readable_project_ids, artifact=None
    ):
        handoffs.append(
            (message, candidates, readable_project_ids, artifact)
        )
        resources.knowledge_store.saved_message_logs.append([message])
        return message["id"], [], True

    async def handler(_kwargs):
        yield _response_event(
            "Answer",
            artifact=artifact.model_dump(mode="json"),
        )

    resources.knowledge_store.finalize_assistant_exchange = (
        persist_assistant
    )
    events = await _collect_turn(
        ctx,
        Message(content="Save this"),
        _FakeTurnOrchestrator(handler),
    )

    assert [event["event"] for event in events] == ["response"]
    assert handoffs[0][1:] == ([], ["project-1"], artifact)
    assert events[0]["data"]["artifact"]["title"] == "Reusable note"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_run_agent_stream_forwards_selected_research_mode(context):
    ctx, _ = context

    async def handler(_kwargs):
        yield _response_event("Answer")

    orchestrator = _FakeTurnOrchestrator(handler)
    events = [
        event
        async for event in ctx.run_agent_stream(
            Message(content="Investigate this"),
            orchestrator=orchestrator,
            research_mode="deep_research",
        )
    ]

    assert [event["event"] for event in events] == ["response"]
    assert orchestrator.calls[0]["research_mode"] == "deep_research"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_run_agent_stream_rejects_an_overlapping_turn_before_persistence(context):
    ctx, resources = context
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    async def handler(kwargs):
        if kwargs["user_query"] == "first":
            first_started.set()
            await release_first.wait()
        yield _response_event(f"answer to {kwargs['user_query']}")

    orchestrator = _FakeTurnOrchestrator(handler)
    first = asyncio.create_task(
        _collect_turn(ctx, Message(content="first"), orchestrator)
    )
    await first_started.wait()
    with pytest.raises(SessionBusyError):
        await ctx.open_agent_run_stream(
            Message(content="second"), orchestrator=orchestrator
        )

    assert [call["user_query"] for call in orchestrator.calls] == ["first"]
    assert [batch[0]["role"] for batch in resources.knowledge_store.saved_message_logs] == [
        "user"
    ]

    release_first.set()
    first_events = await first

    assert [call["user_query"] for call in orchestrator.calls] == ["first"]
    assert first_events[-1]["data"]["content"] == "answer to first"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_duplicate_idempotency_key_replays_the_canonical_response(context):
    ctx, resources = context
    orchestrator_calls = []

    async def handler(kwargs):
        orchestrator_calls.append(kwargs["user_query"])
        yield _response_event("fresh response that must not be replayed")

    async def replay_exchange(_message_id, **_kwargs):
        return UserAgentExchange(
            user_message_id=1,
            exchange_state="closed",
            exchange_outcome="assistant_final",
            assistant_message_id=71,
            assistant_content="canonical durable response",
            assistant_metadata={
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 2,
                    "total_tokens": 5,
                    "approximate": False,
                },
                "research_mode": "normal",
            },
            source_ref_ids=("source-1",),
        )

    resources.knowledge_store.get_user_agent_exchange = replay_exchange
    first = [
        event
        async for event in ctx.run_agent_stream(
            Message(content="Please summarize this"),
            orchestrator=_FakeTurnOrchestrator(handler),
            idempotency_key="summary-1",
        )
    ]
    second = [
        event
        async for event in ctx.run_agent_stream(
            Message(content="Please summarize this"),
            orchestrator=_FakeTurnOrchestrator(handler),
            idempotency_key="summary-1",
        )
    ]

    assert first[-1]["event"] == "response"
    assert orchestrator_calls == ["Please summarize this"]
    assert second == [
        {
            "event": "response",
            "data": {
                "content": "canonical durable response",
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 2,
                    "total_tokens": 5,
                    "approximate": False,
                },
                "assistant_message_id": 71,
                "source_ref_ids": ["source-1"],
                "research_mode": "normal",
            },
        }
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_conflicting_supplied_and_embedded_idempotency_keys_fail_before_acceptance(
    context,
):
    ctx, resources = context
    message = Message(
        content="Summarize",
        metadata={"idempotency_key": "embedded-key"},
    )

    with pytest.raises(IdempotencyConflictError):
        await ctx.open_agent_run_stream(message, idempotency_key="supplied-key")

    assert resources.knowledge_store.saved_message_logs == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_duplicate_acceptance_requires_a_reloadable_exchange(context):
    ctx, resources = context
    resources.knowledge_store.accepted_message_ids["request:missing-exchange"] = 91

    async def missing_exchange(_message_id, **_kwargs):
        return None

    resources.knowledge_store.get_user_agent_exchange = missing_exchange

    with pytest.raises(RuntimeError, match="could not be reloaded"):
        await ctx.open_agent_run_stream(
            Message(content="Summarize"),
            idempotency_key="missing-exchange",
        )


@pytest.mark.runtime
@pytest.mark.no_network
def test_focus_resolution_timestamp_is_not_part_of_request_identity():
    first = create_document_focus(
        mode="request", behavior="restrict", created_at="2026-01-01T00:00:00Z",
        target_type="document", document_id="doc-1", relative_path="notes.pdf",
    )
    second = create_document_focus(
        mode="request", behavior="restrict", created_at="2026-01-02T00:00:00Z",
        target_type="document", document_id="doc-1", relative_path="renamed.pdf",
    )
    inputs = dict(
        message=Message(content="Summarize"), user_timezone=None, model=None,
        agent_id=None, enabled_tools=None, pasted_text_spans=None,
        research_mode="normal",
    )

    assert SessionRuntime._request_fingerprint(document_focus=first, **inputs) == (
        SessionRuntime._request_fingerprint(document_focus=second, **inputs)
    )
    changed_behavior = first.model_copy(update={"behavior": "prefer"})
    changed_target = first.model_copy(update={"document_id": "doc-2"})
    baseline = SessionRuntime._request_fingerprint(document_focus=first, **inputs)
    assert SessionRuntime._request_fingerprint(
        document_focus=changed_behavior, **inputs
    ) != baseline
    assert SessionRuntime._request_fingerprint(
        document_focus=changed_target, **inputs
    ) != baseline


@pytest.mark.runtime
@pytest.mark.no_network
async def test_failed_request_replay_preserves_safe_terminal_error(context):
    ctx, resources = context
    resources.knowledge_store.accepted_message_ids["request:budget-1"] = 81

    async def replay_exchange(_message_id, **_kwargs):
        return UserAgentExchange(
            user_message_id=81,
            exchange_state="closed",
            exchange_outcome="failed",
            assistant_message_id=None,
            assistant_content=None,
            assistant_metadata={},
            source_ref_ids=(),
            terminal_error={"code": "llm_budget_exhausted", "retryable": False},
        )

    resources.knowledge_store.get_user_agent_exchange = replay_exchange
    events = [
        event
        async for event in ctx.run_agent_stream(
            Message(content="Use the model"), idempotency_key="budget-1"
        )
    ]

    assert events[0]["data"]["code"] == "llm_budget_exhausted"
    assert events[0]["data"]["retryable"] is False


@pytest.mark.runtime
@pytest.mark.no_network
async def test_terminal_error_is_closed_with_safe_replay_fields(context):
    ctx, resources = context

    async def handler(_kwargs):
        yield {
            "event": "error",
            "data": {
                "message": "internal wording is not persisted",
                "code": "llm_budget_exhausted",
                "retryable": False,
            },
        }

    events = [
        event
        async for event in ctx.run_agent_stream(
            Message(content="Use the model"),
            orchestrator=_FakeTurnOrchestrator(handler),
        )
    ]

    assert events[0]["event"] == "error"
    assert resources.knowledge_store.closed_exchanges[-1]["terminal_error"] == {
        "code": "llm_budget_exhausted",
        "retryable": False,
    }


@pytest.mark.runtime
@pytest.mark.no_network
async def test_duplicate_idempotency_key_replays_the_canonical_clarification(context):
    ctx, resources = context
    resources.knowledge_store.accepted_message_ids["request:clarification-1"] = 41

    async def replay_exchange(_message_id, **_kwargs):
        return UserAgentExchange(
            user_message_id=41,
            exchange_state="closed",
            exchange_outcome="clarification",
            assistant_message_id=42,
            assistant_content="Which profile should I use?",
            assistant_metadata={
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 0,
                    "total_tokens": 3,
                    "approximate": False,
                },
                "fallback": True,
            },
            source_ref_ids=("source-1",),
        )

    resources.knowledge_store.get_user_agent_exchange = replay_exchange

    events = [
        event
        async for event in ctx.run_agent_stream(
            Message(content="Help me choose a profile"),
            idempotency_key="clarification-1",
        )
    ]

    assert events == [
        {
            "event": "clarification",
            "data": {
                "question": "Which profile should I use?",
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 0,
                    "total_tokens": 3,
                    "approximate": False,
                },
                "assistant_message_id": 42,
                "source_ref_ids": ["source-1"],
                "fallback": True,
            },
        }
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_same_active_idempotency_key_is_in_progress_and_mismatch_conflicts(context):
    ctx, _resources = context
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(_kwargs):
        started.set()
        await release.wait()
        yield _response_event("done")

    orchestrator = _FakeTurnOrchestrator(handler)
    async def run_first():
        return [
            event
            async for event in ctx.run_agent_stream(
                Message(content="original request"),
                orchestrator=orchestrator,
                idempotency_key="same-request",
            )
        ]

    first = asyncio.create_task(run_first())
    await started.wait()

    with pytest.raises(RequestInProgressError):
        await ctx.open_agent_run_stream(
            Message(content="original request"),
            orchestrator=orchestrator,
            idempotency_key="same-request",
        )
    with pytest.raises(IdempotencyConflictError):
        await ctx.open_agent_run_stream(
            Message(content="changed request"),
            orchestrator=orchestrator,
            idempotency_key="same-request",
        )

    release.set()
    await first


@pytest.mark.runtime
@pytest.mark.no_network
async def test_interrupted_idempotent_submission_never_starts_another_agent_run(context):
    ctx, resources = context
    resources.knowledge_store.accepted_message_ids["request:interrupted-1"] = 71

    async def interrupted_exchange(_message_id, **_kwargs):
        return UserAgentExchange(
            user_message_id=71,
            exchange_state="open",
            exchange_outcome=None,
            assistant_message_id=None,
            assistant_content=None,
            assistant_metadata={},
            source_ref_ids=(),
        )

    async def handler(_kwargs):
        raise AssertionError("an interrupted submission must not be rerun")
        yield

    resources.knowledge_store.get_user_agent_exchange = interrupted_exchange

    with pytest.raises(RequestInterruptedError):
        await ctx.open_agent_run_stream(
            Message(content="Continue the interrupted request"),
            orchestrator=_FakeTurnOrchestrator(handler),
            idempotency_key="interrupted-1",
        )


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize("value", [7, "   ", "x" * 201])
def test_idempotency_key_validation_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="idempotency_key"):
        SessionRuntime._normalize_idempotency_key(value)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_idempotent_acceptance_requires_a_request_fingerprint(context):
    ctx, _resources = context

    with pytest.raises(ValueError, match="requires a request fingerprint"):
        await ctx._accept_user_message(
            Message(content="Summarize", metadata={"idempotency_key": "request-1"})
        )


@pytest.mark.runtime
@pytest.mark.no_network
def test_clarification_metadata_retains_only_usage_and_fallback():
    assert SessionRuntime._assistant_clarification_metadata(
        {"usage": {"total_tokens": 3}, "fallback": True, "private": "discard"}
    ) == {"usage": {"total_tokens": 3}, "fallback": True}


@pytest.mark.runtime
@pytest.mark.no_network
def test_unknown_terminal_error_is_reduced_to_safe_retryable_failure():
    assert SessionRuntime._terminal_error_record({"code": "private_provider_error"}) == {
        "code": "run_failed",
        "retryable": True,
    }


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("outcome", "expected_message"),
    [
        ("cancelled", "original request was cancelled"),
        ("failed", "original request failed"),
        ("user_only", "completed without an agent response"),
    ],
)
async def test_terminal_exchange_replay_explains_non_response_outcomes(
    context, outcome, expected_message
):
    ctx, _resources = context
    exchange = UserAgentExchange(
        user_message_id=1,
        exchange_state="closed",
        exchange_outcome=outcome,
        assistant_message_id=None,
        assistant_content=None,
        assistant_metadata={},
        source_ref_ids=(),
    )

    events = [event async for event in ctx._replay_terminal_exchange(exchange)]

    assert events[0]["event"] == "error"
    assert expected_message in events[0]["data"]["message"]


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize("outcome", ["assistant_final", "clarification", "unknown"])
async def test_terminal_exchange_replay_rejects_incomplete_or_unknown_outcomes(
    context, outcome
):
    ctx, _resources = context
    exchange = UserAgentExchange(
        user_message_id=1,
        exchange_state="closed",
        exchange_outcome=outcome,
        assistant_message_id=None,
        assistant_content=None,
        assistant_metadata={},
        source_ref_ids=(),
    )

    with pytest.raises(RuntimeError):
        _ = [event async for event in ctx._replay_terminal_exchange(exchange)]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_cancel_active_agent_run_keeps_only_the_durable_user_turn(context):
    ctx, resources = context
    started = asyncio.Event()
    never_finish = asyncio.Event()

    async def handler(_kwargs):
        started.set()
        await never_finish.wait()
        yield _response_event("This must never be persisted")

    task = asyncio.create_task(
        _collect_turn(
            ctx,
            Message(content="cancel this run"),
            _FakeTurnOrchestrator(handler),
        )
    )
    await started.wait()

    assert await ctx.cancel_active_agent_run() is True
    with pytest.raises(asyncio.CancelledError):
        await task

    assert [
        batch[0]["role"] for batch in resources.knowledge_store.saved_message_logs
    ] == ["user"]
@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_shutdown_cancels_active_run_and_rejects_new_run(context):
    ctx, resources = context
    first_started = asyncio.Event()
    never_finish = asyncio.Event()

    async def handler(kwargs):
        if kwargs["user_query"] == "first":
            first_started.set()
            await never_finish.wait()
        yield _response_event(f"answer to {kwargs['user_query']}")

    orchestrator = _FakeTurnOrchestrator(handler)
    first = asyncio.create_task(
        _collect_turn(ctx, Message(content="first"), orchestrator)
    )
    await first_started.wait()
    with pytest.raises(SessionBusyError):
        await ctx.open_agent_run_stream(
            Message(content="second"), orchestrator=orchestrator
        )

    await ctx.shutdown()

    with pytest.raises(asyncio.CancelledError):
        await first
    assert [call["user_query"] for call in orchestrator.calls] == ["first"]
    assert [batch[0]["role"] for batch in resources.knowledge_store.saved_message_logs] == ["user"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_cancelling_one_session_run_does_not_cancel_another(context):
    ctx, _ = context
    other_resources = FakeResources()
    other = _runtime(other_resources, session_id="session-2", project_id="project-2")

    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release_second = asyncio.Event()

    async def handler(kwargs):
        if kwargs["context"].session_id == "session-1":
            first_started.set()
            await asyncio.Event().wait()
        second_started.set()
        await release_second.wait()
        yield _response_event("second session answer")

    orchestrator = _FakeTurnOrchestrator(handler)
    first = asyncio.create_task(
        _collect_turn(ctx, Message(content="first"), orchestrator)
    )
    second = asyncio.create_task(
        _collect_turn(other, Message(content="second"), orchestrator)
    )
    await first_started.wait()
    await second_started.wait()

    assert await ctx.cancel_active_agent_run() is True
    with pytest.raises(asyncio.CancelledError):
        await first

    release_second.set()
    second_events = await second

    assert second_events[-1]["data"]["content"] == "second session answer"
