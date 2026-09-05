import asyncio
from datetime import datetime, timezone

import pytest

from common.exceptions import SessionBusyError
from common.schema.artifacts import ArtifactDraft, MarkdownArtifactBlock
from common.schema.primitives import Message
from common.schema.source.references import SourceReferenceCandidate
from common.utils.core_utils import fetch_conversation_turns
from runtime.session_runtime import SessionRuntime
from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import FakeConfigValue, FakeIngestionWorker, FakeResources


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


def _response_event(content, *, sources_consulted=None, artifact=None):
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
    return SessionRuntime(
        "ada",
        resources,
        session_id=session_id,
        project_id=project_id,
        project=make_project_state(project_id),
        model=None,
        agent_id=None,
        enabled_tools=None,
    )


@pytest.fixture
def context(monkeypatch):
    resources = FakeResources()
    ctx = _runtime(resources)
    ctx.ingestion_worker = FakeIngestionWorker()
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
async def test_open_run_persists_editable_turn_and_signals_worker(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    await ctx.open_agent_run_stream(
        Message(content="  hello world  ", timestamp=timestamp)
    )

    assert ctx.ingestion_worker.signaled == 1
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
                "ingestion_state": "waiting_for_seal",
                "edit_window_seconds": 600,
            }
        ]
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_open_run_uses_durable_message_acceptance(context):
    ctx, resources = context

    await ctx.open_agent_run_stream(Message(content="durable only"))

    assert ctx.ingestion_worker.signaled == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_overlapping_run_is_rejected_before_second_message_persists(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    await ctx.open_agent_run_stream(Message(content="hello", timestamp=timestamp))
    with pytest.raises(SessionBusyError):
        await ctx.open_agent_run_stream(Message(content="second", timestamp=timestamp))

    assert ctx.ingestion_worker.signaled == 1
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

    assert ctx.ingestion_worker.signaled == 1
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
                    "ingestion_state": "excluded",
                }
        ]
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_exchange_closure_wakes_the_shared_project_owner_and_legacy_worker(
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
    assert ctx.ingestion_worker.signaled == 1


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
                    "ingestion_state": "excluded",
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
        del readable_project_ids
        del artifact
        calls.append((message, candidates))
        if len(calls) == 1:
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
    other.ingestion_worker = FakeIngestionWorker()

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
