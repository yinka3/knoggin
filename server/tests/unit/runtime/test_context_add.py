import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from common.schema.primitives import Message
from common.schema.source.references import SourceReferenceCandidate
from common.utils.core_utils import fetch_conversation_turns
from infrastructure.redis_client import RedisKeys
from runtime.session_runtime import SessionRuntime
from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import FakeConfigValue, FakeConsumer, FakeResources


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


def _response_event(content, *, sources_consulted=None):
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


@pytest.fixture
def context(monkeypatch):
    resources = FakeResources()
    ctx = SessionRuntime("ada", resources)
    ctx.session_id = "session-1"
    ctx.project_id = "project-1"
    ctx.project = make_project_state("project-1", redis=resources.redis)
    ctx.consumer = FakeConsumer()
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
    ctx = SessionRuntime("ada", resources)
    ctx.session_id = "session-1"

    with pytest.raises(RuntimeError, match="not fully initialized"):
        await ctx.add(Message(content="hello"))


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
async def test_context_add_persists_editable_turn_maps_and_signals_consumer(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    msg = await ctx.add(Message(content="  hello world  ", timestamp=timestamp))

    assert msg.id == 1
    assert ctx.consumer.signaled == 1
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
                "episode_eligible": False,
                "edit_window_seconds": 600,
            }
        ]
    ]

    conv_key = RedisKeys.conversation("ada", "session-1")
    recent_key = RedisKeys.recent_conversation("ada", "session-1")
    content_key = RedisKeys.message_content("ada", "session-1")
    project_heartbeat_key = RedisKeys.project_heartbeat_counter("ada", "project-1")

    assert json.loads(resources.redis.hashes[conv_key]["1"])["content"] == "hello world"
    assert resources.redis.zsets[recent_key]["1"] == timestamp.timestamp()

    content_payload = json.loads(resources.redis.hashes[content_key]["msg_1"])
    assert content_payload == {
        "id": 1,
        "message": "hello world",
        "content": "hello world",
        "timestamp": timestamp.isoformat(),
        "role": "user",
    }

    assert await resources.redis.get(project_heartbeat_key) == "1"
    assert resources.redis.expirations


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_deduplicates_same_message_timestamp_and_session(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    first = await ctx.add(Message(content="hello", timestamp=timestamp))
    second = await ctx.add(Message(content="hello", timestamp=timestamp))

    assert first.id == 1
    assert second.id == 1
    assert ctx.consumer.signaled == 2
    assert len(resources.knowledge_store.saved_message_logs) == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_normalizes_naive_message_timestamp_to_utc(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4)

    msg = await ctx.add(Message(content="hello", timestamp=timestamp))

    conv_key = RedisKeys.conversation("ada", "session-1")
    recent_key = RedisKeys.recent_conversation("ada", "session-1")
    payload = json.loads(resources.redis.hashes[conv_key][str(msg.id)])
    expected = timestamp.replace(tzinfo=timezone.utc)

    assert payload["timestamp"] == expected.isoformat()
    assert resources.redis.zsets[recent_key][str(msg.id)] == expected.timestamp()


@pytest.mark.runtime
@pytest.mark.no_network
async def test_conversation_message_uses_one_pipeline_and_normalizes_offset(context):
    ctx, resources = context
    timestamp = datetime(
        2026,
        1,
        2,
        3,
        4,
        tzinfo=timezone(timedelta(hours=-5)),
    )
    initial_pipeline_calls = resources.redis.pipeline_calls

    await ctx._record_conversation_message(
        message_id=42,
        role="assistant",
        content="hello",
        timestamp=timestamp,
    )

    conv_key = RedisKeys.conversation("ada", "session-1")
    recent_key = RedisKeys.recent_conversation("ada", "session-1")
    payload = json.loads(resources.redis.hashes[conv_key]["42"])
    expected = timestamp.astimezone(timezone.utc)

    assert resources.redis.pipeline_calls == initial_pipeline_calls + 1
    assert payload["message_id"] == 42
    assert payload["timestamp"] == expected.isoformat()
    assert resources.redis.zsets[recent_key]["42"] == expected.timestamp()


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_retries_after_durable_acceptance_write_failure(context, monkeypatch):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    original_persist = ctx._persist_user_turn
    attempts = 0

    async def fail_once(msg, *, acceptance_key):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("temporary Redis failure")
        return await original_persist(msg, acceptance_key=acceptance_key)

    monkeypatch.setattr(ctx, "_persist_user_turn", fail_once)

    with pytest.raises(ConnectionError, match="temporary Redis failure"):
        await ctx.add(Message(content="hello", timestamp=timestamp))

    retried = await ctx.add(Message(content="hello", timestamp=timestamp))

    assert retried.id == 2
    assert ctx.consumer.signaled == 1
@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_reuses_durable_acceptance_after_signal_failure(
    context, monkeypatch
):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    original_incr = resources.redis.incr
    attempts = 0

    async def fail_once(key):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("temporary signal failure")
        return await original_incr(key)

    monkeypatch.setattr(resources.redis, "incr", fail_once)

    with pytest.raises(ConnectionError, match="temporary signal failure"):
        await ctx.add(Message(content="hello", timestamp=timestamp))

    retried = await ctx.add(Message(content="hello", timestamp=timestamp))

    assert retried.id == 1
    assert ctx.consumer.signaled == 1
    assert [
        batch[0]["id"] for batch in resources.knowledge_store.saved_message_logs
    ] == [
        1,
    ]
@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_persists_a_durable_acceptance_key(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    accepted = await ctx.add(Message(content="hello", timestamp=timestamp))

    assert accepted.id == 1
    assert resources.knowledge_store.saved_message_logs[0][0]["acceptance_key"].startswith(
        "content:"
    )
@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_assistant_turn_uses_canonical_message_sequence(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    await ctx.add_assistant_turn("hello from assistant", timestamp)

    conv_key = RedisKeys.conversation("ada", "session-1")
    recent_key = RedisKeys.recent_conversation("ada", "session-1")
    content_key = RedisKeys.message_content("ada", "session-1")
    assert json.loads(resources.redis.hashes[conv_key]["1"])["message_id"] == 1
    assert resources.redis.zsets[recent_key]["1"] == timestamp.timestamp()
    assert json.loads(resources.redis.hashes[content_key]["msg_1"])["id"] == 1
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
                    "user_msg_id": None,
                    "lifecycle_state": "sealed",
                    "sealed_at_ms": int(timestamp.timestamp() * 1000),
                    "ingestion_state": "excluded",
                    "episode_eligible": False,
                }
        ]
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_assistant_turn_persists_source_candidates_with_message(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    candidate = _pasted_source_candidate()
    calls = []

    async def save_atomically(message, candidates, *, readable_project_ids):
        calls.append((message, candidates, readable_project_ids))
        return []

    resources.knowledge_store.save_assistant_message_with_source_refs = save_atomically

    await ctx.add_assistant_turn(
        "hello from assistant",
        timestamp,
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
                    "user_msg_id": None,
                    "lifecycle_state": "sealed",
                    "sealed_at_ms": int(timestamp.timestamp() * 1000),
                    "ingestion_state": "excluded",
                    "episode_eligible": False,
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

    async def fail_once_then_save(message, candidates, *, readable_project_ids):
        del readable_project_ids
        calls.append((message, candidates))
        if len(calls) == 1:
            raise ConnectionError("temporary transaction failure")
        return []

    async def skip_retry_delay(_delay):
        return None

    resources.knowledge_store.save_assistant_message_with_source_refs = (
        fail_once_then_save
    )
    monkeypatch.setattr("runtime.session_runtime.asyncio.sleep", skip_retry_delay)

    await ctx.add_assistant_turn(
        "hello from assistant",
        timestamp,
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

    async def fail_atomically(_message, _candidates, *, readable_project_ids):
        del readable_project_ids
        nonlocal attempts
        attempts += 1
        raise ConnectionError("source reference write failed")

    async def skip_retry_delay(_delay):
        return None

    resources.knowledge_store.save_assistant_message_with_source_refs = fail_atomically
    monkeypatch.setattr("runtime.session_runtime.asyncio.sleep", skip_retry_delay)

    with pytest.raises(ConnectionError, match="source reference write failed"):
        await ctx.add_assistant_turn(
            "failed assistant response",
            timestamp,
            source_candidates=[_pasted_source_candidate()],
        )

    conv_key = RedisKeys.conversation("ada", "session-1")
    assert resources.redis.hashes[conv_key] == {}
    assert resources.knowledge_store.saved_message_logs == []
    assert attempts == 3


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_assistant_turn_failure_rolls_back_redis_and_raises(
    context,
    monkeypatch,
):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    attempts = 0

    async def fail_save(messages):
        nonlocal attempts
        attempts += 1
        raise ConnectionError("Postgres unavailable")

    async def skip_retry_delay(delay):
        return None

    monkeypatch.setattr(resources.knowledge_store, "save_message_logs", fail_save)
    monkeypatch.setattr(
        "runtime.session_runtime.asyncio.sleep",
        skip_retry_delay,
    )

    with pytest.raises(ConnectionError, match="Postgres unavailable"):
        await ctx.add_assistant_turn("failed assistant response", timestamp)

    conv_key = RedisKeys.conversation("ada", "session-1")
    recent_key = RedisKeys.recent_conversation("ada", "session-1")
    content_key = RedisKeys.message_content("ada", "session-1")
    assert resources.redis.hashes[conv_key] == {}
    assert resources.redis.zsets[recent_key] == {}
    assert resources.redis.hashes[content_key] == {}
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

    async def persist_assistant(message, candidates, *, readable_project_ids):
        nonlocal persisted
        source_handoffs.append((message, candidates, readable_project_ids))
        resources.knowledge_store.saved_message_logs.append([message])
        persisted = True
        return candidates

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
    resources.knowledge_store.save_assistant_message_with_source_refs = (
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
    assert ctx.agent_run_snapshot() == {
        "state": "completed",
        "active": False,
        "queued_message_ids": [],
        "queue_paused": False,
    }


@pytest.mark.runtime
@pytest.mark.no_network
async def test_run_agent_stream_serializes_accepted_turns_in_fifo_order(context):
    ctx, _ = context
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    active_runs = 0
    max_active_runs = 0

    async def handler(kwargs):
        nonlocal active_runs, max_active_runs
        active_runs += 1
        max_active_runs = max(max_active_runs, active_runs)
        try:
            if kwargs["user_query"] == "first":
                first_started.set()
                await release_first.wait()
            yield _response_event(f"answer to {kwargs['user_query']}")
        finally:
            active_runs -= 1

    orchestrator = _FakeTurnOrchestrator(handler)
    first = asyncio.create_task(
        _collect_turn(ctx, Message(content="first"), orchestrator)
    )
    await first_started.wait()
    second = asyncio.create_task(
        _collect_turn(ctx, Message(content="second"), orchestrator)
    )

    for _ in range(5):
        await asyncio.sleep(0)

    assert [call["user_query"] for call in orchestrator.calls] == ["first"]
    assert ctx.agent_run_snapshot()["queued_message_ids"] == [1, 2]

    release_first.set()
    first_events, second_events = await asyncio.gather(first, second)

    assert [call["user_query"] for call in orchestrator.calls] == ["first", "second"]
    assert max_active_runs == 1
    assert first_events[-1]["data"]["content"] == "answer to first"
    assert second_events[-1]["data"]["content"] == "answer to second"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_failed_run_pauses_the_next_accepted_turn_until_resumed(context):
    ctx, _ = context
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def handler(kwargs):
        if kwargs["user_query"] == "first":
            first_started.set()
            await release_first.wait()
            yield {"event": "error", "data": {"message": "agent failed"}}
            return
        yield _response_event("answer to second")

    orchestrator = _FakeTurnOrchestrator(handler)
    first = asyncio.create_task(
        _collect_turn(ctx, Message(content="first"), orchestrator)
    )
    await first_started.wait()
    second = asyncio.create_task(
        _collect_turn(ctx, Message(content="second"), orchestrator)
    )

    for _ in range(5):
        await asyncio.sleep(0)
    release_first.set()
    assert (await first) == [{"event": "error", "data": {"message": "agent failed"}}]

    for _ in range(5):
        await asyncio.sleep(0)
    assert [call["user_query"] for call in orchestrator.calls] == ["first"]
    assert ctx.agent_run_snapshot() == {
        "state": "failed",
        "active": False,
        "queued_message_ids": [2],
        "queue_paused": True,
    }

    assert await ctx.resume_agent_queue() is True
    second_events = await second

    assert [call["user_query"] for call in orchestrator.calls] == ["first", "second"]
    assert second_events[-1]["data"]["content"] == "answer to second"


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
    assert ctx.agent_run_snapshot() == {
        "state": "cancelled",
        "active": False,
        "queued_message_ids": [],
        "queue_paused": True,
    }


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_shutdown_cancels_active_run_and_prevents_queued_run(context):
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
    second = asyncio.create_task(
        _collect_turn(ctx, Message(content="second"), orchestrator)
    )

    for _ in range(5):
        await asyncio.sleep(0)
    assert ctx.agent_run_snapshot()["queued_message_ids"] == [1, 2]

    await ctx.shutdown()

    with pytest.raises(asyncio.CancelledError):
        await first
    second_events = await second

    assert [call["user_query"] for call in orchestrator.calls] == ["first"]
    assert second_events == [
        {
            "event": "error",
            "data": {
                "message": "The response could not be completed or saved. Please try again."
            },
        }
    ]
    assert [batch[0]["role"] for batch in resources.knowledge_store.saved_message_logs] == [
        "user",
        "user",
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_cancelling_one_session_run_does_not_cancel_another(context):
    ctx, _ = context
    other_resources = FakeResources()
    other = SessionRuntime("ada", other_resources)
    other.session_id = "session-2"
    other.project_id = "project-2"
    other.project = make_project_state("project-2", redis=other_resources.redis)
    other.consumer = FakeConsumer()

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
    assert other.agent_run_snapshot()["state"] == "completed"
