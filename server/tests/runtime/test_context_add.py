import json
from datetime import datetime, timedelta, timezone

import pytest

from common.schema.primitives import Message
from common.schema.source_reference import SourceReferenceCandidate
from core.session.context import Session
from infrastructure.redis_client import RedisKeys
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


@pytest.fixture
def context(monkeypatch):
    resources = FakeResources()
    ctx = Session("ada", ["General"], resources)
    ctx.session_id = "session-1"
    ctx.project_id = "project-1"
    ctx.project = make_project_state("project-1", redis=resources.redis)
    ctx.consumer = FakeConsumer()
    monkeypatch.setattr(
        Session,
        "current_config",
        property(lambda self: FakeConfigValue(conversation_context_turns=100)),
    )
    return ctx, resources


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_fails_fast_when_ingestion_wiring_is_incomplete():
    resources = FakeResources()
    ctx = Session("ada", ["General"], resources)
    ctx.session_id = "session-1"

    with pytest.raises(RuntimeError, match="not fully initialized"):
        await ctx.add(Message(content="hello"))


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_persists_maps_enqueues_and_signals_consumer(context):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    msg = await ctx.add(Message(content="  hello world  ", timestamp=timestamp))

    assert msg.id == 1
    assert ctx.project.scheduler.activity_count == 1
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
            }
        ]
    ]

    conv_key = RedisKeys.conversation("ada", "session-1")
    recent_key = RedisKeys.recent_conversation("ada", "session-1")
    content_key = RedisKeys.message_content("ada", "session-1")
    buffer_key = RedisKeys.buffer("ada", "session-1")
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

    buffer_payload = json.loads(resources.redis.lists[buffer_key][0])
    assert buffer_payload == {
        "id": 1,
        "message": "hello world",
        "timestamp": timestamp.isoformat(),
        "role": "user",
    }
    assert await resources.redis.get(project_heartbeat_key) == "1"
    assert resources.redis.expirations


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_deduplicates_same_message_timestamp_and_session(context):
    ctx, _ = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    first = await ctx.add(Message(content="hello", timestamp=timestamp))
    second = await ctx.add(Message(content="hello", timestamp=timestamp))

    assert first.id == 1
    assert second.id == 1
    assert ctx.consumer.signaled == 1
    assert ctx.project.scheduler.activity_count == 1


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
async def test_context_add_releases_dedup_claim_after_failure(context, monkeypatch):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    original_persist = ctx._persist_user_turn
    attempts = 0

    async def fail_once(msg):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("temporary Redis failure")
        await original_persist(msg)

    monkeypatch.setattr(ctx, "_persist_user_turn", fail_once)

    with pytest.raises(ConnectionError, match="temporary Redis failure"):
        await ctx.add(Message(content="hello", timestamp=timestamp))

    retried = await ctx.add(Message(content="hello", timestamp=timestamp))

    assert retried.id == 2
    assert ctx.consumer.signaled == 1
    assert ctx.project.scheduler.activity_count == 1
    assert len(resources.redis.lists[RedisKeys.buffer("ada", "session-1")]) == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_keeps_durable_pending_claim_and_recovers_enqueue_failure(
    context, monkeypatch
):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    original_rpush = resources.redis.rpush
    attempts = 0

    async def fail_once(key, value):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("temporary queue failure")
        return await original_rpush(key, value)

    monkeypatch.setattr(resources.redis, "rpush", fail_once)

    with pytest.raises(ConnectionError, match="temporary queue failure"):
        await ctx.add(Message(content="hello", timestamp=timestamp))

    dedup_keys = [
        key
        for key in resources.redis.strings
        if key.startswith("msg_dedup:ada:session-1:")
    ]
    assert len(dedup_keys) == 1
    dedup_key = dedup_keys[0]
    assert await resources.redis.get(dedup_key) == "pending:1"

    retried = await ctx.add(Message(content="hello", timestamp=timestamp))

    assert retried.id == 1
    assert await resources.redis.get(dedup_key) == "accepted:1"
    assert len(resources.redis.lists[RedisKeys.buffer("ada", "session-1")]) == 1
    assert ctx.consumer.signaled == 1
    assert [
        batch[0]["id"] for batch in resources.knowledge_store.saved_message_logs
    ] == [
        1,
        1,
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_keeps_claim_after_message_is_queued(context, monkeypatch):
    ctx, resources = context
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)

    async def fail_activity():
        raise ConnectionError("temporary activity failure")

    monkeypatch.setattr(ctx.project, "record_session_activity", fail_activity)

    with pytest.raises(ConnectionError, match="temporary activity failure"):
        await ctx.add(Message(content="hello", timestamp=timestamp))

    retried = await ctx.add(Message(content="hello", timestamp=timestamp))

    assert retried.id == 1
    assert len(resources.redis.lists[RedisKeys.buffer("ada", "session-1")]) == 1


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

    async def save_atomically(message, candidates):
        calls.append((message, candidates))
        return []

    resources.knowledge_store.save_assistant_message_with_source_refs = (
        save_atomically
    )

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
            },
            [candidate],
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

    async def fail_once_then_save(message, candidates):
        calls.append((message, candidates))
        if len(calls) == 1:
            raise ConnectionError("temporary transaction failure")
        return []

    async def skip_retry_delay(_delay):
        return None

    resources.knowledge_store.save_assistant_message_with_source_refs = (
        fail_once_then_save
    )
    monkeypatch.setattr("core.session.context.asyncio.sleep", skip_retry_delay)

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

    async def fail_atomically(_message, _candidates):
        nonlocal attempts
        attempts += 1
        raise ConnectionError("source reference write failed")

    async def skip_retry_delay(_delay):
        return None

    resources.knowledge_store.save_assistant_message_with_source_refs = fail_atomically
    monkeypatch.setattr("core.session.context.asyncio.sleep", skip_retry_delay)

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
        "core.session.context.asyncio.sleep",
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
