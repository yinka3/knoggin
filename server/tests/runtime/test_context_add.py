import json
from datetime import datetime, timedelta, timezone

import pytest

from common.schema.primitives import Message
from infrastructure.redis_client import RedisKeys
from core.session.context import Session
from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import FakeConfigValue, FakeConsumer, FakeResources


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
