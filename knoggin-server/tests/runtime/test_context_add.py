import json
from datetime import datetime, timezone

import pytest

from common.schema.primitives import Message
from infrastructure.redis_client import AsyncRedisClient, RedisKeys
from knoggin_server.session.context import Context
from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import FakeConfigValue, FakeConsumer, FakeResources


@pytest.fixture
def context(monkeypatch):
    resources = FakeResources()
    ctx = Context("ada", ["General"], resources)
    ctx.session_id = "session-1"
    ctx.project_id = "project-1"
    ctx.project = make_project_state("project-1", redis=resources.redis)
    ctx.consumer = FakeConsumer()
    monkeypatch.setattr(AsyncRedisClient, "_instance", resources.redis)
    monkeypatch.setattr(
        Context,
        "current_config",
        property(lambda self: FakeConfigValue(conversation_context_turns=100)),
    )
    return ctx, resources


@pytest.mark.runtime
@pytest.mark.no_network
async def test_context_add_fails_fast_when_ingestion_wiring_is_incomplete():
    resources = FakeResources()
    ctx = Context("ada", ["General"], resources)
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
    mapping_key = RedisKeys.msg_to_turn_lookup("ada", "session-1")
    content_key = RedisKeys.message_content("ada", "session-1")
    buffer_key = RedisKeys.buffer("ada", "session-1")
    project_heartbeat_key = RedisKeys.project_heartbeat_counter("ada", "project-1")

    assert json.loads(resources.redis.hashes[conv_key]["1"])["content"] == "hello world"
    assert resources.redis.zsets[recent_key]["1"] == timestamp.timestamp()
    assert resources.redis.hashes[mapping_key]["1"] == "1"

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
