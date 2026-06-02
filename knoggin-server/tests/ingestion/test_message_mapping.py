import json

import pytest

from infrastructure.redis_client import AsyncRedisClient, RedisKeys
from tests.fixtures.fakes import FakeRedis


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_update_message_mapping_stores_structured_msg_id_payload(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(AsyncRedisClient, "_instance", redis)

    await AsyncRedisClient.update_message_mapping(
        user_name="ada",
        session_id="session-1",
        msg_id=42,
        turn_id=7,
        content="hello",
        timestamp="2026-01-01T00:00:00+00:00",
        role="user",
    )

    lookup_key = RedisKeys.msg_to_turn_lookup("ada", "session-1")
    content_key = RedisKeys.message_content("ada", "session-1")

    assert redis.hashes[lookup_key] == {"42": "7"}
    assert set(redis.hashes[content_key]) == {"msg_42"}
    assert json.loads(redis.hashes[content_key]["msg_42"]) == {
        "id": 42,
        "message": "hello",
        "content": "hello",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "role": "user",
    }


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_update_message_mapping_can_record_turn_without_content(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(AsyncRedisClient, "_instance", redis)

    await AsyncRedisClient.update_message_mapping(
        user_name="ada",
        session_id="session-1",
        msg_id=42,
        turn_id=7,
        content=None,
    )

    assert redis.hashes[RedisKeys.msg_to_turn_lookup("ada", "session-1")] == {
        "42": "7"
    }
    assert redis.hashes[RedisKeys.message_content("ada", "session-1")] == {}
