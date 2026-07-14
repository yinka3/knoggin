import pytest

from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeRedis


@pytest.mark.unit
@pytest.mark.no_network
def test_session_scoped_keys_include_runtime_cleanup_targets():
    keys = RedisKeys.session_keys("ada", "session-1")

    assert RedisKeys.buffer("ada", "session-1") in keys
    assert RedisKeys.message_content("ada", "session-1") in keys
    assert RedisKeys.recent_conversation("ada", "session-1") in keys
    assert RedisKeys.conversation("ada", "session-1") in keys
    assert RedisKeys.recent_conversation("ada", "session-1") in keys
    assert RedisKeys.heartbeat_counter("ada", "session-1") in keys
    assert RedisKeys.project_profile_complete("ada", "project-1") not in keys
    assert RedisKeys.project_topic_config("ada") not in keys


@pytest.mark.unit
@pytest.mark.no_network
def test_user_and_project_scoped_keys_do_not_collide():
    assert RedisKeys.agent_directives("ada", "agent-1") != RedisKeys.agent_directives(
        "grace", "agent-1"
    )
    assert RedisKeys.community_agent_memory(
        "ada", "agent-1"
    ) != RedisKeys.community_agent_memory("grace", "agent-1")
    assert RedisKeys.community_discussion_active(
        "ada", "project-1"
    ) != RedisKeys.community_discussion_active("ada", "project-2")
    assert RedisKeys.maintenance_attempts(
        "ada", "project-1", "topic_evaluation:project-1"
    ) != RedisKeys.maintenance_attempts(
        "ada", "project-2", "topic_evaluation:project-2"
    )
    assert RedisKeys.maintenance_cooldown(
        "ada", "project-1", "topic_evaluation:project-1"
    ) != RedisKeys.maintenance_cooldown(
        "ada", "project-2", "topic_evaluation:project-2"
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_session_scan_patterns_match_their_key_families():
    assert RedisKeys.message_dedup_pattern("ada", "session-1") == (
        "msg_dedup:ada:session-1:*"
    )
    assert RedisKeys.session_memory_pattern("ada", "session-1") == (
        "memory:ada:session-1:*"
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_project_cleanup_inventory_covers_fixed_and_variable_key_families():
    keys = RedisKeys.project_cleanup_keys("ada", "project-1")
    patterns = RedisKeys.project_cleanup_patterns("ada", "project-1")

    assert RedisKeys.dirty_entities("ada", "project-1") in keys
    assert RedisKeys.dlq_state("ada", "project-1") in keys
    assert RedisKeys.project_sessions("ada", "project-1") in keys
    assert RedisKeys.community_discussion_active("ada", "project-1") in keys
    assert "last_profile_update:ada:project-1:*" in patterns
    assert "merge_intent:ada:project-1:*" in patterns
    assert "job_lease:ada:project-1:*" in patterns
    assert "maintenance_attempts:ada:project-1:*" in patterns


@pytest.mark.unit
@pytest.mark.no_network
async def test_fake_redis_expiration_applies_to_every_supported_key_type():
    redis = FakeRedis()
    await redis.set("string", "value")
    await redis.hset("hash", "field", "value")
    await redis.sadd("set", "value")
    await redis.rpush("list", "value")
    await redis.zadd("zset", {"value": 1})

    keys = ("string", "hash", "set", "list", "zset")
    for key in keys:
        assert await redis.expire(key, 60) is True
        redis.key_expirations[key] = 0

    assert await redis.get("string") is None
    assert await redis.hgetall("hash") == {}
    assert await redis.smembers("set") == set()
    assert await redis.lrange("list", 0, -1) == []
    assert await redis.zrange("zset", 0, -1) == []
    assert await redis.scan() == (0, [])
    assert await redis.expire("missing", 60) is False
