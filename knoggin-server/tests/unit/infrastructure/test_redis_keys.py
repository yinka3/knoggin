import pytest

from infrastructure.redis_client import RedisKeys


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


@pytest.mark.unit
@pytest.mark.no_network
def test_session_scan_patterns_match_their_key_families():
    assert RedisKeys.message_dedup_pattern("ada", "session-1") == (
        "msg_dedup:ada:session-1:*"
    )
    assert RedisKeys.session_memory_pattern("ada", "session-1") == (
        "memory:ada:session-1:*"
    )
