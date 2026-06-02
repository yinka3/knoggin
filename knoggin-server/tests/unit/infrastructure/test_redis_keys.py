import pytest

from infrastructure.redis_client import RedisKeys


@pytest.mark.unit
@pytest.mark.no_network
def test_session_scoped_keys_include_runtime_cleanup_targets():
    keys = RedisKeys.get_session_scoped_keys("ada", "session-1")

    assert RedisKeys.buffer("ada", "session-1") in keys
    assert RedisKeys.message_content("ada", "session-1") in keys
    assert RedisKeys.msg_to_turn_lookup("ada", "session-1") in keys
    assert RedisKeys.conversation("ada", "session-1") in keys
    assert RedisKeys.recent_conversation("ada", "session-1") in keys
    assert RedisKeys.heartbeat_counter("ada", "session-1") in keys
