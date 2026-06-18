import pytest
from unittest.mock import MagicMock, patch

from infrastructure.redis_client import AsyncRedisClient, RedisKeys


@pytest.fixture
def mock_redis():
    from unittest.mock import AsyncMock
    with patch("infrastructure.redis_client.AsyncRedisClient.get_instance", new_callable=AsyncMock) as mock_get:
        mock_instance = MagicMock()
        mock_pipeline = MagicMock()
        mock_pipeline.execute = AsyncMock()
        mock_instance.pipeline.return_value = mock_pipeline
        mock_get.return_value = mock_instance
        yield mock_instance, mock_pipeline


@pytest.mark.unit
@pytest.mark.no_network
async def test_log_conversation_turn_executes_pipeline(mock_redis):
    mock_instance, mock_pipeline = mock_redis
    
    payload = {"role": "user", "content": "hello"}
    await AsyncRedisClient.log_conversation_turn(
        user_name="ada", 
        session_id="sess-1", 
        turn_id=42, 
        payload=payload, 
        max_history=10
    )
    
    assert mock_pipeline.hset.call_count == 1
    assert mock_pipeline.zadd.call_count == 1
    assert mock_pipeline.zremrangebyrank.call_count == 1
    assert mock_pipeline.execute.call_count == 1
    
    # Verify exact calls
    hset_args = mock_pipeline.hset.call_args[0]
    assert hset_args[0] == RedisKeys.conversation("ada", "sess-1")
    assert hset_args[1] == "42"
    
    zrem_args = mock_pipeline.zremrangebyrank.call_args[0]
    assert zrem_args[0] == RedisKeys.recent_conversation("ada", "sess-1")
    assert zrem_args[1] == 0
    assert zrem_args[2] == -11  # -(max_history + 1)


@pytest.mark.unit
@pytest.mark.no_network
async def test_refresh_session_ttls_executes_pipeline(mock_redis):
    mock_instance, mock_pipeline = mock_redis
    
    await AsyncRedisClient.refresh_session_ttls("ada", "sess-1", 3600)
    
    keys = RedisKeys.get_session_scoped_keys("ada", "sess-1")
    
    assert mock_pipeline.expire.call_count == len(keys)
    assert mock_pipeline.execute.call_count == 1
    
    # Verify the first call is for a valid key
    first_call_args = mock_pipeline.expire.call_args_list[0][0]
    assert first_call_args[0] in keys
    assert first_call_args[1] == 3600


@pytest.mark.unit
@pytest.mark.no_network
async def test_update_message_mapping(mock_redis):
    mock_instance, mock_pipeline = mock_redis
    
    await AsyncRedisClient.update_message_mapping(
        user_name="ada",
        session_id="sess-1",
        msg_id=101,
        turn_id=42,
        content="hello again"
    )
    
    assert mock_pipeline.hset.call_count == 2
    assert mock_pipeline.execute.call_count == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_delete_message_turn_removes_all_assistant_redis_state(mock_redis):
    _, mock_pipeline = mock_redis

    await AsyncRedisClient.delete_message_turn(
        user_name="ada",
        session_id="sess-1",
        msg_id=101,
        turn_id=42,
    )

    assert mock_pipeline.hdel.call_count == 3
    mock_pipeline.zrem.assert_called_once_with(
        RedisKeys.recent_conversation("ada", "sess-1"),
        "42",
    )
    assert mock_pipeline.execute.call_count == 1
