import json
from unittest.mock import AsyncMock

import pytest

from common.utils.events import CommunityEventEmitter
from infrastructure.redis_client import RedisKeys


@pytest.mark.unit
@pytest.mark.no_network
async def test_community_emitter_publishes_through_bound_redis():
    redis = AsyncMock()
    emitter = CommunityEventEmitter()
    emitter.bind_redis(redis)

    await emitter.emit("ada", "community", "message_added", {"id": "msg-1"})

    channel, payload = redis.publish.await_args.args
    assert channel == RedisKeys.community_pubsub_channel()
    assert json.loads(payload) == {
        "ts": json.loads(payload)["ts"],
        "user_name": "ada",
        "component": "community",
        "event": "message_added",
        "data": {"id": "msg-1"},
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_unbound_community_emitter_still_delivers_locally():
    emitter = CommunityEventEmitter()
    queue = await emitter.subscribe("ada")

    await emitter.emit("ada", "community", "message_added", {"id": "msg-1"})

    event = queue.get_nowait()
    assert event["event"] == "message_added"
    assert event["data"] == {"id": "msg-1"}


@pytest.mark.unit
@pytest.mark.no_network
async def test_redis_publish_failure_does_not_abort_local_delivery():
    redis = AsyncMock()
    redis.publish.side_effect = ConnectionError("redis unavailable")
    emitter = CommunityEventEmitter()
    emitter.bind_redis(redis)
    queue = await emitter.subscribe("ada")

    await emitter.emit("ada", "community", "message_added", {"id": "msg-1"})

    assert queue.get_nowait()["data"] == {"id": "msg-1"}


@pytest.mark.unit
@pytest.mark.no_network
def test_unbind_only_clears_matching_redis():
    first = AsyncMock()
    second = AsyncMock()
    emitter = CommunityEventEmitter()
    emitter.bind_redis(first)

    emitter.unbind_redis(second)
    assert emitter._redis is first

    emitter.unbind_redis(first)
    assert emitter._redis is None
