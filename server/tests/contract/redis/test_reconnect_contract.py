"""Real Redis reconnect contracts after an established connection is lost."""

from uuid import uuid4

import pytest

from common.schema.settings import RedisConnectionSettings
from infrastructure.redis_client import AsyncRedisClient


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_reconnects_after_established_pool_disconnect():
    """The stable redis-py client reconnects after its pooled socket is lost."""

    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    key = f"knoggin:p2:reconnect:{uuid4()}"

    try:
        assert await client.set(key, "before-disconnect", ex=30)
        await client.connection_pool.disconnect(inuse_connections=True)
        assert await client.get(key) == "before-disconnect"
    finally:
        await client.delete(key)
        await manager.close()
