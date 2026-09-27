import asyncio
from unittest.mock import AsyncMock

import pytest
from knoggin import Knoggin


async def test_failed_close_can_retry():
    runtime = AsyncMock()
    runtime.shutdown.side_effect = [RuntimeError("cleanup"), None]
    client = Knoggin(runtime)
    with pytest.raises(RuntimeError):
        await client.close()
    assert not client._closed
    await client.close()
    await client.close()
    assert runtime.shutdown.await_count == 2


async def test_concurrent_close_is_serialized():
    entered, release = asyncio.Event(), asyncio.Event()
    runtime = AsyncMock()

    async def stop():
        entered.set()
        await release.wait()

    runtime.shutdown.side_effect = stop
    client = Knoggin(runtime)
    first = asyncio.create_task(client.close())
    await entered.wait()
    second = asyncio.create_task(client.close())
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)
    runtime.shutdown.assert_awaited_once()
