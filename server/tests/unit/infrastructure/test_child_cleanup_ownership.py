import asyncio
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

import pytest

from infrastructure.background_work import BackgroundWorkCoordinator
from infrastructure.model_work import ModelWorkCoordinator, ModelWorkPriority
from infrastructure.postgres_client import PostgresClient


async def test_failed_pool_close_retains_pool_for_retry():
    client = PostgresClient("postgresql://unused")
    pool = AsyncMock()
    pool.close.side_effect = [RuntimeError("pool close"), None]
    client._pool = pool
    with pytest.raises(RuntimeError):
        await client.close()
    assert client._pool is pool
    await client.close()
    assert client._pool is None


async def test_cancelled_waiter_preserves_background_cleanup_join():
    coordinator = BackgroundWorkCoordinator(max_concurrency=1)
    entered, stopping, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def operation():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopping.set()
            await release.wait()

    work = asyncio.create_task(coordinator.submit("project", operation, name="held"))
    try:
        await entered.wait()
        close = asyncio.create_task(coordinator.shutdown())
        await stopping.wait()
        close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close
        assert not coordinator._shutdown_task.done()
        release.set()
        await coordinator.shutdown()
        await asyncio.gather(work, return_exceptions=True)
        assert not coordinator._workers
    finally:
        release.set()
        await coordinator.shutdown()


async def test_cancelled_shutdown_waiter_does_not_cancel_model_drain():
    executor = ThreadPoolExecutor(max_workers=1)
    coordinator = ModelWorkCoordinator(executor, foreground_timeout_seconds=None)
    entered, release = asyncio.Event(), asyncio.Event()

    async def operation():
        entered.set()
        await release.wait()
        return "finished"

    work = asyncio.create_task(coordinator.submit(operation, priority=ModelWorkPriority.FOREGROUND, name="held"))
    try:
        await entered.wait()
        close = asyncio.create_task(coordinator.shutdown())
        await asyncio.sleep(0)
        close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close
        assert not coordinator._shutdown_task.done()
        release.set()
        await coordinator.shutdown()
        assert await work == "finished"
        assert not coordinator._workers
    finally:
        release.set()
        await coordinator.shutdown()
        executor.shutdown(wait=True)
