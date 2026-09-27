import asyncio
from unittest.mock import AsyncMock

import pytest

from runtime.resources import RuntimeResources, RuntimeResourcesShutdownError


async def test_model_failure_keeps_executor_and_models_alive():
    resources = RuntimeResources()
    resources.model_work = AsyncMock()
    resources.model_work.shutdown.side_effect = [RuntimeError("busy"), None]
    resources.postgres = AsyncMock()
    pool = resources.postgres
    with pytest.raises(RuntimeResourcesShutdownError):
        await resources.shutdown()
    pool.close.assert_not_called()
    await resources.shutdown()
    pool.close.assert_awaited_once()


async def test_leaf_failure_retries_only_failed_leaf():
    resources = RuntimeResources()
    resources.postgres = AsyncMock()
    resources.postgres.close.side_effect = [RuntimeError("pool close"), None]
    pool = resources.postgres
    resources.llm_service = AsyncMock()
    llm = resources.llm_service
    with pytest.raises(RuntimeResourcesShutdownError):
        await resources.shutdown()
    assert resources.postgres is pool
    assert resources.llm_service is None
    await resources.shutdown()
    assert pool.close.await_count == 2
    llm.close.assert_awaited_once()


async def test_readiness_closes_immediately_and_concurrent_shutdown_serializes():
    resources = RuntimeResources()
    resources._started = True
    entered, release = asyncio.Event(), asyncio.Event()

    async def stop():
        entered.set()
        await release.wait()

    resources.background_work = AsyncMock()
    background = resources.background_work
    background.shutdown.side_effect = stop
    first = asyncio.create_task(resources.shutdown())
    await entered.wait()
    with pytest.raises(RuntimeError, match="not ready"):
        resources.require_ready()
    second = asyncio.create_task(resources.shutdown())
    await asyncio.sleep(0)
    background.shutdown.assert_awaited_once()
    release.set()
    await asyncio.gather(first, second)
    assert resources._shutdown_complete
