import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import torch

from common.exceptions import DependencyError
from runtime.resources import RuntimeResources


async def test_cancelled_bootstrap_settles_cleanup_before_propagating(monkeypatch):
    entered, cleaned = asyncio.Event(), asyncio.Event()
    owner = None

    async def start(self, *, num_workers):
        nonlocal owner
        owner = self
        self.postgres = SimpleNamespace(close=AsyncMock(side_effect=lambda: cleaned.set()))
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(RuntimeResources, "_start", start)
    bootstrap = asyncio.create_task(RuntimeResources.create())
    await entered.wait()
    bootstrap.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bootstrap
    assert cleaned.is_set()
    assert owner.postgres is None


async def test_failed_bootstrap_retains_failed_cleanup_owner(monkeypatch):
    async def start(self, *, num_workers):
        self.postgres = SimpleNamespace(close=AsyncMock(side_effect=[RuntimeError("close"), None]))
        raise DependencyError("primary")

    monkeypatch.setattr(RuntimeResources, "_start", start)
    with pytest.raises(DependencyError, match="primary") as failure:
        await RuntimeResources.create()
    owner = failure.value.cleanup_owner
    assert owner.postgres is not None
    await owner.shutdown()
    assert owner.postgres is None


@pytest.mark.parametrize("cancel", [False, True])
async def test_model_loaders_settle_siblings_before_returning(monkeypatch, cancel):
    resources = RuntimeResources()
    entered, release = asyncio.Event(), asyncio.Event()

    async def delayed():
        entered.set()
        await release.wait()

    async def fail():
        raise RuntimeError("loader")

    resources.llm_service = SimpleNamespace(load_tokenizer=fail)
    resources.embedding = SimpleNamespace(load_models=delayed)
    resources.model_work = SimpleNamespace(run_blocking=AsyncMock(return_value=object()))
    monkeypatch.setattr(resources, "get_vp01", AsyncMock())
    task = asyncio.create_task(resources._load_heavyweight_models(device=torch.device("cpu")))
    await entered.wait()
    if cancel:
        task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError if cancel else DependencyError):
        await task
