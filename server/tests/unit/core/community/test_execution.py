import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from core.community.execution import config_owner, execute_aac_run


@pytest.mark.parametrize("failure", ["run", "executor"])
async def test_setup_failure_closes_tools(failure):
    tools = SimpleNamespace(close=AsyncMock())
    open_run = Mock(side_effect=RuntimeError("run") if failure == "run" else None)
    factory = Mock(side_effect=RuntimeError("executor"))
    with pytest.raises(RuntimeError, match=failure):
        await execute_aac_run(open_run=open_run, executor_factory=factory,
                              llm=None, tools=tools, on_completion=None, budget=None)
    tools.close.assert_awaited_once()


async def test_cancelled_close_waits_for_owned_cleanup():
    entered, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def close():
        entered.set()
        await release.wait()
        closed.set()

    class Executor:
        def __init__(self, *args, **kwargs):
            pass

        async def execute(self):
            yield {"event": "response", "data": {"content": "done"}}

    task = asyncio.create_task(execute_aac_run(
        open_run=lambda: None, executor_factory=Executor, llm=None,
        tools=SimpleNamespace(close=close), on_completion=None, budget=None,
    ))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()


async def test_seeder_does_not_swallow_cleanup_failure():
    tools = SimpleNamespace(close=AsyncMock(side_effect=RuntimeError("close")))
    with pytest.raises(RuntimeError, match="close"):
        await execute_aac_run(open_run=Mock(side_effect=RuntimeError("run")),
                              executor_factory=Mock(), llm=None, tools=tools,
                              on_completion=None, budget=None, skip_on_error=True)


def test_injected_config_owner_does_not_consult_global_provider():
    owner = SimpleNamespace(config=object(), get=Mock(side_effect=AssertionError("global")))
    assert config_owner(owner) is owner
    owner.get.assert_not_called()
