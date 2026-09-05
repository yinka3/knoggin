import asyncio
from types import SimpleNamespace

import pytest

from common.schema.primitives import Message
from runtime.session_runtime import SessionRuntime
from tests.fixtures.fakes import FakeResources


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_shutdown_cancels_agent_work_before_unsubscribers(
    monkeypatch,
):
    calls = []
    started = asyncio.Event()

    async def active_agent_work():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            calls.append("agent_cancelled")

    async def record_emit(*_args, **_kwargs):
        calls.append("emit")

    session = SessionRuntime(
        "ada",
        FakeResources(),
        session_id="session-1",
        project_id="project-1",
        project=SimpleNamespace(scheduler=object()),
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    session.config_unsubscribers = [
        lambda: calls.append("unsubscribe:first"),
        lambda: calls.append("unsubscribe:second"),
    ]
    monkeypatch.setattr("runtime.session_runtime.emit", record_emit)

    active_task = asyncio.create_task(active_agent_work())
    await started.wait()
    session._active_agent_task = active_task

    await session.shutdown()

    assert active_task.cancelled()
    assert calls == [
        "agent_cancelled",
        "unsubscribe:first",
        "unsubscribe:second",
        "emit",
    ]
    assert session._closed is True
    with pytest.raises(RuntimeError, match="shutting down"):
        await session.open_agent_run_stream(Message(content="hello"))


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_shutdown_runs_remaining_cleanup_despite_unsubscribe_failure(monkeypatch):
    calls = []

    async def record_emit(*_args, **_kwargs):
        calls.append("emit")

    session = SessionRuntime(
        "ada",
        FakeResources(),
        session_id="session-1",
        project_id="project-1",
        project=SimpleNamespace(scheduler=object()),
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    session.config_unsubscribers = [
        lambda: (_ for _ in ()).throw(RuntimeError("unsubscribe failed")),
        lambda: calls.append("unsubscribe:second"),
    ]
    monkeypatch.setattr("runtime.session_runtime.emit", record_emit)

    with pytest.raises(RuntimeError, match="SessionRuntime shutdown failed"):
        await session.shutdown()

    assert calls == ["unsubscribe:second", "emit"]
    assert session._closed is True


@pytest.mark.runtime
@pytest.mark.no_network
async def test_concurrent_session_shutdown_runs_cleanup_once(monkeypatch):
    calls = []

    async def record_emit(*_args, **_kwargs):
        calls.append("emit")

    session = SessionRuntime(
        "ada",
        FakeResources(),
        session_id="session-1",
        project_id="project-1",
        project=SimpleNamespace(scheduler=object()),
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    monkeypatch.setattr("runtime.session_runtime.emit", record_emit)

    first = asyncio.create_task(session.shutdown())
    second = asyncio.create_task(session.shutdown())
    await asyncio.gather(first, second)

    assert calls == ["emit"]
