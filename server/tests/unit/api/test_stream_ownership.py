import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request

from api.app import OwnedStreamingResponse, create_app
from common.schema.public import StartRunRequest
from common.utils.streams import ClosingAsyncIterator


async def test_response_send_failure_before_iteration_closes_admission():
    started = False

    async def body():
        nonlocal started
        started = True
        yield "data"

    owner = AsyncMock()
    response = OwnedStreamingResponse(body(), owner=owner)

    async def send(message):
        raise OSError("disconnected")

    with pytest.raises(Exception):
        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, AsyncMock(), send)
    assert not started
    owner.aclose.assert_awaited_once()


async def test_response_cancellation_at_yield_closes_body_and_owner():
    entered, finished = asyncio.Event(), asyncio.Event()

    async def body():
        try:
            yield "data"
        finally:
            finished.set()

    owner = AsyncMock()
    response = OwnedStreamingResponse(body(), owner=owner)

    async def send(message):
        if message["type"] == "http.response.body":
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(response({"type": "http", "asgi": {"spec_version": "2.4"}}, AsyncMock(), send))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    owner.aclose.assert_awaited_once()


async def test_adapter_concurrent_close_closes_each_owner_once():
    inner, owner = AsyncMock(), AsyncMock()
    stream = ClosingAsyncIterator(inner, owner)
    await asyncio.gather(stream.aclose(), stream.aclose())
    inner.aclose.assert_awaited_once()
    owner.aclose.assert_awaited_once()


async def test_adapter_retains_failed_owner_close_for_retry():
    inner, owner = AsyncMock(), AsyncMock()
    owner.aclose.side_effect = [RuntimeError("close"), None]
    stream = ClosingAsyncIterator(inner, owner)
    with pytest.raises(RuntimeError):
        await stream.aclose()
    await stream.aclose()
    inner.aclose.assert_awaited_once()
    assert owner.aclose.await_count == 2


async def test_nonstream_cancelled_request_closes_admitted_owner():
    admitted = asyncio.Event()

    class Owner:
        aclose = AsyncMock()

        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.Event().wait()

    owner = Owner()

    async def open_stream(**kwargs):
        admitted.set()
        return owner

    app = create_app(SimpleNamespace(open_run_stream=open_stream))
    route = next(route for route in app.routes if getattr(route, "path", None) == "/v1/runs")
    task = asyncio.create_task(route.endpoint(
        StartRunRequest(session_id="session", query="hello"),
        Request({"type": "http", "headers": []}), user_name="ada",
    ))
    await asyncio.wait_for(admitted.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    owner.aclose.assert_awaited_once()


async def test_cancelled_adapter_close_settles_both_owners():
    entered, release = asyncio.Event(), asyncio.Event()
    inner, owner = AsyncMock(), AsyncMock()

    async def close():
        entered.set()
        await release.wait()

    inner.aclose.side_effect = close
    stream = ClosingAsyncIterator(inner, owner)
    task = asyncio.create_task(stream.aclose())
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    owner.aclose.assert_awaited_once()
    await stream.aclose()
    inner.aclose.assert_awaited_once()
