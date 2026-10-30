"""Events a tool sends while it is still running, such as command output."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
from typing import Any, AsyncIterator, Awaitable, Callable

_sink: ContextVar[tuple[asyncio.Queue, str, str] | None] = ContextVar("tool_event_sink", default=None)

RESULTS = "__tool_results__"


def emit_tool_event(event: str, data: dict[str, Any]) -> None:
    """Send an event to the run from inside a running tool. Outside a run,
    for example in a unit test calling the tool directly, it does nothing."""

    sink = _sink.get()
    if sink is None:
        return
    queue, call_id, tool = sink
    queue.put_nowait({"event": event, "data": {**data, "call_id": call_id, "tool": tool}})


async def stream_tool_calls(
    calls: list[tuple[str, str, Callable[[], Awaitable[Any]]]],
) -> AsyncIterator[dict]:
    """Run (call_id, tool, invoke) calls together, yielding their events as
    they arrive, then one ``RESULTS`` item holding each call's result or
    exception in order. Cancelling the stream cancels the calls."""

    queue: asyncio.Queue = asyncio.Queue()

    async def run(call_id: str, tool: str, invoke):
        _sink.set((queue, call_id, tool))
        try:
            return await invoke()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # returned so one call's failure does not stop the rest
            return exc

    tasks = [asyncio.create_task(run(*call)) for call in calls]
    try:
        while True:
            waiter = asyncio.ensure_future(queue.get())
            done, _ = await asyncio.wait(
                [waiter, *tasks], return_when=asyncio.FIRST_COMPLETED
            )
            if waiter in done:
                yield waiter.result()
                continue
            if not waiter.cancel() and not waiter.cancelled():
                # It finished just after wait() returned; keep that event.
                yield waiter.result()
            if all(task.done() for task in tasks):
                break
        while not queue.empty():
            yield queue.get_nowait()
        yield {"event": RESULTS, "data": [task.result() for task in tasks]}
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
