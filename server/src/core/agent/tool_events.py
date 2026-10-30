"""Events a tool sends while it is still running, such as command output."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar, Token
from typing import Any, AsyncIterator, Awaitable, Callable
from uuid import uuid4

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


class DecisionBroker:
    """Questions a run's tools are waiting on, answered by the user through
    the API. One per run; closing it gives every waiting tool its default."""

    def __init__(self) -> None:
        self._pending: dict[str, tuple[asyncio.Future, tuple[str, ...]]] = {}

    async def ask(
        self, question: str, options: tuple[str, ...], *, timeout_seconds: float, default: str,
    ) -> str:
        if default not in options:
            raise ValueError("default must be one of the options")
        decision_id = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        self._pending[decision_id] = (future, options)
        emit_tool_event("decision_required", {
            "decision_id": decision_id, "question": question, "options": list(options),
            "default": default, "timeout_seconds": timeout_seconds,
        })
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout_seconds)
        except TimeoutError:
            return default
        finally:
            self._pending.pop(decision_id, None)
            if future.done() and not future.cancelled():
                choice = future.result()
                emit_tool_event("decision_resolved", {"decision_id": decision_id, "choice": choice})

    def answer(self, decision_id: str, choice: str) -> bool:
        """Deliver the user's choice; False when nothing is waiting under that
        id. Raises ValueError for a choice that was not offered."""
        entry = self._pending.get(decision_id)
        if entry is None or entry[0].done():
            return False
        future, options = entry
        if choice not in options:
            raise ValueError(f"choice must be one of {', '.join(options)}")
        future.set_result(choice)
        return True

    def close(self) -> None:
        for future, _ in self._pending.values():
            if not future.done():
                future.cancel()


_broker: ContextVar[DecisionBroker | None] = ContextVar("decision_broker", default=None)


def bind_decision_broker(broker: DecisionBroker) -> Token:
    return _broker.set(broker)


def unbind_decision_broker(token: Token) -> None:
    try:
        _broker.reset(token)
    except ValueError:
        pass  # closed from another task; that task's context never had it


async def ask_user(
    question: str, options: tuple[str, ...], *, timeout_seconds: float, default: str,
) -> str:
    """Ask the user from inside a running tool and wait for the choice. With
    no one to ask (no live run) or no answer in time, returns ``default``."""

    broker = _broker.get()
    if broker is None:
        return default
    try:
        return await broker.ask(question, options, timeout_seconds=timeout_seconds, default=default)
    except asyncio.CancelledError:
        if asyncio.current_task().cancelling():
            raise
        return default  # the run ended; the broker was closed
