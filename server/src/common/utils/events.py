"""Engine-owned operational event recording.

HTTP, WebSocket, and browser replay are adapter responsibilities. The engine
only records scoped facts for coordination and diagnostics.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from loguru import logger

from common.schema.events import InternalEvent
from common.utils.coordination_log import write_coordination_event
from common.utils.event_persistence_policy import normalize_coordination_event
from common.utils.time_utils import get_now_iso


class EventEmitter:
    """Record engine events without owning client subscriptions or replay."""

    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    @classmethod
    def get(cls) -> "EventEmitter":
        return _EVENT_EMITTER

    def _remember_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        self._loop = loop
        return loop

    def get_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        if self._loop and self._loop.is_running():
            return self._loop
        return None

    async def emit(
        self,
        scope_id: str,
        component: str,
        event: str,
        data: dict[str, Any] | None = None,
        verbose_only: bool = False,
    ) -> None:
        self._remember_loop()
        internal_event = InternalEvent(
            ts=get_now_iso(),
            scope_id=scope_id,
            component=component,
            event=event,
            data=data or {},
            verbose_only=verbose_only,
        )
        self._persist_coordination_event(internal_event)

    @staticmethod
    def _persist_coordination_event(internal_event: InternalEvent) -> None:
        fields = normalize_coordination_event(internal_event)
        if fields is not None:
            write_coordination_event(fields)


_EVENT_EMITTER = EventEmitter()


async def emit(
    scope_id: str,
    component: str,
    event: str,
    data: dict[str, Any] | None = None,
    verbose_only: bool = False,
) -> None:
    """Record one scoped engine event."""

    await EventEmitter.get().emit(scope_id, component, event, data, verbose_only)


def emit_sync(
    scope_id: str,
    component: str,
    event: str,
    data: dict[str, Any] | None = None,
    verbose_only: bool = False,
) -> None:
    """Schedule event recording from synchronous engine code when possible."""

    emitter = EventEmitter.get()
    coro = emitter.emit(scope_id, component, event, data, verbose_only)

    def _log_failure(task: asyncio.Future[Any]) -> None:
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except Exception as exc:
            logger.error(f"emit_sync result inspection failed: {exc}")
            return
        if exc:
            logger.error(f"emit_sync failed for {component}.{event}: {exc}")

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = emitter.get_loop()
        if loop is None:
            coro.close()
            logger.warning(
                f"Dropped sync event {component}.{event}: "
                "no running event loop is registered"
            )
            return
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        future.add_done_callback(_log_failure)
        return

    task = loop.create_task(coro)
    task.add_done_callback(_log_failure)
