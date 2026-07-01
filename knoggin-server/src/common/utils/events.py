import asyncio
import json
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

import redis.asyncio as aioredis
from loguru import logger

from common.utils.coordination_log import write_coordination_event
from common.utils.event_persistence_policy import normalize_coordination_event
from common.utils.time_utils import get_now, get_now_iso, parse_iso_time
from infrastructure.redis_client import RedisKeys


@dataclass
class DebugEvent:
    ts: str
    session_id: str
    component: str
    event: str
    data: Dict[str, Any]
    verbose_only: bool = False


@dataclass
class Subscriber:
    queue: asyncio.Queue
    last_active: float
    failed_emits: int = 0

    def __hash__(self):
        return id(self)


class BaseEventEmitter:
    """Base class for session or user scoped event emitters."""

    def __init__(self, history_maxlen: int = 5):
        self._emit_count = 0
        self._subscribers: Dict[str, Set[Subscriber]] = {}
        self._history: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
        self._history_maxlen = history_maxlen
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _remember_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        self._loop = loop
        return loop

    def get_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        if self._loop and self._loop.is_running():
            return self._loop
        return None

    async def subscribe(self, scope_id: str) -> asyncio.Queue:
        loop = self._remember_loop()
        async with self._lock:
            if scope_id not in self._subscribers:
                self._subscribers[scope_id] = set()
            queue = asyncio.Queue(maxsize=500)
            for evt in self._history.get(scope_id, []):
                queue.put_nowait(evt)
            sub = Subscriber(queue=queue, last_active=loop.time())
            self._subscribers[scope_id].add(sub)
            return queue

    async def unsubscribe(self, scope_id: str, queue: asyncio.Queue):
        self._remember_loop()
        async with self._lock:
            if scope_id in self._subscribers:
                self._subscribers[scope_id] = {
                    s for s in self._subscribers[scope_id] if s.queue is not queue
                }
                if not self._subscribers[scope_id]:
                    del self._subscribers[scope_id]

    def has_subscribers(self, scope_id: str) -> bool:
        return scope_id in self._subscribers and len(self._subscribers[scope_id]) > 0

    async def _emit_to_subs(self, scope_id: str, event_obj: Any):
        """Internal helper to push events to all subscribers in a scope."""
        self._remember_loop()
        if scope_id not in self._history:
            self._history[scope_id] = deque(maxlen=self._history_maxlen)
        self._history[scope_id].append(event_obj)

        if not self.has_subscribers(scope_id):
            return

        async with self._lock:
            subs = self._subscribers.get(scope_id, set())
            to_remove = set()

            for sub in subs:
                try:
                    sub.queue.put_nowait(event_obj)
                    sub.failed_emits = 0
                except asyncio.QueueFull:
                    sub.failed_emits += 1
                    if sub.failed_emits > 50:
                        to_remove.add(sub)
                        continue
                    try:
                        sub.queue.get_nowait()
                        sub.queue.put_nowait(event_obj)
                    except (asyncio.QueueEmpty, asyncio.QueueFull):
                        pass

            if to_remove:
                self._subscribers[scope_id] = subs - to_remove
                logger.warning(
                    f"Dropped {len(to_remove)} stale subscribers for scope {scope_id}"
                )

    async def cleanup_scope(self, scope_id: str):
        self._remember_loop()
        async with self._lock:
            self._history.pop(scope_id, None)
            self._subscribers.pop(scope_id, None)

    async def _base_cleanup(self, max_age_hours: int = 24):
        """Shared stale history cleanup logic."""
        self._remember_loop()
        async with self._lock:
            stale = []
            now = get_now()

            for scope_id, history in self._history.items():
                if scope_id in self._subscribers and self._subscribers[scope_id]:
                    continue
                if not history:
                    stale.append(scope_id)
                    continue

                last_event = history[-1]
                ts_str = (
                    last_event.ts
                    if hasattr(last_event, "ts")
                    else last_event.get("ts", "")
                )

                try:
                    last_ts = parse_iso_time(ts_str)
                    if (now - last_ts).total_seconds() > max_age_hours * 3600:
                        stale.append(scope_id)
                except Exception:
                    stale.append(scope_id)

            for scope_id in stale:
                self._history.pop(scope_id, None)

            return stale


class EventEmitter(BaseEventEmitter):
    """Session-scoped event emitter for debug WebSocket streaming."""

    def __init__(self):
        super().__init__(history_maxlen=5)
        self.project_sessions: Dict[str, Set[str]] = {}

    @classmethod
    def get(cls) -> "EventEmitter":
        return _DEBUG_EMITTER

    def register_session(self, project_id: str, session_id: str):
        if project_id not in self.project_sessions:
            self.project_sessions[project_id] = set()
        self.project_sessions[project_id].add(session_id)
        logger.debug(
            f"Registered session {session_id} to project {project_id} in emitter"
        )

    def unregister_session(self, project_id: str, session_id: str):
        if project_id in self.project_sessions:
            self.project_sessions[project_id].discard(session_id)
            if not self.project_sessions[project_id]:
                del self.project_sessions[project_id]
        logger.debug(
            f"Unregistered session {session_id} from project {project_id} in emitter"
        )

    async def subscribe(self, session_id: str) -> asyncio.Queue:
        # Override to use loguru context or just for naming
        logger.debug(f"Debug subscriber added for session {session_id}")
        return await super().subscribe(session_id)

    async def emit(
        self,
        session_id: str,
        component: str,
        event: str,
        data: Dict[str, Any] = None,
        verbose_only: bool = False,
    ):
        ts = get_now_iso()
        self._persist_coordination_event(
            ts=ts,
            scope_id=session_id,
            component=component,
            event=event,
            data=data,
            verbose_only=verbose_only,
        )
        # If session_id is a registered project_id, fan out to all its active sessions
        if session_id in self.project_sessions:
            active_sess_list = list(self.project_sessions[session_id])
            for active_sess in active_sess_list:
                evt = DebugEvent(
                    ts=ts,
                    session_id=active_sess,
                    component=component,
                    event=event,
                    data=data or {},
                    verbose_only=verbose_only,
                )
                await self._emit_to_subs(active_sess, evt)
        else:
            evt = DebugEvent(
                ts=ts,
                session_id=session_id,
                component=component,
                event=event,
                data=data or {},
                verbose_only=verbose_only,
            )
            await self._emit_to_subs(session_id, evt)

        self._emit_count += 1
        if self._emit_count % 100 == 0:
            asyncio.create_task(self.cleanup_stale_sessions())

    def _persist_coordination_event(
        self,
        *,
        ts: str,
        scope_id: str,
        component: str,
        event: str,
        data: Dict[str, Any] = None,
        verbose_only: bool = False,
    ) -> None:
        record = normalize_coordination_event(
            ts=ts,
            scope_id=scope_id,
            component=component,
            event=event,
            data=data,
            verbose_only=verbose_only,
        )
        if record is None:
            return
        write_coordination_event(record.fields)

    async def cleanup_stale_sessions(self, max_age_hours: int = 24):
        stale = await self._base_cleanup(max_age_hours)
        if stale:
            logger.debug(f"Pruned {len(stale)} stale debug event histories")


class CommunityEventEmitter(BaseEventEmitter):
    """Global (user-scoped) event emitter for Community live streaming."""

    def __init__(self):
        super().__init__(history_maxlen=20)
        self._redis: Optional[aioredis.Redis] = None

    @classmethod
    def get(cls) -> "CommunityEventEmitter":
        return _COMMUNITY_EMITTER

    def bind_redis(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    def unbind_redis(self, redis: aioredis.Redis) -> None:
        if self._redis is redis:
            self._redis = None

    async def emit(
        self, user_name: str, component: str, event: str, data: Dict[str, Any] = None
    ):
        evt = {
            "ts": get_now_iso(),
            "user_name": user_name,
            "component": component,
            "event": event,
            "data": data or {},
        }

        await self._emit_to_subs(user_name, evt)

        if self._redis is not None:
            try:
                await self._redis.publish(
                    RedisKeys.community_pubsub_channel(),
                    json.dumps(evt),
                )
            except Exception as e:
                logger.error(f"Failed to publish community event to Redis: {e}")

        self._emit_count += 1
        if self._emit_count % 100 == 0:
            asyncio.create_task(self.cleanup())

    async def cleanup(self, max_age_hours: int = 24):
        stale = await self._base_cleanup(max_age_hours)
        if stale:
            logger.debug(f"Pruned {len(stale)} stale community event histories")

    async def shutdown(self):
        async with self._lock:
            for scope_id, subs in self._subscribers.items():
                for sub in subs:
                    # Best effort cleanup
                    pass
            self._subscribers.clear()
            self._history.clear()
        logger.info("Community event emitter shutdown complete")


_DEBUG_EMITTER = EventEmitter()
_COMMUNITY_EMITTER = CommunityEventEmitter()


async def emit(
    session_id: str,
    component: str,
    event: str,
    data: Dict[str, Any] = None,
    verbose_only: bool = False,
):
    await EventEmitter.get().emit(session_id, component, event, data, verbose_only)


def emit_sync(
    session_id: str,
    component: str,
    event: str,
    data: Dict[str, Any] = None,
    verbose_only: bool = False,
):
    emitter = EventEmitter.get()
    coro = emitter.emit(session_id, component, event, data, verbose_only)

    def _log_failure(task: asyncio.Future):
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except Exception as e:
            logger.error(f"emit_sync result inspection failed: {e}")
            return
        if exc:
            logger.error(f"emit_sync failed for {component}.{event}: {exc}")

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = emitter.get_loop()
        if not loop:
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


async def emit_community(
    user_name: str, component: str, event: str, data: Dict[str, Any] = None
):
    await CommunityEventEmitter.get().emit(user_name, component, event, data)
