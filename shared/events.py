
import asyncio
from collections import deque
from datetime import datetime, timezone
import threading
from typing import Dict, Optional, Set, Any
from dataclasses import dataclass
from loguru import logger

HISTORY_SIZE = 5


@dataclass
class DebugEvent:
    ts: str
    session_id: str
    component: str
    event: str
    data: Dict[str, Any]
    verbose_only: bool = False


class DebugEventEmitter:
    """Session-scoped event emitter for debug WebSocket streaming."""
    
    _instance: Optional["DebugEventEmitter"] = None
    
    def __init__(self):
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}
        self._history: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
        
    
    @classmethod
    def get(cls) -> "DebugEventEmitter":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    async def subscribe(self, session_id: str) -> asyncio.Queue:
        async with self._lock:
            if session_id not in self._subscribers:
                self._subscribers[session_id] = set()
            queue = asyncio.Queue(maxsize=1000)
            # Replay recent events so the UI isn't empty on connect
            for evt in self._history.get(session_id, []):
                queue.put_nowait(evt)
            self._subscribers[session_id].add(queue)
            logger.debug(f"Debug subscriber added for session {session_id}")
            return queue
    
    async def unsubscribe(self, session_id: str, queue: asyncio.Queue):
        async with self._lock:
            if session_id in self._subscribers:
                self._subscribers[session_id].discard(queue)
                if not self._subscribers[session_id]:
                    del self._subscribers[session_id]
    
    def has_subscribers(self, session_id: str) -> bool:
        return session_id in self._subscribers and len(self._subscribers[session_id]) > 0
    
    async def emit(
        self,
        session_id: str,
        component: str,
        event: str,
        data: Dict[str, Any] = None,
        verbose_only: bool = False
    ):
        evt = DebugEvent(
            ts=datetime.now(timezone.utc).isoformat(),
            session_id=session_id,
            component=component,
            event=event,
            data=data or {},
            verbose_only=verbose_only
        )
        
        # Always record in history, even with no subscribers
        if session_id not in self._history:
            self._history[session_id] = deque(maxlen=HISTORY_SIZE)
        self._history[session_id].append(evt)
        
        if not self.has_subscribers(session_id):
            return
        
        async with self._lock:
            queues = self._subscribers.get(session_id, set()).copy()
        
        for queue in queues:
            try:
                queue.put_nowait(evt)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                    queue.put_nowait(evt)
                    logger.debug(f"Event queued up: {evt}")
                except asyncio.QueueEmpty:
                    pass
    
    async def cleanup_session(self, session_id: str):
        async with self._lock:
            self._history.pop(session_id, None)
            self._subscribers.pop(session_id, None)


class CommunityEventEmitter:
    """Global (user-scoped) event emitter for Community live streaming."""
    
    _instance: Optional["CommunityEventEmitter"] = None
    
    def __init__(self):
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}
        self._history: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
        
    @classmethod
    def get(cls) -> "CommunityEventEmitter":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    async def subscribe(self, user_name: str) -> asyncio.Queue:
        async with self._lock:
            if user_name not in self._subscribers:
                self._subscribers[user_name] = set()
            queue = asyncio.Queue(maxsize=1000)
            for evt in self._history.get(user_name, []):
                queue.put_nowait(evt)
            self._subscribers[user_name].add(queue)
            return queue
    
    async def unsubscribe(self, user_name: str, queue: asyncio.Queue):
        async with self._lock:
            if user_name in self._subscribers:
                self._subscribers[user_name].discard(queue)
                if not self._subscribers[user_name]:
                    del self._subscribers[user_name]
    
    async def emit(
        self,
        user_name: str,
        component: str,
        event: str,
        data: Dict[str, Any] = None
    ):
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "user_name": user_name,
            "component": component,
            "event": event,
            "data": data or {}
        }
        
        # 1. Update in-memory history (for local subscribers)
        if user_name not in self._history:
            self._history[user_name] = deque(maxlen=20)
        self._history[user_name].append(evt)
        
        # 2. Notify local in-process subscribers
        async with self._lock:
            queues = self._subscribers.get(user_name, set()).copy()
        
        for queue in queues:
            try:
                queue.put_nowait(evt)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                    queue.put_nowait(evt)
                except asyncio.QueueEmpty:
                    pass

        # 3. Publish to Redis for cross-component/process communication
        try:
            from shared.redisclient import AsyncRedisClient, RedisKeys
            await AsyncRedisClient.publish(RedisKeys.community_pubsub_channel(), evt)
        except Exception as e:
            logger.error(f"Failed to publish community event to Redis: {e}")


# Convenience wrappers

async def emit(
    session_id: str,
    component: str,
    event: str,
    data: Dict[str, Any] = None,
    verbose_only: bool = False
):
    """Async emit for use in async functions."""
    await DebugEventEmitter.get().emit(session_id, component, event, data, verbose_only)


def emit_sync(
    session_id: str,
    component: str,
    event: str,
    data: Dict[str, Any] = None,
    verbose_only: bool = False
):
    """Fire-and-forget emit for sync code. Schedules on running loop."""
    emitter = DebugEventEmitter.get()
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(emitter.emit(session_id, component, event, data, verbose_only))
    except RuntimeError:
        # No running loop - event dropped (expected during shutdown/init)
        pass


async def emit_community(
    user_name: str,
    component: str,
    event: str,
    data: Dict[str, Any] = None
):
    """Emit a community event."""
    await CommunityEventEmitter.get().emit(user_name, component, event, data)
