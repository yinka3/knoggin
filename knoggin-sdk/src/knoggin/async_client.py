"""Lightweight SDK client — interacts via HTTP API."""

import httpx
from typing import Dict, Any
from loguru import logger
from .session import KnogginAsyncSession
from .managers import (
    AgentAsyncManager, 
    SessionAsyncManager, 
    TopicAsyncManager, 
    FileAsyncManager, 
    MCPAsyncManager
)

class KnogginAsyncClient:
    """Lightweight SDK client that connects to the Knoggin REST Server."""

    def __init__(self, base_url: str = "http://localhost:8000", dev_mode: bool = False):
        self.base_url = base_url.rstrip("/")
        self.dev_mode = dev_mode
        self.http = httpx.AsyncClient(base_url=self.base_url, timeout=60.0)
        self.agents = AgentAsyncManager(self)
        self.sessions = SessionAsyncManager(self)
        self.topics = TopicAsyncManager(self)
        self.files = FileAsyncManager(self)
        self.mcp = MCPAsyncManager(self)
        self._handlers = {}

    @classmethod
    async def boot(cls, base_url: str = "http://localhost:8000", dev_mode: bool = False) -> "KnogginAsyncClient":
        client = cls(base_url=base_url, dev_mode=dev_mode)
        # Check connection
        try:
            res = await client.http.get("/v1/sessions/")
            res.raise_for_status()
            if dev_mode:
                logger.info(f"[DevMode] KnogginClient booted, connected to {base_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Knoggin server at {base_url}: {e}")
            raise
        return client

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.http.aclose()
        if self.dev_mode:
            logger.info(f"[DevMode] KnogginClient closed")

    async def session(
        self,
        user_name: str,
        topics: dict = None,
        session_id: str = None,
        agent_id: str = None,
        model: str = None,
        enabled_tools: list = None
    ) -> KnogginAsyncSession:
        """Create or resume a session via the API."""
        payload = {
            "topics_config": topics or {},
            "model": model,
            "agent_id": agent_id,
            "enabled_tools": enabled_tools
        }
        res = await self.http.post("/v1/sessions/", json={k: v for k, v in payload.items() if v is not None})
        res.raise_for_status()
        data = res.json()
        
        sid = data["session_id"]
        if self.dev_mode:
            logger.info(f"[DevMode] Session created/resumed: {sid}")
            
        return KnogginAsyncSession(
            session_id=sid,
            user_name=user_name,
            client=self
        )

    async def chat(self, user_name: str, query: str, **kwargs) -> "AgentResult":
        """Shorthand to chat with an agent without manually managing sessions."""
        session = await self.session(user_name, **kwargs)
        return await session.agent.chat(query)

    # ════════════════════════════════════════════════════════
    #  EVENT SYSTEM
    # ════════════════════════════════════════════════════════
    
    def on(self, event_name: str):
        """Decorator to register an event handler."""
        def decorator(func):
            if event_name not in self._handlers:
                self._handlers[event_name] = []
            self._handlers[event_name].append(func)
            return func
        return decorator

    def on_any(self):
        """Decorator to register a wildcard event handler."""
        return self.on("*")

    def emit(self, source: str, event: str, data: Dict[str, Any]):
        """Emit an event to registered handlers."""
        if self.dev_mode:
            logger.debug(f"[DevMode] Emit -> {source}:{event} | Data: {data}")
            
        handlers = self._handlers.get(event, []) + self._handlers.get("*", [])
        for handler in handlers:
            try:
                handler(source, event, data)
            except Exception as e:
                logger.error(f"Error in event handler for {event}: {e}")