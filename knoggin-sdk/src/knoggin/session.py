"""Lightweight SDK session representation."""

from typing import TYPE_CHECKING, Optional, List, Dict, Any
from loguru import logger

if TYPE_CHECKING:
    from .async_client import KnogginAsyncClient
    
class KnogginAsyncSession:
    """A session pointing to the Knoggin REST Server."""

    def __init__(self, session_id: str, user_name: str, client: "KnogginAsyncClient"):
        self.session_id = session_id
        self.user_name = user_name
        self._client = client

    @property
    def agent(self):
        """Lazy-loaded KnogginAsyncAgent."""
        if not hasattr(self, "_agent"):
            from knoggin.agent_sdk import KnogginAsyncAgent
            self._agent = KnogginAsyncAgent(session=self)
        return self._agent

    @property
    def extractor(self):
        """Lazy-loaded KnogginAsyncExtractor."""
        if not hasattr(self, "_extractor"):
            from knoggin.extraction import KnogginAsyncExtractor
            self._extractor = KnogginAsyncExtractor(session=self)
        return self._extractor

    @property
    def topics(self):
        """Topic management manager for this session."""
        if not hasattr(self, "_topics"):
            from .managers import TopicAsyncManager
            self._topics = TopicAsyncManager(client=self._client, session_id=self.session_id)
        return self._topics

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "_client", None) and self._client.dev_mode:
            logger.info(f"[DevMode] Session context exited: {self.session_id}")

    async def update(
        self,
        title: Optional[str] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None
    ) -> bool:
        """Update session metadata."""
        payload = {
            "title": title,
            "model": model,
            "agent_id": agent_id,
            "enabled_tools": enabled_tools
        }
        res = await self._client.http.patch(
            f"/v1/sessions/{self.session_id}",
            json={k: v for k, v in payload.items() if v is not None}
        )
        res.raise_for_status()
        return res.json().get("success", False)

    async def delete(self, force: bool = False) -> bool:
        """Delete this session."""
        res = await self._client.http.delete(f"/v1/sessions/{self.session_id}", params={"force": force})
        res.raise_for_status()
        return res.json().get("success", False)

    async def export(self) -> Dict[str, Any]:
        """Export session conversation history."""
        res = await self._client.http.get(f"/v1/sessions/{self.session_id}/export")
        res.raise_for_status()
        return res.json()

    # No background jobs or local processing needed here!