"""SDK extraction pipeline — lightweight HTTP wrapper."""

from typing import Dict, List, Optional, TYPE_CHECKING
from loguru import logger

if TYPE_CHECKING:
    from .session import KnogginAsyncSession

class KnogginAsyncExtractor:
    """An extractor pointing to the Knoggin REST Server."""

    def __init__(self, session: "KnogginAsyncSession"):
        self.session = session
        self.client = session._client

    async def add(self, text: str, role: str = "user") -> Dict:
        """Add a message. In the heavy version, this writes to buffer. Here, we just log it."""
        logger.warning("add() is a no-op in the Light SDK. Messages are automatically extracted during chat(). Use extract() directly if needed.")
        return {"status": "ignored"}

    async def process_batch(self, messages: List[Dict]) -> Dict:
        """No-op. Processing is handled by server background jobs."""
        logger.warning("process_batch() is a no-op in the Light SDK as extraction runs asynchronously via the server.")
        return {"success": True}

    async def extract_mentions(self, text: str) -> List:
        """Not directly exposed in v1 simple routes, stubbing."""
        logger.warning("extract_mentions is currently unavailable in the v1 REST API.")
        return []

    async def trigger_extraction(self, content: str, user_msg_id: int) -> Dict:
        """Trigger fact extraction explicitly for a specific message."""
        try:
            url = f"/v1/extract/{self.session.session_id}"
            res = await self.client.http.post(url, json={
                "content": content,
                "user_msg_id": user_msg_id
            })
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"Extraction trigger failed: {e}")
            return {"status": "error", "error": str(e)}