from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    import redis.asyncio as aioredis

    from infrastructure.graph_interface import GraphInterface
    from knoggin_server.knowledge.services.entity_service import EntityManager
    from knoggin_server.knowledge.services.memory_service import MemoryManager


class MemoryTools:
    redis: aioredis.Redis
    graph_client: GraphInterface
    entities: EntityManager
    memory: Optional[MemoryManager]


    async def save_memory(self, content: str, topic: str = "General") -> Dict:
        """Save a note to persistent session memory."""
        if self.memory:
            return await self.memory.save_memory_dict(content, topic)

        return {"error": "No memory manager configured"}

    async def forget_memory(self, memory_id: str) -> Dict:
        """Remove a memory by ID."""
        if self.memory:
            return await self.memory.forget_memory_dict(memory_id)
        return {"error": "No memory manager configured"}

    async def get_memory_blocks(
        self, hot_topics: List[str] = None
    ) -> Dict[str, List[Dict]]:
        """Fetch memory blocks for prompt injection."""
        if self.memory:
            return await self.memory.get_memory_blocks_dict(hot_topics)
        return {}

    async def save_insight(self, content: str) -> Dict:
        return {"error": "save_insight is only available in community discussions."}

    async def spawn_specialist(
        self,
        name: str,
        persona: str,
        initial_directives: List[Dict] = None,
    ) -> Dict:
        return {"error": "spawn_specialist is only available in community discussions."}

    @staticmethod
    def _is_message_id(msg_id) -> bool:
        """Check whether the value identifies a canonical message."""
        if isinstance(msg_id, str):
            return msg_id.startswith("msg_")
        return isinstance(msg_id, int)

    @staticmethod
    def _format_message_id(msg_id) -> str:
        """Format a canonical message ID."""
        if isinstance(msg_id, str):
            return msg_id
        return f"msg_{msg_id}"
