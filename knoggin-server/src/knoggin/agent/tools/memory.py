from typing import Dict, List


class MemoryToolsMixin:
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

    async def save_insight(self, content: str) -> Dict:
        return {"error": "save_insight is only available in community discussions."}

    async def spawn_specialist(
        self,
        name: str,
        persona: str,
        initial_rules: List[str] = None,
        initial_preferences: List[str] = None,
        initial_icks: List[str] = None,
    ) -> Dict:
        return {"error": "spawn_specialist is only available in community discussions."}

    def _is_message_id(msg_id) -> bool:
        """Check if numeric ID belongs to message collection or turn collection."""
        if isinstance(msg_id, str):
            return msg_id.startswith("msg_")
        return msg_id < 1_000_000_000

    def _format_message_id(msg_id) -> str:
        """Format an ID as a string for message/turn reference."""
        if isinstance(msg_id, str):
            return msg_id
        return (
            f"msg_{msg_id}"
            if msg_id < 1_000_000_000
            else f"turn_{msg_id - 1_000_000_000}"
        )
