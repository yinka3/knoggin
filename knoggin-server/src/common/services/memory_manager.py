import json
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from loguru import logger

from common.infra.redis import RedisKeys
from common.config.topics_config import TopicConfig
from common.schema.memory import (
    MemoryEntry,
    MemorySaveResult,
    MemoryForgetResult,
    MemoryListResult,
    WorkingMemoryEntry,
    WorkingMemoryAddResult,
    WorkingMemoryRemoveResult,
    WorkingMemoryListResult,
    WorkingMemoryClearResult,
)


MEMORY_CATEGORIES = ("rules", "preferences", "icks")
MAX_BLOCK_SIZE = 10
MAX_CONTENT_LEN = 200


class MemoryManager:
    """Owns all Redis-backed memory operations for a session/agent.

    Covers two tiers:
      - Session memory blocks: topic-scoped notes (save/forget/list)
      - Working memory: agent-level rules/preferences/icks (add/remove/list/clear)

    Accepts an optional event emitter so callers (SDK, server) can plug in
    their own telemetry without the manager importing from either side.
    """

    def __init__(
        self,
        redis: aioredis.Redis,
        user_name: str,
        session_id: str,
        agent_id: str,
        topic_config: TopicConfig,
        on_event: Optional[Callable] = None,
    ):
        self.redis = redis
        self.user_name = user_name
        self.session_id = session_id
        self.agent_id = agent_id
        self.topic_config = topic_config
        self._emit = on_event  # (source, event, data) -> None

    # ── helpers ──────────────────────────────────────────────

    def _fire(self, source: str, event: str, data: dict):
        if self._emit:
            try:
                self._emit(source, event, data)
            except Exception as e:
                logger.warning(f"MemoryManager event error: {e}")

    # ════════════════════════════════════════════════════════
    #  SESSION MEMORY BLOCKS
    # ════════════════════════════════════════════════════════

    async def save_memory(self, content: str, topic: str = "General") -> MemorySaveResult:
        """Save a note to persistent session memory."""
        if not content or not content.strip():
            return MemorySaveResult(success=False, error="Empty memory content")

        content = content.strip()
        if len(content) > MAX_CONTENT_LEN:
            return MemorySaveResult(
                success=False,
                error=f"Memory too long ({len(content)} chars). Max {MAX_CONTENT_LEN}. Condense and retry.",
            )

        normalized = self.topic_config.normalize_topic(topic) if topic else None
        if not normalized:
            active = self.topic_config.active_topics
            normalized = active[0] if active else None
            if not normalized:
                return MemorySaveResult(success=False, error="No active topics available.")

        if normalized not in self.topic_config.active_topics:
            return MemorySaveResult(
                success=False, error=f"Topic '{topic}' is not active."
            )

        key = RedisKeys.agent_memory(self.user_name, self.session_id, normalized)
        existing = await self.redis.hgetall(key)
        if len(existing) >= MAX_BLOCK_SIZE:
            return MemorySaveResult(
                success=False,
                error=f"Memory block '{normalized}' is full ({MAX_BLOCK_SIZE}/{MAX_BLOCK_SIZE}). "
                       "Use forget_memory to remove outdated entries first.",
            )

        mem_id = f"mem_{uuid.uuid4().hex[:8]}"
        payload = json.dumps({
            "content": content,
            "topic": normalized,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_session": self.session_id,
        })
        await self.redis.hset(key, mem_id, payload)

        self._fire("agent", "memory_saved", {"topic": normalized, "memory_id": mem_id})
        return MemorySaveResult(
            success=True, memory_id=mem_id, topic=normalized, content=content,
        )

    async def forget_memory(self, memory_id: str) -> MemoryForgetResult:
        """Remove a session memory block by ID (searches all topics)."""
        if not memory_id:
            return MemoryForgetResult(success=False, error="No memory_id provided")

        all_topics = list(set(
            self.topic_config.active_topics + list(self.topic_config.raw.keys())
        ))

        pipe = self.redis.pipeline()
        for topic in all_topics:
            key = RedisKeys.agent_memory(self.user_name, self.session_id, topic)
            pipe.hdel(key, memory_id)
            
        results = await pipe.execute()
        
        for idx, removed in enumerate(results):
            if removed:
                matched_topic = all_topics[idx]
                self._fire("agent", "memory_forgotten", {
                    "topic": matched_topic, "memory_id": memory_id,
                })
                return MemoryForgetResult(
                    success=True, memory_id=memory_id, topic=matched_topic,
                )

        return MemoryForgetResult(
            success=False, error=f"Memory '{memory_id}' not found in any block",
        )

    async def get_memory_blocks(
        self, hot_topics: List[str] = None,
    ) -> MemoryListResult:
        """Fetch session memory blocks. Always includes General + hot topics."""
        topics_to_fetch: List[str] = []
        if self.topic_config.is_active("General"):
            topics_to_fetch.append("General")
        for t in (hot_topics or []):
            if t not in topics_to_fetch:
                topics_to_fetch.append(t)

        blocks: Dict[str, List[MemoryEntry]] = {}
        for topic in topics_to_fetch:
            key = RedisKeys.agent_memory(self.user_name, self.session_id, topic)
            raw = await self.redis.hgetall(key)
            if not raw:
                continue

            entries = []
            for mem_id, payload in raw.items():
                try:
                    data = json.loads(payload)
                    entries.append(MemoryEntry(
                        id=mem_id,
                        content=data["content"],
                        topic=data.get("topic", topic),
                        created_at=data.get("created_at", ""),
                    ))
                except json.JSONDecodeError:
                    logger.warning(f"Corrupt memory block {mem_id} in {topic}")
            entries.sort(key=lambda e: e.created_at)
            blocks[topic] = entries

        total = sum(len(v) for v in blocks.values())
        return MemoryListResult(blocks=blocks, total=total)

    # ════════════════════════════════════════════════════════
    #  WORKING MEMORY (rules, preferences, icks)
    # ════════════════════════════════════════════════════════

    async def add_working_memory(
        self, category: str, content: str,
    ) -> WorkingMemoryAddResult:
        """Add entry to a working memory category."""
        if category not in MEMORY_CATEGORIES:
            return WorkingMemoryAddResult(
                success=False,
                error=f"Invalid category. Must be one of: {MEMORY_CATEGORIES}",
            )

        mem_id = f"mem_{uuid.uuid4().hex[:8]}"
        key = RedisKeys.agent_working_memory(self.agent_id, category)
        payload = json.dumps({
            "content": content,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        await self.redis.hset(key, mem_id, payload)

        self._fire("agent", "working_memory_added", {
            "category": category, "memory_id": mem_id,
        })
        return WorkingMemoryAddResult(
            success=True, memory_id=mem_id, content=content, category=category,
        )

    async def remove_working_memory(
        self, category: str, memory_id: str,
    ) -> WorkingMemoryRemoveResult:
        """Remove a working memory entry."""
        if category not in MEMORY_CATEGORIES:
            return WorkingMemoryRemoveResult(
                success=False,
                error=f"Invalid category. Must be one of: {MEMORY_CATEGORIES}",
            )

        key = RedisKeys.agent_working_memory(self.agent_id, category)
        deleted = await self.redis.hdel(key, memory_id)
        if not deleted:
            return WorkingMemoryRemoveResult(
                success=False,
                error=f"Memory '{memory_id}' not found in {category}",
            )

        self._fire("agent", "working_memory_removed", {
            "category": category, "memory_id": memory_id,
        })
        return WorkingMemoryRemoveResult(
            success=True, memory_id=memory_id, category=category,
        )

    async def list_working_memory(
        self, category: str = None,
    ) -> WorkingMemoryListResult:
        """List working memory. Pass category or None for all."""
        categories = [category] if category else list(MEMORY_CATEGORIES)
        blocks: Dict[str, List[WorkingMemoryEntry]] = {}

        for cat in categories:
            if cat not in MEMORY_CATEGORIES:
                continue
            key = RedisKeys.agent_working_memory(self.agent_id, cat)
            raw = await self.redis.hgetall(key)

            entries = []
            if raw:
                for mem_id, payload in raw.items():
                    try:
                        data = json.loads(payload)
                        entries.append(WorkingMemoryEntry(
                            id=mem_id,
                            content=data["content"],
                            created_at=data.get("created_at", ""),
                        ))
                    except json.JSONDecodeError:
                        logger.warning(f"Corrupt working memory {mem_id} in {cat}")
                entries.sort(key=lambda e: e.created_at)
            blocks[cat] = entries

        return WorkingMemoryListResult(blocks=blocks)

    async def clear_working_memory(self, category: str) -> WorkingMemoryClearResult:
        """Clear all entries in a working memory category."""
        if category not in MEMORY_CATEGORIES:
            return WorkingMemoryClearResult(
                success=False,
                error=f"Invalid category. Must be one of: {MEMORY_CATEGORIES}",
            )

        key = RedisKeys.agent_working_memory(self.agent_id, category)
        count = await self.redis.hlen(key)
        await self.redis.delete(key)

        self._fire("agent", "working_memory_cleared", {
            "category": category, "cleared": count,
        })
        return WorkingMemoryClearResult(
            success=True, cleared=count, category=category,
        )

    # ════════════════════════════════════════════════════════
    #  PROMPT CONTEXT LOADING
    # ════════════════════════════════════════════════════════

    async def load_prompt_strings(
        self, hot_topics: List[str] = None,
    ) -> Tuple[str, str, str, str]:
        """Load all memory as formatted strings for prompt injection.

        Returns (memory_ctx, rules, prefs, icks).
        Caller wraps these into whatever context object they need
        (SDK uses PromptContext, server uses loose variables).
        """
        from agent.formatters import format_memory_context

        blocks = await self.get_memory_blocks(hot_topics)
        raw_blocks = {
            topic: [{"id": e.id, "content": e.content, "created_at": e.created_at} for e in entries]
            for topic, entries in blocks.blocks.items()
        }
        memory_ctx = format_memory_context(raw_blocks)

        rules, prefs, icks = await self._load_working_memory_strings()

        return memory_ctx, rules, prefs, icks

    async def _load_working_memory_strings(self) -> Tuple[str, str, str]:
        """Load working memory as formatted strings for prompt injection."""
        result = []
        for category in MEMORY_CATEGORIES:
            key = RedisKeys.agent_working_memory(self.agent_id, category)
            raw = await self.redis.hgetall(key)
            if raw:
                entries = []
                for v in raw.values():
                    try:
                        entries.append(f"- {json.loads(v)['content']}")
                    except json.JSONDecodeError:
                        continue
                result.append("\n".join(entries))
            else:
                result.append("")
        return tuple(result)

    # ════════════════════════════════════════════════════════
    #  DICT RETURNS (for tool dispatch compatibility)
    # ════════════════════════════════════════════════════════

    async def save_memory_dict(self, content: str, topic: str = "General") -> dict:
        """save_memory returning a raw dict — used by the tool dispatch path."""
        r = await self.save_memory(content, topic)
        if not r.success:
            return {"error": r.error}
        return {
            "saved": True,
            "memory_id": r.memory_id,
            "topic": r.topic,
            "content": r.content,
        }

    async def forget_memory_dict(self, memory_id: str) -> dict:
        """forget_memory returning a raw dict — used by the tool dispatch path."""
        r = await self.forget_memory(memory_id)
        if not r.success:
            return {"error": r.error}
        return {"removed": True, "memory_id": r.memory_id, "topic": r.topic}

    async def get_memory_blocks_dict(self, hot_topics: List[str] = None) -> Dict[str, List[Dict]]:
        """get_memory_blocks returning raw dicts — used by the tool dispatch path."""
        result = await self.get_memory_blocks(hot_topics)
        return {
            topic: [{"id": e.id, "content": e.content, "created_at": e.created_at} for e in entries]
            for topic, entries in result.blocks.items()
        }