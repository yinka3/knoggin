import json
import uuid
from typing import Callable, Dict, List, NamedTuple, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.conf.topics_config import TopicConfig
from common.schema.memory import (
    DirectiveAddResult,
    DirectiveClearResult,
    DirectiveEntry,
    DirectiveListResult,
    DirectiveRemoveResult,
    MemoryEntry,
    MemoryForgetResult,
    MemoryListResult,
    MemorySaveResult,
)
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now_iso
from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.formatters import format_memory_context


class PromptStrings(NamedTuple):
    memory_ctx: str
    directives: str

MAX_BLOCK_SIZE = 10
MAX_CONTENT_LEN = 200
DIRECTIVES_CATEGORY = "directives"
DIRECTIVE_MODES = ("require", "prefer", "avoid")
DIRECTIVE_LABELS = {
    "require": "Required",
    "prefer": "Preferred",
    "avoid": "Avoid",
}


def format_directives_for_prompt(directives: List[DirectiveEntry]) -> str:
    sections = []
    for mode in DIRECTIVE_MODES:
        lines = [
            f"- {directive.content}"
            for directive in directives
            if directive.mode == mode and directive.content
        ]
        if lines:
            sections.append(f"{DIRECTIVE_LABELS[mode]}:\n" + "\n".join(lines))
    return "\n\n".join(sections)


class MemoryManager:
    """Owns all Redis-backed memory operations for a session/agent.

    Covers two tiers:
      - Session memory blocks: topic-scoped notes (save/forget/list)
      - Directives: agent-level behavioral guidance (add/remove/list/clear)

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

    # helpers

    def _fire(self, source: str, event: str, data: dict):
        if self._emit:
            try:
                self._emit(source, event, data)
            except Exception as e:
                logger.warning(f"MemoryManager event error: {e}")

    # SESSION MEMORY BLOCKS

    async def save_memory(
        self, content: str, topic: str = "General"
    ) -> MemorySaveResult:
        """Save a note to persistent session memory."""
        if not content or not content.strip():
            return MemorySaveResult(success=False, error="Empty memory content")

        content = content.strip()
        if len(content) > MAX_CONTENT_LEN:
            return MemorySaveResult(
                success=False,
                error=(
                    f"Memory too long ({len(content)} chars). "
                    f"Max {MAX_CONTENT_LEN}. Condense and retry."
                ),
            )

        normalized = self.topic_config.normalize_topic(topic) if topic else None
        if not normalized:
            active_list = ", ".join(self.topic_config.active_topics)
            if not active_list:
                return MemorySaveResult(
                    success=False, error="No active topics available."
                )
            return MemorySaveResult(
                success=False,
                error=f"Topic '{topic}' is invalid. Active topics are: {active_list}"
            )

        if normalized not in self.topic_config.active_topics:
            return MemorySaveResult(
                success=False, error=f"Topic '{topic}' is not active."
            )

        key = RedisKeys.session_memory(self.user_name, self.session_id, normalized)
        existing = await self.redis.hgetall(key)
        if len(existing) >= MAX_BLOCK_SIZE:
            return MemorySaveResult(
                success=False,
                error=(
                    f"Memory block '{normalized}' is full "
                    f"({MAX_BLOCK_SIZE}/{MAX_BLOCK_SIZE}). "
                    "Use forget_memory to remove outdated entries first."
                ),
            )

        mem_id = f"mem_{uuid.uuid4().hex[:8]}"
        payload = json.dumps(
            {
                "content": content,
                "topic": normalized,
                "created_at": get_now_iso(),
                "source_session": self.session_id,
            }
        )
        await self.redis.hset(key, mem_id, payload)

        self._fire("agent", "memory_saved", {"topic": normalized, "memory_id": mem_id})
        return MemorySaveResult(
            success=True,
            memory_id=mem_id,
            topic=normalized,
            content=content,
        )

    async def forget_memory(self, memory_id: str) -> MemoryForgetResult:
        """Remove a session memory block by ID (searches all topics)."""
        if not memory_id:
            return MemoryForgetResult(success=False, error="No memory_id provided")

        all_topics = list(
            set(self.topic_config.active_topics + list(self.topic_config.raw.keys()))
        )

        pipe = self.redis.pipeline()
        for topic in all_topics:
            key = RedisKeys.session_memory(self.user_name, self.session_id, topic)
            pipe.hdel(key, memory_id)

        results = await pipe.execute()

        for idx, removed in enumerate(results):
            if removed:
                matched_topic = all_topics[idx]
                self._fire(
                    "agent",
                    "memory_forgotten",
                    {
                        "topic": matched_topic,
                        "memory_id": memory_id,
                    },
                )
                return MemoryForgetResult(
                    success=True,
                    memory_id=memory_id,
                    topic=matched_topic,
                )

        return MemoryForgetResult(
            success=False,
            error=f"Memory '{memory_id}' not found in any block",
        )

    async def get_memory_blocks(
        self,
        hot_topics: List[str] = None,
    ) -> MemoryListResult:
        """Fetch requested active session memory blocks."""
        topics_to_fetch: List[str] = []
        for t in hot_topics or []:
            normalized = self.topic_config.alias_lookup.get(t.lower()) if t else None
            if (
                normalized
                and self.topic_config.is_active(normalized)
                and normalized not in topics_to_fetch
            ):
                topics_to_fetch.append(normalized)

        blocks: Dict[str, List[MemoryEntry]] = {}
        for topic in topics_to_fetch:
            key = RedisKeys.session_memory(self.user_name, self.session_id, topic)
            raw = await self.redis.hgetall(key)
            if not raw:
                continue

            entries = []
            for mem_id, payload in raw.items():
                data = safe_json_loads(payload)
                if data and isinstance(data, dict):
                    entries.append(
                        MemoryEntry(
                            id=mem_id,
                            content=data.get("content", ""),
                            topic=data.get("topic", topic),
                            created_at=data.get("created_at", ""),
                        )
                    )
                else:
                    logger.warning(f"Corrupt memory block {mem_id} in {topic}")
            entries.sort(key=lambda e: e.created_at)
            blocks[topic] = entries

        total = sum(len(v) for v in blocks.values())
        return MemoryListResult(blocks=blocks, total=total)

    # DIRECTIVES

    def _directive_key(self) -> str:
        return RedisKeys.agent_directives(self.user_name, self.agent_id)

    def _validate_directive_mode(self, mode: str) -> Optional[str]:
        normalized = (mode or "").strip().lower()
        return normalized if normalized in DIRECTIVE_MODES else None

    async def add_directive(
        self,
        mode: str,
        content: str,
    ) -> DirectiveAddResult:
        """Add agent behavioral guidance."""
        normalized_mode = self._validate_directive_mode(mode)
        if normalized_mode is None:
            return DirectiveAddResult(
                success=False,
                error=f"Invalid directive mode. Must be one of: {DIRECTIVE_MODES}",
            )

        if not content or not content.strip():
            return DirectiveAddResult(success=False, error="Empty directive content")

        content = content.strip()
        if len(content) > MAX_CONTENT_LEN:
            return DirectiveAddResult(
                success=False,
                error=(
                    f"Directive too long ({len(content)} chars). "
                    f"Max {MAX_CONTENT_LEN}."
                ),
            )

        directive_id = f"directive_{uuid.uuid4().hex[:8]}"
        payload = json.dumps(
            {
                "mode": normalized_mode,
                "content": content,
                "created_at": get_now_iso(),
            }
        )
        await self.redis.hset(self._directive_key(), directive_id, payload)

        self._fire(
            "agent",
            "directive_added",
            {
                "mode": normalized_mode,
                "directive_id": directive_id,
            },
        )
        return DirectiveAddResult(
            success=True,
            directive_id=directive_id,
            mode=normalized_mode,
            content=content,
        )

    async def remove_directive(self, directive_id: str) -> DirectiveRemoveResult:
        """Remove a directive by its internal storage ID."""
        deleted = await self.redis.hdel(self._directive_key(), directive_id)
        if not deleted:
            return DirectiveRemoveResult(
                success=False,
                error=f"Directive '{directive_id}' not found",
            )

        self._fire(
            "agent",
            "directive_removed",
            {
                "directive_id": directive_id,
            },
        )
        return DirectiveRemoveResult(success=True, directive_id=directive_id)

    async def list_directives(self, mode: str = None) -> DirectiveListResult:
        """List directives, optionally filtered by mode."""
        normalized_mode = None
        if mode is not None:
            normalized_mode = self._validate_directive_mode(mode)
            if normalized_mode is None:
                return DirectiveListResult()

        raw = await self.redis.hgetall(self._directive_key())
        directives: List[DirectiveEntry] = []
        for directive_id, payload in (raw or {}).items():
            data = safe_json_loads(payload)
            if not data or not isinstance(data, dict):
                logger.warning(f"Corrupt directive {directive_id}")
                continue

            directive_mode = self._validate_directive_mode(data.get("mode"))
            if directive_mode is None:
                logger.warning(f"Directive {directive_id} has invalid mode")
                continue
            if normalized_mode is not None and directive_mode != normalized_mode:
                continue

            directives.append(
                DirectiveEntry(
                    directive_id=directive_id,
                    mode=directive_mode,
                    content=data.get("content", ""),
                    created_at=data.get("created_at", ""),
                )
            )

        directives.sort(key=lambda e: e.created_at)
        return DirectiveListResult(directives=directives)

    async def clear_directives(self, mode: str = None) -> DirectiveClearResult:
        """Clear directives. Pass mode to remove only one mode."""
        normalized_mode = None
        if mode is not None:
            normalized_mode = self._validate_directive_mode(mode)
            if normalized_mode is None:
                return DirectiveClearResult(
                    success=False,
                    error=f"Invalid directive mode. Must be one of: {DIRECTIVE_MODES}",
                )

        key = self._directive_key()
        if normalized_mode is None:
            count = await self.redis.hlen(key)
            await self.redis.delete(key)
        else:
            result = await self.list_directives(normalized_mode)
            count = 0
            for directive in result.directives:
                count += int(await self.redis.hdel(key, directive.directive_id))

        self._fire(
            "agent",
            "directives_cleared",
            {
                "mode": normalized_mode or "",
                "cleared": count,
            },
        )
        return DirectiveClearResult(
            success=True,
            cleared=count,
            mode=normalized_mode or "",
        )

    async def load_prompt_strings(
        self,
        hot_topics: List[str] = None,
    ) -> PromptStrings:
        """Load all memory as formatted strings for prompt injection.

        Returns PromptStrings(memory_ctx, directives).
        Caller wraps these into whatever context object they need
        (SDK uses PromptContext, server uses loose variables).
        """
        blocks = await self.get_memory_blocks(hot_topics)
        raw_blocks = {
            topic: [
                {"id": e.id, "content": e.content, "created_at": e.created_at}
                for e in entries
            ]
            for topic, entries in blocks.blocks.items()
        }
        memory_ctx = format_memory_context(raw_blocks)

        directives = await self.load_directive_string()

        return PromptStrings(memory_ctx=memory_ctx, directives=directives)

    async def load_directive_string(self) -> str:
        """Load all directives as a prompt-ready grouped string."""
        result = await self.list_directives()
        return format_directives_for_prompt(result.directives)

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

    async def get_memory_blocks_dict(
        self, hot_topics: List[str] = None
    ) -> Dict[str, List[Dict]]:
        """get_memory_blocks returning raw dicts — used by the tool dispatch path."""
        result = await self.get_memory_blocks(hot_topics)
        return {
            topic: [
                {"id": e.id, "content": e.content, "created_at": e.created_at}
                for e in entries
            ]
            for topic, entries in result.blocks.items()
        }
