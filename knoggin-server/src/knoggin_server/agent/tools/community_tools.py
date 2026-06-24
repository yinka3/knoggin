import json
import uuid
from typing import Dict, List

from common.conf.manager import ConfigManager
from common.schema.aac_schema import AAC_DEFAULT_ENABLED_TOOLS
from common.schema.agent_contracts import AgentConfig
from common.utils.events import emit_community
from common.utils.time_utils import get_now_iso
from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.tools.registry import Tools
from knoggin_server.community.community_store import CommunityStore
from knoggin_server.knowledge.services.memory_service import (
    DIRECTIVE_MODES,
    MemoryManager,
)

MAX_SPAWNED_SPECIALISTS = 10


class CommunityTools(Tools):
    """
    Restricted suite of tools for Autonomous Agent Community (AAC) agents.
    Inherits from core Tools for read access, but restricts write operations
    strictly to the community's isolated discussion space.
    """

    def __init__(
        self,
        user_name: str,
        base_tools: Tools,
        community_store: CommunityStore,
        discussion_id: str,
        agent_id: str,
        memory_mgr: MemoryManager,
        participants: List[str] = None,
    ):
        super().__init__(
            user_name=user_name,
            entities=base_tools.entities,
            session_id=base_tools.session_id,
            topic_config=base_tools.topic_config,
            search_config=base_tools.search_cfg,
            file_rag=base_tools.file_rag,
            memory=memory_mgr,
            knowledge_store=base_tools.knowledge_store,
            redis=base_tools.redis,
        )
        self.readable_project_ids = list(base_tools.readable_project_ids)
        self.community_store = community_store
        self.discussion_id = discussion_id
        self.agent_id = agent_id
        self.current_participants = participants or []

    async def save_insight(self, content: str) -> Dict:
        """Saves a synthesized insight back to the community discussion stream."""
        await self.community_store.add_message(
            discussion_id=self.discussion_id,
            agent_id="system",
            content=f"INSIGHT: {content}",
            role="insight",
        )
        return {"saved": True, "type": "insight"}

    async def save_memory(self, content: str, topic: str = "General") -> Dict:
        """
        Saves persistent AAC memory for this user and agent across discussions.
        The ten-entry lifetime cap forces summarization over accumulation.
        """
        key = RedisKeys.community_agent_memory(self.user_name, self.agent_id)
        count = await self.redis.hlen(key)
        if count >= 10:
            return {"error": "Memory full (10/10). No new memories can be saved."}
        mem_id = f"comm_mem_{uuid.uuid4().hex[:8]}"
        payload = json.dumps(
            {
                "content": content,
                "topic": topic,
                "created_at": get_now_iso(),
                "discussion_id": self.discussion_id,
            }
        )
        await self.redis.hset(key, mem_id, payload)
        return {"saved": True, "memory_id": mem_id}

    async def spawn_specialist(
        self,
        name: str,
        persona: str,
        initial_directives: List[Dict] = None,
    ) -> Dict:
        """Spawn a new specialist sub-agent."""
        spawned_count = await self._count_spawned_participants()
        if spawned_count >= MAX_SPAWNED_SPECIALISTS:
            return {
                "error": (
                    "Spawn limit reached. Max "
                    f"{MAX_SPAWNED_SPECIALISTS} sub-agents per discussion."
                )
            }

        new_id = f"spawned_{uuid.uuid4().hex[:8]}"

        llm_config = ConfigManager.get().config.llm
        new_agent = AgentConfig(
            id=new_id,
            name=name,
            persona=persona,
            model=llm_config.agent_model,
            enabled_tools=AAC_DEFAULT_ENABLED_TOOLS,
            is_spawned=True,
            spawned_by=self.agent_id,
        )

        await self.redis.hset(
            RedisKeys.agents(self.user_name), new_id, json.dumps(new_agent.to_dict())
        )

        now = get_now_iso()
        seeded_directives = 0
        directives_key = RedisKeys.agent_directives(self.user_name, new_id)
        for directive in initial_directives or []:
            if not isinstance(directive, dict):
                continue
            mode = (directive.get("mode") or "").strip().lower()
            content = (directive.get("content") or "").strip()
            if mode not in DIRECTIVE_MODES or not content:
                continue

            directive_id = f"directive_{uuid.uuid4().hex[:8]}"
            payload = json.dumps(
                {
                    "mode": mode,
                    "content": content,
                    "created_at": now,
                    "seeded_by": self.agent_id,
                }
            )
            await self.redis.hset(directives_key, directive_id, payload)
            seeded_directives += 1

        await self.community_store.register_agent_spawn(self.agent_id, new_id, persona)
        self.current_participants.append(new_id)

        await emit_community(
            self.user_name,
            "community",
            "agent_spawned",
            {
                "discussion_id": self.discussion_id,
                "parent_agent_id": self.agent_id,
                "agent_id": new_id,
                "name": name,
                "persona": persona,
                "seeded_directives": seeded_directives,
            },
        )

        return {
            "id": new_id,
            "message": f"Spawned {name} and added to discussion pool.",
            "seeded_directives": seeded_directives,
        }

    async def _count_spawned_participants(self) -> int:
        count = 0
        for agent_id in self.current_participants:
            raw = await self.redis.hget(RedisKeys.agents(self.user_name), agent_id)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
            if data.get("is_spawned"):
                count += 1
        return count
