import asyncio
from functools import partial
import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict

from agent.tools.registry import Tools
from common.config.base import get_config_value, get_config
from common.utils.events import emit_community
from common.infra.redis import RedisKeys
from db.community_store import CommunityStore
from common.schema.dtypes import AgentConfig
from services.memory_manager import MemoryManager

class CommunityTools(Tools):
    """
    Restricted suite of tools specifically designed for Autonomous Agent Community (AAC) agents.
    Inherits from core Tools for read access, but restricts write operations 
    strictly to the community's isolated discussion space.
    """
    def __init__(self, user_name: str, base_tools: Tools, community_store: CommunityStore, discussion_id: str, agent_id: str, memory_mgr: MemoryManager, participants: List[str] = None):
        super().__init__(
            user_name=user_name,
            store=base_tools.store,
            ent_resolver=base_tools.resolver,
            redis_client=base_tools.redis,
            session_id=base_tools.session_id,
            topic_config=base_tools.topic_config,
            search_config=base_tools.search_cfg,
            file_rag=base_tools.file_rag,
            mcp_manager=base_tools.mcp_manager,
            memory=memory_mgr
        )
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
            role="insight"
        )
        return {"saved": True, "type": "insight"}
    
    async def save_memory(self, content: str) -> Dict:
        """
        Saves a short-term working memory specifically for this active agent instance.
        Capped at 10 memories per sub-agent to force summarization over accumulation.
        """
        key = RedisKeys.community_agent_memory(self.user_name, self.agent_id)
        count = await self.redis.hlen(key)
        if count >= 10:
            return {"error": "Memory full (10/10). No new memories can be saved."}
        mem_id = f"comm_mem_{uuid.uuid4().hex[:8]}"
        payload = json.dumps({
            "content": content,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "discussion_id": self.discussion_id
        })
        await self.redis.hset(key, mem_id, payload)
        return {"saved": True, "memory_id": mem_id}
    
    async def spawn_specialist(
        self, 
        name: str, 
        persona: str, 
        initial_rules: List[str] = None,
        initial_preferences: List[str] = None,
        initial_icks: List[str] = None
    ) -> Dict:
        """Spawn a new specialist sub-agent. Max 3 per discussion."""
        spawned_count = sum(1 for p in self.current_participants if p.startswith("spawned_"))
        if spawned_count >= 3:
            return {"error": "Spawn limit reached. Max 3 sub-agents per discussion."}

        new_id = f"spawned_{uuid.uuid4().hex[:8]}"
        self.current_participants.append(new_id)
        
        llm_config = get_config().llm
        new_agent = AgentConfig(
            id=new_id,
            name=name,
            persona=persona,
            model=llm_config.agent_model,
            is_spawned=True,
            spawned_by=self.agent_id
        )
        
        await self.redis.hset(
            RedisKeys.agents(self.user_name),
            new_id,
            json.dumps(new_agent.to_dict())
        )

        now = datetime.now(timezone.utc).isoformat()
        seeded_counts = {"rules": 0, "preferences": 0, "icks": 0}
        
        initial_data = {
            "rules": initial_rules or [],
            "preferences": initial_preferences or [],
            "icks": initial_icks or []
        }
        
        for category, items in initial_data.items():
            if items:
                key = RedisKeys.agent_working_memory(new_id, category)
                for content in items:
                    mem_id = f"mem_{uuid.uuid4().hex[:8]}"
                    payload = json.dumps({
                        "content": content,
                        "created_at": now,
                        "seeded_by": self.agent_id
                    })
                    await self.redis.hset(key, mem_id, payload)
                    seeded_counts[category] += 1

        await self.community_store.register_agent_spawn(self.agent_id, new_id, persona)

        await emit_community(self.user_name, "community", "agent_spawned", {
            "discussion_id": self.discussion_id,
            "parent_agent_id": self.agent_id,
            "agent_id": new_id,
            "name": name,
            "persona": persona,
            "seeded_memory": seeded_counts
        })

        return {
            "id": new_id, 
            "message": f"Spawned {name} and added to discussion pool.",
            "seeded_memory": seeded_counts
        }
