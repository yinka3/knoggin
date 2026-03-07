import asyncio
from functools import partial
import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict

from agent.tools import Tools as BaseTools
from shared.config.base import get_config_value
from shared.utils.events import emit_community
from shared.infra.redis import RedisKeys
from db.community_store import CommunityStore
from shared.models.schema.dtypes import AgentConfig

class CommunityTools:
    """
    Restricted suite of tools specifically designed for Autonomous Agent Community (AAC) agents.
    Provides read access to the main knowledge graph, but restricts write operations 
    strictly to the community's isolated discussion space to prevent main graph contamination.
    """
    def __init__(self, user_name: str, base_tools: BaseTools, community_store: CommunityStore, discussion_id: str, agent_id: str):
        self.user_name = user_name
        self.base = base_tools
        self.community_store = community_store
        self.discussion_id = discussion_id
        self.agent_id = agent_id

    async def save_insight(self, content: str) -> Dict:
        """Saves a synthesized insight back to the community discussion stream."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            partial(
                self.community_store.add_message,
                discussion_id=self.discussion_id,
                agent_id="system",
                content=f"INSIGHT: {content}",
                role="insight"
            )
        )
        return {"saved": True, "type": "insight"}
    
    async def save_memory(self, content: str) -> Dict:
        """
        Saves a short-term working memory specifically for this active agent instance.
        Capped at 10 memories per sub-agent to force summarization over accumulation.
        """
        redis = self.base.redis
        key = RedisKeys.community_agent_memory(self.user_name, self.agent_id)
        existing = await redis.hgetall(key)
        if len(existing) >= 10:
            return {"error": "Memory full (10/10). No new memories can be saved."}
        mem_id = f"comm_mem_{uuid.uuid4().hex[:8]}"
        payload = json.dumps({
            "content": content,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "discussion_id": self.discussion_id
        })
        await redis.hset(key, mem_id, payload)
        return {"saved": True, "memory_id": mem_id}
    
    async def spawn_specialist(
        self, 
        name: str, 
        persona: str, 
        discussion_participants: List[str],
        initial_rules: List[str] = None,
        initial_preferences: List[str] = None,
        initial_icks: List[str] = None
    ) -> Dict:
        """Spawn a new specialist sub-agent. Max 3 per discussion."""
        spawned_count = sum(1 for p in discussion_participants if p.startswith("spawned_"))
        if spawned_count >= 3:
            return {"error": "Spawn limit reached. Max 3 sub-agents per discussion."}

        new_id = f"spawned_{uuid.uuid4().hex[:8]}"
        
        llm_config = get_config_value("llm", {})
        new_agent = AgentConfig(
            id=new_id,
            name=name,
            persona=persona,
            model=llm_config.get("agent_model"),
            is_spawned=True,
            spawned_by=self.agent_id
        )
        
        await self.base.redis.hset(
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
                    await self.base.redis.hset(key, mem_id, payload)
                    seeded_counts[category] += 1

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, 
            partial(self.community_store.register_agent_spawn, self.agent_id, new_id, persona)
        )

        discussion_participants.append(new_id)

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
