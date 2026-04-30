import os
import asyncio
import json
import uuid
from loguru import logger
from datetime import datetime, timezone
from typing import Dict, List, Optional
from core.context import Context
from common.rag.file_rag import FileRAGService
from common.config.topics_config import TopicConfig
from common.schema.dtypes import AgentConfig
from common.infra.redis import RedisKeys
from common.infra.resources import ResourceManager
from common.config.base import load_config, get_config
from dotenv import load_dotenv

class AgentManager:
    def __init__(self, resources, user_name, active_sessions):
        self.resources = resources
        self.user_name = user_name
        self.active_sessions = active_sessions

    async def list_agents(self) -> List[AgentConfig]:
        """List all agents."""
        raw_agents = await self.resources.redis.hgetall(RedisKeys.agents(self.user_name))
        
        if not raw_agents:
            await self._seed_default_agents()
            raw_agents = await self.resources.redis.hgetall(RedisKeys.agents(self.user_name))
        
        agents = []
        for agent_id, data in raw_agents.items():
            try:
                agents.append(AgentConfig.from_dict(json.loads(data)))
            except json.JSONDecodeError:
                logger.warning(f"Malformed agent data for {agent_id}")
        return agents

    async def get_agent(self, agent_id: str) -> Optional[AgentConfig]:
        """Get agent by ID."""
        raw = await self.resources.redis.hget(RedisKeys.agents(self.user_name), agent_id)
        if not raw:
            return None
            
        try:
            data = json.loads(raw)
            return AgentConfig.from_dict(data)
        except json.JSONDecodeError:
            logger.warning(f"Malformed agent data for {agent_id}")
            return None

    async def get_agent_by_name(self, name: str) -> Optional[AgentConfig]:
        """Get agent by name (case-insensitive)."""
        agents = await self.list_agents()
        name_lower = name.lower()
        for agent in agents:
            if agent.name.lower() == name_lower:
                return agent
        return None

    async def get_default_agent_id(self) -> str:
        """Get default agent ID. Seeds defaults if none exist."""
        default_id = await self.resources.redis.get(RedisKeys.agents_default(self.user_name))
        
        if not default_id:
            await self._seed_default_agents()
            default_id = await self.resources.redis.get(RedisKeys.agents_default(self.user_name))
        
        return default_id

    async def create_agent(self, name: str, persona: str, instructions: Optional[str] = None, model: str = None, temperature: Optional[float] = 0.7, enabled_tools: Optional[List[str]] = None) -> AgentConfig:
        """Create a new agent."""
        agent_id = str(uuid.uuid4())
        config = AgentConfig(
            id=agent_id,
            name=name,
            persona=persona,
            instructions=instructions,
            model=model,
            temperature=temperature,
            enabled_tools=enabled_tools,
            is_default=False
        )
        
        await self.resources.redis.hset(
            RedisKeys.agents(self.user_name),
            agent_id,
            json.dumps(config.to_dict())
        )
        
        logger.info(f"Created agent: {name} ({agent_id})")
        return config

    async def update_agent(self, agent_id: str, name: str = None, persona: str = None, instructions: str = None, model: str = None, temperature: Optional[float] = None, enabled_tools: Optional[List[str]] = None) -> Optional[AgentConfig]:
        """Update an existing agent. Returns None if not found."""
        config = await self.get_agent(agent_id)
        if not config:
            return None
        
        if name is not None:
            config.name = name
        if persona is not None:
            config.persona = persona
        if instructions is not None:
            config.instructions = instructions
        if model is not None:
            config.model = model if model else None
        if temperature is not None:
            config.temperature = temperature
        if enabled_tools is not None:
            config.enabled_tools = enabled_tools
        
        await self.resources.redis.hset(
            RedisKeys.agents(self.user_name),
            agent_id,
            json.dumps(config.to_dict())
        )
        
        logger.info(f"Updated agent: {config.name} ({agent_id})")
        return config

    async def delete_agent(self, agent_id: str) -> bool:
        """Delete an agent. Returns False if not found or is default."""
        config = await self.get_agent(agent_id)
        if not config or config.is_default:
            return False
        
        await self.resources.redis.hdel(RedisKeys.agents(self.user_name), agent_id)
        logger.info(f"Deleted agent: {agent_id}")
        return True

    async def set_default_agent(self, agent_id: str) -> bool:
        """Set an agent as default. Returns False if not found."""
        new_default = await self.get_agent(agent_id)
        if not new_default:
            return False
        
        agents = await self.list_agents()
        for agent in agents:
            if agent.is_default and agent.id != agent_id:
                agent.is_default = False
                await self.resources.redis.hset(
                    RedisKeys.agents(self.user_name),
                    agent.id,
                    json.dumps(agent.to_dict())
                )
        
        new_default.is_default = True
        await self.resources.redis.hset(
            RedisKeys.agents(self.user_name),
            agent_id,
            json.dumps(new_default.to_dict())
        )
        await self.resources.redis.set(RedisKeys.agents_default(self.user_name), agent_id)
        
        logger.info(f"Set default agent: {agent_id}")
        return True

    async def _seed_default_agents(self):
        """Seed Redis with default agents."""
        agents_key = RedisKeys.agents(self.user_name)
        default_key = RedisKeys.agents_default(self.user_name)
        
        stella_id = str(uuid.uuid4())
        stella = AgentConfig(
            id=stella_id,
            name="STELLA",
            persona="Warm and direct. Match their energy. No corporate filler.",
            model=None,
            temperature=0.7,
            enabled_tools=None,
            is_default=True
        )
        
        await self.resources.redis.hset(agents_key, stella_id, json.dumps(stella.to_dict()))
        await self.resources.redis.set(default_key, stella_id)
        
        logger.info("Seeded default agents")

