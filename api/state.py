from datetime import datetime, timezone
import json
from typing import Dict, List, Optional
import uuid

from loguru import logger
from main.context import Context
from shared.topics_config import TopicConfig
from shared.schema.dtypes import AgentConfig
from shared.redisclient import RedisKeys
from shared.resource import ResourceManager
from shared.config import load_config
from dotenv import load_dotenv

load_dotenv()

class AppState:

    def __init__(self, resources: ResourceManager, active_sessions: Dict[str, Context], user_name: str):
        self.resources = resources
        self.active_sessions = active_sessions
        self.user_name = user_name
    
    async def list_sessions(self) -> Dict[str, dict]:
        try:
            raw = await self.resources.redis.hgetall(RedisKeys.sessions(self.user_name))
            return {sid: json.loads(data) for sid, data in raw.items()}
        except Exception as e:
            logger.warning(f"Failed to list sessions: {e}")
            return {}

    async def create_session(self, topics_config: dict = None, model: str = None, agent_id: str = None) -> Context:
        session_id = str(uuid.uuid4())
        
        if topics_config is None:
            config = load_config()
            topics_config = config.get("default_topics") if config else None
            
            if not topics_config:
                topics_config = {
                    "General": {
                        "active": True,
                        "labels": [],
                        "hierarchy": {},
                        "aliases": [],
                        "label_aliases": {}
                    },
                    "Identity": {
                        "active": True,
                        "labels": ["person"],
                        "hierarchy": {},
                        "aliases": [],
                        "label_aliases": {}
                    }
                }
        
        
        context = await Context.create(
            user_name=self.user_name,
            resources=self.resources,
            topics_config=topics_config,
            session_id=session_id,
            model=model
        )
        
        metadata = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "topics_config": topics_config,
            "last_active": datetime.now(timezone.utc).isoformat(),
            "model": model,
            "agent_id": agent_id
        }
        
        await self.resources.redis.hset(
            RedisKeys.sessions(self.user_name),
            session_id,
            json.dumps(metadata)
        )
        
        self.active_sessions[session_id] = context
        logger.info(f"Created session: {session_id}")
        return context

    async def get_or_resume_session(self, session_id: str) -> Optional[Context]:
        if session_id in self.active_sessions:
            return self.active_sessions[session_id]
        
        raw = await self.resources.redis.hget(RedisKeys.sessions(self.user_name), session_id)
        if not raw:
            return None
        
        metadata = json.loads(raw)

        current_topics = await TopicConfig.load(
            self.resources.redis,
            self.user_name,
            session_id
        )
        
        topics_to_use = current_topics.raw if current_topics.raw != TopicConfig.DEFAULT_CONFIG else metadata.get("topics_config")
    
        context = await Context.create(
            user_name=self.user_name,
            resources=self.resources,
            topics_config=topics_to_use,
            session_id=session_id,
            model=metadata.get("model")
        )
        
        self.active_sessions[session_id] = context
        
        metadata["last_active"] = datetime.now(timezone.utc).isoformat()
        await self.resources.redis.hset(
            RedisKeys.sessions(self.user_name),
            session_id,
            json.dumps(metadata)
        )
        
        logger.info(f"Resumed session: {session_id}")
        return context
    
    async def close_session(self, session_id: str) -> bool:
        if session_id not in self.active_sessions:
            return False
        
        context = self.active_sessions[session_id]
        await context.shutdown()
        del self.active_sessions[session_id]
        
        raw = await self.resources.redis.hget(RedisKeys.sessions(self.user_name), session_id)
        if raw:
            metadata = json.loads(raw)
            metadata["last_active"] = datetime.now(timezone.utc).isoformat()
            await self.resources.redis.hset(
                RedisKeys.sessions(self.user_name),
                session_id,
                json.dumps(metadata)
            )
        
        logger.info(f"Closed session: {session_id}")
        return True
    
    async def list_agents(self) -> List[AgentConfig]:
        """List all agents."""
        raw_agents = await self.resources.redis.hgetall(RedisKeys.agents(self.user_name))
        
        if not raw_agents:
            await self._seed_default_agents()
            raw_agents = await self.resources.redis.hgetall(RedisKeys.agents(self.user_name))
        
        return [AgentConfig.from_dict(json.loads(data)) for data in raw_agents.values()]

    async def get_agent(self, agent_id: str) -> Optional[AgentConfig]:
        """Get agent by ID."""
        raw = await self.resources.redis.hget(RedisKeys.agents(self.user_name), agent_id)
        if not raw:
            return None
        return AgentConfig.from_dict(json.loads(raw))

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

    async def create_agent(self, name: str, persona: str, model: str = None) -> AgentConfig:
        """Create a new agent."""
        agent_id = str(uuid.uuid4())
        config = AgentConfig(
            id=agent_id,
            name=name,
            persona=persona,
            model=model,
            is_default=False
        )
        
        await self.resources.redis.hset(
            RedisKeys.agents(self.user_name),
            agent_id,
            json.dumps(config.to_dict())
        )
        
        logger.info(f"Created agent: {name} ({agent_id})")
        return config

    async def update_agent(self, agent_id: str, name: str = None, persona: str = None, model: str = None) -> Optional[AgentConfig]:
        """Update an existing agent. Returns None if not found."""
        config = await self.get_agent(agent_id)
        if not config:
            return None
        
        if name is not None:
            config.name = name
        if persona is not None:
            config.persona = persona
        if model is not None:
            config.model = model if model else None
        
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
            is_default=True
        )
        
        await self.resources.redis.hset(agents_key, stella_id, json.dumps(stella.to_dict()))
        await self.resources.redis.set(default_key, stella_id)
        
        logger.info("Seeded default agents")
    
    async def delete_session_data(self, session_id: str) -> int:
        """
        Delete all Redis keys associated with a session.
        Returns count of keys deleted.
        """
        user = self.user_name
        redis = self.resources.redis
        
        direct_keys = [
            RedisKeys.global_next_turn_id(user, session_id),
            RedisKeys.buffer(user, session_id),
            RedisKeys.checkpoint(user, session_id),
            RedisKeys.message_content(user, session_id),
            RedisKeys.dirty_entities(user, session_id),
            RedisKeys.profile_complete(user, session_id),
            RedisKeys.merge_queue(user, session_id),
            RedisKeys.dlq(user, session_id),
            RedisKeys.dlq_parked(user, session_id),
            RedisKeys.last_processed(user, session_id),
            RedisKeys.conversation(user, session_id),
            RedisKeys.recent_conversation(user, session_id),
            RedisKeys.msg_to_turn_lookup(user, session_id),
            RedisKeys.last_activity(user, session_id),
            RedisKeys.merge_proposals(user, session_id),
            RedisKeys.user_profile_ran(user, session_id),
        ]
        
        job_names = ["cleaner", "profile", "merger", "dlq", "archival"]
        for job in job_names:
            direct_keys.append(RedisKeys.job_last_run(job, user, session_id))
            direct_keys.append(RedisKeys.job_pending(user, session_id, job))
        
        deleted = 0
        
        if direct_keys:
            deleted += await redis.delete(*direct_keys)
        
        await redis.hdel(RedisKeys.session_config(user), session_id)
        
        logger.info(f"Cleaned up {deleted} Redis keys for session {session_id}")
        return deleted

    async def shutdown(self):
        for session_id in list(self.active_sessions.keys()):
            await self.close_session(session_id)
        
        await self.resources.shutdown()
        logger.info("AppState shutdown complete")

    

    


