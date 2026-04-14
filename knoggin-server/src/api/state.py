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

load_dotenv()

class AppState:

    def __init__(self, resources: ResourceManager, active_sessions: Dict[str, Context], user_name: str):
        self.resources = resources
        self.active_sessions = active_sessions
        self.user_name = user_name
        self.global_scheduler = None
        self._session_locks: Dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def start_scheduler(self):
        """Lazy-start the global scheduler if user_name is present."""
        if not self.user_name or self.global_scheduler:
            return

        from jobs.scheduler import Scheduler
        from jobs.aac_job import AACJob
        
        self.global_scheduler = Scheduler(self.user_name, "global", self.resources.redis, self.resources)
        self.global_scheduler.register(AACJob(resources=self.resources))
        await self.global_scheduler.start()
        logger.info(f"Global scheduler started lazily for user: {self.user_name}")
    
    async def list_sessions(self) -> Dict[str, dict]:
        try:
            raw = await self.resources.redis.hgetall(RedisKeys.sessions(self.user_name))
            result = {}
            for sid, data in raw.items():
                try:
                    result[sid] = json.loads(data)
                except json.JSONDecodeError:
                    logger.warning(f"Malformed session data for {sid}")
            return result
        except Exception as e:
            logger.error(f"Failed to list sessions (check Redis connection): {e}")
            raise # Bubbling up instead of returning silent empty dict

    async def create_session(self, topics_config=None, model=None, agent_id=None, enabled_tools=None) -> Context:
        session_id = str(uuid.uuid4())
        
        if topics_config is None:
            config = get_config()
            topics_config = config.default_topics
        
        
        async with self._lock:
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
                "agent_id": agent_id,
                "enabled_tools": enabled_tools
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
        
        async with self._lock:
            if session_id not in self._session_locks:
                self._session_locks[session_id] = asyncio.Lock()
            lock = self._session_locks[session_id]
        
        async with lock:
            if session_id in self.active_sessions:
                return self.active_sessions[session_id]
            
            raw = await self.resources.redis.hget(RedisKeys.sessions(self.user_name), session_id)
            if not raw:
                return None
            
            try:
                metadata = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning(f"Malformed session data for {session_id}")
                return None

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
        async with self._lock:
            if session_id not in self.active_sessions:
                return False
            
            context = self.active_sessions.pop(session_id)
            self._session_locks.pop(session_id, None)
        
        await context.shutdown()
        
        raw = await self.resources.redis.hget(RedisKeys.sessions(self.user_name), session_id)
        if raw:
            try:
                metadata = json.loads(raw)
                metadata["last_active"] = datetime.now(timezone.utc).isoformat()
                await self.resources.redis.hset(
                    RedisKeys.sessions(self.user_name),
                    session_id,
                    json.dumps(metadata)
                )
            except json.JSONDecodeError:
                pass
        
        logger.info(f"Closed session: {session_id}")
        return True
    
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
    
    async def get_session_history_readonly(self, session_id: str, limit: int = 1000) -> List[Dict]:
        """Read conversation history from Redis without resuming the session."""
        sorted_key = RedisKeys.recent_conversation(self.user_name, session_id)
        conv_key = RedisKeys.conversation(self.user_name, session_id)
        
        turn_ids = await self.resources.redis.zrange(sorted_key, 0, limit - 1)
        if not turn_ids:
            return []
        
        turn_data = await self.resources.redis.hmget(conv_key, *turn_ids)
        
        turns = []
        for raw in turn_data:
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                logger.warning("Skipping corrupted turn in readonly history")
                continue
            turns.append({
                "role": parsed["role"],
                "content": parsed["content"],
                "timestamp": parsed["timestamp"]
            })
        
        return turns
    
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
            RedisKeys.merge_intents_index(user, session_id),
            RedisKeys.user_profile_ran(user, session_id),
            RedisKeys.heartbeat_counter(user, session_id),
        ]

        memory_pattern = f"memory:{user}:{session_id}:*"
        cursor = 0
        deleted = 0
        while True:
            cursor, keys = await redis.scan(cursor, match=memory_pattern, count=100)
            if keys:
                deleted += int(await redis.delete(*keys))  # type: ignore
            if cursor == 0:
                break
            
        if session_id in self.active_sessions:
            ctx = self.active_sessions[session_id]
            if ctx.file_rag:
                ctx.file_rag.cleanup_session()
        else:
            
            upload_dir = os.path.join(os.getenv("CONFIG_DIR", "./config"), "uploads")
            temp_rag = FileRAGService(
                session_id=session_id,
                chroma_client=self.resources.chroma,
                embedding_service=self.resources.embedding,
                upload_dir=upload_dir,
            )
            temp_rag.cleanup_session()

        job_names = ["cleaner", "profile", "merger", "dlq", "archival"]
        for job in job_names:
            direct_keys.append(RedisKeys.job_last_run(job, user, session_id))
            direct_keys.append(RedisKeys.job_pending(user, session_id, job))
        
        
        
        if direct_keys:
            deleted += await redis.delete(*direct_keys)
        
        await redis.hdel(RedisKeys.session_config(user), session_id)
        await redis.hdel(RedisKeys.sessions(user), session_id)
        
        logger.info(f"Cleaned up {deleted} Redis keys for session {session_id}")
        return deleted

    async def shutdown(self):
        if self.global_scheduler:
            await self.global_scheduler.stop()
            self.global_scheduler = None

        for session_id in list(self.active_sessions.keys()):
            try:
                await self.close_session(session_id)
            except Exception as e:
                logger.error(f"Failed to close session {session_id} during shutdown: {e}")
        
        await self.resources.shutdown()
        logger.info("AppState shutdown complete")

    

    


