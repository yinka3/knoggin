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

class SessionManager:
    def __init__(self, resources, user_name, active_sessions):
        self.resources = resources
        self.user_name = user_name
        self.active_sessions = active_sessions
        self._session_locks = {}
        self._lock = asyncio.Lock()

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
            raise

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


    async def update_session_metadata(self, session_id: str, new_data: dict) -> dict:
        """Update session metadata directly via dict unpacking."""
        raw = await self.resources.redis.hget(RedisKeys.sessions(self.user_name), session_id)
        metadata = {}
        if raw:
            try:
                metadata = json.loads(raw)
            except json.JSONDecodeError:
                pass
        
        metadata.update(new_data)
        await self.resources.redis.hset(
            RedisKeys.sessions(self.user_name),
            session_id,
            json.dumps(metadata)
        )
        return metadata
