import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

from common.conf.base import get_config
from common.utils.events import DebugEventEmitter
from common.utils.json_utils import safe_json_loads
from infrastructure.redis_client import RedisKeys
from knoggin.knowledge.services.file_rag import FileRAGService
from knoggin.project.services.project_manager import ProjectManager
from knoggin.session.context import Context


class SessionManager:
    def __init__(self, resources: Any, user_name: str, active_sessions: Dict[str, Context], project_manager: ProjectManager):
        self.resources = resources
        self.user_name = user_name
        self.active_sessions = active_sessions
        self.project_manager = project_manager
        self._session_locks: Dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def list_sessions(self) -> Dict[str, dict]:
        try:
            raw = await self.resources.redis.hgetall(RedisKeys.sessions(self.user_name))
            result = {}
            for sid, data in raw.items():
                parsed = safe_json_loads(data)
                if parsed is not None:
                    result[sid] = parsed
            return result
        except Exception as e:
            logger.error(f"Failed to list sessions (check Redis connection): {e}")
            raise

    async def create_session(
        self,
        topics_config: Optional[dict] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        project_id: Optional[str] = None,
    ) -> Context:
        session_id = str(uuid.uuid4())

        if topics_config is None:
            config = get_config()
            topics_config = config.default_topics

        async with self._lock:
            # Phase 1B: Ensure project exists and get its runtime state
            # If no project_id provided, we use a global fallback for now (or raise error if strict)
            actual_project_id = project_id or "global"
            project_state = await self.project_manager.get_or_start_project(actual_project_id)

            context = await Context.create(
                user_name=self.user_name,
                resources=self.resources,
                session_id=session_id,
                model=model,
                project_state=project_state,
            )

            metadata = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "topics_config": topics_config,
                "last_active": datetime.now(timezone.utc).isoformat(),
                "model": model,
                "agent_id": agent_id,
                "enabled_tools": enabled_tools,
                "project_id": project_id,
            }

            await self.resources.redis.hset(
                RedisKeys.sessions(self.user_name), session_id, json.dumps(metadata)
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

            raw = await self.resources.redis.hget(
                RedisKeys.sessions(self.user_name), session_id
            )
            if not raw:
                return None

            metadata = safe_json_loads(raw)
            if not metadata:
                return None

            actual_project_id = metadata.get("project_id") or "global"
            project_state = await self.project_manager.get_or_start_project(actual_project_id)

            context = await Context.create(
                user_name=self.user_name,
                resources=self.resources,
                session_id=session_id,
                model=metadata.get("model"),
                project_state=project_state,
            )

            self.active_sessions[session_id] = context

            metadata["last_active"] = datetime.now(timezone.utc).isoformat()
            await self.resources.redis.hset(
                RedisKeys.sessions(self.user_name), session_id, json.dumps(metadata)
            )

            logger.info(f"Resumed session: {session_id}")
            return context

    async def close_session(self, session_id: str) -> bool:
        async with self._lock:
            if session_id not in self.active_sessions:
                return False

            context = self.active_sessions.pop(session_id)
            self._session_locks.pop(session_id, None)

        if context.project_id:
            DebugEventEmitter.get().unregister_session(context.project_id, session_id)
            await self.project_manager.release_project(context.project_id)

        # Context shutdown is now lightweight (batch consumer, file_rag)
        if hasattr(context, "shutdown"):
            await context.shutdown()

        raw = await self.resources.redis.hget(
            RedisKeys.sessions(self.user_name), session_id
        )
        if raw:
            metadata = safe_json_loads(raw, {})
            if metadata:
                metadata["last_active"] = datetime.now(timezone.utc).isoformat()
                await self.resources.redis.hset(
                    RedisKeys.sessions(self.user_name), session_id, json.dumps(metadata)
                )

        logger.info(f"Closed session: {session_id}")
        return True

    async def get_session_history_readonly(
        self, session_id: str, limit: int = 1000
    ) -> List[Dict]:
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
            parsed = safe_json_loads(raw)
            if not parsed or not isinstance(parsed, dict):
                logger.warning("Skipping corrupted turn in readonly history")
                continue
            turns.append(
                {
                    "role": parsed["role"],
                    "content": parsed["content"],
                    "timestamp": parsed["timestamp"],
                }
            )

        return turns

    async def delete_session_data(self, session_id: str) -> int:
        """
        Delete all Redis keys associated with a session.
        Returns count of keys deleted.
        """
        user = self.user_name
        redis = self.resources.redis

        direct_keys = RedisKeys.get_session_scoped_keys(user, session_id)

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
            temp_rag = FileRAGService(
                session_id=session_id,
                embedding_service=self.resources.embedding,
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
        raw = await self.resources.redis.hget(
            RedisKeys.sessions(self.user_name), session_id
        )
        metadata = {}
        if raw:
            metadata = safe_json_loads(raw, {})

        metadata.update(new_data)
        await self.resources.redis.hset(
            RedisKeys.sessions(self.user_name), session_id, json.dumps(metadata)
        )
        return metadata
