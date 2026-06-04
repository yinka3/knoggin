import asyncio
import json
import os
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from dotenv import load_dotenv
from loguru import logger

from common.exceptions import DependencyError
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import parse_iso_time

load_dotenv()

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")


class AsyncRedisClient:
    """Singleton async Redis client with health checks and auto-reconnection."""

    _instance = None
    _lock = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    async def get_instance(cls) -> aioredis.Redis:
        """Async-safe singleton accessor with health check."""
        async with cls._get_lock():
            is_healthy = False
            if cls._instance is not None:
                try:
                    await asyncio.wait_for(cls._instance.ping(), timeout=2.0)
                    is_healthy = True
                except (aioredis.ConnectionError, asyncio.TimeoutError):
                    logger.warning("Redis connection lost, attempting to reconnect...")
                    await cls._close_unlocked()

            if not is_healthy:
                try:
                    pool = aioredis.ConnectionPool.from_url(
                        url=f"redis://{REDIS_HOST}:{REDIS_PORT}",
                        decode_responses=True,
                        max_connections=10,
                        retry_on_timeout=True,
                        health_check_interval=30,
                    )
                    cls._instance = aioredis.Redis(connection_pool=pool)
                    await cls._instance.ping()
                    logger.info(f"Redis connected: {REDIS_HOST}:{REDIS_PORT}")
                except Exception as e:
                    cls._instance = None
                    raise DependencyError(
                        f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}",
                        details={
                            "error": str(e),
                            "host": REDIS_HOST,
                            "port": REDIS_PORT,
                        },
                    )

            return cls._instance

    @classmethod
    async def _close_unlocked(cls):
        """Internal teardown without acquiring the lock."""
        if cls._instance is not None:
            await cls._instance.close()
            cls._instance = None
            logger.info("Redis connection closed")

    @classmethod
    async def close_redis(cls):
        """Close the Redis connection pool."""
        async with cls._get_lock():
            await cls._close_unlocked()

    @classmethod
    async def publish(cls, channel: str, message: Any):
        """Publish a message to a channel."""
        redis = await cls.get_instance()
        data = json.dumps(message) if not isinstance(message, str) else message
        await redis.publish(channel, data)

    @classmethod
    async def subscribe(cls, channel: str):
        """Get a pubsub instance and subscribe to a channel."""
        redis = await cls.get_instance()
        ps = redis.pubsub()
        await ps.subscribe(channel)
        return ps

    @classmethod
    async def log_conversation_turn(
        cls,
        user_name: str,
        session_id: str,
        turn_id: int,
        payload: dict,
        max_history: int = 100,
    ):
        """
        Atomically logs a turn to history and prunes old entries.
        Updates both the data (Hash) and the timeline (Sorted Set).
        """
        redis = await cls.get_instance()
        conv_key = RedisKeys.conversation(user_name, session_id)
        recent_key = RedisKeys.recent_conversation(user_name, session_id)
        turn_key = str(turn_id)

        pipe = redis.pipeline()
        pipe.hset(conv_key, turn_key, json.dumps(payload))

        timestamp = payload.get("timestamp")
        score = turn_id
        if timestamp:
            try:
                if isinstance(timestamp, str):
                    score = parse_iso_time(timestamp).timestamp()
            except Exception:
                pass
        pipe.zadd(recent_key, {turn_key: score})

        # Keep history within limits
        pipe.zremrangebyrank(recent_key, 0, -(max_history + 1))

        await pipe.execute()

    @classmethod
    async def update_message_mapping(
        cls,
        user_name: str,
        session_id: str,
        msg_id: int,
        turn_id: Optional[int],
        content: Optional[str] = None,
        timestamp: Optional[str] = None,
        role: str = "user",
    ):
        """Maps a message ID to a turn ID and optionally stores content."""
        redis = await cls.get_instance()
        lookup_key = RedisKeys.msg_to_turn_lookup(user_name, session_id)
        content_key = RedisKeys.message_content(user_name, session_id)

        pipe = redis.pipeline()
        if turn_id is not None:
            pipe.hset(lookup_key, str(msg_id), str(turn_id))
        if content is not None:
            payload = {
                "id": msg_id,
                "message": content,
                "content": content,
                "timestamp": timestamp or "",
                "role": role,
            }
            pipe.hset(content_key, f"msg_{msg_id}", json.dumps(payload))

        await pipe.execute()

    @classmethod
    async def refresh_session_ttls(cls, user_name: str, session_id: str, ttl: int):
        """Refreshes TTLs for all session-scoped keys in a single pipeline."""
        redis = await cls.get_instance()
        keys = RedisKeys.get_session_scoped_keys(user_name, session_id)

        pipe = redis.pipeline()
        for key in keys:
            pipe.expire(key, ttl)
        await pipe.execute()

    @classmethod
    async def load_formatted_memories(
        cls, agent_id: str, categories: List[str]
    ) -> Dict[str, str]:
        """
        Loads multiple memory categories and formats them as markdown lists.
        Returns {category: "\n- content1\n- content2"}.
        """
        redis = await cls.get_instance()
        pipe = redis.pipeline()
        for cat in categories:
            pipe.hgetall(RedisKeys.agent_working_memory(agent_id, cat))

        raw_results = await pipe.execute()
        formatted = {}

        for i, raw in enumerate(raw_results):
            cat = categories[i]
            if not raw:
                formatted[cat] = ""
                continue

            lines = []
            # Sort by timestamp if available in payload
            parsed = []
            for v in raw.values():
                try:
                    data = safe_json_loads(v)
                    if data:
                        parsed.append(data)
                except Exception:
                    continue

            # Sort by created_at
            parsed.sort(key=lambda x: x.get("created_at", ""))

            for item in parsed:
                lines.append(f"- {item['content']}")

            formatted[cat] = "\n".join(lines)

        return formatted


class RedisKeys:
    """Centralized Redis key patterns - session-scoped by default."""

    @staticmethod
    def projects(user: str) -> str:
        """Hash: project_id → JSON metadata"""
        return f"projects:{user}"

    @staticmethod
    def project_sessions(user: str, project_id: str) -> str:
        """Set of session_ids belonging to a project"""
        return f"project_sessions:{user}:{project_id}"

    @staticmethod
    def dirty_entities(user: str, project_id: str) -> str:
        return f"dirty_entities:{user}:{project_id}"

    @staticmethod
    def merge_queue(user_name: str, project_id: str) -> str:
        return f"merge_queue:{user_name}:{project_id}"

    @staticmethod
    def merge_proposals(user: str, project_id: str) -> str:
        return f"merge_proposals:{user}:{project_id}"

    @staticmethod
    def dlq(user: str, project_id: str) -> str:
        return f"dlq:{user}:{project_id}"

    @staticmethod
    def dlq_parked(user: str, project_id: str) -> str:
        return f"dlq:parked:{user}:{project_id}"

    @staticmethod
    def last_profile_update(user: str, project_id: str, entity_id: int) -> str:
        return f"last_profile_update:{user}:{project_id}:{entity_id}"

    @staticmethod
    def profile_complete(user: str, scope_id: str) -> str:
        return f"profile_complete:{user}:{scope_id}"

    @staticmethod
    def get_session_scoped_keys(user: str, session: str) -> list[str]:
        """Returns all Redis keys that are scoped to a specific session."""
        return [
            RedisKeys.global_next_turn_id(user, session),
            RedisKeys.buffer(user, session),
            RedisKeys.checkpoint(user, session),
            RedisKeys.message_content(user, session),
            RedisKeys.profile_complete(user, session),
            RedisKeys.last_processed(user, session),
            RedisKeys.conversation(user, session),
            RedisKeys.recent_conversation(user, session),
            RedisKeys.msg_to_turn_lookup(user, session),
            RedisKeys.last_activity(user, session),
            RedisKeys.user_profile_ran(user, session),
            RedisKeys.heartbeat_counter(user, session),
        ]

    @staticmethod
    def global_next_turn_id(user: str, session: str) -> str:
        return f"global:next_turn_id:{user}:{session}"

    @staticmethod
    def buffer(user: str, session: str) -> str:
        return f"buffer:{user}:{session}"

    @staticmethod
    def checkpoint(user: str, session: str) -> str:
        return f"checkpoint_count:{user}:{session}"

    @staticmethod
    def message_content(user: str, session: str) -> str:
        return f"message_content:{user}:{session}"

    @staticmethod
    def last_processed(user: str, session: str) -> str:
        return f"last_processed_msg:{user}:{session}"

    @staticmethod
    def conversation(user: str, session: str) -> str:
        return f"conversation:{user}:{session}"

    @staticmethod
    def recent_conversation(user: str, session: str) -> str:
        return f"recent_conversation:{user}:{session}"

    @staticmethod
    def msg_to_turn_lookup(user: str, session: str) -> str:
        return f"lookup:msg_to_turn:{user}:{session}"

    @staticmethod
    def last_activity(user: str, session: str) -> str:
        return f"last_activity:{user}:{session}"

    @staticmethod
    def merge_undo(session: str, primary_id: int, secondary_id: int) -> str:
        return f"merge_undo:{session}:{primary_id}:{secondary_id}"

    @staticmethod
    def user_profile_ran(user: str, scope_id: str) -> str:
        return f"user_profile_ran:{user}:{scope_id}"

    @staticmethod
    def merge_intent(
        user: str, scope_id: str, primary_id: int, secondary_id: int
    ) -> str:
        return f"merge_intent:{user}:{scope_id}:{primary_id}:{secondary_id}"

    @staticmethod
    def merge_intents_index(user: str, scope_id: str) -> str:
        return f"merge_intents_index:{user}:{scope_id}"

    @staticmethod
    def job_last_run(job_name: str, user: str, scope_id: str) -> str:
        return f"last_run:{job_name}:{user}:{scope_id}"

    @staticmethod
    def job_pending(user: str, scope_id: str, job_name: str) -> str:
        return f"pending:{user}:{scope_id}:{job_name}"

    @staticmethod
    def agent_memory(user: str, session: str, topic: str) -> str:
        return f"memory:{user}:{session}:{topic}"

    @staticmethod
    def heartbeat_counter(user: str, session: str) -> str:
        return f"heartbeat_counter:{user}:{session}"

    @staticmethod
    def global_next_msg_id() -> str:
        return "global:next_msg_id"

    @staticmethod
    def global_next_ent_id() -> str:
        return "global:next_ent_id"

    @staticmethod
    def sessions(user: str) -> str:
        return f"sessions:{user}"

    @staticmethod
    def session_config(user: str) -> str:
        return f"session_config:{user}"

    @staticmethod
    def agents_default(user: str) -> str:
        return f"agents:default:{user}"

    @staticmethod
    def agents(user: str) -> str:
        return f"agents:{user}"

    @staticmethod
    def agent_working_memory(agent_id: str, category: str) -> str:
        return f"agent_memory:{agent_id}:{category}"

    @staticmethod
    def global_stats() -> str:
        return "global:stats"

    @staticmethod
    def community_config() -> str:
        return "community:config"

    @staticmethod
    def community_discussion_active() -> str:
        return "community:discussion:active"

    @staticmethod
    def community_discussion_history() -> str:
        return "community:discussion:history"

    @staticmethod
    def community_discussion_messages(discussion_id: str) -> str:
        return f"community:discussion:{discussion_id}:messages"

    @staticmethod
    def community_agent_hierarchy() -> str:
        return "community:agent_hierarchy"

    @staticmethod
    def community_agent_memory(user_name: str, agent_id: str) -> str:
        return f"community:{user_name}:agent_memory:{agent_id}"

    @staticmethod
    def community_pubsub_channel() -> str:
        return "community:events"
