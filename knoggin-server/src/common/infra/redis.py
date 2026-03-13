import asyncio
import json
import os
from typing import Any
from loguru import logger
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")



class AsyncRedisClient:
    """Singleton async Redis client."""
    _instance = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_instance(cls) -> aioredis.Redis:
        """Async-safe singleton accessor."""
        async with cls._lock:
            if cls._instance is None:
                try:
                    pool = aioredis.ConnectionPool.from_url(
                        url=f"redis://{REDIS_HOST}:{REDIS_PORT}",
                        decode_responses=True,
                        max_connections=10,
                        retry_on_timeout=True,
                        health_check_interval=30
                    )
                    cls._instance = aioredis.Redis(connection_pool=pool)
                    
                    await cls._instance.ping()
                    logger.info(f"Redis connected: {REDIS_HOST}:{REDIS_PORT}")
                    
                except aioredis.ConnectionError as e:
                    raise ConnectionError(
                        f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}. "
                        f"Ensure Redis is running and credentials are correct. Error: {e}"
                    )
                    
            return cls._instance
    
    @classmethod
    async def close_redis(cls):
        """Close the Redis connection pool."""
        async with cls._lock:
            if cls._instance is not None:
                await cls._instance.close()
                cls._instance = None
                logger.info("Redis connection closed")

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


class RedisKeys:
    """Centralized Redis key patterns - session-scoped by default."""
    
    # ============ SESSION-SCOPED ============

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
    def dirty_entities(user: str, session: str) -> str:
        return f"dirty_entities:{user}:{session}"
    
    @staticmethod
    def profile_complete(user: str, session: str) -> str:
        return f"profile_complete:{user}:{session}"

    @staticmethod
    def merge_queue(user_name: str, session: str) -> str:
        return f"merge_queue:{user_name}:{session}"
    
    @staticmethod
    def dlq(user: str, session: str) -> str:
        return f"dlq:{user}:{session}"
    
    @staticmethod
    def dlq_parked(user: str, session: str) -> str:
        return f"dlq:parked:{user}:{session}"
    
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
    def merge_proposals(user: str, session: str) -> str:
        return f"merge_proposals:{user}:{session}"
    
    @staticmethod
    def merge_undo(session: str, primary_id: int, secondary_id: int) -> str:
        return f"merge_undo:{session}:{primary_id}:{secondary_id}"
    
    @staticmethod
    def user_profile_ran(user: str, session: str) -> str:
        return f"user_profile_ran:{user}:{session}"
    
    @staticmethod
    def job_last_run(job_name: str, user: str, session: str) -> str:
        return f"last_run:{job_name}:{user}:{session}"
    
    @staticmethod
    def job_pending(user: str, session: str, job_name: str) -> str:
        return f"pending:{user}:{session}:{job_name}"
    
    @staticmethod
    def agent_memory(user: str, session: str, topic: str) -> str:
        return f"memory:{user}:{session}:{topic}"
    
    @staticmethod
    def heartbeat_counter(user: str, session: str) -> str:
        return f"heartbeat_counter:{user}:{session}"
    
    # ============ GLOBAL (no session) ============
    
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
    
    # ============ COMMUNITY (Global) ============

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
    