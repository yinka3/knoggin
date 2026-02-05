import asyncio
import os
from loguru import logger
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = os.environ.get("REDIS_PORT")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")

if not REDIS_PASSWORD:
    raise ValueError("REDIS_PASSWORD not set in environment")


class AsyncRedisClient:
    """Singleton async Redis client."""
    _instance = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_instance(cls) -> aioredis.Redis:
        """Async-safe singleton accessor."""
        async with cls._lock:
            if cls._instance is None:
                if not REDIS_PASSWORD:
                    raise ValueError(
                        "REDIS_PASSWORD not set in environment. "
                        "Please set REDIS_PASSWORD in your .env file or environment variables."
                    )
                
                try:
                    pool = aioredis.ConnectionPool.from_url(
                        url=f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}",
                        decode_responses=True,
                        max_connections=10
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
    def agents_default(user: str) -> str:
        return f"agents:default:{user}"
    
    @staticmethod
    def agents(user: str) -> str:
        return f"agents:{user}"
    
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
    