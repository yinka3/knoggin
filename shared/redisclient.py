import os
import redis.asyncio as async_redis
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

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            pool = async_redis.ConnectionPool.from_url(
                url=f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}",
                decode_responses=True,
                max_connections=10
            )
            cls._instance.client = async_redis.Redis(connection_pool=pool)
        return cls._instance

    def get_client(self) -> async_redis.Redis:
        return self.client


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
    def emotions(user: str, session: str) -> str:
        return f"emotions:{user}:{session}"
    
    @staticmethod
    def dirty_entities(user: str, session: str) -> str:
        return f"dirty_entities:{user}:{session}"
    
    @staticmethod
    def profile_complete(user: str, session: str) -> str:
        return f"profile_complete:{user}:{session}"
    
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
    def merge_ran(user: str, session: str) -> str:
        return f"merge_ran:{user}:{session}"
    
    @staticmethod
    def merge_proposals(user: str, session: str) -> str:
        return f"merge_proposals:{user}:{session}"
    
    @staticmethod
    def user_profile_ran(user: str, session: str) -> str:
        return f"user_profile_ran:{user}:{session}"
    
    @staticmethod
    def job_last_run(job_name: str, user: str, session: str) -> str:
        return f"last_run:{job_name}:{user}:{session}"
    
    @staticmethod
    def job_pending(user: str, session: str, job_name: str) -> str:
        return f"pending:{user}:{session}:{job_name}"
    
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
    def system_active_job_warning() -> str:
        return "system:active_job_warning"