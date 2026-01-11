import redis.asyncio as async_redis
from dotenv import load_dotenv
import os
load_dotenv()

VESTIGE_USER_NAME = os.environ.get("VESTIGE_USER_NAME")
REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = os.environ.get("REDIS_PORT")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")

if not REDIS_PASSWORD:
    raise ValueError("REDIS_PASSWORD not set in environment")

class AsyncRedisClient:
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