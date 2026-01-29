from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import redis.asyncio as redis



@dataclass
class JobContext:
    """Context passed to every job method."""
    user_name: str
    redis: redis.Redis
    idle_seconds: float = 0.0
    last_run: Optional[datetime] = None
    session_id: Optional[str] = None


@dataclass 
class JobResult:
    """Result returned from job execution."""
    success: bool = True
    summary: str = ""
    reschedule_seconds: Optional[float] = None


class JobNotifier:
    """
    Sets a global 'Maintenance Mode' flag in Redis while a job runs.
    """
    KEY = "system:active_job_warning"
    
    def __init__(self, redis_client: redis.Redis, message: str, ttl: int = 600):
        self.redis = redis_client
        self.message = message
        self.ttl = ttl  # Auto-expire after 10 mins if we crash

    async def __aenter__(self):
        # Set the warning message visible to the Agent
        await self.redis.setex(self.KEY, self.ttl, self.message)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Clear the warning when job finishes (or fails)
        await self.redis.delete(self.KEY)


class BaseJob(ABC):
    """Base class for scheduled jobs."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError
    
    @abstractmethod
    async def should_run(self, ctx: JobContext) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    async def execute(self, ctx: JobContext) -> JobResult:
        raise NotImplementedError
    
    async def on_shutdown(self, ctx: JobContext) -> None:
        """Override for cleanup. Default no-op."""
        raise NotImplementedError