from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from loguru import logger
from typing import Optional
from loguru import logger

@dataclass
class JobContext:
    """Context passed to every job method."""
    user_name: str
    session_id: str
    idle_seconds: float = 0.0
    last_run: Optional[datetime] = None


@dataclass 
class JobResult:
    """Result returned from job execution."""
    success: bool = True
    summary: str = ""
    reschedule_seconds: Optional[float] = None


class BaseJob(ABC):
    """Base class for scheduled jobs."""
    
    enabled: bool = True
    
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
        pass
    
    def update_settings(self, **kwargs):
        """
        Standard interface for runtime tuning.
        Updates instance attributes if they match the keys provided.
        """
        updates = []
        for key, value in kwargs.items():
            if value is not None:
                # Security: Only update if the job actually HAS this attribute.
                # This prevents adding random garbage attributes to the instance.
                if hasattr(self, key):
                    setattr(self, key, value)
                    updates.append(f"{key}={value}")
                else:
                    logger.warning(f"Job {self.name} received unknown setting: {key}")
        
        if updates:
            logger.info(f"Job {self.name} reconfigured: {', '.join(updates)}")