from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class JobContext:
    """Context passed to every job method."""

    user_name: str
    project_id: str
    idle_seconds: float = 0.0


@dataclass
class JobResult:
    """Result returned from job execution."""

    success: bool = True
    summary: str = ""


class BaseJob(ABC):
    """Base class for scheduled jobs."""

    enabled: bool = True
    cadence_seconds: float | None = None
    run_immediately_on_first_check: bool = False

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
