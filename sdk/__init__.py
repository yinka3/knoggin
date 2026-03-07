"""
Knoggin SDK — Self-hosted knowledge graph memory for AI agents.

    from sdk import KnogginClient

    async with KnogginClient.from_env() as client:
        async with client.session("Adeyinka", topics={}) as session:
            async for chunk in session.agent.chat("What do I know about Project X?"):
                print(chunk.content)
"""

from sdk.client import KnogginClient
from sdk.agent_sdk import KnogginAgent
from sdk.extraction import KnogginExtractor
from sdk.topics_sdk import TopicBuilder, TopicManager
from sdk.events import console_handler
from sdk.config import KnogginConfig
from cli.config import load_toml
from shared.services.memory import MemoryManager
from sdk.session import KnogginSession

# Result types
from sdk.types import (
    AgentResult,
    ExtractionResult,
    Mention,
    ResolvedEntity,
    Connection,
    ExtractedFact,
    MemorySaveResult,
    MemoryForgetResult,
    MemoryListResult,
    WorkingMemoryAddResult,
    WorkingMemoryRemoveResult,
    WorkingMemoryListResult,
    WorkingMemoryClearResult,
    PromptContext,
)

# Advanced: job system (for custom background jobs)
from jobs.base import BaseJob, JobContext, JobResult

__all__ = [
    # Client
    "KnogginClient",
    "KnogginSession",

    # Agent
    "KnogginAgent",
    "AgentResult",

    # Extraction
    "KnogginExtractor",
    "ExtractionResult",
    "Mention",
    "ResolvedEntity",
    "Connection",
    "ExtractedFact",

    # Memory
    "MemoryManager",
    "MemorySaveResult",
    "MemoryForgetResult",
    "MemoryListResult",
    "WorkingMemoryAddResult",
    "WorkingMemoryRemoveResult",
    "WorkingMemoryListResult",
    "WorkingMemoryClearResult",

    # Topics
    "TopicBuilder",
    "TopicManager",

    # Config
    "load_toml",
    "KnogginConfig",
    "PromptContext",

    # Events
    "console_handler",

    # Jobs (advanced)
    "BaseJob",
    "JobContext",
    "JobResult",
]