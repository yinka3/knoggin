"""SDK session — all session-scoped components built from KnogginClient resources.

Created by client.session(). Consumed by KnogginExtractor and KnogginAgent.
"""

import asyncio
import os
import uuid
from dataclasses import dataclass, field
from functools import partial
from typing import Callable, Dict, List, Optional

from loguru import logger

from main.processor import BatchProcessor
from main.consumer import BatchConsumer
from main.nlp_pipe import NLPPipeline
from main.entity_resolve import EntityResolver
from agent.tools import Tools
from shared.services.memory import MemoryManager
from shared.services.rag import FileRAGService
from shared.config.topics_config import TopicConfig
from shared.infra.redis import RedisKeys


@dataclass
class KnogginSession:
    """The central hub for interacting with Knoggin workflows.
    
    Provides access to the extractor, agent, and background jobs.
    Safely integrates with `async with` for resource cleanup.
    """

    # Identity
    session_id: str = ""
    user_name: str = ""

    # Topic config (mutable — TopicManager can update it)
    topic_config: TopicConfig = None

    # Core pipeline components
    resolver: EntityResolver = None
    nlp: NLPPipeline = None
    processor: BatchProcessor = None
    consumer: BatchConsumer = None
    # Agent components
    tools: Tools = None
    memory: MemoryManager = None
    file_rag: FileRAGService = None
    scheduler: object = None
    
    # Back-reference to client (for store, llm, executor, etc.)
    _client: object = field(default=None, repr=False)

    @property
    def extractor(self):
        """Lazy-loaded KnogginExtractor."""
        if not hasattr(self, "_extractor"):
            from sdk.extraction import KnogginExtractor
            self._extractor = KnogginExtractor(session=self)
        return self._extractor

    @property
    def agent(self):
        """Lazy-loaded KnogginAgent."""
        if not hasattr(self, "_agent"):
            from sdk.agent_sdk import KnogginAgent
            self._agent = KnogginAgent(session=self)
        return self._agent

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Stop background processes and release memory."""
        if getattr(self, "_client", None):
            await self._client._close_session(self.session_id)

    async def start_jobs(self, job_config: Optional[dict] = None):
        """Manually start background jobs for this session."""
        if getattr(self, "_client", None):
            await self._client._start_jobs(self.user_name, self.session_id, job_config=job_config)

    async def stop_jobs(self):
        """Stop all background jobs attached to this session."""
        if getattr(self, "_client", None):
            await self._client._stop_jobs(self.session_id)

    async def run_job(self, job_name: str) -> dict:
        """Manually trigger a specific job out-of-band."""
        if getattr(self, "_client", None):
            return await self._client._run_job(self.session_id, job_name)
        return {"error": "Session unattached"}