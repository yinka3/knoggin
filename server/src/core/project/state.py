import asyncio
from typing import Any, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.conf.topics_config import TopicConfig
from common.scoping import require_scope_value, require_visible_project_ids
from core.ingestion.services.processor import TextProcessor
from core.knowledge.documents import DocumentService
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.job.scheduler import Scheduler
from infrastructure.postgres_client import PostgresClient
from infrastructure.background_work import BackgroundWorkCoordinator


class ProjectState:
    """
    Holds the runtime shared resources for a Project.
    """

    COMMUNITY_TASK_SHUTDOWN_TIMEOUT = 30.0

    def __init__(
        self,
        project_id: str,
        topic_config: TopicConfig,
        entities: EntityResolver,
        pipeline: TextProcessor,
        scheduler: Scheduler,
        user_name: str,
        redis_client: aioredis.Redis,
        readable_project_ids: list[str],
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
        batch_processor: Optional[Any] = None,
        background_work: Optional[BackgroundWorkCoordinator] = None,
    ):
        self.project_id = require_scope_value(
            project_id,
            "project_id",
            "ProjectState",
        )
        self.readable_project_ids = require_visible_project_ids(
            readable_project_ids,
            "ProjectState",
        )
        self.topic_config = topic_config
        self.entities = entities
        self.pipeline = pipeline
        self.scheduler = scheduler
        self.user_name = user_name
        self.redis_client = redis_client
        self.postgres_client = postgres_client
        self.embedding_service = embedding_service
        self.batch_processor = batch_processor
        self.document_service = DocumentService(
            project_id=project_id,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            background_work=background_work,
        )

        self.profile_job: Optional[Any] = None
        self._community_task: Optional[asyncio.Task] = None
        self.active_runtime_sessions_count = 0
        self.config_unsubscribers: list[Any] = []

    async def record_session_activity(self):
        """Record user activity against the project-level scheduler."""
        await self.scheduler.record_activity()

    def add_config_unsubscriber(self, unsubscribe):
        self.config_unsubscribers.append(unsubscribe)

    def track_community_task(self, task: asyncio.Task) -> None:
        """Associate the project's one long-running AAC task with its runtime."""
        self._community_task = task
        task.add_done_callback(self._clear_community_task)

    def _clear_community_task(self, task: asyncio.Task) -> None:
        if self._community_task is task:
            self._community_task = None

    async def _stop_community_task(self) -> None:
        task = self._community_task
        self._community_task = None
        if task is None or task.done():
            return

        logger.info(f"Cancelling AAC discussion for {self.project_id}")
        task.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(task, return_exceptions=True),
                timeout=self.COMMUNITY_TASK_SHUTDOWN_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Timed out waiting for AAC discussion shutdown "
                f"for {self.project_id}"
            )

    async def shutdown(self):
        """Cleanly shuts down project-level background resources."""
        logger.info(f"Shutting down ProjectState resources for {self.project_id}")
        for unsubscribe in self.config_unsubscribers:
            unsubscribe()
        self.config_unsubscribers.clear()
        await self._stop_community_task()
        await self.document_service.shutdown()
        if self.scheduler:
            await self.scheduler.stop()
        # EntityResolver and others don't have explicit shutdown methods,
        # but they will be garbage collected.

    async def update_topics_config(self, new_config: dict):
        """Replace project topics, persist to Postgres, and refresh runtime mappings."""
        self.topic_config.replace(new_config)
        await self.topic_config.save(
            self.postgres_client,
            self.user_name,
            self.project_id,
        )
        self.refresh_topic_mappings()

    def refresh_topic_mappings(self):
        """Refresh runtime consumers after the shared TopicConfig changes."""
        self.entities.hierarchy_config = self.topic_config.hierarchy
        if self.batch_processor is not None:
            self.batch_processor.refresh_topic_mappings()
        else:
            self.pipeline.refresh_topic_mappings()
