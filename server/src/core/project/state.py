from pathlib import Path
from typing import Any, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.conf.topics_config import TopicConfig
from common.scoping import require_scope_value, require_visible_project_ids
from infrastructure.job.scheduler import Scheduler
from infrastructure.postgres_client import PostgresClient
from core.ingestion.services.processor import TextProcessor
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.documents import DocumentService
from core.knowledge.services.embedding_service import EmbeddingService


class ProjectState:
    """
    Holds the runtime shared resources for a Project.
    """

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
        document_storage_root: Path,
        embedding_service: EmbeddingService,
        batch_processor: Optional[Any] = None,
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
        self.document_storage_root = document_storage_root
        self.embedding_service = embedding_service
        self.batch_processor = batch_processor
        self.document_service = DocumentService(
            project_id=project_id,
            postgres_client=postgres_client,
            storage_root=document_storage_root,
            embedding_service=embedding_service,
        )

        self.profile_job: Optional[Any] = None
        self.active_runtime_sessions_count = 0
        self.config_unsubscribers: list[Any] = []

    async def record_session_activity(self):
        """Record user activity against the project-level scheduler."""
        await self.scheduler.record_activity()

    def add_config_unsubscriber(self, unsubscribe):
        self.config_unsubscribers.append(unsubscribe)

    async def shutdown(self):
        """Cleanly shuts down project-level background resources."""
        logger.info(f"Shutting down ProjectState resources for {self.project_id}")
        for unsubscribe in self.config_unsubscribers:
            unsubscribe()
        self.config_unsubscribers.clear()
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
