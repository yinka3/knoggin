from typing import Any, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.conf.topics_config import TopicConfig
from infrastructure.job.scheduler import Scheduler
from knoggin.ingestion.services.processor import TextProcessor
from knoggin.knowledge.services.entity_service import EntityManager


class ProjectState:
    """
    Holds the runtime shared resources for a Project.
    """

    def __init__(
        self,
        project_id: str,
        topic_config: TopicConfig,
        entities: EntityManager,
        pipeline: TextProcessor,
        scheduler: Scheduler,
        user_name: str,
        redis_client: aioredis.Redis,
    ):
        self.project_id = project_id
        self.topic_config = topic_config
        self.entities = entities
        self.pipeline = pipeline
        self.scheduler = scheduler
        self.user_name = user_name
        self.redis_client = redis_client

        self.profile_job: Optional[Any] = None
        self.merge_job: Optional[Any] = None
        self.active_sessions_count = 0

    async def shutdown(self):
        """Cleanly shuts down project-level background resources."""
        logger.info(f"Shutting down ProjectState resources for {self.project_id}")
        if self.scheduler:
            await self.scheduler.stop()
        # EntityManager and others don't have explicit shutdown methods,
        # but they will be garbage collected.

    async def update_topics_config(self, new_config: dict):
        """Update topic config, save to redis, and refresh project components."""
        self.topic_config.update(new_config)
        await self.topic_config.save(self.redis_client, self.user_name, self.project_id)
        self.entities.hierarchy_config = self.topic_config.hierarchy
        self.pipeline.refresh_topic_mappings(self.topic_config)

