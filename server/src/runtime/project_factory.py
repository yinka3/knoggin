import os

import redis.asyncio as aioredis

from common.conf.domain_config import DomainConfig
from common.scoping import require_scope_value, require_visible_project_ids
from core.ingestion.text_processor import TextProcessor
from core.knowledge.documents import DocumentService
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.services.embedding_service import EmbeddingService
from core.project.domain_config_store import DomainConfigStore
from infrastructure.background_work import BackgroundWorkCoordinator
from infrastructure.job.scheduler import Scheduler
from infrastructure.postgres_client import PostgresClient
from infrastructure.resource_profile import ResourceProfile
from runtime.project_runtime import ProjectRuntime


class ProjectFactory:
    """Construct project-scoped services and their live runtime container."""

    @staticmethod
    def create_runtime(
        *,
        project_id: str,
        domain_config: DomainConfig,
        entities: EntityResolver,
        pipeline: TextProcessor,
        scheduler: Scheduler,
        user_name: str,
        redis_client: aioredis.Redis,
        readable_project_ids: list[str],
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
        batch_processor=None,
        background_work: BackgroundWorkCoordinator | None = None,
        domain_config_store: DomainConfigStore | None = None,
    ) -> ProjectRuntime:
        """Build the document boundary before creating the live runtime."""
        require_scope_value(project_id, "project_id", "ProjectRuntime")
        require_visible_project_ids(readable_project_ids, "ProjectRuntime")
        resource_profile = ResourceProfile.from_environment()
        document_service = DocumentService(
            project_id=project_id,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            background_work=background_work,
            document_rerank_enabled=os.getenv(
                "KNOGGIN_DOCUMENT_RERANK_ENABLED", "true"
            )
            .strip()
            .lower()
            in {"1", "true", "yes", "on"},
            document_rerank_candidates=int(
                os.getenv("KNOGGIN_DOCUMENT_RERANK_CANDIDATES", "15")
            ),
            workspace_prepare_concurrency=resource_profile.workspace_prepare_concurrency,
        )
        return ProjectRuntime(
            project_id=project_id,
            entities=entities,
            pipeline=pipeline,
            scheduler=scheduler,
            user_name=user_name,
            redis_client=redis_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            domain_config=domain_config,
            document_service=document_service,
            readable_project_ids=readable_project_ids,
            batch_processor=batch_processor,
            background_work=background_work,
            domain_config_store=domain_config_store,
        )
