"""Composition of project-scoped live runtime state."""

from __future__ import annotations

import asyncio
import os
from functools import partial
from typing import Any, Callable

from common.conf.manager import ConfigManager
from common.scoping import (
    IDENTITY_ENTITY_ID,
    require_scope_value,
    require_visible_project_ids,
)
from core.community.community_job import AACJob
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.text_processor import TextProcessor
from core.knowledge.documents import DocumentService
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.episodes.job import EpisodeJob
from core.knowledge.jobs.audit_retention_cleanup_job import (
    AuditRetentionCleanupJob,
)
from core.knowledge.jobs.conflict_discovery_job import ConflictDiscoveryJob
from core.knowledge.jobs.merge_rollback_cleanup_job import MergeCleanupJob
from core.knowledge.retrieval import KnowledgeRetrieval
from core.project.domain_config_store import DomainConfigStore
from infrastructure.job.scheduler import Scheduler
from runtime.project_runtime import ProjectRuntime
from runtime.resources import RuntimeResources


class ProjectRuntimeFactory:
    """Build a complete project runtime from explicit engine-wide resources."""

    def __init__(
        self,
        *,
        resources: RuntimeResources,
        user_name: str,
        episode_window_size_provider: Callable[[str], Any],
    ) -> None:
        self.resources = resources
        self.user_name = user_name
        self._episode_window_size_provider = episode_window_size_provider

    @property
    def dev_settings(self):
        return ConfigManager.get().config.developer_settings

    async def create(
        self,
        *,
        project_id: str,
        readable_project_ids: list[str],
    ) -> ProjectRuntime:
        """Construct, register, and start all project-scoped runtime services."""

        require_scope_value(project_id, "project_id", "ProjectRuntimeFactory")
        require_visible_project_ids(readable_project_ids, "ProjectRuntimeFactory")
        if (
            self.resources.knowledge_store is None
            or self.resources.embedding is None
            or self.resources.executor is None
            or self.resources.redis is None
            or self.resources.postgres is None
            or self.resources.llm_service is None
        ):
            raise RuntimeError("Runtime resources are not ready for project startup")

        domain_store = DomainConfigStore(self.resources.postgres)
        domain_config = await domain_store.load(self.user_name, project_id)
        compiled_domain = domain_config.compile()
        entity_settings = self.dev_settings.entity_resolution
        entities = EntityResolver(
            project_id=project_id,
            readable_project_ids=readable_project_ids,
            knowledge_store=self.resources.knowledge_store,
            embedding_service=self.resources.embedding,
            fuzzy_substring_threshold=entity_settings.fuzzy_substring_threshold,
            fuzzy_non_substring_threshold=entity_settings.fuzzy_non_substring_threshold,
            generic_token_freq=entity_settings.generic_token_freq,
            candidate_fuzzy_threshold=entity_settings.candidate_fuzzy_threshold,
            candidate_vector_threshold=entity_settings.candidate_vector_threshold,
        )
        await self._verify_user_entity(entities)

        runtime_config = ConfigManager.get().config
        retrieval = KnowledgeRetrieval(
            project_id=project_id,
            readable_project_ids=readable_project_ids,
            user_name=self.user_name,
            entities=entities,
            embedding_service=self.resources.embedding,
            knowledge_store=self.resources.knowledge_store,
            postgres=self.resources.postgres,
            redis=self.resources.redis,
            search_config={
                **runtime_config.developer_settings.search.model_dump(),
                **runtime_config.search.model_dump(),
            },
            active_topics=list(compiled_domain.active_topics),
        )

        pipeline = await asyncio.get_running_loop().run_in_executor(
            self.resources.executor,
            partial(
                TextProcessor,
                llm=self.resources.llm_service,
                get_known_aliases=entities.get_known_aliases,
                get_alias_version=entities.get_alias_version,
                get_profile=entities.get_profile,
                gliner=self.resources.gliner,
                spacy=self.resources.spacy,
                settings=self.dev_settings.nlp_pipeline,
                model_work=self.resources.model_work,
            ),
        )
        project_processor = IngestionPipeline(
            project_id=project_id,
            llm=self.resources.llm_service,
            entities=entities,
            processor=pipeline,
            knowledge_store=self.resources.knowledge_store,
            cpu_executor=self.resources.executor,
            user_name=self.user_name,
            compiled_domain=compiled_domain,
            get_next_ent_id=self.resources.knowledge_store.allocate_entity_id,
            resolution_threshold=entity_settings.resolution_threshold,
            common_word_frequency_threshold=(
                entity_settings.common_word_frequency_threshold
            ),
            sparse_context_verbs=entity_settings.sparse_context_verbs,
        )
        scheduler = Scheduler(
            self.user_name,
            project_id,
            background_work=self.resources.background_work,
        )
        document_service = self._create_document_service(
            project_id,
            readable_project_ids=readable_project_ids,
        )
        runtime = ProjectRuntime(
            project_id=project_id,
            entities=entities,
            knowledge_retrieval=retrieval,
            pipeline=pipeline,
            scheduler=scheduler,
            user_name=self.user_name,
            readable_project_ids=readable_project_ids,
            domain_config=domain_config,
            document_service=document_service,
            domain_config_store=domain_store,
            batch_processor=project_processor,
            background_work=self.resources.background_work,
        )
        episode_job = self._create_episode_job(project_id)
        runtime.episode_job = episode_job

        try:
            await runtime.document_indexer.start()
            self._register_background_jobs(
                runtime,
                entities=entities,
                processor=project_processor,
                episode_job=episode_job,
            )
            await scheduler.start()
        except Exception:
            await runtime.shutdown()
            raise
        return runtime

    def _create_document_service(
        self,
        project_id: str,
        *,
        readable_project_ids: list[str],
    ) -> DocumentService:
        resource_profile = self.resources.resource_profile
        if resource_profile is None:
            raise RuntimeError("Runtime resource profile is unavailable")
        return DocumentService(
            project_id=project_id,
            postgres_client=self.resources.postgres,
            embedding_service=self.resources.embedding,
            background_work=self.resources.background_work,
            readable_project_ids=readable_project_ids,
            document_rerank_enabled=os.getenv("KNOGGIN_DOCUMENT_RERANK_ENABLED", "true")
            .strip()
            .lower()
            in {"1", "true", "yes", "on"},
            document_rerank_candidates=int(
                os.getenv("KNOGGIN_DOCUMENT_RERANK_CANDIDATES", "15")
            ),
            workspace_prepare_concurrency=resource_profile.workspace_prepare_concurrency,
        )

    async def _verify_user_entity(self, entities: EntityResolver) -> None:
        user_id = await entities.get_id(self.user_name)
        if user_id != IDENTITY_ENTITY_ID:
            raise RuntimeError(
                f"Configured user '{self.user_name}' did not resolve to reserved "
                f"entity ID {IDENTITY_ENTITY_ID}"
            )

    def _create_episode_job(self, project_id: str) -> EpisodeJob:
        return EpisodeJob(
            knowledge_store=self.resources.knowledge_store,
            settings=self.dev_settings.jobs.episode,
            llm=self.resources.llm_service,
            embedding_service=self.resources.embedding,
            episode_window_size_provider=lambda: self._episode_window_size_provider(
                project_id
            ),
        )

    def _register_background_jobs(
        self,
        runtime: ProjectRuntime,
        *,
        entities: EntityResolver,
        processor: IngestionPipeline,
        episode_job: EpisodeJob,
    ) -> None:
        scheduler = runtime.scheduler
        jobs = self.dev_settings.jobs
        config_manager = ConfigManager.get()

        def update_entity_resolution(settings):
            entities.update_settings(settings)
            processor.update_settings(settings)

        runtime.add_config_unsubscriber(
            config_manager.subscribe(
                update_entity_resolution,
                "developer_settings.entity_resolution",
            )
        )
        runtime.add_config_unsubscriber(
            config_manager.subscribe(
                processor.update_settings,
                "developer_settings.nlp_pipeline",
            )
        )
        scheduler.register(episode_job)
        runtime.add_config_unsubscriber(
            config_manager.subscribe(
                episode_job.update_settings,
                "developer_settings.jobs.episode",
            )
        )

        merge_cleanup_job = MergeCleanupJob(
            knowledge_store=self.resources.knowledge_store,
            settings=jobs.merge_rollback,
        )
        scheduler.register(merge_cleanup_job)
        runtime.add_config_unsubscriber(
            config_manager.subscribe(
                merge_cleanup_job.update_settings,
                "developer_settings.jobs.merge_rollback",
            )
        )

        audit_retention_job = AuditRetentionCleanupJob(
            knowledge_store=self.resources.knowledge_store,
            settings=jobs.audit_retention,
        )
        scheduler.register(audit_retention_job)
        runtime.add_config_unsubscriber(
            config_manager.subscribe(
                audit_retention_job.update_settings,
                "developer_settings.jobs.audit_retention",
            )
        )

        conflict_discovery_job = ConflictDiscoveryJob(
            knowledge_store=self.resources.knowledge_store,
            settings=jobs.conflict_discovery,
            llm=self.resources.llm_service,
        )
        scheduler.register(conflict_discovery_job)
        runtime.add_config_unsubscriber(
            config_manager.subscribe(
                conflict_discovery_job.update_settings,
                "developer_settings.jobs.conflict_discovery",
            )
        )
        scheduler.register(AACJob(runtime, self.resources))
