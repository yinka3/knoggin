"""Composition of project-scoped live runtime state."""

from __future__ import annotations

import asyncio
from functools import partial
from typing import Any, Callable, cast

from common.conf.manager import ConfigManager
from common.scoping import (
    IDENTITY_ENTITY_ID,
    require_scope_value,
    require_visible_project_ids,
)
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.text_processor import TextProcessor
from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.documents import (
    DocumentIndexer,
    DocumentIndexPolicy,
    DocumentService,
    ProjectFilesystemFactory,
)
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
from runtime.resources import ReadyRuntimeResources, RuntimeResources


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
        resources = self.resources.require_ready()

        domain_store = DomainConfigStore(resources.postgres)
        domain_config = await domain_store.load(self.user_name, project_id)
        compiled_domain = domain_config.compile()
        entity_settings = self.dev_settings.entity_resolution
        entities = EntityResolver(
            project_id=project_id,
            readable_project_ids=readable_project_ids,
            knowledge_store=resources.knowledge_store,
            embedding_service=resources.embedding,
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
            embedding_service=resources.embedding,
            knowledge_store=resources.knowledge_store,
            postgres=resources.postgres,
            search_config={
                **runtime_config.developer_settings.search.model_dump(),
                **runtime_config.search.model_dump(),
            },
            active_topics=list(compiled_domain.active_topics),
        )

        text_processor = await asyncio.get_running_loop().run_in_executor(
            resources.executor,
            partial(
                TextProcessor,
                llm=resources.llm_service,
                get_known_aliases=entities.get_known_aliases,
                get_alias_version=entities.get_alias_version,
                get_profile=entities.get_profile,
                gliner=resources.gliner,
                spacy=resources.spacy,
                settings=self.dev_settings.nlp_pipeline,
                model_work=resources.model_work,
            ),
        )
        ingestion_pipeline = IngestionPipeline(
            project_id=project_id,
            llm=resources.llm_service,
            entities=entities,
            processor=text_processor,
            knowledge_store=resources.knowledge_store,
            cpu_executor=resources.executor,
            user_name=self.user_name,
            compiled_domain=compiled_domain,
            get_next_ent_id=resources.knowledge_store.allocate_entity_id,
            resolution_threshold=entity_settings.resolution_threshold,
            common_word_frequency_threshold=(
                entity_settings.common_word_frequency_threshold
            ),
            sparse_context_verbs=entity_settings.sparse_context_verbs,
        )
        scheduler = Scheduler(
            self.user_name,
            project_id,
            background_work=resources.background_work,
        )
        document_service = self._create_document_service(
            project_id,
            readable_project_ids=readable_project_ids,
            resources=resources,
        )
        runtime = ProjectRuntime(
            project_id=project_id,
            entities=entities,
            knowledge_retrieval=retrieval,
            text_processor=text_processor,
            scheduler=scheduler,
            user_name=self.user_name,
            readable_project_ids=readable_project_ids,
            domain_config=domain_config,
            document_service=document_service,
            domain_config_store=domain_store,
            ingestion_pipeline=ingestion_pipeline,
            background_work=resources.background_work,
        )
        episode_job = self._create_episode_job(project_id, resources=resources)
        runtime.episode_job = episode_job

        try:
            await runtime.document_indexer.start()
            self._register_background_jobs(
                runtime,
                entities=entities,
                processor=ingestion_pipeline,
                episode_job=episode_job,
                resources=resources,
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
        resources: ReadyRuntimeResources | None = None,
    ) -> DocumentService:
        resources = resources or cast(ReadyRuntimeResources, self.resources)
        resource_profile = resources.resource_profile
        runtime_config = ConfigManager.get().config
        document_settings = runtime_config.developer_settings.documents
        reader = DocumentReader(
            resources.postgres,
            project_id,
            readable_project_ids=readable_project_ids,
        )
        writer = DocumentWriter(resources.postgres, project_id)
        indexer = DocumentIndexer(
            project_id=project_id,
            reader=reader,
            writer=writer,
            embedding_service=resources.embedding,
            policy=DocumentIndexPolicy.capture(
                workspace_prepare_concurrency=(
                    resource_profile.workspace_prepare_concurrency
                )
            ),
            blocking_runner=asyncio.to_thread,
            background_work=resources.background_work,
            filesystem=ProjectFilesystemFactory(
                document_settings.project_library_root
            ).for_project(project_id),
        )
        document_service = DocumentService(
            project_id=project_id,
            postgres_client=resources.postgres,
            embedding_service=resources.embedding,
            background_work=resources.background_work,
            readable_project_ids=readable_project_ids,
            reader=reader,
            writer=writer,
            indexer=indexer,
            blocking_runner=asyncio.to_thread,
            document_rerank_enabled=document_settings.rerank_enabled,
            document_rerank_candidates=document_settings.rerank_candidates,
            filesystem_factory=ProjectFilesystemFactory(
                document_settings.project_library_root
            ),
            reconciliation_interval_seconds=(
                runtime_config.developer_settings.jobs.document_indexing.reconciliation_interval_seconds
            ),
        )
        return document_service

    async def _verify_user_entity(self, entities: EntityResolver) -> None:
        user_id = await entities.get_id(self.user_name)
        if user_id != IDENTITY_ENTITY_ID:
            raise RuntimeError(
                f"Configured user '{self.user_name}' did not resolve to reserved "
                f"entity ID {IDENTITY_ENTITY_ID}"
            )

    def _create_episode_job(
        self,
        project_id: str,
        *,
        resources: ReadyRuntimeResources | None = None,
    ) -> EpisodeJob:
        resources = resources or cast(ReadyRuntimeResources, self.resources)
        return EpisodeJob(
            knowledge_store=resources.knowledge_store,
            settings=self.dev_settings.jobs.episode,
            llm=resources.llm_service,
            embedding_service=resources.embedding,
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
        resources: ReadyRuntimeResources | None = None,
    ) -> None:
        resources = resources or cast(ReadyRuntimeResources, self.resources)
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
            knowledge_store=resources.knowledge_store,
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
            knowledge_store=resources.knowledge_store,
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
            knowledge_store=resources.knowledge_store,
            settings=jobs.conflict_discovery,
            llm=resources.llm_service,
        )
        scheduler.register(conflict_discovery_job)
        runtime.add_config_unsubscriber(
            config_manager.subscribe(
                conflict_discovery_job.update_settings,
                "developer_settings.jobs.conflict_discovery",
            )
        )
