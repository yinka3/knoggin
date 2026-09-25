"""Composition of project-scoped live runtime state."""

from __future__ import annotations

import asyncio
from functools import partial
from typing import cast

from common.conf.manager import ConfigManager
from common.scoping import (
    IDENTITY_ENTITY_ID,
    require_scope_value,
    require_visible_project_ids,
)
from core.ingestion.context_entity_build import ContextEntityBuildService
from core.ingestion.project_semantic_processor import ProjectSemanticProcessor
from core.ingestion.relationship_extractor import ContextRelationshipExtractor
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from core.ingestion.text_processor import TextProcessor
from core.knowledge.context.projection import ContextProjection
from core.knowledge.context.updater import ContextUpdater
from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.documents import (
    DocumentIndexer,
    DocumentIndexPolicy,
    DocumentService,
    ProjectFilesystemFactory,
)
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.episodes.generator import EpisodeGenerator
from core.knowledge.jobs.conflict_discovery_job import ConflictDiscoveryJob
from core.knowledge.retrieval import KnowledgeRetrieval
from core.project.domain_config_store import DomainConfigStore
from core.project.maintenance_service import ProjectMaintenanceService
from infrastructure.job.base import JobContext
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
        maintenance_service: ProjectMaintenanceService | None = None,
        config_manager: ConfigManager | None = None,
    ) -> None:
        self.resources = resources
        self.user_name = user_name
        self._maintenance_service = maintenance_service
        self._config_manager = config_manager

    @property
    def dev_settings(self):
        return self._config().config.developer_settings

    def _config(self) -> ConfigManager:
        return self._config_manager or ConfigManager.get()

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
            candidate_fuzzy_threshold=entity_settings.candidate_fuzzy_threshold,
        )
        await self._verify_user_entity(entities)

        runtime_config = self._config().config
        retrieval = KnowledgeRetrieval(
            project_id=project_id,
            readable_project_ids=readable_project_ids,
            user_name=self.user_name,
            entities=entities,
            embedding_service=resources.embedding,
            knowledge_store=resources.knowledge_store,
            search_config={
                **runtime_config.developer_settings.search.model_dump(),
                **runtime_config.search.model_dump(),
            },
        )

        text_processor = await asyncio.get_running_loop().run_in_executor(
            resources.executor,
            partial(
                TextProcessor,
                get_known_aliases=entities.get_known_aliases,
                get_alias_version=entities.get_alias_version,
                get_profile=entities.get_profile,
                vp01=await resources.get_vp01(compiled_domain.vp01_language),
                spacy=resources.spacy,
                settings=self.dev_settings.nlp_pipeline,
                model_work=resources.model_work,
                get_vp01=resources.get_vp01,
                llm=resources.llm_service,
                user_name=self.user_name,
            ),
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
            background_work=resources.background_work,
            get_vp01=resources.get_vp01,
        )
        project_semantic_processor = self._create_project_semantic_processor(
            runtime, resources=resources
        )
        runtime.project_semantic_processor = project_semantic_processor
        conflict_discovery_job = self._create_conflict_discovery_job(
            resources=resources
        )
        runtime.conflict_discovery_job = conflict_discovery_job

        try:
            await project_semantic_processor.synchronize_context_file(
                JobContext(user_name=self.user_name, project_id=project_id),
                allow_user_edit=True,
            )
            await runtime.document_service.indexer.start()
            self._register_background_jobs(
                runtime,
                entities=entities,
                processor=text_processor,
                project_semantic_processor=project_semantic_processor,
                conflict_discovery_job=conflict_discovery_job,
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
        runtime_config = self._config().config
        document_settings = runtime_config.developer_settings.documents
        filesystem_factory = ProjectFilesystemFactory(
            self._config().resolve_path(document_settings.project_library_root)
        )
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
            policy=DocumentIndexPolicy.capture(),
            blocking_runner=asyncio.to_thread,
            background_work=resources.background_work,
            filesystem=filesystem_factory.for_project(project_id),
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
            filesystem_factory=filesystem_factory,
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

    def _create_project_semantic_processor(
        self,
        runtime: ProjectRuntime,
        *,
        resources: ReadyRuntimeResources | None = None,
    ) -> ProjectSemanticProcessor:
        resources = resources or cast(ReadyRuntimeResources, self.resources)
        token_counter = getattr(resources.llm_service, "count_tokens", None)
        if not callable(token_counter):
            raise RuntimeError(
                "LLM service must provide count_tokens for semantic windows"
            )
        admission = SemanticWindowAdmission(
            resources.knowledge_store,
            self.dev_settings.ingestion,
            token_counter=token_counter,
            episode_settings=self.dev_settings.jobs.episode,
        )
        episode_generator = EpisodeGenerator(
            llm=resources.llm_service,
            embedding_service=resources.embedding,
        )
        context_filesystem = ProjectFilesystemFactory(
            self._config().resolve_path(
                self.dev_settings.documents.project_library_root
            )
        ).for_project(runtime.project_id)
        context_projection = ContextProjection(
            reader=ProjectContextReader(resources.postgres),
            writer=ProjectContextWriter(resources.postgres),
            filesystem=context_filesystem,
        )
        return ProjectSemanticProcessor(
            admission,
            resources.knowledge_store,
            episode_generator,
            settings=self.dev_settings.ingestion,
            capture_semantic_policy=runtime.capture_semantic_policy,
            context_updater=ContextUpdater(llm=resources.llm_service),
            context_projection=context_projection,
            context_entity_builder=ContextEntityBuildService(
                processor=runtime.text_processor,
                resolver=runtime.entities,
                allocate_entity_id=resources.knowledge_store.allocate_entity_id,
            ),
            context_relationship_extractor=ContextRelationshipExtractor(
                user_name=self.user_name,
                llm=resources.llm_service,
                entities=runtime.entities,
            ),
            publish_committed_entity_ids=runtime.entities.publish_committed_entity_ids,
        )

    def _create_conflict_discovery_job(
        self,
        *,
        resources: ReadyRuntimeResources | None = None,
    ) -> ConflictDiscoveryJob | None:
        if self._maintenance_service is None:
            return None
        resources = resources or cast(ReadyRuntimeResources, self.resources)
        return ConflictDiscoveryJob(
            self._maintenance_service,
            self.dev_settings.jobs.conflict_discovery,
            llm=resources.llm_service,
        )

    def _register_background_jobs(
        self,
        runtime: ProjectRuntime,
        *,
        entities: EntityResolver,
        processor: TextProcessor,
        project_semantic_processor: ProjectSemanticProcessor | None = None,
        conflict_discovery_job: ConflictDiscoveryJob | None = None,
        resources: ReadyRuntimeResources | None = None,
    ) -> None:
        scheduler = runtime.scheduler
        config_manager = self._config()

        def update_entity_resolution(settings):
            entities.update_settings(settings)

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
        if project_semantic_processor is not None:
            scheduler.register(project_semantic_processor)
            runtime.add_config_unsubscriber(
                config_manager.subscribe(
                    project_semantic_processor.update_settings,
                    "developer_settings.ingestion",
                )
            )
            runtime.add_config_unsubscriber(
                config_manager.subscribe(
                    project_semantic_processor.update_episode_settings,
                    "developer_settings.jobs.episode",
                )
            )
        if conflict_discovery_job is not None:
            scheduler.register(conflict_discovery_job)
            runtime.add_config_unsubscriber(
                config_manager.subscribe(
                    conflict_discovery_job.update_settings,
                    "developer_settings.jobs.conflict_discovery",
                )
            )
