from typing import Callable, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.document import DocumentFocus
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.worker import IngestionWorker
from runtime.project_runtime import ProjectRuntime
from runtime.resources import RuntimeResources
from runtime.session_runtime import SessionRuntime


class SessionRuntimeFactory:
    """
    Wires together the infrastructure, services, and background jobs for a session.
    Decouples construction from the Session state container.
    """

    def __init__(
        self,
        user_name: str,
        resources: RuntimeResources,
        *,
        health_service=None,
        agent_orchestrator=None,
    ):
        self.user_name = user_name
        self.resources = resources
        self.health_service = health_service
        self.agent_orchestrator = agent_orchestrator

    @property
    def config(self):
        return ConfigManager.get().config

    @property
    def dev_settings(self):
        return self.config.developer_settings

    async def create(
        self,
        project_state: ProjectRuntime,
        *,
        session_id: str,
        model: Optional[str],
        agent_id: Optional[str],
        enabled_tools: Optional[list[str]],
        document_focus: Optional[DocumentFocus] = None,
    ) -> SessionRuntime:
        """Create and launch one fully wired session runtime."""
        ctx = await self._assemble(
            project_state,
            session_id=session_id,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            document_focus=document_focus,
        )
        try:
            await self._launch(ctx)
        except Exception:
            await ctx.shutdown()
            raise
        return ctx

    async def _assemble(
        self,
        project_state: ProjectRuntime,
        *,
        session_id: str,
        model: Optional[str],
        agent_id: Optional[str],
        enabled_tools: Optional[list[str]],
        document_focus: Optional[DocumentFocus] = None,
    ) -> SessionRuntime:
        """
        Wires together services and infrastructure into a Session.
        Does NOT start background loops.
        """
        # Instantiate the live session shell after durable identity exists.
        ctx = SessionRuntime(
            self.user_name,
            self.resources,
            session_id=session_id,
            project_id=project_state.project_id,
            project=project_state,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            document_focus=document_focus,
            health_service=self.health_service,
            agent_orchestrator=self.agent_orchestrator,
        )

        # Use the project-owned processor so config updates and background jobs
        # share the same ingestion runtime as session workers.
        ingestion_pipeline = project_state.ingestion_pipeline
        if ingestion_pipeline is None:
            raise RuntimeError("project_state.ingestion_pipeline not wired")
        ctx.ingestion_pipeline = ingestion_pipeline

        # Initialize the session-owned ingestion worker with direct callbacks.
        ingestion_worker = self._init_ingestion_worker(
            session_id,
            ingestion_pipeline,
            get_session_context=ctx.get_conversation_context,
            write_to_graph=ctx._write_to_graph_callback,
        )
        ctx.ingestion_worker = ingestion_worker

        ctx.config_unsubscribers.append(
            ConfigManager.get().subscribe(
                ingestion_worker.update_settings, "developer_settings.ingestion"
            )
        )

        # Sessions share the project-owned document boundary.
        ctx.document_service = project_state.document_service

        return ctx

    async def _launch(self, ctx: SessionRuntime):
        """Starts background tasks and jobs for the context."""
        if ctx.ingestion_worker:
            if ctx.ingestion_worker.get_session_context is None:
                raise RuntimeError("ingestion_worker.get_session_context callback not wired")
            if ctx.ingestion_worker.write_to_graph is None:
                raise RuntimeError("ingestion_worker.write_to_graph callback not wired")

        if ctx.ingestion_pipeline:
            if ctx.ingestion_pipeline._get_next_ent_id is None:
                raise RuntimeError(
                    "ingestion_pipeline.get_next_ent_id callback not wired"
                )

        if ctx.ingestion_worker:
            reset_message_ids = await self.resources.knowledge_store.reset_claimed_ingestion(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
                session_id=ctx.session_id,
            )
            if reset_message_ids:
                logger.info(
                    "Released {} stale ingestion claims for session {}",
                    len(reset_message_ids),
                    ctx.session_id,
                )
            ctx.ingestion_worker.start()

        logger.info(f"System launched successfully for session {ctx.session_id}")

    def _init_ingestion_worker(
        self,
        session_id: str,
        processor: IngestionPipeline,
        get_session_context: Callable,
        write_to_graph: Callable,
    ) -> IngestionWorker:
        ingest_cfg = self.dev_settings.ingestion

        return IngestionWorker(
            user_name=self.user_name,
            session_id=session_id,
            knowledge_store=self.resources.knowledge_store,
            processor=processor,
            get_session_context=get_session_context,
            write_to_graph=write_to_graph,
            settings=ingest_cfg,
        )
