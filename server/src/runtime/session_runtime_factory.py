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
    ):
        self.user_name = user_name
        self.resources = resources
        self.health_service = health_service

    @property
    def config(self):
        return ConfigManager.get().config

    @property
    def dev_settings(self):
        return self.config.developer_settings

    async def bootstrap(
        self,
        project_state: ProjectRuntime,
        *,
        session_id: str,
        model: Optional[str],
        agent_id: Optional[str],
        enabled_tools: Optional[list[str]],
        document_focus: Optional[DocumentFocus] = None,
    ) -> SessionRuntime:
        """Perform the multi-phase boot sequence: assemble + launch."""
        ctx = await self.assemble(
            project_state,
            session_id=session_id,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            document_focus=document_focus,
        )
        try:
            await self.launch(ctx)
        except Exception:
            await ctx.shutdown()
            raise
        return ctx

    async def assemble(
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
            health_service=self.health_service,
        )
        ctx.session_id = session_id
        ctx.project_id = project_state.project_id
        ctx.project = project_state
        ctx.model = model
        ctx.agent_id = agent_id
        ctx.enabled_tools = (
            list(enabled_tools) if enabled_tools is not None else None
        )
        ctx.document_focus = document_focus

        # Use the project-owned processor so config updates and background jobs
        # share the same ingestion runtime as session consumers.
        processor = project_state.batch_processor
        if processor is None:
            raise RuntimeError("project_state.batch_processor not wired")
        ctx.batch_processor = processor

        # Initialize Batch Consumer with direct callbacks
        consumer = self._init_batch_consumer(
            session_id,
            processor,
            get_session_context=ctx.get_conversation_context,
            write_to_graph=ctx._write_to_graph_callback,
        )
        ctx.consumer = consumer

        ctx.config_unsubscribers.append(
            ConfigManager.get().subscribe(
                consumer.update_settings, "developer_settings.ingestion"
            )
        )

        # Sessions share the project-owned document boundary.
        ctx.document_service = project_state.document_service

        return ctx

    async def launch(self, ctx: SessionRuntime):
        """Starts background tasks and jobs for the context."""
        if ctx.consumer:
            if ctx.consumer.get_session_context is None:
                raise RuntimeError("consumer.get_session_context callback not wired")
            if ctx.consumer.write_to_graph is None:
                raise RuntimeError("consumer.write_to_graph callback not wired")

        if ctx.batch_processor:
            if ctx.batch_processor._get_next_ent_id is None:
                raise RuntimeError("batch_processor.get_next_ent_id callback not wired")

        if ctx.consumer:
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
            ctx.consumer.start()

        logger.info(f"System launched successfully for session {ctx.session_id}")

    def _init_batch_consumer(
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
