import uuid
from typing import Callable, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.utils.events import EventEmitter
from infrastructure.resources import ResourceManager
from knoggin_server.ingestion.services.batch_consumer import IngestionWorker
from knoggin_server.ingestion.services.pipeline_service import IngestionPipeline
from knoggin_server.project.state import ProjectState
from knoggin_server.session.context import Session

class SessionFactory:
    """
    Wires together the infrastructure, services, and background jobs for a session.
    Decouples construction from the Session state container.
    """

    def __init__(self, user_name: str, resources: ResourceManager):
        self.user_name = user_name
        self.resources = resources

    @property
    def config(self):
        return ConfigManager.get().config

    @property
    def dev_settings(self):
        return self.config.developer_settings

    async def bootstrap(
        self,
        project_state: ProjectState,
        session_id: Optional[str] = None,
        model: Optional[str] = None,
    ) -> Session:
        """Perform the multi-phase boot sequence: assemble + launch."""
        ctx = await self.assemble(project_state, session_id, model)
        await self.launch(ctx)
        return ctx

    async def assemble(
        self,
        project_state: ProjectState,
        session_id: Optional[str] = None,
        model: Optional[str] = None,
    ) -> Session:
        """
        Wires together services and infrastructure into a Session.
        Does NOT start background loops.
        """
        session_id = session_id or str(uuid.uuid4())

        # Instantiate Session shell first
        ctx = Session(
            self.user_name,
            list(project_state.topic_config.raw.keys()),
            self.resources,
        )
        ctx.session_id = session_id
        ctx.project_id = project_state.project_id
        ctx.project = project_state
        ctx.model = model

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

        # Register session to emitter for project event propagation
        EventEmitter.get().register_session(project_state.project_id, session_id)

        return ctx

    async def launch(self, ctx: Session):
        """Starts background tasks and jobs for the context."""
        if ctx.consumer:
            if ctx.consumer.get_session_context is None:
                raise RuntimeError("consumer.get_session_context callback not wired")
            if ctx.consumer.write_to_graph is None:
                raise RuntimeError("consumer.write_to_graph callback not wired")

        if ctx.batch_processor:
            if ctx.batch_processor._get_next_ent_id is None:
                raise RuntimeError("batch_processor.get_next_ent_id callback not wired")

        # Start the project scheduler if not already running
        if ctx.project and ctx.project.scheduler and not ctx.project.scheduler.running:
            await ctx.project.scheduler.start()

        if ctx.consumer:
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
        batch_size = ingest_cfg.batch_size
        batch_timeout = ingest_cfg.batch_timeout
        checkpoint_interval = batch_size * 4
        session_window = batch_size * 3

        return IngestionWorker(
            user_name=self.user_name,
            session_id=session_id,
            knowledge_store=self.resources.knowledge_store,
            redis=self.resources.redis,
            processor=processor,
            get_session_context=get_session_context,
            write_to_graph=write_to_graph,
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            checkpoint_interval=checkpoint_interval,
            session_window=session_window,
        )
