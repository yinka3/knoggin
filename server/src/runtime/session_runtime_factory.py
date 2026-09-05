from typing import Optional

from loguru import logger

from common.schema.document import DocumentFocus
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

        # Sessions share the project-owned document boundary.
        ctx.document_service = project_state.document_service

        return ctx

    async def _launch(self, ctx: SessionRuntime):
        """Confirm the project-owned semantic owner is available."""
        if ctx.project.project_semantic_job is None:
            raise RuntimeError("project semantic job is not registered")
        logger.info(f"System launched successfully for session {ctx.session_id}")
