"""Direct programmatic access to an installed Knoggin engine."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional

from common.schema.document import DocumentFocus as EngineDocumentFocus
from common.schema.document import create_document_focus
from common.schema.primitives import Message
from common.utils.time_utils import get_now_iso
from runtime.application import ApplicationRuntime

from .contracts import (
    DocumentFocus,
    DocumentFocusDocument,
    DocumentFocusFolderUpload,
    DocumentFocusSubtree,
    SessionHandle,
    Turn,
)


class Knoggin:
    """Programmatic surface for one installed Knoggin engine."""

    def __init__(self, runtime: ApplicationRuntime):
        self.runtime = runtime
        self._closed = False

    @classmethod
    async def start(
        cls,
        *,
        user_name: str,
        num_workers: Optional[int] = None,
    ) -> "Knoggin":
        runtime = await ApplicationRuntime.start(
            user_name=user_name,
            num_workers=num_workers,
        )
        return cls(runtime)

    async def get_engine_health(self) -> dict[str, Any]:
        snapshot = await self.runtime.health_service.get_engine_health()
        return snapshot.model_dump(mode="json")

    async def create_project(
        self,
        *,
        name: str,
        domain_config: dict[str, Any],
        description: str | None = None,
    ) -> dict[str, Any] | None:
        return await self.runtime.projects.create_project(
            name=name,
            domain_config=domain_config,
            description=description,
        )

    async def create_session(
        self,
        *,
        project_id: str,
        model: str | None = None,
        agent_id: str | None = None,
        enabled_tools: list[str] | None = None,
    ) -> SessionHandle:
        session = await self.runtime.sessions.create_session(
            project_id=project_id,
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
        )
        return SessionHandle(
            session_id=session.session_id,
            project_id=session.project_id,
            model=session.model,
        )

    async def open_turn_stream(
        self,
        *,
        session_id: str,
        turn: Turn,
        idempotency_key: str | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Admit and persist a turn, then return its canonical engine stream.

        The returned stream is not a detached run resource. The engine remains
        responsible for serializing session execution and durably committing
        its final answer before exposing the response event. Admission happens
        before this method returns, so an overlapping turn raises immediately.
        """

        session_id = session_id.strip()
        if not session_id:
            raise ValueError("session_id is required")
        context = await self.runtime.sessions.get_or_resume_session(session_id)
        if context is None:
            raise LookupError("session_not_found")

        document_focus = await _resolve_document_focus(context, turn.document_focus)
        message = Message(
            content=turn.content.strip(),
            metadata={"idempotency_key": (idempotency_key or "").strip()},
        )
        return await context.open_agent_run_stream(
            message,
            model=turn.model,
            agent_id=turn.agent_id,
            enabled_tools=list(turn.enabled_tools) if turn.enabled_tools else None,
            document_focus=document_focus,
            idempotency_key=(idempotency_key or "").strip() or None,
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.runtime.shutdown()


async def _resolve_document_focus(
    context: Any,
    focus: DocumentFocus | None,
) -> EngineDocumentFocus | None:
    """Resolve an SDK selection through the session's visible documents."""

    if focus is None:
        return None
    if context.document_service is None:
        raise ValueError("No project document service is available for this request")

    try:
        if isinstance(focus, DocumentFocusDocument):
            target = await context.document_service.resolve_focus_target(
                session_id=context.session_id,
                document_id=focus.document_id,
            )
        elif isinstance(focus, DocumentFocusSubtree):
            target = await context.document_service.resolve_focus_target(
                session_id=context.session_id,
                folder_root_id=focus.folder_root_id,
                path_prefix=focus.path_prefix,
            )
        elif isinstance(focus, DocumentFocusFolderUpload):
            target = await context.document_service.resolve_focus_target(
                session_id=context.session_id,
                folder_root_id=focus.folder_root_id,
            )
        else:
            raise TypeError("document_focus has an unsupported type")
    except FileNotFoundError as exc:
        raise ValueError(
            "The selected document focus is not visible in this session"
        ) from exc

    return create_document_focus(
        mode="request",
        created_at=get_now_iso(),
        **target,
    )
