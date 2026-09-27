"""Direct programmatic access to an installed Knoggin engine."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Optional
from uuid import uuid4

from common.schema.document import DocumentFocus as EngineDocumentFocus
from common.schema.document import FolderScanSettings, create_document_focus
from common.schema.primitives import Message
from common.utils.time_utils import get_now_iso
from runtime.application import ApplicationRuntime

from .contracts import (
    DocumentFocus,
    DocumentFocusDocument,
    DocumentFocusSubtree,
    SessionHandle,
    Turn,
)

_UNSET = object()


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
        config_dir: str | os.PathLike[str] | None = None,
        num_workers: Optional[int] = None,
    ) -> "Knoggin":
        resolved_config_dir = (
            Path(config_dir).expanduser()
            if config_dir is not None
            else _default_config_dir()
        )
        runtime = await ApplicationRuntime.start(
            user_name=user_name,
            config_dir=resolved_config_dir,
            num_workers=num_workers,
        )
        return cls(runtime)

    async def get_engine_health(self) -> dict[str, Any]:
        snapshot = await self.runtime.health_service.get_engine_health()
        return snapshot.model_dump(mode="json")

    async def get_resource_health(self, *, project_id: str) -> dict[str, Any]:
        snapshot = await self.runtime.health_service.get_resource_health(
            project_id=project_id
        )
        return snapshot.model_dump(mode="json")

    async def get_ingestion_health(self, *, project_id: str) -> dict[str, Any]:
        snapshot = await self.runtime.health_service.get_ingestion_health(
            user_name=self.runtime.sessions.user_name,
            project_id=project_id,
        )
        return snapshot.model_dump(mode="json")

    async def get_background_health(self, *, project_id: str) -> dict[str, Any]:
        snapshot = await self.runtime.health_service.get_background_health(
            project_id=project_id
        )
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

    @asynccontextmanager
    async def _project_documents(self, project_id: str):
        lease_id = f"sdk-documents:{uuid4()}"
        project = await self.runtime.projects.acquire_project_for_session(
            project_id,
            lease_id,
        )
        try:
            yield project.document_service
        finally:
            await self.runtime.projects.release_project_for_session(
                project_id, lease_id
            )

    async def list_documents(self, *, project_id: str, limit: int = 100):
        async with self._project_documents(project_id) as documents:
            return await documents.list_documents(limit=limit)

    async def get_document(self, *, project_id: str, document_id: str):
        async with self._project_documents(project_id) as documents:
            return await documents.get_document_info(document_id=document_id)

    async def read_document(
        self,
        *,
        project_id: str,
        document_id: str,
        start_line: int = 1,
        end_line: int | None = None,
    ):
        async with self._project_documents(project_id) as documents:
            return await documents.read_document(
                document_id=document_id,
                start_line=start_line,
                end_line=end_line,
            )

    async def upload_document(
        self,
        *,
        project_id: str,
        content: bytes,
        original_name: str,
        relative_path: str | None = None,
    ):
        async with self._project_documents(project_id) as documents:
            return await documents.submit_document(
                content=content,
                original_name=original_name,
                relative_path=relative_path,
            )

    async def reindex_document(self, *, project_id: str, document_id: str):
        async with self._project_documents(project_id) as documents:
            return await documents.reindex_document(document_id=document_id)

    async def delete_document(self, *, project_id: str, document_id: str):
        async with self._project_documents(project_id) as documents:
            return await documents.delete_document(document_id=document_id)

    async def list_saved_web_links(self, *, project_id: str, limit: int = 50):
        async with self._project_documents(project_id) as documents:
            return await documents.list_saved_web_links(limit=limit)

    async def update_saved_web_link(
        self,
        *,
        project_id: str,
        link_id: str,
        title: str | None | object = _UNSET,
        summary: str | None | object = _UNSET,
    ):
        updates = {}
        if title is not _UNSET:
            updates["title"] = title
        if summary is not _UNSET:
            updates["summary"] = summary
        async with self._project_documents(project_id) as documents:
            return await documents.update_saved_web_link(
                link_id=link_id,
                **updates,
            )

    async def delete_saved_web_link(self, *, project_id: str, link_id: str):
        async with self._project_documents(project_id) as documents:
            return await documents.delete_saved_web_link(link_id=link_id)

    async def get_document_scan_settings(self, *, project_id: str):
        async with self._project_documents(project_id) as documents:
            return (await documents.get_scan_settings()).model_dump(mode="json")

    async def set_document_scan_settings(
        self,
        *,
        project_id: str,
        settings: FolderScanSettings | dict[str, Any],
    ):
        async with self._project_documents(project_id) as documents:
            saved = await documents.save_scan_settings(settings)
            return saved.model_dump(mode="json")

    async def reset_document_scan_settings(self, *, project_id: str):
        async with self._project_documents(project_id) as documents:
            settings = await documents.reset_scan_settings()
            return settings.model_dump(mode="json")

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
        )
        return await context.open_agent_run_stream(
            message,
            model=turn.model,
            agent_id=turn.agent_id,
            enabled_tools=list(turn.enabled_tools)
            if turn.enabled_tools is not None
            else None,
            document_focus=document_focus,
            idempotency_key=(idempotency_key or "").strip() or None,
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.runtime.shutdown()


def _default_config_dir() -> Path:
    configured = os.environ.get("KNOGGIN_CONFIG_DIR")
    if configured:
        return Path(configured).expanduser()
    xdg_root = os.environ.get("XDG_CONFIG_HOME")
    if xdg_root:
        return Path(xdg_root).expanduser() / "knoggin"
    return Path.home() / ".config" / "knoggin"


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
                document_id=focus.document_id,
            )
        elif isinstance(focus, DocumentFocusSubtree):
            target = await context.document_service.resolve_focus_target(
                path_prefix=focus.path_prefix,
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
