"""Application-port implementation backed by the canonical runtime owners.

The FastAPI module deliberately remains dependency-injected.  This adapter is
the composition seam for a live :class:`ApplicationRuntime`: it translates
public requests into the existing project/session workflows and translates the
internal agent event stream back into the versioned public stream contract.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from common.conf.domain_config import DomainConfig
from common.exceptions import NotFoundError
from common.schema.document import DocumentSelection, create_document_focus
from common.schema.primitives import Message
from common.schema.public import (
    ArtifactListResponse,
    ArtifactResponse,
    CreateProjectRequest,
    CreateSessionRequest,
    DocumentFocusResponse,
    MessageDeltaEvent,
    ProjectResponse,
    PublicError,
    RunCompletedEvent,
    RunFailedEvent,
    RunResult,
    RunStartedEvent,
    SetDocumentFocusRequest,
    SourceAddedEvent,
    StartRunRequest,
    ToolCompletedEvent,
    ToolStartedEvent,
    Usage,
    UsageUpdatedEvent,
)
from common.schema.source.references import SourceConsulted
from runtime.application import ApplicationRuntime


def _default_domain_config() -> DomainConfig:
    """Return the minimal valid domain for the public project-create route."""

    return DomainConfig.from_mapping(
        {
            "version": 0,
            "topics": {"Identity": {}, "General": {}},
            "entity_types": {
                "Identity": {"topic": "Identity", "labels": ["person"]},
                "Concept": {"topic": "General", "labels": ["concept"]},
            },
        }
    )


class ApplicationRuntimePort:
    """Implement ``api.app.ApplicationPort`` for one running application.

    ``ApplicationRuntime`` is already user-scoped through its managers.  The
    adapter therefore rejects a request for a different user instead of
    accidentally treating the header as a new storage scope.
    """

    def __init__(
        self,
        runtime: ApplicationRuntime,
        *,
        default_domain_config: DomainConfig | Mapping[str, Any] | None = None,
    ) -> None:
        self.runtime = runtime
        self.default_domain_config = default_domain_config or _default_domain_config()

    def _require_user(self, user_name: str) -> str:
        configured_user = getattr(self.runtime.sessions, "user_name", None)
        if not isinstance(configured_user, str) or not configured_user.strip():
            raise RuntimeError("Application runtime has no configured user")
        if user_name != configured_user:
            raise PermissionError("Request user does not match the running application")
        return configured_user

    async def create_project(
        self,
        *,
        user_name: str,
        request: CreateProjectRequest,
    ) -> ProjectResponse:
        user_name = self._require_user(user_name)
        project = await self.runtime.projects.create_project(
            name=request.name,
            description=request.description,
            domain_config=self.default_domain_config,
        )
        return ProjectResponse.model_validate(
            {
                **dict(project),
                "id": project.get("id", project.get("project_id")),
                "allowed_projects": tuple(project.get("allowed_projects") or ()),
            }
        )

    async def create_session(
        self,
        *,
        user_name: str,
        request: CreateSessionRequest,
    ):
        self._require_user(user_name)
        session = await self.runtime.sessions.create_session(
            project_id=request.project_id,
            model=request.model,
            agent_id=request.agent_id,
            enabled_tools=request.enabled_tools,
        )
        return {
            "session_id": session.session_id,
            "project_id": session.project_id or request.project_id,
            "status": "open",
            "model": session.model,
            "agent_id": session.agent_id,
            "enabled_tools": session.enabled_tools,
        }

    async def _session(self, *, user_name: str, session_id: str):
        self._require_user(user_name)
        session = await self.runtime.sessions.get_or_resume_session(session_id)
        if session is None:
            raise NotFoundError("session")
        return session

    async def get_document_focus(
        self,
        *,
        user_name: str,
        session_id: str,
    ) -> DocumentFocusResponse | None:
        self._require_user(user_name)
        try:
            focus = await self.runtime.sessions.get_document_focus(session_id)
        except FileNotFoundError as exc:
            raise NotFoundError("session") from exc
        return None if focus is None else DocumentFocusResponse.model_validate(focus)

    async def set_document_focus(
        self,
        *,
        user_name: str,
        session_id: str,
        request: SetDocumentFocusRequest,
    ) -> DocumentFocusResponse:
        self._require_user(user_name)
        target = request.model_dump()
        try:
            focus = await self.runtime.sessions.set_document_focus(
                session_id,
                document_id=(
                    target["document_id"]
                    if target["target_type"] == "document"
                    else None
                ),
                folder_root_id=(
                    target["folder_root_id"]
                    if target["target_type"] != "document"
                    else None
                ),
                path_prefix=(
                    target["path_prefix"]
                    if target["target_type"] == "subtree"
                    else None
                ),
            )
        except FileNotFoundError as exc:
            raise NotFoundError("session") from exc
        return DocumentFocusResponse.model_validate(focus)

    async def clear_document_focus(
        self,
        *,
        user_name: str,
        session_id: str,
    ) -> None:
        self._require_user(user_name)
        try:
            await self.runtime.sessions.clear_document_focus(session_id)
        except FileNotFoundError as exc:
            raise NotFoundError("session") from exc

    async def _request_document_focus(
        self,
        *,
        session: Any,
        request: StartRunRequest,
    ):
        """Resolve untrusted run focus into the internal server-owned model."""
        requested = request.document_focus
        if requested is None:
            return None
        document_service = getattr(session, "document_service", None)
        if document_service is None:
            raise RuntimeError("Session document service is unavailable")

        target = await document_service.resolve_focus_target(
            session_id=session.session_id,
            document_id=(
                requested.document_id
                if requested.target_type == "document"
                else None
            ),
            folder_root_id=(
                requested.folder_root_id
                if requested.target_type != "document"
                else None
            ),
            path_prefix=(
                requested.path_prefix
                if requested.target_type == "subtree"
                else None
            ),
        )
        if requested.target_type == "document" and requested.selection is not None:
            resolved = await document_service.resolve_document_selection(
                document_id=requested.document_id,
                selection=requested.selection,
                session_id=session.session_id,
            )
            target["selection"] = DocumentSelection(
                content_hash=resolved["content_hash"],
                locator=resolved["locator"],
            )
        return create_document_focus(
            mode="request",
            created_at=datetime.now(timezone.utc),
            **target,
        )

    async def list_artifacts(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
        limit: int = 50,
    ) -> ArtifactListResponse:
        self._require_user(user_name)
        artifacts = await self.runtime.resources.knowledge_store.list_project_artifacts(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            limit=limit,
        )
        return ArtifactListResponse(
            artifacts=tuple(self._artifact_response(artifact) for artifact in artifacts)
        )

    async def get_artifact(
        self,
        *,
        user_name: str,
        project_id: str,
        artifact_id: str,
        session_id: str | None = None,
    ) -> ArtifactResponse | None:
        self._require_user(user_name)
        artifact = await self.runtime.resources.knowledge_store.get_project_artifact(
            artifact_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        return None if artifact is None else self._artifact_response(artifact)

    async def get_artifact_revision(
        self,
        *,
        user_name: str,
        project_id: str,
        artifact_id: str,
        revision: int,
        session_id: str | None = None,
    ):
        self._require_user(user_name)
        return await self.runtime.resources.knowledge_store.get_project_artifact_revision(
            artifact_id,
            revision,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def open_run_stream(
        self,
        *,
        user_name: str,
        request: StartRunRequest,
    ) -> AsyncIterator[object]:
        """Admit the run before returning its public event stream."""

        session = await self._session(
            user_name=user_name,
            session_id=request.session_id,
        )
        document_focus = await self._request_document_focus(
            session=session,
            request=request,
        )
        agent_stream = await session.open_agent_run_stream(
            Message(content=request.query),
            model=request.model,
            agent_id=request.agent_id,
            enabled_tools=request.enabled_tools,
            document_focus=document_focus,
            research_mode=request.research_mode,
        )
        return self._public_run_stream(
            session=session,
            request=request,
            agent_stream=agent_stream,
        )

    async def run_stream(
        self,
        *,
        user_name: str,
        request: StartRunRequest,
    ) -> AsyncIterator[object]:
        stream = await self.open_run_stream(user_name=user_name, request=request)
        async for event in stream:
            yield event

    async def _public_run_stream(
        self,
        *,
        session: Any,
        request: StartRunRequest,
        agent_stream: AsyncIterator[dict[str, Any]],
    ) -> AsyncIterator[object]:
        run_id = str(uuid4())
        sequence = 0

        def event(event_type: type, **values: Any):
            nonlocal sequence
            result = event_type(
                run_id=run_id,
                sequence=sequence,
                timestamp=datetime.now(timezone.utc),
                **values,
            )
            sequence += 1
            return result

        yield event(RunStartedEvent)
        response_seen = False
        terminal_seen = False
        async for raw_event in agent_stream:
            event_name = raw_event.get("event") if isinstance(raw_event, Mapping) else None
            data = raw_event.get("data", {}) if isinstance(raw_event, Mapping) else {}
            if event_name == "token":
                yield event(MessageDeltaEvent, content=str(data.get("content", "")))
            elif event_name == "tool_start":
                yield event(ToolStartedEvent, tool_name=str(data["tool"]))
            elif event_name == "tool_end":
                yield event(
                    ToolCompletedEvent,
                    tool_name=str(data["tool"]),
                    succeeded=True,
                )
            elif event_name == "tool_error":
                yield event(
                    ToolCompletedEvent,
                    tool_name=str(data["tool"]),
                    succeeded=False,
                )
            elif event_name == "response":
                if response_seen:
                    raise RuntimeError("Agent stream emitted multiple final responses")
                response_seen = True
                message_id = data.get("assistant_message_id")
                sources = await self._message_sources(session, message_id)
                for source in sources:
                    yield event(SourceAddedEvent, source=source)
                usage = Usage.model_validate(data["usage"])
                yield event(UsageUpdatedEvent, usage=usage)
                artifact = await self._message_artifact(session, message_id)
                result = RunResult(
                    run_id=run_id,
                    content=str(data.get("content", "")),
                    sources=tuple(sources),
                    usage=usage,
                    research_mode=data.get("research_mode", request.research_mode),
                    assistant_message_id=message_id,
                    source_ref_ids=tuple(data.get("source_ref_ids", ())),
                    artifact=(
                        self._artifact_response(artifact) if artifact is not None else None
                    ),
                )
                yield event(RunCompletedEvent, result=result)
            elif event_name == "clarification":
                question = str(data.get("question", "The agent needs clarification."))
                terminal_seen = True
                yield event(
                    RunFailedEvent,
                    error=PublicError(
                        code="clarification_required",
                        message=question[:500],
                        retryable=False,
                        run_id=run_id,
                    ),
                )
            elif event_name == "error":
                terminal_seen = True
                yield event(
                    RunFailedEvent,
                    error=PublicError(
                        code="run_failed",
                        message="The response could not be completed or saved.",
                        retryable=True,
                        run_id=run_id,
                    ),
                )

        if not response_seen and not terminal_seen:
            yield event(
                RunFailedEvent,
                error=PublicError(
                    code="run_failed",
                    message="The response did not produce a final answer.",
                    retryable=True,
                    run_id=run_id,
                ),
            )

    async def _message_sources(self, session: Any, message_id: Any) -> list[SourceConsulted]:
        if not isinstance(message_id, int) or message_id <= 0:
            return []
        store = self.runtime.resources.knowledge_store
        reader = getattr(store, "get_message_source_refs", None)
        if not callable(reader):
            return []
        values = await reader(
            message_id,
            user_name=session.user_name,
            project_id=session.project_id,
            session_id=session.session_id,
        )
        return [value if isinstance(value, SourceConsulted) else SourceConsulted.model_validate(value) for value in values]

    async def _message_artifact(self, session: Any, message_id: Any):
        if not isinstance(message_id, int) or message_id <= 0:
            return None
        reader = getattr(self.runtime.resources.knowledge_store, "get_message_artifact", None)
        if not callable(reader):
            return None
        return await reader(
            message_id,
            user_name=session.user_name,
            project_id=session.project_id,
            session_id=session.session_id,
        )

    @staticmethod
    def _artifact_response(value: Any) -> ArtifactResponse:
        payload = value.model_dump(mode="json") if hasattr(value, "model_dump") else dict(value)
        payload["artifact_id"] = str(payload["artifact_id"])
        return ArtifactResponse.model_validate(payload)


__all__ = ["ApplicationRuntimePort"]
