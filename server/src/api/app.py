"""Small dependency-injected FastAPI boundary for the public contracts.

This module intentionally does not know how the server's runtime is assembled.
``create_app`` receives an application port and delegates to it, which keeps
the HTTP import path safe for tooling, tests, and worker processes that do not
need to start PostgreSQL or an embedding model.
"""

from __future__ import annotations

import inspect
import json
import re
from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import ValidationError

from common.exceptions import (
    DependencyError,
    LLMProviderError,
    NotFoundError,
    SessionBusyError,
    StorageError,
    ToolExecutionError,
)
from common.schema.public import (
    ArtifactListResponse,
    ArtifactResponse,
    ArtifactRevisionResponse,
    CreateProjectRequest,
    CreateSessionRequest,
    DocumentFocusResponse,
    ProjectResponse,
    PublicError,
    RunCancelledEvent,
    RunCompletedEvent,
    RunFailedEvent,
    RunResult,
    SetDocumentFocusRequest,
    StartRunRequest,
    to_public_error,
    validate_public_stream_event,
)


class ApplicationPort(Protocol):
    """The narrow application boundary used by the HTTP adapter.

    Implementations may wrap the existing project/session managers and
    orchestrator, but the adapter does not require those internal classes.  A
    port method may return the corresponding public model or a mapping/object
    that can be projected to it.
    """

    async def create_project(
        self,
        *,
        user_name: str,
        request: CreateProjectRequest,
    ) -> ProjectResponse | Mapping[str, Any]: ...

    async def create_session(
        self,
        *,
        user_name: str,
        request: CreateSessionRequest,
    ) -> Any: ...

    async def get_document_focus(
        self,
        *,
        user_name: str,
        session_id: str,
    ) -> DocumentFocusResponse | Mapping[str, Any] | None: ...

    async def set_document_focus(
        self,
        *,
        user_name: str,
        session_id: str,
        request: SetDocumentFocusRequest,
    ) -> DocumentFocusResponse | Mapping[str, Any]: ...

    async def clear_document_focus(
        self,
        *,
        user_name: str,
        session_id: str,
    ) -> None: ...

    async def run_stream(
        self,
        *,
        user_name: str,
        request: StartRunRequest,
    ) -> AsyncIterator[object]: ...

    async def list_artifacts(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
        limit: int = 50,
    ) -> Any: ...

    async def get_artifact(
        self,
        *,
        user_name: str,
        project_id: str,
        artifact_id: str,
        session_id: str | None = None,
    ) -> Any: ...

    async def get_artifact_revision(
        self,
        *,
        user_name: str,
        project_id: str,
        artifact_id: str,
        revision: int,
        session_id: str | None = None,
    ) -> Any: ...


class UnsupportedOperation(RuntimeError):
    """Raised when an optional final-run or stream operation is not wired."""


class PublicOperationError(RuntimeError):
    """An already-sanitized failure returned by a run port."""

    def __init__(self, error: PublicError):
        super().__init__(error.message)
        self.error = error


def _request_id(request: Request) -> str:
    value = getattr(request.state, "request_id", None)
    if value:
        return value
    return str(uuid4())


_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def _safe_request_id(value: str | None) -> str:
    """Keep caller correlation IDs safe to echo as an HTTP header."""

    return value if value and _REQUEST_ID_RE.fullmatch(value) else str(uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_data(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, Mapping):
            return dict(dumped)
    try:
        return dict(vars(value))
    except TypeError as exc:
        raise ValueError("application port returned an unsupported result") from exc


def _value(data: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in data:
            return data[name]
    return default


def _project_response(value: Any) -> ProjectResponse:
    if isinstance(value, ProjectResponse):
        return value
    data = _as_data(value)
    status = _value(data, "status", default="active")
    status = getattr(status, "value", status)
    return ProjectResponse.model_validate(
        {
            "id": _value(data, "id", "project_id"),
            "name": _value(data, "name"),
            "description": _value(data, "description"),
            "status": status,
            "session_count": _value(data, "session_count", default=0) or 0,
            "allowed_projects": tuple(
                _value(data, "allowed_projects", default=[]) or []
            ),
            "created_at": _value(data, "created_at"),
            "updated_at": _value(data, "updated_at"),
        }
    )


def _session_response(value: Any, request: CreateSessionRequest):
    from common.schema.public import SessionResponse

    if isinstance(value, SessionResponse):
        return value
    data = _as_data(value)
    status = _value(data, "status", default="open")
    status = getattr(status, "value", status)
    enabled_tools = _value(data, "enabled_tools", default=request.enabled_tools)
    return SessionResponse.model_validate(
        {
            "session_id": _value(data, "session_id", "id"),
            "project_id": _value(data, "project_id", default=request.project_id),
            "status": status,
            "model": _value(data, "model", default=request.model),
            "agent_id": _value(data, "agent_id", default=request.agent_id),
            "enabled_tools": tuple(enabled_tools)
            if enabled_tools is not None
            else None,
            "created_at": _value(data, "created_at"),
            "last_active_at": _value(data, "last_active_at"),
        }
    )


def _document_focus_response(value: Any) -> DocumentFocusResponse:
    if isinstance(value, DocumentFocusResponse):
        return value
    return DocumentFocusResponse.model_validate(_as_data(value))


def _run_result(value: Any) -> RunResult:
    if isinstance(value, RunResult):
        return value
    data = _as_data(value)
    artifact_value = _value(data, "artifact")
    return RunResult.model_validate(
        {
            "run_id": _value(data, "run_id", "id"),
            "content": _value(data, "content", "response", default=""),
            "sources": tuple(_value(data, "sources", default=[]) or []),
            "usage": _value(data, "usage"),
            "research_mode": _value(data, "research_mode", default="normal"),
            "assistant_message_id": _value(data, "assistant_message_id"),
            "source_ref_ids": tuple(_value(data, "source_ref_ids", default=[]) or []),
            "artifact": (
                _artifact_response(artifact_value).model_dump(mode="json")
                if artifact_value is not None
                else None
            ),
        }
    )


def _artifact_response(value: Any) -> ArtifactResponse:
    if isinstance(value, ArtifactResponse):
        return value
    data = _as_data(value)
    return ArtifactResponse.model_validate(
        {
            "artifact_id": str(_value(data, "artifact_id", "id")),
            "project_id": _value(data, "project_id"),
            "session_id": _value(data, "session_id"),
            "originating_message_id": _value(
                data, "originating_message_id", "message_id"
            ),
            "kind": _value(data, "kind"),
            "title": _value(data, "title"),
            "status": _value(data, "status"),
            "current_revision": _value(data, "current_revision", default=1),
            "created_at": _value(data, "created_at"),
            "updated_at": _value(data, "updated_at"),
        }
    )


def _artifact_revision_response(value: Any) -> ArtifactRevisionResponse:
    if isinstance(value, ArtifactRevisionResponse):
        return value
    data = _as_data(value)
    return ArtifactRevisionResponse.model_validate(
        {
            "artifact_id": str(_value(data, "artifact_id", "id")),
            "revision": _value(data, "revision"),
            "schema_version": _value(data, "schema_version", default=1),
            "kind": _value(data, "kind"),
            "title": _value(data, "title"),
            "blocks": tuple(_value(data, "blocks", default=[]) or []),
            "status": _value(data, "status"),
            "markdown": _value(data, "markdown"),
            "content_hash": _value(data, "content_hash"),
            "created_at": _value(data, "created_at"),
        }
    )


def _artifact_list_response(value: Any) -> ArtifactListResponse:
    if isinstance(value, ArtifactListResponse):
        return value
    if value is None:
        values = []
    elif isinstance(value, Mapping) and "artifacts" in value:
        values = value["artifacts"] or []
    else:
        values = value
    return ArtifactListResponse(
        artifacts=tuple(_artifact_response(item) for item in values)
    )


def _error_response(
    error: Exception,
    *,
    request_id: str,
    status_code: int | None = None,
    run_id: str | None = None,
) -> JSONResponse:
    if isinstance(error, PublicOperationError):
        public_error = error.error.model_copy(update={"request_id": request_id})
    elif isinstance(error, (RequestValidationError, ValidationError)):
        public_error = PublicError(
            code="invalid_request",
            message="The request is invalid.",
            request_id=request_id,
            run_id=run_id,
        )
    else:
        public_error = to_public_error(error, request_id=request_id, run_id=run_id)
    if status_code is None:
        status_code = _status_for_error(error)
    return JSONResponse(
        status_code=status_code,
        content={"error": public_error.model_dump(mode="json")},
        headers={"X-Request-ID": request_id},
    )


def _status_for_error(error: Exception) -> int:
    if isinstance(error, PublicOperationError):
        if error.error.code == "invalid_request":
            return 422
        if error.error.code == "not_found":
            return 404
        return 503 if error.error.retryable else 502
    if isinstance(error, UnsupportedOperation):
        return 501
    if isinstance(error, (ValueError, ValidationError, RequestValidationError)):
        return 422
    if isinstance(error, NotFoundError):
        return 404
    if isinstance(error, SessionBusyError):
        return 409
    if isinstance(
        error,
        (DependencyError, StorageError, LLMProviderError),
    ):
        return 503
    if isinstance(error, ToolExecutionError):
        return 502
    return 500


async def _call(method: Any, **kwargs: Any) -> Any:
    value = method(**kwargs)
    if inspect.isawaitable(value):
        return await value
    return value


async def _stream_from_port(port: Any, **kwargs: Any) -> AsyncIterator[object]:
    method = getattr(port, "run_stream", None)
    if method is None:
        method = getattr(port, "stream_run", None)
    if method is None:
        raise UnsupportedOperation("run streaming is not configured")
    result = method(**kwargs)
    if inspect.isawaitable(result):
        result = await result
    if hasattr(result, "__aiter__"):
        async for event in result:
            yield event
        return
    if isinstance(result, (list, tuple)):
        for event in result:
            yield event
        return
    raise ValueError("application port returned a non-streaming run result")


async def _open_stream_from_port(port: Any, **kwargs: Any) -> AsyncIterator[object]:
    """Open a stream early when the port supports explicit run admission."""

    method = getattr(port, "open_run_stream", None)
    if method is None:
        return _stream_from_port(port, **kwargs)
    result = await _call(method, **kwargs)
    if hasattr(result, "__aiter__"):
        return result
    if isinstance(result, (list, tuple)):

        async def static_events() -> AsyncIterator[object]:
            for event in result:
                yield event

        return static_events()
    raise ValueError("application port returned a non-streaming run result")


def _sse_frame(event: Any) -> str:
    data = event.model_dump(mode="json")
    return (
        f"event: {data['type']}\n"
        f"data: {json.dumps(data, separators=(',', ':'), ensure_ascii=False)}\n\n"
    )


def create_app(port: ApplicationPort, *, title: str = "Knoggin API") -> FastAPI:
    """Build the public API around an injected application port."""

    app = FastAPI(title=title, version="1", docs_url="/docs", redoc_url=None)

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        request_id = _safe_request_id(request.headers.get("X-Request-ID"))
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers.setdefault("X-Request-ID", request_id)
        return response

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(request: Request, exc: RequestValidationError):
        return _error_response(exc, request_id=_request_id(request), status_code=422)

    @app.exception_handler(Exception)
    async def unhandled_error(request: Request, exc: Exception):
        return _error_response(exc, request_id=_request_id(request))

    async def current_user(
        x_user_name: str | None = Header(default=None, alias="X-User-Name"),
    ) -> str:
        # Authentication is intentionally outside this transport slice.  The
        # default makes local development and fake-backed contract tests useful;
        # a real adapter can replace this dependency at composition time.
        user_name = (x_user_name or "default").strip()
        if not user_name:
            raise ValueError("X-User-Name must not be blank")
        return user_name

    @app.post("/v1/projects", response_model=ProjectResponse, status_code=201)
    async def create_project(
        body: CreateProjectRequest,
        request: Request,
        user_name: str = Depends(current_user),
    ) -> ProjectResponse:
        try:
            return _project_response(
                await _call(port.create_project, user_name=user_name, request=body)
            )
        except Exception as exc:
            raise exc

    @app.post("/v1/sessions", status_code=201)
    async def create_session(
        body: CreateSessionRequest,
        request: Request,
        user_name: str = Depends(current_user),
    ):
        try:
            return _session_response(
                await _call(port.create_session, user_name=user_name, request=body),
                body,
            )
        except Exception as exc:
            raise exc

    @app.get(
        "/v1/sessions/{session_id}/document-focus",
        response_model=DocumentFocusResponse | None,
    )
    async def get_document_focus(
        session_id: str,
        request: Request,
        user_name: str = Depends(current_user),
    ) -> DocumentFocusResponse | None:
        value = await _call(
            port.get_document_focus,
            user_name=user_name,
            session_id=session_id,
        )
        return None if value is None else _document_focus_response(value)

    @app.put(
        "/v1/sessions/{session_id}/document-focus",
        response_model=DocumentFocusResponse,
    )
    async def set_document_focus(
        session_id: str,
        body: SetDocumentFocusRequest,
        request: Request,
        user_name: str = Depends(current_user),
    ) -> DocumentFocusResponse:
        return _document_focus_response(
            await _call(
                port.set_document_focus,
                user_name=user_name,
                session_id=session_id,
                request=body,
            )
        )

    @app.delete("/v1/sessions/{session_id}/document-focus", status_code=204)
    async def clear_document_focus(
        session_id: str,
        request: Request,
        user_name: str = Depends(current_user),
    ) -> None:
        await _call(
            port.clear_document_focus,
            user_name=user_name,
            session_id=session_id,
        )

    @app.get(
        "/v1/projects/{project_id}/artifacts",
        response_model=ArtifactListResponse,
    )
    async def list_artifacts(
        project_id: str,
        request: Request,
        session_id: str | None = None,
        limit: int = Query(50, ge=1, le=200),
        user_name: str = Depends(current_user),
    ) -> ArtifactListResponse:
        method = getattr(port, "list_artifacts", None) or getattr(
            port, "list_project_artifacts", None
        )
        if method is None:
            raise UnsupportedOperation("artifact listing is not configured")
        value = await _call(
            method,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            limit=limit,
        )
        return _artifact_list_response(value)

    @app.get(
        "/v1/projects/{project_id}/artifacts/{artifact_id}/revisions/{revision}",
        response_model=ArtifactRevisionResponse,
    )
    async def get_artifact_revision(
        project_id: str,
        artifact_id: str,
        request: Request,
        revision: int = Path(..., ge=1),
        session_id: str | None = None,
        user_name: str = Depends(current_user),
    ) -> ArtifactRevisionResponse:
        method = getattr(port, "get_artifact_revision", None) or getattr(
            port, "get_project_artifact_revision", None
        )
        if method is None:
            raise UnsupportedOperation("artifact revision reads are not configured")
        value = await _call(
            method,
            user_name=user_name,
            project_id=project_id,
            artifact_id=artifact_id,
            revision=revision,
            session_id=session_id,
        )
        if value is None:
            raise PublicOperationError(
                PublicError(code="not_found", message="The artifact was not found.")
            )
        return _artifact_revision_response(value)

    @app.get(
        "/v1/projects/{project_id}/artifacts/{artifact_id}",
        response_model=ArtifactResponse,
    )
    async def get_artifact(
        project_id: str,
        artifact_id: str,
        request: Request,
        session_id: str | None = None,
        user_name: str = Depends(current_user),
    ) -> ArtifactResponse:
        method = getattr(port, "get_artifact", None) or getattr(
            port, "get_project_artifact", None
        )
        if method is None:
            raise UnsupportedOperation("artifact reads are not configured")
        value = await _call(
            method,
            user_name=user_name,
            project_id=project_id,
            artifact_id=artifact_id,
            session_id=session_id,
        )
        if value is None:
            raise PublicOperationError(
                PublicError(code="not_found", message="The artifact was not found.")
            )
        return _artifact_response(value)

    @app.post("/v1/runs", response_model=RunResult)
    async def run(
        body: StartRunRequest,
        request: Request,
        user_name: str = Depends(current_user),
    ) -> RunResult:
        try:
            direct = getattr(port, "run", None)
            if direct is not None:
                result = await _call(
                    direct,
                    user_name=user_name,
                    request=body,
                )
                if isinstance(result, (RunResult, Mapping)) or hasattr(
                    result, "model_dump"
                ):
                    return _run_result(result)
                if hasattr(result, "__aiter__"):
                    events = [event async for event in result]
                elif isinstance(result, (list, tuple)):
                    events = list(result)
                else:
                    raise ValueError("application port returned an invalid run result")
            else:
                events = [
                    event
                    async for event in await _open_stream_from_port(
                        port, user_name=user_name, request=body
                    )
                ]
            parsed = [validate_public_stream_event(event) for event in events]
            completed = [event for event in parsed if isinstance(event, RunCompletedEvent)]
            if completed:
                return completed[-1].result
            failed = [event for event in parsed if isinstance(event, RunFailedEvent)]
            if failed:
                raise PublicOperationError(failed[-1].error)
            raise ValueError("run stream did not contain a terminal result")
        except Exception as exc:
            raise exc

    @app.post("/v1/runs/stream")
    async def run_stream(
        body: StartRunRequest,
        request: Request,
        user_name: str = Depends(current_user),
    ) -> StreamingResponse:
        request_id = _request_id(request)
        stream = await _open_stream_from_port(
            port,
            user_name=user_name,
            request=body,
        )

        async def events() -> AsyncIterator[str]:
            run_id: str | None = None
            previous_sequence = -1
            terminal = False
            try:
                async for raw_event in stream:
                    event = validate_public_stream_event(raw_event)
                    if run_id is None:
                        run_id = event.run_id
                    if event.run_id != run_id:
                        raise ValueError("public stream events must belong to one run")
                    if event.sequence <= previous_sequence:
                        raise ValueError("public stream sequence must increase monotonically")
                    if terminal:
                        raise ValueError("public stream cannot continue after terminal event")
                    previous_sequence = event.sequence
                    yield _sse_frame(event)
                    if isinstance(
                        event,
                        (RunCompletedEvent, RunFailedEvent, RunCancelledEvent),
                    ):
                        terminal = True
                if not terminal:
                    raise ValueError("public stream must contain a terminal event")
            except Exception as exc:
                # A malformed event after a valid terminal cannot be repaired
                # without violating the one-terminal stream contract.  Keep
                # the already-emitted terminal event as the public result.
                if terminal:
                    return
                run_id = run_id or str(uuid4())
                failed = RunFailedEvent(
                    run_id=run_id,
                    sequence=previous_sequence + 1,
                    timestamp=_now(),
                    error=(
                        PublicError(
                            code="invalid_request",
                            message="The request is invalid.",
                            request_id=request_id,
                            run_id=run_id,
                        )
                        if isinstance(exc, (RequestValidationError, ValidationError))
                        else to_public_error(
                            exc,
                            request_id=request_id,
                            run_id=run_id,
                        )
                    ),
                )
                yield _sse_frame(failed)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Request-ID": request_id,
            },
        )

    return app


__all__ = ["ApplicationPort", "UnsupportedOperation", "create_app"]
