"""Small dependency-injected FastAPI boundary for the public contracts.

This module intentionally does not know how the server's runtime is assembled.
``create_app`` receives an application port and delegates to it, which keeps
the HTTP import path safe for tooling, tests, and worker processes that do not
need to start PostgreSQL or an embedding model.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from loguru import logger

from api.upload_limits import DocumentUploadLimitMiddleware
from common.schema.document import FolderScanSettings
from common.schema.health import HealthSnapshot
from common.schema.public import (
    AACAdmissionResponse,
    AACDiscussionResponse,
    AACInsightResponse,
    AACInsightVoteResponse,
    AACParticipationRequest,
    AACParticipationResponse,
    AACStopResponse,
    AACTimelineResponse,
    ArtifactListResponse,
    ArtifactResponse,
    ArtifactRevisionResponse,
    BatchSourceFailed,
    BatchSourceRequest,
    BatchSourceResponse,
    CreateProjectRequest,
    CreateSessionRequest,
    DocumentContentResponse,
    DocumentDeletedResponse,
    DocumentFocusResponse,
    DocumentResponse,
    EntityMergeRollbackRequest,
    EpisodeEditedResponse,
    EpisodeResponse,
    MaintenanceOperationResponse,
    MaintenanceReviewDecisionRequest,
    MaintenanceReviewDetailResponse,
    MaintenanceReviewListResponse,
    MaintenanceReviewPreviewResponse,
    MaintenanceReviewResponse,
    ProjectDeletedResponse,
    ProjectResponse,
    PromoteSourceRequest,
    PublicError,
    PublicStreamContractError,
    PublicStreamState,
    RunCompletedEvent,
    RunFailedEvent,
    RunResult,
    SavedWebLinkDeletedResponse,
    SavedWebLinkResponse,
    SessionDeletedResponse,
    SessionHistoryMessage,
    SessionResponse,
    SetDocumentFocusRequest,
    SettingsApplyStatusResponse,
    SettingsOperationResponse,
    SettingsResponse,
    SettingsUpdateRequest,
    StartRunRequest,
    UpdateEpisodeRequest,
    UpdateProjectRequest,
    UpdateSavedWebLinkRequest,
    UpdateSessionRequest,
    UploadDocumentRequest,
    public_error_status,
    sanitize_public_error,
    to_public_error,
)
from common.utils.lifecycle import settle_owned_task


class ApplicationPort(Protocol):
    """The narrow application boundary used by the HTTP adapter.

    Implementations may wrap the existing project/session managers and
    orchestrator, but the adapter does not require those internal classes.  A
    port method returns canonical public fields, never arbitrary object aliases.
    """

    async def get_engine_health(self, *, user_name: str) -> HealthSnapshot: ...

    async def get_resource_health(
        self, *, user_name: str, project_id: str
    ) -> HealthSnapshot: ...

    async def get_ingestion_health(
        self, *, user_name: str, project_id: str
    ) -> HealthSnapshot: ...

    async def get_background_health(
        self, *, user_name: str, project_id: str
    ) -> HealthSnapshot: ...

    async def create_project(
        self,
        *,
        user_name: str,
        request: CreateProjectRequest,
    ) -> ProjectResponse | Mapping[str, Any]: ...

    async def admit_sources_batch(self, *, user_name: str, project_id: str, request: BatchSourceRequest) -> BatchSourceResponse: ...

    async def get_settings(self, *, user_name: str) -> SettingsResponse: ...
    async def get_settings_status(self, *, user_name: str) -> SettingsApplyStatusResponse: ...
    async def update_settings(self, *, user_name: str, request: SettingsUpdateRequest) -> SettingsOperationResponse: ...
    async def reload_settings(self, *, user_name: str) -> SettingsOperationResponse: ...
    async def retry_settings_applies(self, *, user_name: str) -> SettingsApplyStatusResponse: ...

    async def trigger_aac(self, *, user_name: str) -> AACAdmissionResponse: ...
    async def stop_aac(self, *, user_name: str) -> AACStopResponse: ...
    async def set_aac_participation(self, *, user_name: str, agent_id: str, enabled: bool) -> AACParticipationResponse: ...
    async def list_aac_discussions(self, *, user_name: str, limit: int = 20) -> list[AACDiscussionResponse]: ...
    async def list_aac_timeline(self, *, user_name: str, discussion_id: str, limit: int = 100, after_sequence: int = 0) -> list[AACTimelineResponse]: ...
    async def list_aac_insights(self, *, user_name: str, query: str | None = None, limit: int = 20) -> list[AACInsightResponse]: ...
    async def list_aac_insight_votes(self, *, user_name: str, insight_id: str) -> list[AACInsightVoteResponse]: ...

    async def list_projects(self, *, user_name: str) -> list[ProjectResponse]: ...
    async def get_project(self, *, user_name: str, project_id: str) -> ProjectResponse: ...
    async def update_project(self, *, user_name: str, project_id: str, request: UpdateProjectRequest) -> ProjectResponse: ...
    async def archive_project(self, *, user_name: str, project_id: str) -> ProjectResponse: ...
    async def delete_project(self, *, user_name: str, project_id: str) -> ProjectDeletedResponse: ...

    async def get_episode(
        self, *, user_name: str, project_id: str, episode_id: str
    ) -> EpisodeResponse: ...

    async def update_episode(
        self, *, user_name: str, project_id: str, episode_id: str,
        request: UpdateEpisodeRequest,
    ) -> EpisodeEditedResponse: ...

    async def create_session(
        self,
        *,
        user_name: str,
        request: CreateSessionRequest,
    ) -> SessionResponse | Mapping[str, Any]: ...

    async def list_sessions(self, *, user_name: str) -> list[SessionResponse]: ...

    async def get_session_history(
        self, *, user_name: str, session_id: str, limit: int = 100
    ) -> list[SessionHistoryMessage]: ...

    async def update_session(
        self, *, user_name: str, session_id: str, request: UpdateSessionRequest
    ) -> SessionResponse: ...

    async def delete_session(
        self, *, user_name: str, session_id: str
    ) -> SessionDeletedResponse: ...

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

    async def promote_source(
        self,
        *,
        user_name: str,
        project_id: str,
        request: PromoteSourceRequest,
    ) -> SavedWebLinkResponse: ...

    async def list_documents(self, *, user_name: str, project_id: str, limit: int = 100) -> list[DocumentResponse]: ...
    async def get_document(self, *, user_name: str, project_id: str, document_id: str) -> DocumentResponse: ...
    async def read_document(self, *, user_name: str, project_id: str, document_id: str, start_line: int = 1, end_line: int | None = None) -> DocumentContentResponse: ...
    async def upload_document(self, *, user_name: str, project_id: str, request: UploadDocumentRequest) -> DocumentResponse: ...
    async def reindex_document(self, *, user_name: str, project_id: str, document_id: str) -> DocumentResponse: ...
    async def delete_document(self, *, user_name: str, project_id: str, document_id: str) -> DocumentDeletedResponse: ...
    async def list_saved_web_links(self, *, user_name: str, project_id: str, limit: int = 50) -> list[SavedWebLinkResponse]: ...
    async def update_saved_web_link(self, *, user_name: str, project_id: str, link_id: str, request: UpdateSavedWebLinkRequest) -> SavedWebLinkResponse: ...
    async def delete_saved_web_link(self, *, user_name: str, project_id: str, link_id: str) -> SavedWebLinkDeletedResponse: ...
    async def get_document_scan_settings(self, *, user_name: str, project_id: str) -> FolderScanSettings: ...
    async def set_document_scan_settings(self, *, user_name: str, project_id: str, settings: FolderScanSettings) -> FolderScanSettings: ...
    async def reset_document_scan_settings(self, *, user_name: str, project_id: str) -> FolderScanSettings: ...

    async def open_run_stream(
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
    ) -> ArtifactListResponse: ...

    async def get_artifact(
        self,
        *,
        user_name: str,
        project_id: str,
        artifact_id: str,
        session_id: str | None = None,
    ) -> ArtifactResponse | None: ...

    async def get_artifact_revision(
        self,
        *,
        user_name: str,
        project_id: str,
        artifact_id: str,
        revision: int,
        session_id: str | None = None,
    ) -> ArtifactRevisionResponse | None: ...

    async def list_global_maintenance_reviews(
        self,
        *,
        user_name: str,
    ) -> list[MaintenanceReviewResponse]: ...

    async def decide_global_maintenance_review(
        self,
        *,
        user_name: str,
        review_id: str,
        request: MaintenanceReviewDecisionRequest,
    ) -> dict[str, Any]: ...

    async def list_project_maintenance_reviews(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> list[MaintenanceReviewResponse]: ...

    async def get_project_maintenance_review(
        self,
        *,
        user_name: str,
        project_id: str,
        review_id: str,
    ) -> MaintenanceReviewDetailResponse: ...

    async def preview_project_maintenance_review(
        self,
        *,
        user_name: str,
        project_id: str,
        review_id: str,
    ) -> MaintenanceReviewPreviewResponse: ...

    async def decide_project_maintenance_review(
        self,
        *,
        user_name: str,
        project_id: str,
        review_id: str,
        request: MaintenanceReviewDecisionRequest,
    ) -> dict[str, Any]: ...

    async def preview_entity_merge_rollback(
        self,
        *,
        user_name: str,
        merge_id: str,
    ) -> dict[str, Any]: ...

    async def rollback_entity_merge(
        self,
        *,
        user_name: str,
        merge_id: str,
        request: EntityMergeRollbackRequest,
    ) -> dict[str, Any]: ...


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


def _project_response(value: ProjectResponse) -> ProjectResponse:
    return ProjectResponse.model_validate(value)


def _session_response(value: SessionResponse) -> SessionResponse:
    return SessionResponse.model_validate(value)


def _document_focus_response(value: DocumentFocusResponse) -> DocumentFocusResponse:
    return DocumentFocusResponse.model_validate(value)


def _artifact_response(value: ArtifactResponse) -> ArtifactResponse:
    return ArtifactResponse.model_validate(value)


def _artifact_revision_response(value: ArtifactRevisionResponse) -> ArtifactRevisionResponse:
    return ArtifactRevisionResponse.model_validate(value)


def _artifact_list_response(value: ArtifactListResponse) -> ArtifactListResponse:
    return ArtifactListResponse.model_validate(value)


def _maintenance_review_list_response(value: list[MaintenanceReviewResponse]) -> MaintenanceReviewListResponse:
    return MaintenanceReviewListResponse(reviews=tuple(value))


def _maintenance_operation_response(value: dict[str, Any]) -> MaintenanceOperationResponse:
    return MaintenanceOperationResponse(result=value)


def _error_response(
    error: Exception,
    *,
    request_id: str,
    status_code: int | None = None,
    run_id: str | None = None,
) -> JSONResponse:
    if isinstance(error, PublicOperationError):
        public_error = sanitize_public_error(error.error, request_id=request_id, run_id=run_id)
    elif isinstance(error, RequestValidationError):
        public_error = PublicError(
            code="invalid_request",
            message="The request is invalid.",
            request_id=request_id,
            run_id=run_id,
        )
    elif isinstance(error, UnsupportedOperation):
        public_error = PublicError(code="unsupported_operation", message="This operation is not supported.",
                                   request_id=request_id, run_id=run_id)
    else:
        public_error = to_public_error(error, request_id=request_id, run_id=run_id)
    if public_error.code == "internal_error":
        logger.error("Public response failure request={} run={}", request_id, run_id)
    if status_code is None:
        status_code = _status_for_error(error)
    return JSONResponse(
        status_code=status_code,
        content={"error": public_error.model_dump(mode="json")},
        headers={"X-Request-ID": request_id},
    )


def _status_for_error(error: Exception) -> int:
    if isinstance(error, RequestValidationError):
        return 422
    if isinstance(error, UnsupportedOperation):
        return 501
    public = error.error if isinstance(error, PublicOperationError) else to_public_error(error)
    return public_error_status(sanitize_public_error(public))


async def _call(method: Any, **kwargs: Any) -> Any:
    return await method(**kwargs)


async def _open_stream_from_port(port: Any, **kwargs: Any) -> AsyncIterator[object]:
    """Open a stream early when the port supports explicit run admission."""

    result = await port.open_run_stream(**kwargs)
    if hasattr(result, "__aiter__") and callable(getattr(result, "aclose", None)):
        return result
    raise PublicStreamContractError("application port returned a non-streaming run result")


async def _close_owned_stream(stream, *, request_id=None) -> None:
    cleanup = asyncio.create_task(stream.aclose())
    try:
        await asyncio.shield(cleanup)
    except asyncio.CancelledError:
        try:
            await settle_owned_task(cleanup)
        except Exception:
            logger.error("Run stream cleanup failed during cancellation request={}", request_id)
        raise
    except Exception:
        logger.error("Run stream cleanup failed request={}", request_id)


class OwnedStreamingResponse(StreamingResponse):
    """Own admission even when sending fails before body iteration starts."""

    def __init__(self, content, *, owner, request_id=None, **kwargs):
        super().__init__(content, **kwargs)
        self.owner = owner
        self.request_id = request_id

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            try:
                await _close_owned_stream(self.body_iterator, request_id=self.request_id)
            finally:
                await _close_owned_stream(self.owner, request_id=self.request_id)


def _sse_frame(event: Any) -> str:
    data = event.model_dump(mode="json")
    return (
        f"event: {data['type']}\n"
        f"data: {json.dumps(data, separators=(',', ':'), ensure_ascii=False)}\n\n"
    )


def create_app(port: ApplicationPort, *, title: str = "Knoggin API") -> FastAPI:
    """Build the public API around an injected application port."""

    app = FastAPI(title=title, version="1", docs_url="/docs", redoc_url=None)
    app.add_middleware(DocumentUploadLimitMiddleware)

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

    @app.get("/v1/health", response_model=HealthSnapshot)
    async def get_engine_health(
        user_name: str = Depends(current_user),
    ) -> HealthSnapshot:
        return await _call(port.get_engine_health, user_name=user_name)

    @app.get("/v1/projects/{project_id}/health/resources", response_model=HealthSnapshot)
    async def get_resource_health(
        project_id: str,
        user_name: str = Depends(current_user),
    ) -> HealthSnapshot:
        return await _call(
            port.get_resource_health, user_name=user_name, project_id=project_id
        )

    @app.get("/v1/projects/{project_id}/health/ingestion", response_model=HealthSnapshot)
    async def get_ingestion_health(
        project_id: str,
        user_name: str = Depends(current_user),
    ) -> HealthSnapshot:
        return await _call(
            port.get_ingestion_health, user_name=user_name, project_id=project_id
        )

    @app.get("/v1/projects/{project_id}/health/background", response_model=HealthSnapshot)
    async def get_background_health(
        project_id: str,
        user_name: str = Depends(current_user),
    ) -> HealthSnapshot:
        return await _call(
            port.get_background_health, user_name=user_name, project_id=project_id
        )

    @app.post("/v1/projects", response_model=ProjectResponse, status_code=201)
    async def create_project(
        body: CreateProjectRequest,
        request: Request,
        user_name: str = Depends(current_user),
    ) -> ProjectResponse:
        return _project_response(
            await _call(port.create_project, user_name=user_name, request=body)
        )

    @app.post("/v1/projects/{project_id}/sources/batch", response_model=BatchSourceResponse)
    async def admit_sources_batch(body: BatchSourceRequest, request: Request, project_id: str = Path(min_length=1), user_name: str = Depends(current_user)):
        result = await port.admit_sources_batch(user_name=user_name, project_id=project_id, request=body)
        return result.model_copy(update={"results": tuple(
            item.model_copy(update={"error": sanitize_public_error(item.error, request_id=_request_id(request))})
            if isinstance(item, BatchSourceFailed) else item for item in result.results
        )})

    @app.get("/v1/settings", response_model=SettingsResponse)
    async def get_settings(user_name: str = Depends(current_user)):
        return await port.get_settings(user_name=user_name)

    @app.patch("/v1/settings", response_model=SettingsOperationResponse)
    async def update_settings(body: SettingsUpdateRequest, user_name: str = Depends(current_user)):
        return await port.update_settings(user_name=user_name, request=body)

    @app.post("/v1/settings/reload", response_model=SettingsOperationResponse)
    async def reload_settings(user_name: str = Depends(current_user)):
        return await port.reload_settings(user_name=user_name)

    @app.get("/v1/settings/status", response_model=SettingsApplyStatusResponse)
    async def get_settings_status(user_name: str = Depends(current_user)):
        return await port.get_settings_status(user_name=user_name)

    @app.post("/v1/settings/retry-applies", response_model=SettingsApplyStatusResponse)
    async def retry_settings_applies(user_name: str = Depends(current_user)):
        return await port.retry_settings_applies(user_name=user_name)

    @app.post("/v1/aac/trigger", response_model=AACAdmissionResponse)
    async def trigger_aac(user_name: str = Depends(current_user)):
        return await port.trigger_aac(user_name=user_name)

    @app.post("/v1/aac/stop", response_model=AACStopResponse)
    async def stop_aac(user_name: str = Depends(current_user)):
        return await port.stop_aac(user_name=user_name)

    @app.put("/v1/aac/agents/{agent_id}/participation", response_model=AACParticipationResponse)
    async def set_aac_participation(body: AACParticipationRequest, agent_id: str = Path(min_length=1), user_name: str = Depends(current_user)):
        return await port.set_aac_participation(user_name=user_name, agent_id=agent_id, enabled=body.enabled)

    @app.get("/v1/aac/discussions", response_model=list[AACDiscussionResponse])
    async def list_aac_discussions(limit: int = Query(default=20, ge=1, le=100), user_name: str = Depends(current_user)):
        return await port.list_aac_discussions(user_name=user_name, limit=limit)

    @app.get("/v1/aac/discussions/{discussion_id}/timeline", response_model=list[AACTimelineResponse])
    async def list_aac_timeline(discussion_id: str = Path(min_length=1), limit: int = Query(default=100, ge=1, le=100), after_sequence: int = Query(default=0, ge=0), user_name: str = Depends(current_user)):
        return await port.list_aac_timeline(user_name=user_name, discussion_id=discussion_id, limit=limit, after_sequence=after_sequence)

    @app.get("/v1/aac/insights", response_model=list[AACInsightResponse])
    async def list_aac_insights(query: str | None = Query(default=None, max_length=4000), limit: int = Query(default=20, ge=1, le=100), user_name: str = Depends(current_user)):
        return await port.list_aac_insights(user_name=user_name, query=query, limit=limit)

    @app.get("/v1/aac/insights/{insight_id}/votes", response_model=list[AACInsightVoteResponse])
    async def list_aac_insight_votes(insight_id: str = Path(min_length=1), user_name: str = Depends(current_user)):
        return await port.list_aac_insight_votes(user_name=user_name, insight_id=insight_id)

    @app.get("/v1/projects", response_model=list[ProjectResponse])
    async def list_projects(user_name: str = Depends(current_user)):
        return await port.list_projects(user_name=user_name)

    @app.get("/v1/projects/{project_id}", response_model=ProjectResponse)
    async def get_project(project_id: str = Path(min_length=1), user_name: str = Depends(current_user)):
        return await port.get_project(user_name=user_name, project_id=project_id)

    @app.patch("/v1/projects/{project_id}", response_model=ProjectResponse)
    async def update_project(body: UpdateProjectRequest, project_id: str = Path(min_length=1), user_name: str = Depends(current_user)):
        return await port.update_project(user_name=user_name, project_id=project_id, request=body)

    @app.post("/v1/projects/{project_id}/archive", response_model=ProjectResponse)
    async def archive_project(project_id: str = Path(min_length=1), user_name: str = Depends(current_user)):
        return await port.archive_project(user_name=user_name, project_id=project_id)

    @app.delete("/v1/projects/{project_id}", response_model=ProjectDeletedResponse)
    async def delete_project(project_id: str = Path(min_length=1), user_name: str = Depends(current_user)):
        return await port.delete_project(user_name=user_name, project_id=project_id)

    @app.post("/v1/sessions", response_model=SessionResponse, status_code=201)
    async def create_session(
        body: CreateSessionRequest,
        request: Request,
        user_name: str = Depends(current_user),
    ):
        return _session_response(
            await _call(port.create_session, user_name=user_name, request=body),
        )

    @app.get("/v1/sessions", response_model=list[SessionResponse])
    async def list_sessions(user_name: str = Depends(current_user)):
        return await port.list_sessions(user_name=user_name)

    @app.get("/v1/sessions/{session_id}/history", response_model=list[SessionHistoryMessage])
    async def get_session_history(
        session_id: str = Path(min_length=1),
        limit: int = Query(default=100, ge=1, le=1000),
        user_name: str = Depends(current_user),
    ):
        return await port.get_session_history(
            user_name=user_name, session_id=session_id, limit=limit
        )

    @app.patch("/v1/sessions/{session_id}", response_model=SessionResponse)
    async def update_session(
        body: UpdateSessionRequest,
        session_id: str = Path(min_length=1),
        user_name: str = Depends(current_user),
    ):
        return await port.update_session(user_name=user_name, session_id=session_id, request=body)

    @app.delete("/v1/sessions/{session_id}", response_model=SessionDeletedResponse)
    async def delete_session(
        session_id: str = Path(min_length=1), user_name: str = Depends(current_user)
    ):
        return await port.delete_session(user_name=user_name, session_id=session_id)

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

    @app.post(
        "/v1/projects/{project_id}/sources/promote",
        response_model=SavedWebLinkResponse,
        status_code=201,
    )
    async def promote_source(
        project_id: str,
        body: PromoteSourceRequest,
        request: Request,
        user_name: str = Depends(current_user),
    ):
        return await _call(
            port.promote_source,
            user_name=user_name,
            project_id=project_id,
            request=body,
        )

    @app.get("/v1/projects/{project_id}/documents", response_model=list[DocumentResponse])
    async def list_documents(
        project_id: str,
        limit: int = Query(default=100, ge=1, le=100),
        user_name: str = Depends(current_user),
    ):
        return await _call(port.list_documents, user_name=user_name, project_id=project_id, limit=limit)

    @app.post("/v1/projects/{project_id}/documents", response_model=DocumentResponse, status_code=201)
    async def upload_document(
        project_id: str, body: UploadDocumentRequest,
        user_name: str = Depends(current_user),
    ):
        return await _call(port.upload_document, user_name=user_name, project_id=project_id, request=body)

    @app.get("/v1/projects/{project_id}/documents/{document_id}", response_model=DocumentResponse)
    async def get_document(
        project_id: str, document_id: str,
        user_name: str = Depends(current_user),
    ):
        return await _call(port.get_document, user_name=user_name, project_id=project_id, document_id=document_id)

    @app.get("/v1/projects/{project_id}/documents/{document_id}/content", response_model=DocumentContentResponse)
    async def read_document(
        project_id: str, document_id: str,
        start_line: int = Query(default=1, ge=1),
        end_line: int | None = Query(default=None, ge=1),
        user_name: str = Depends(current_user),
    ):
        return await _call(
            port.read_document, user_name=user_name, project_id=project_id,
            document_id=document_id, start_line=start_line, end_line=end_line,
        )

    @app.post("/v1/projects/{project_id}/documents/{document_id}/reindex", response_model=DocumentResponse)
    async def reindex_document(
        project_id: str, document_id: str,
        user_name: str = Depends(current_user),
    ):
        return await _call(port.reindex_document, user_name=user_name, project_id=project_id, document_id=document_id)

    @app.delete("/v1/projects/{project_id}/documents/{document_id}", response_model=DocumentDeletedResponse)
    async def delete_document(
        project_id: str, document_id: str,
        user_name: str = Depends(current_user),
    ):
        return await _call(port.delete_document, user_name=user_name, project_id=project_id, document_id=document_id)

    @app.get("/v1/projects/{project_id}/saved-web-links", response_model=list[SavedWebLinkResponse])
    async def list_saved_web_links(
        project_id: str,
        limit: int = Query(default=50, ge=1, le=100),
        user_name: str = Depends(current_user),
    ):
        return await _call(port.list_saved_web_links, user_name=user_name, project_id=project_id, limit=limit)

    @app.patch("/v1/projects/{project_id}/saved-web-links/{link_id}", response_model=SavedWebLinkResponse)
    async def update_saved_web_link(
        project_id: str, link_id: str, body: UpdateSavedWebLinkRequest,
        user_name: str = Depends(current_user),
    ):
        return await _call(
            port.update_saved_web_link, user_name=user_name, project_id=project_id,
            link_id=link_id, request=body,
        )

    @app.delete("/v1/projects/{project_id}/saved-web-links/{link_id}", response_model=SavedWebLinkDeletedResponse)
    async def delete_saved_web_link(
        project_id: str, link_id: str,
        user_name: str = Depends(current_user),
    ):
        return await _call(port.delete_saved_web_link, user_name=user_name, project_id=project_id, link_id=link_id)

    @app.get("/v1/projects/{project_id}/document-scan-settings", response_model=FolderScanSettings)
    async def get_document_scan_settings(
        project_id: str, user_name: str = Depends(current_user),
    ) -> FolderScanSettings:
        return await _call(port.get_document_scan_settings, user_name=user_name, project_id=project_id)

    @app.put("/v1/projects/{project_id}/document-scan-settings", response_model=FolderScanSettings)
    async def set_document_scan_settings(
        project_id: str, body: FolderScanSettings,
        user_name: str = Depends(current_user),
    ) -> FolderScanSettings:
        return await _call(port.set_document_scan_settings, user_name=user_name, project_id=project_id, settings=body)

    @app.delete("/v1/projects/{project_id}/document-scan-settings", response_model=FolderScanSettings)
    async def reset_document_scan_settings(
        project_id: str, user_name: str = Depends(current_user),
    ) -> FolderScanSettings:
        return await _call(port.reset_document_scan_settings, user_name=user_name, project_id=project_id)

    @app.get(
        "/v1/maintenance/reviews",
        response_model=MaintenanceReviewListResponse,
    )
    async def list_global_maintenance_reviews(
        user_name: str = Depends(current_user),
    ) -> MaintenanceReviewListResponse:
        return _maintenance_review_list_response(
            await _call(port.list_global_maintenance_reviews, user_name=user_name)
        )

    @app.post(
        "/v1/maintenance/reviews/{review_id}/decision",
        response_model=MaintenanceOperationResponse,
    )
    async def decide_global_maintenance_review(
        body: MaintenanceReviewDecisionRequest,
        review_id: str = Path(min_length=1),
        user_name: str = Depends(current_user),
    ) -> MaintenanceOperationResponse:
        return _maintenance_operation_response(
            await _call(
                port.decide_global_maintenance_review,
                user_name=user_name,
                review_id=review_id,
                request=body,
            )
        )

    @app.get(
        "/v1/projects/{project_id}/maintenance/reviews",
        response_model=MaintenanceReviewListResponse,
    )
    async def list_project_maintenance_reviews(
        project_id: str = Path(min_length=1),
        user_name: str = Depends(current_user),
    ) -> MaintenanceReviewListResponse:
        return _maintenance_review_list_response(
            await _call(
                port.list_project_maintenance_reviews,
                user_name=user_name,
                project_id=project_id,
            )
        )

    @app.post(
        "/v1/projects/{project_id}/maintenance/reviews/{review_id}/decision",
        response_model=MaintenanceOperationResponse,
    )
    async def decide_project_maintenance_review(
        body: MaintenanceReviewDecisionRequest,
        project_id: str = Path(min_length=1),
        review_id: str = Path(min_length=1),
        user_name: str = Depends(current_user),
    ) -> MaintenanceOperationResponse:
        return _maintenance_operation_response(
            await _call(
                port.decide_project_maintenance_review,
                user_name=user_name,
                project_id=project_id,
                review_id=review_id,
                request=body,
            )
        )

    @app.get(
        "/v1/projects/{project_id}/maintenance/reviews/{review_id}",
        response_model=MaintenanceReviewDetailResponse,
    )
    async def get_project_maintenance_review(
        project_id: str = Path(min_length=1),
        review_id: str = Path(min_length=1),
        user_name: str = Depends(current_user),
    ) -> MaintenanceReviewDetailResponse:
        return MaintenanceReviewDetailResponse.model_validate(
            await _call(
                port.get_project_maintenance_review,
                user_name=user_name,
                project_id=project_id,
                review_id=review_id,
            )
        )

    @app.get(
        "/v1/projects/{project_id}/maintenance/reviews/{review_id}/preview",
        response_model=MaintenanceReviewPreviewResponse,
    )
    async def preview_project_maintenance_review(
        project_id: str = Path(min_length=1),
        review_id: str = Path(min_length=1),
        user_name: str = Depends(current_user),
    ) -> MaintenanceReviewPreviewResponse:
        return MaintenanceReviewPreviewResponse.model_validate(
            await _call(
                port.preview_project_maintenance_review,
                user_name=user_name,
                project_id=project_id,
                review_id=review_id,
            )
        )

    @app.get(
        "/v1/maintenance/entity-merges/{merge_id}/rollback",
        response_model=MaintenanceOperationResponse,
    )
    async def preview_entity_merge_rollback(
        merge_id: str = Path(min_length=1),
        user_name: str = Depends(current_user),
    ) -> MaintenanceOperationResponse:
        return _maintenance_operation_response(
            await _call(
                port.preview_entity_merge_rollback,
                user_name=user_name,
                merge_id=merge_id,
            )
        )

    @app.post(
        "/v1/maintenance/entity-merges/{merge_id}/rollback",
        response_model=MaintenanceOperationResponse,
    )
    async def rollback_entity_merge(
        body: EntityMergeRollbackRequest,
        merge_id: str = Path(min_length=1),
        user_name: str = Depends(current_user),
    ) -> MaintenanceOperationResponse:
        return _maintenance_operation_response(
            await _call(
                port.rollback_entity_merge,
                user_name=user_name,
                merge_id=merge_id,
                request=body,
            )
        )

    @app.get(
        "/v1/projects/{project_id}/episodes/{episode_id}",
        response_model=EpisodeResponse,
    )
    async def get_episode(
        project_id: str = Path(min_length=1, max_length=200),
        episode_id: str = Path(min_length=1, max_length=200),
        user_name: str = Depends(current_user),
    ) -> EpisodeResponse:
        return await port.get_episode(
            user_name=user_name, project_id=project_id, episode_id=episode_id,
        )

    @app.patch(
        "/v1/projects/{project_id}/episodes/{episode_id}",
        response_model=EpisodeEditedResponse,
    )
    async def update_episode(
        body: UpdateEpisodeRequest,
        project_id: str = Path(min_length=1, max_length=200),
        episode_id: str = Path(min_length=1, max_length=200),
        user_name: str = Depends(current_user),
    ) -> EpisodeEditedResponse:
        return await port.update_episode(
            user_name=user_name, project_id=project_id, episode_id=episode_id,
            request=body,
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
        stream = await _open_stream_from_port(port, user_name=user_name, request=body)
        try:
            state = PublicStreamState()
            async for event in stream:
                state.accept(event)
            state.finish()
            if isinstance(state.terminal, RunCompletedEvent):
                return state.terminal.result
            if isinstance(state.terminal, RunFailedEvent):
                raise PublicOperationError(state.terminal.error)
            raise PublicOperationError(PublicError(code="run_cancelled", message="The run was cancelled."))
        finally:
            await _close_owned_stream(stream, request_id=_request_id(request))

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
            state = PublicStreamState()
            try:
                async for raw_event in stream:
                    event = state.accept(raw_event)
                    if isinstance(event, RunFailedEvent):
                        event = event.model_copy(update={"error": sanitize_public_error(
                            event.error, request_id=request_id, run_id=event.run_id,
                        )})
                    yield _sse_frame(event)
                state.finish()
            except Exception as exc:
                # A malformed event after a valid terminal cannot be repaired
                # without violating the one-terminal stream contract.  Keep
                # the already-emitted terminal event as the public result.
                if state.terminal is not None:
                    logger.error("Invalid post-terminal stream request={} run={}", request_id, state.run_id)
                    return
                run_id = state.run_id or str(uuid4())
                logger.error("Public stream failure request={} run={}", request_id, run_id)
                failed = RunFailedEvent(
                    run_id=run_id,
                    sequence=state.sequence + 1,
                    timestamp=_now(),
                    error=to_public_error(
                        PublicStreamContractError("Invalid server output") if isinstance(exc, ValueError) else exc,
                        request_id=request_id, run_id=run_id,
                    ),
                )
                yield _sse_frame(failed)

        return OwnedStreamingResponse(
            events(),
            owner=stream,
            request_id=request_id,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Request-ID": request_id,
            },
        )

    return app


__all__ = ["ApplicationPort", "UnsupportedOperation", "create_app"]
