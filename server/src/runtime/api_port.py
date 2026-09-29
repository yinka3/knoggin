"""Application-port implementation backed by the canonical runtime owners.

The FastAPI module deliberately remains dependency-injected.  This adapter is
the composition seam for a live :class:`ApplicationRuntime`: it translates
public requests into the existing project/session workflows and translates the
internal agent event stream back into the versioned public stream contract.
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from loguru import logger
from pydantic import ValidationError

from common.conf.domain_config import DomainConfig
from common.document_limits import MAX_DOCUMENT_BASE64_LENGTH, MAX_DOCUMENT_SIZE
from common.exceptions import (
    EpisodeEditConflictError,
    NotFoundError,
    PayloadTooLargeError,
)
from common.schema.document import (
    DocumentSelection,
    FolderScanSettings,
    create_document_focus,
)
from common.schema.health import HealthSnapshot
from common.schema.primitives import Message
from common.schema.public import (
    AACAdmissionResponse,
    AACDiscussionResponse,
    AACInsightResponse,
    AACInsightVoteResponse,
    AACParticipationResponse,
    AACStopResponse,
    AACTimelineResponse,
    ArtifactListResponse,
    ArtifactResponse,
    BatchDocumentAccepted,
    BatchDocumentItem,
    BatchSourceFailed,
    BatchSourceRequest,
    BatchSourceResponse,
    BatchWebLinkAccepted,
    CreateProjectRequest,
    CreateSessionRequest,
    DocumentContentResponse,
    DocumentDeletedResponse,
    DocumentFocusResponse,
    DocumentResponse,
    EntityMergeRollbackRequest,
    EpisodeEditedResponse,
    EpisodeResponse,
    MaintenanceReviewDecisionRequest,
    MaintenanceReviewDetailResponse,
    MaintenanceReviewPreviewResponse,
    MaintenanceReviewResponse,
    MessageDeltaEvent,
    ProjectDeletedResponse,
    ProjectResponse,
    PromoteSourceRequest,
    PublicError,
    PublicLLMSettings,
    PublicSearchSettings,
    RunCompletedEvent,
    RunFailedEvent,
    RunResult,
    RunStartedEvent,
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
    SourceAddedEvent,
    StartRunRequest,
    ToolCompletedEvent,
    ToolStartedEvent,
    UpdateEpisodeRequest,
    UpdateProjectRequest,
    UpdateSavedWebLinkRequest,
    UpdateSessionRequest,
    UploadDocumentRequest,
    Usage,
    UsageUpdatedEvent,
    project_public_model,
    to_public_error,
)
from common.schema.source.references import SourceConsulted
from common.scoping import require_scope_value
from common.utils.lifecycle import settle_owned_task
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

    async def _require_health_project(self, user_name: str, project_id: str) -> None:
        self._require_user(user_name)
        if await self.runtime.projects.get_project(project_id) is None:
            raise NotFoundError("project")

    async def get_engine_health(self, *, user_name: str) -> HealthSnapshot:
        self._require_user(user_name)
        return await self.runtime.health_service.get_engine_health()

    async def get_resource_health(
        self, *, user_name: str, project_id: str
    ) -> HealthSnapshot:
        await self._require_health_project(user_name, project_id)
        return await self.runtime.health_service.get_resource_health(
            project_id=project_id
        )

    async def get_ingestion_health(
        self, *, user_name: str, project_id: str
    ) -> HealthSnapshot:
        await self._require_health_project(user_name, project_id)
        return await self.runtime.health_service.get_ingestion_health(
            user_name=user_name,
            project_id=project_id,
        )

    async def get_background_health(
        self, *, user_name: str, project_id: str
    ) -> HealthSnapshot:
        await self._require_health_project(user_name, project_id)
        return await self.runtime.health_service.get_background_health(
            project_id=project_id
        )

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

    async def get_settings(self, *, user_name: str) -> SettingsResponse:
        self._require_user(user_name)
        config = self.runtime.config_manager.config
        return SettingsResponse(
            user_aliases=tuple(config.user_aliases),
            llm=PublicLLMSettings(
                agent_model=config.llm.agent_model, extraction_model=config.llm.extraction_model,
                merge_model=config.llm.merge_model, spending_budget=config.llm.spending_budget,
                api_key_configured=bool(config.llm.api_key),
            ),
            search=PublicSearchSettings(provider=config.search.provider,
                brave_api_key_configured=bool(config.search.brave_api_key),
                tavily_api_key_configured=bool(config.search.tavily_api_key)),
            developer_settings=config.developer_settings,
        )

    async def get_settings_status(self, *, user_name: str) -> SettingsApplyStatusResponse:
        self._require_user(user_name)
        status = self.runtime.config_manager.last_apply_status
        return SettingsApplyStatusResponse(
            generation=status.generation, persisted=status.persisted, activated=status.activated,
            failed_subscriptions=status.failed_subscriptions, pending_subscriptions=status.pending_subscriptions,
            fully_applied=status.fully_applied,
        )

    async def update_settings(self, *, user_name: str, request: SettingsUpdateRequest) -> SettingsOperationResponse:
        self._require_user(user_name)
        manager = self.runtime.config_manager
        try:
            manager.validate_updates(request.updates)
        except (ValidationError, ValueError, TypeError):
            raise ValueError("Invalid settings update") from None
        # These calls must stay on the subscriber thread; never use to_thread.
        accepted = manager.update_settings(request.updates)
        return SettingsOperationResponse(accepted=accepted, status=await self.get_settings_status(user_name=user_name))

    async def reload_settings(self, *, user_name: str) -> SettingsOperationResponse:
        self._require_user(user_name)
        accepted = self.runtime.config_manager.load()
        return SettingsOperationResponse(accepted=accepted, status=await self.get_settings_status(user_name=user_name))

    async def retry_settings_applies(self, *, user_name: str) -> SettingsApplyStatusResponse:
        self._require_user(user_name)
        self.runtime.config_manager.retry_failed_applies()
        return await self.get_settings_status(user_name=user_name)

    def _aac(self, user_name: str):
        self._require_user(user_name)
        return self.runtime.aac_runtime

    async def trigger_aac(self, *, user_name: str) -> AACAdmissionResponse:
        admission = await self._aac(user_name).trigger_discussion()
        return AACAdmissionResponse(outcome=admission.outcome, reason=admission.reason,
                                    discussion_id=admission.discussion_id)

    async def stop_aac(self, *, user_name: str) -> AACStopResponse:
        return AACStopResponse(stop_requested=await self._aac(user_name).request_stop())

    async def set_aac_participation(self, *, user_name: str, agent_id: str, enabled: bool) -> AACParticipationResponse:
        agent = await self._aac(user_name).set_participation(agent_id, enabled)
        if agent is None:
            raise NotFoundError("agent")
        return AACParticipationResponse(agent_id=agent.id, enabled=agent.aac_enabled)

    async def list_aac_discussions(self, *, user_name: str, limit: int = 20) -> list[AACDiscussionResponse]:
        rows = await self._aac(user_name).list_discussions(limit=limit)
        return [project_public_model(AACDiscussionResponse, row) for row in rows]

    async def list_aac_timeline(self, *, user_name: str, discussion_id: str, limit: int = 100, after_sequence: int = 0) -> list[AACTimelineResponse]:
        rows = await self._aac(user_name).list_timeline(discussion_id, limit=limit, after_sequence=after_sequence)
        return [project_public_model(AACTimelineResponse, row) for row in rows]

    async def list_aac_insights(self, *, user_name: str, query: str | None = None, limit: int = 20) -> list[AACInsightResponse]:
        rows = await self._aac(user_name).list_insights(query=query, limit=limit)
        return [project_public_model(AACInsightResponse, row) for row in rows]

    async def list_aac_insight_votes(self, *, user_name: str, insight_id: str) -> list[AACInsightVoteResponse]:
        rows = await self._aac(user_name).list_insight_votes(insight_id)
        return [project_public_model(AACInsightVoteResponse, row) for row in rows]

    async def list_projects(self, *, user_name: str) -> list[ProjectResponse]:
        self._require_user(user_name)
        rows = await self.runtime.projects.list_projects()
        return [project_public_model(ProjectResponse, row) for row in rows]

    async def get_project(self, *, user_name: str, project_id: str) -> ProjectResponse:
        self._require_user(user_name)
        row = await self.runtime.projects.get_project(project_id)
        if row is None:
            raise NotFoundError("project")
        return project_public_model(ProjectResponse, row)

    async def update_project(
        self, *, user_name: str, project_id: str, request: UpdateProjectRequest
    ) -> ProjectResponse:
        self._require_user(user_name)
        row = await self.runtime.projects.update_project(
            project_id, **request.model_dump(exclude_unset=True)
        )
        if row is None:
            raise NotFoundError("project")
        return project_public_model(ProjectResponse, row)

    async def archive_project(self, *, user_name: str, project_id: str) -> ProjectResponse:
        self._require_user(user_name)
        row = await self.runtime.projects.archive_project(project_id)
        if row is None:
            raise NotFoundError("project")
        return project_public_model(ProjectResponse, row)

    async def delete_project(self, *, user_name: str, project_id: str) -> ProjectDeletedResponse:
        self._require_user(user_name)
        # Do not pre-read: a retry may only have pending file cleanup left.
        row = await self.runtime.projects.delete_project(project_id)
        if row is None:
            raise NotFoundError("project")
        return ProjectDeletedResponse(
            project_id=project_id, file_cleanup_status=row["file_cleanup_status"]
        )

    async def _owned_episode(self, *, user_name: str, project_id: str, episode_id: str):
        episode = await self.runtime.resources.knowledge_store.get_project_episode(
            episode_id, user_name=user_name, project_id=project_id,
            visible_project_ids=[project_id],
        )
        if episode is None or episode.project_id != project_id or episode.episode_id != episode_id:
            raise NotFoundError("episode")
        return episode

    async def get_episode(
        self, *, user_name: str, project_id: str, episode_id: str,
    ) -> EpisodeResponse:
        self._require_user(user_name)
        project_id = require_scope_value(project_id, "project_id", "get_episode")
        episode_id = require_scope_value(episode_id, "episode_id", "get_episode")
        project = await self.runtime.projects.get_project(project_id)
        if project is None or project["status"] not in {"active", "archived"}:
            raise NotFoundError("project")
        episode = await self._owned_episode(
            user_name=user_name, project_id=project_id, episode_id=episode_id,
        )
        return project_public_model(EpisodeResponse, episode)

    async def update_episode(
        self, *, user_name: str, project_id: str, episode_id: str,
        request: UpdateEpisodeRequest,
    ) -> EpisodeEditedResponse:
        self._require_user(user_name)
        project_id = require_scope_value(project_id, "project_id", "update_episode")
        episode_id = require_scope_value(episode_id, "episode_id", "update_episode")
        project = await self.runtime.projects.get_project(project_id)
        if project is None or project["status"] == "deleted":
            raise NotFoundError("project")
        if project["status"] != "active":
            raise PermissionError("Archived projects are read-only")
        maximum = self.runtime.config_manager.config.developer_settings.jobs.episode.max_narrative_chars
        if request.narrative_character_count() > maximum:
            raise ValueError("Episode narrative exceeds the configured character limit")

        # The exact lease blocks archive/delete while embedding and CAS persist.
        # This is project-owned editing; no session is resumed or fabricated.
        async with self._project_runtime(
            user_name=user_name, project_id=project_id, lease_prefix="api-episode-edit",
        ):
            episode = await self._owned_episode(
                user_name=user_name, project_id=project_id, episode_id=episode_id,
            )
            if episode.updated_at != request.expected_updated_at:
                raise EpisodeEditConflictError()
            worker = asyncio.create_task(self.runtime.resources.knowledge_store.edit_episode(
                episode_id=episode_id, user_name=user_name, project_id=project_id,
                summary=request.summary, new_developments=request.new_developments,
                updates=request.updates, unresolved=request.unresolved,
                expected_updated_at=request.expected_updated_at,
            ))
            try:
                updated_at = await asyncio.shield(worker)
            except asyncio.CancelledError:
                try:
                    await settle_owned_task(worker)
                except Exception:
                    # Preserve caller cancellation, not a worker's raw failure.
                    logger.error("Cancelled episode edit worker failed")
                raise
            return EpisodeEditedResponse(
                episode_id=episode_id, project_id=project_id, updated_at=updated_at,
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

    async def list_sessions(self, *, user_name: str) -> list[SessionResponse]:
        self._require_user(user_name)
        rows = await self.runtime.sessions.list_sessions()
        return [project_public_model(SessionResponse, row) for row in rows.values()]

    async def _session_metadata(self, *, user_name: str, session_id: str) -> SessionResponse:
        # Durable metadata read only: do not resume a runtime or acquire a lease.
        rows = await self.list_sessions(user_name=user_name)
        for row in rows:
            if row.session_id == session_id:
                return row
        raise NotFoundError("session")

    async def get_session_history(
        self, *, user_name: str, session_id: str, limit: int = 100
    ) -> list[SessionHistoryMessage]:
        if not 1 <= limit <= 1000:
            raise ValueError("History limit must be between 1 and 1000")
        await self._session_metadata(user_name=user_name, session_id=session_id)
        rows = await self.runtime.sessions.get_session_history_readonly(session_id, limit=limit)
        return [project_public_model(SessionHistoryMessage, row) for row in rows]

    async def update_session(
        self, *, user_name: str, session_id: str, request: UpdateSessionRequest
    ) -> SessionResponse:
        await self._session_metadata(user_name=user_name, session_id=session_id)
        await self.runtime.sessions.update_session(
            session_id, request.model_dump(exclude_unset=True)
        )
        return await self._session_metadata(user_name=user_name, session_id=session_id)

    async def delete_session(
        self, *, user_name: str, session_id: str
    ) -> SessionDeletedResponse:
        await self._session_metadata(user_name=user_name, session_id=session_id)
        await self.runtime.sessions.delete_session(session_id)
        return SessionDeletedResponse(session_id=session_id)

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
                path_prefix=(
                    target["path_prefix"]
                    if target["target_type"] == "subtree"
                    else None
                ),
                behavior=target.get("behavior", "prefer"),
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

    async def promote_source(
        self,
        *,
        user_name: str,
        project_id: str,
        request: PromoteSourceRequest,
    ) -> SavedWebLinkResponse:
        """Promote a cited assistant source only after an explicit user action."""
        session = await self._session(
            user_name=user_name,
            session_id=request.session_id,
        )
        if session.project_id != project_id:
            raise PermissionError("Source promotion must target the session project")
        source = await self.runtime.resources.knowledge_store.get_source_reference(
            request.source_ref_id,
            user_name=user_name,
            project_id=project_id,
            session_id=request.session_id,
        )
        if source is None:
            raise NotFoundError("source")
        if session.document_service is None:
            raise RuntimeError("Session document service is unavailable")
        return project_public_model(SavedWebLinkResponse, await session.document_service.promote_source(
            source,
            title=request.title,
            summary=request.summary,
        ))

    @asynccontextmanager
    async def _project_runtime(
        self, *, user_name: str, project_id: str, lease_prefix: str = "api-documents",
    ):
        self._require_user(user_name)
        lease_id = f"{lease_prefix}:{uuid4()}"
        try:
            project = await self.runtime.projects.acquire_project_for_session(
                project_id,
                lease_id,
            )
        except ValueError as exc:
            raise NotFoundError("project") from exc
        try:
            yield project
        finally:
            cleanup = asyncio.create_task(
                self.runtime.projects.release_project_for_session(project_id, lease_id)
            )
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await settle_owned_task(cleanup)
                raise

    @asynccontextmanager
    async def _project_documents(self, *, user_name: str, project_id: str):
        async with self._project_runtime(user_name=user_name, project_id=project_id) as project:
            yield project.document_service

    async def list_documents(self, *, user_name: str, project_id: str, limit: int = 100) -> list[DocumentResponse]:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return [project_public_model(DocumentResponse, row) for row in await service.list_documents(limit=limit)]

    async def get_document(self, *, user_name: str, project_id: str, document_id: str) -> DocumentResponse:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return project_public_model(DocumentResponse, await service.get_document_info(document_id=document_id))

    async def read_document(
        self, *, user_name: str, project_id: str, document_id: str,
        start_line: int = 1, end_line: int | None = None,
    ) -> DocumentContentResponse:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return project_public_model(DocumentContentResponse, await service.read_document(
                document_id=document_id, start_line=start_line, end_line=end_line
            ))

    async def admit_sources_batch(
        self, *, user_name: str, project_id: str, request: BatchSourceRequest
    ) -> BatchSourceResponse:
        self._require_user(user_name)
        decoded = {}
        failures = {}
        total_bytes = 0
        for index, item in enumerate(request.items):
            if isinstance(item, BatchDocumentItem):
                try:
                    content = base64.b64decode(item.content_base64, validate=True)
                except ValueError:
                    failures[index] = ValueError("Invalid base64 content")
                    continue
                total_bytes += len(content)
                if total_bytes > MAX_DOCUMENT_SIZE:
                    raise PayloadTooLargeError()
                decoded[index] = content
        results = []
        # One exact project lease covers the bounded sequential batch.
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            for index, item in enumerate(request.items):
                try:
                    if index in failures:
                        raise failures[index]
                    if isinstance(item, BatchDocumentItem):
                        row = await service.submit_document(content=decoded[index], original_name=item.original_name,
                                                            relative_path=item.relative_path)
                        results.append(BatchDocumentAccepted(index=index, document=project_public_model(DocumentResponse, row)))
                    else:
                        try:
                            row = await service.save_web_link(url=item.url, title=item.title, summary=item.summary)
                        except ValidationError:
                            raise ValueError("Invalid web link") from None
                        results.append(BatchWebLinkAccepted(index=index, link=project_public_model(SavedWebLinkResponse, row)))
                except Exception as error:
                    # Cancellation is not swallowed; accepted items are not rolled back.
                    results.append(BatchSourceFailed(index=index, error=to_public_error(error)))
        return BatchSourceResponse(results=tuple(results))

    async def upload_document(
        self, *, user_name: str, project_id: str, request: UploadDocumentRequest,
    ) -> DocumentResponse:
        if len(request.content_base64) > MAX_DOCUMENT_BASE64_LENGTH:
            raise PayloadTooLargeError()
        try:
            content = base64.b64decode(request.content_base64, validate=True)
        except ValueError as exc:
            raise ValueError("content_base64 must be valid base64") from exc
        if len(content) > MAX_DOCUMENT_SIZE:
            raise PayloadTooLargeError()
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return project_public_model(DocumentResponse, await service.submit_document(
                content=content,
                original_name=request.original_name,
                relative_path=request.relative_path,
            ))

    async def reindex_document(self, *, user_name: str, project_id: str, document_id: str) -> DocumentResponse:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return project_public_model(DocumentResponse, await service.reindex_document(document_id=document_id))

    async def delete_document(self, *, user_name: str, project_id: str, document_id: str) -> DocumentDeletedResponse:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return project_public_model(DocumentDeletedResponse, await service.delete_document(document_id=document_id))

    async def list_saved_web_links(self, *, user_name: str, project_id: str, limit: int = 50) -> list[SavedWebLinkResponse]:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return [project_public_model(SavedWebLinkResponse, row) for row in await service.list_saved_web_links(limit=limit)]

    async def update_saved_web_link(
        self, *, user_name: str, project_id: str, link_id: str,
        request: UpdateSavedWebLinkRequest,
    ) -> SavedWebLinkResponse:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return project_public_model(SavedWebLinkResponse, await service.update_saved_web_link(
                link_id=link_id,
                **request.model_dump(exclude_unset=True),
            ))

    async def delete_saved_web_link(self, *, user_name: str, project_id: str, link_id: str) -> SavedWebLinkDeletedResponse:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return project_public_model(SavedWebLinkDeletedResponse, await service.delete_saved_web_link(link_id=link_id))

    async def get_document_scan_settings(self, *, user_name: str, project_id: str) -> FolderScanSettings:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return await service.get_scan_settings()

    async def set_document_scan_settings(
        self, *, user_name: str, project_id: str, settings: FolderScanSettings,
    ) -> FolderScanSettings:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return await service.save_scan_settings(settings)

    async def reset_document_scan_settings(self, *, user_name: str, project_id: str) -> FolderScanSettings:
        async with self._project_documents(user_name=user_name, project_id=project_id) as service:
            return await service.reset_scan_settings()

    @staticmethod
    def _maintenance_review_response(review: Any) -> MaintenanceReviewResponse:
        return MaintenanceReviewResponse.model_validate(
            {
                "review_id": review.review_id,
                "scope": review.scope,
                "project_id": review.project_id,
                "kind": review.kind,
                "reasoning": review.reasoning,
                "proposed_plan": review.proposed_plan.model_dump(mode="json"),
                "expected_state": review.expected_state,
                "status": review.status,
                "created_at": review.created_at,
                "resolved_at": review.resolved_at,
            }
        )

    async def list_global_maintenance_reviews(
        self,
        *,
        user_name: str,
    ) -> list[MaintenanceReviewResponse]:
        self._require_user(user_name)
        reviews = await self.runtime.projects.list_global_maintenance_reviews()
        return [self._maintenance_review_response(review) for review in reviews]

    async def decide_global_maintenance_review(
        self,
        *,
        user_name: str,
        review_id: str,
        request: MaintenanceReviewDecisionRequest,
    ) -> dict:
        self._require_user(user_name)
        if request.action == "apply":
            return await self.runtime.projects.apply_global_entity_merge_review(
                review_id,
                expected_state=request.expected_state,
            )
        review = await self.runtime.projects.dismiss_global_maintenance_review(
            review_id,
            expected_state=request.expected_state,
            reason=request.reason,
        )
        return {"review": self._maintenance_review_response(review).model_dump(mode="json")}

    async def list_project_maintenance_reviews(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> list[MaintenanceReviewResponse]:
        self._require_user(user_name)
        reviews = await self.runtime.projects.maintenance_service.list_maintenance_reviews(
            project_id
        )
        return [self._maintenance_review_response(review) for review in reviews]

    async def get_project_maintenance_review(
        self,
        *,
        user_name: str,
        project_id: str,
        review_id: str,
    ) -> MaintenanceReviewDetailResponse:
        self._require_user(user_name)
        detail = await self.runtime.projects.maintenance_service.get_maintenance_review_detail(
            project_id, review_id
        )
        return MaintenanceReviewDetailResponse(
            review=self._maintenance_review_response(detail.review),
            stored_snapshot=detail.stored_snapshot,
            current_evidence=detail.current_evidence,
            unavailable_pointers=detail.unavailable_pointers,
            evidence_state=detail.evidence_state,
        )

    async def preview_project_maintenance_review(
        self,
        *,
        user_name: str,
        project_id: str,
        review_id: str,
    ) -> MaintenanceReviewPreviewResponse:
        self._require_user(user_name)
        detail, impact = await self.runtime.projects.maintenance_service.preview_maintenance_review(
            project_id, review_id
        )
        return MaintenanceReviewPreviewResponse(
            detail=MaintenanceReviewDetailResponse(
                review=self._maintenance_review_response(detail.review),
                stored_snapshot=detail.stored_snapshot,
                current_evidence=detail.current_evidence,
                unavailable_pointers=detail.unavailable_pointers,
                evidence_state=detail.evidence_state,
            ),
            impact=impact,
        )

    async def decide_project_maintenance_review(
        self,
        *,
        user_name: str,
        project_id: str,
        review_id: str,
        request: MaintenanceReviewDecisionRequest,
    ) -> dict:
        self._require_user(user_name)
        review = await self.runtime.projects.maintenance_service.transition_maintenance_review(
            project_id,
            review_id,
            status="applied" if request.action == "apply" else "dismissed",
            expected_state=request.expected_state,
            reason=request.reason,
        )
        return {"review": self._maintenance_review_response(review).model_dump(mode="json")}

    async def preview_entity_merge_rollback(
        self,
        *,
        user_name: str,
        merge_id: str,
    ) -> dict:
        self._require_user(user_name)
        return await self.runtime.projects.preview_global_entity_merge_rollback(merge_id)

    async def rollback_entity_merge(
        self,
        *,
        user_name: str,
        merge_id: str,
        request: EntityMergeRollbackRequest,
    ) -> dict:
        self._require_user(user_name)
        return await self.runtime.projects.rollback_global_entity_merge(
            merge_id,
            approved_mutation_ids=request.approved_mutation_ids,
        )

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
            document_id=(
                requested.document_id
                if requested.target_type == "document"
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
            )
            target["selection"] = DocumentSelection(
                content_hash=resolved["content_hash"],
                parse_snapshot_id=resolved["parse_snapshot_id"],
                locator=resolved["locator"],
            )
        return create_document_focus(
            mode="request",
            behavior=requested.behavior,
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
            idempotency_key=request.idempotency_key,
            research_mode=request.research_mode,
        )
        from common.utils.streams import ClosingAsyncIterator

        return ClosingAsyncIterator(
            self._public_run_stream(
                session=session, request=request, agent_stream=agent_stream,
            ),
            agent_stream,
        )

    async def run_stream(
        self,
        *,
        user_name: str,
        request: StartRunRequest,
    ) -> AsyncIterator[object]:
        stream = await self.open_run_stream(user_name=user_name, request=request)
        try:
            async for event in stream:
                yield event
        finally:
            await stream.aclose()

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
                error_code = data.get("code")
                yield event(
                    ToolCompletedEvent,
                    tool_name=str(data["tool"]),
                    succeeded=False,
                    error_code=(
                        error_code
                        if error_code in {"tool_failed", "workspace_conflict"}
                        else None
                    ),
                    retryable=bool(data.get("retryable", False)),
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
                error_code = data.get("code")
                if error_code == "llm_budget_exhausted":
                    public_code = "llm_budget_exhausted"
                    message = "The model budget is exhausted."
                    retryable = False
                elif error_code == "workspace_conflict":
                    public_code = "workspace_conflict"
                    message = "The workspace changed before the request could be applied."
                    retryable = False
                else:
                    public_code = "run_failed"
                    message = "The response could not be completed or saved."
                    retryable = True
                yield event(
                    RunFailedEvent,
                    error=PublicError(
                        code=public_code,
                        message=message,
                        retryable=retryable,
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
        values = await store.get_message_source_refs(
            message_id,
            user_name=session.user_name,
            project_id=session.project_id,
            session_id=session.session_id,
        )
        return [value if isinstance(value, SourceConsulted) else SourceConsulted.model_validate(value) for value in values]

    async def _message_artifact(self, session: Any, message_id: Any):
        if not isinstance(message_id, int) or message_id <= 0:
            return None
        return await self.runtime.resources.knowledge_store.get_message_artifact(
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
