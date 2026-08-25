"""Scoped reads for project artifacts and immutable revisions."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from common.schema.artifacts import ArtifactReference, ArtifactRevision
from common.scoping import require_scope_value


class ArtifactReader:
    """Read artifacts only through the owning user/project/session scope."""

    def __init__(self, client) -> None:
        self.client = client

    async def get_artifact(
        self,
        artifact_id: str | UUID,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
    ) -> ArtifactReference | None:
        artifact_id = self._artifact_id(artifact_id)
        user_name, project_id = self._project_scope(
            user_name, project_id, "get_artifact"
        )
        row = await self.client.fetch_one(
            """
            SELECT artifact.artifact_id, artifact.project_id, artifact.session_id,
                   artifact.originating_message_id, artifact.kind, artifact.title,
                   artifact.status, artifact.current_revision,
                   artifact.created_at, artifact.updated_at
            FROM public.project_artifacts AS artifact
            JOIN public.sessions AS session
              ON session.session_id = artifact.session_id
             AND session.project_id = artifact.project_id
            WHERE artifact.artifact_id = %s
              AND artifact.user_name = %s
              AND artifact.project_id = %s
              AND (%s::text IS NULL OR artifact.session_id = %s)
              AND session.user_name = %s
              AND session.status = 'open'
            """,
            (
                str(artifact_id),
                user_name,
                project_id,
                session_id,
                session_id,
                user_name,
            ),
        )
        return None if row is None else self._reference_from_row(row)

    async def list_project_artifacts(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
        limit: int = 50,
    ) -> list[ArtifactReference]:
        user_name = require_scope_value(user_name, "user_name", "list_artifacts")
        project_id = require_scope_value(project_id, "project_id", "list_artifacts")
        if session_id is not None:
            session_id = require_scope_value(session_id, "session_id", "list_artifacts")
        if limit < 1 or limit > 200:
            raise ValueError("limit must be between 1 and 200")
        rows = await self.client.fetch_all(
            """
            SELECT artifact.artifact_id, artifact.project_id, artifact.session_id,
                   artifact.originating_message_id, artifact.kind, artifact.title,
                   artifact.status, artifact.current_revision,
                   artifact.created_at, artifact.updated_at
            FROM public.project_artifacts AS artifact
            JOIN public.sessions AS session
              ON session.session_id = artifact.session_id
             AND session.project_id = artifact.project_id
            WHERE artifact.user_name = %s
              AND artifact.project_id = %s
              AND (%s::text IS NULL OR artifact.session_id = %s)
              AND session.user_name = %s
              AND session.status = 'open'
            ORDER BY artifact.updated_at DESC, artifact.artifact_id DESC
            LIMIT %s
            """,
            (user_name, project_id, session_id, session_id, user_name, limit),
        )
        return [self._reference_from_row(row) for row in rows]

    async def get_for_assistant_message(
        self,
        message_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> ArtifactReference | None:
        """Read the artifact durably attached to one assistant message."""

        if message_id <= 0:
            raise ValueError("message_id must be positive")
        user_name, project_id, session_id = self._scope(
            user_name,
            project_id,
            session_id,
            "get_message_artifact",
        )
        row = await self.client.fetch_one(
            """
            SELECT artifact.artifact_id, artifact.project_id, artifact.session_id,
                   artifact.originating_message_id, artifact.kind, artifact.title,
                   artifact.status, artifact.current_revision,
                   artifact.created_at, artifact.updated_at
            FROM public.project_artifacts AS artifact
            JOIN public.sessions AS session
              ON session.session_id = artifact.session_id
             AND session.project_id = artifact.project_id
            WHERE artifact.originating_message_id = %s
              AND artifact.user_name = %s
              AND artifact.project_id = %s
              AND artifact.session_id = %s
              AND session.user_name = %s
              AND session.status = 'open'
            """,
            (message_id, user_name, project_id, session_id, user_name),
        )
        return None if row is None else self._reference_from_row(row)

    async def get_revision(
        self,
        artifact_id: str | UUID,
        revision: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
    ) -> ArtifactRevision | None:
        artifact_id = self._artifact_id(artifact_id)
        if revision < 1:
            raise ValueError("revision must be positive")
        user_name, project_id = self._project_scope(
            user_name, project_id, "get_artifact_revision"
        )
        row = await self.client.fetch_one(
            """
            SELECT revision.artifact_id, revision.revision,
                   revision.schema_version, revision.kind, revision.title,
                   revision.status, revision.blocks, revision.markdown,
                   revision.content_hash, revision.created_at
            FROM public.project_artifact_revisions AS revision
            JOIN public.project_artifacts AS artifact
              ON artifact.artifact_id = revision.artifact_id
            JOIN public.sessions AS session
              ON session.session_id = artifact.session_id
             AND session.project_id = artifact.project_id
            WHERE revision.artifact_id = %s
              AND revision.revision = %s
              AND artifact.user_name = %s
              AND artifact.project_id = %s
              AND (%s::text IS NULL OR artifact.session_id = %s)
              AND session.user_name = %s
              AND session.status = 'open'
            """,
            (
                str(artifact_id),
                revision,
                user_name,
                project_id,
                session_id,
                session_id,
                user_name,
            ),
        )
        if row is None:
            return None
        return ArtifactRevision.model_validate(row)

    @staticmethod
    def _artifact_id(value: str | UUID) -> UUID:
        try:
            return UUID(str(value))
        except (TypeError, ValueError) as exc:
            raise ValueError("artifact_id must be a UUID") from exc

    @staticmethod
    def _scope(user_name: str, project_id: str, session_id: str, operation: str):
        return (
            require_scope_value(user_name, "user_name", operation),
            require_scope_value(project_id, "project_id", operation),
            require_scope_value(session_id, "session_id", operation),
        )

    @staticmethod
    def _project_scope(user_name: str, project_id: str, operation: str):
        return (
            require_scope_value(user_name, "user_name", operation),
            require_scope_value(project_id, "project_id", operation),
        )

    @staticmethod
    def _reference_from_row(row: dict[str, Any]) -> ArtifactReference:
        return ArtifactReference.model_validate(row)
