"""Durable, versioned project-artifact writes."""

from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from typing import Any

from common.schema.artifacts import (
    ArtifactDraft,
    ArtifactReference,
    artifact_content_hash,
    artifact_json,
    render_artifact_markdown,
)
from common.scoping import require_scope_value


class ArtifactWriter:
    """Persist one artifact and its first immutable revision for a message."""

    def __init__(self, client) -> None:
        self.client = client

    @asynccontextmanager
    async def _cursor(self, cursor):
        if cursor is not None:
            yield cursor
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

    async def write_for_assistant_message(
        self,
        message_id: int,
        artifact: ArtifactDraft,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        cursor=None,
    ) -> ArtifactReference:
        if message_id <= 0:
            raise ValueError("message_id must be positive")
        user_name = require_scope_value(
            user_name, "user_name", "write_for_assistant_message_artifact"
        )
        project_id = require_scope_value(
            project_id, "project_id", "write_for_assistant_message_artifact"
        )
        session_id = require_scope_value(
            session_id, "session_id", "write_for_assistant_message_artifact"
        )
        if not isinstance(artifact, ArtifactDraft):
            raise TypeError("artifact must be an ArtifactDraft")

        markdown = render_artifact_markdown(artifact)
        content_hash = artifact_content_hash(artifact, markdown)
        artifact_id = uuid.uuid4()
        async with self._cursor(cursor) as cur:
            await cur.execute(
                """
                INSERT INTO public.project_artifacts (
                    artifact_id, user_name, project_id, session_id,
                    originating_message_id, kind, title, status,
                    current_revision, created_at, updated_at
                )
                SELECT %s, %s, %s, %s, message.message_id, %s, %s, %s,
                       1, clock_timestamp(), clock_timestamp()
                FROM public.messages AS message
                JOIN public.sessions AS session
                  ON session.session_id = message.session_id
                 AND session.project_id = message.project_id
                WHERE message.message_id = %s
                  AND message.project_id = %s
                  AND message.session_id = %s
                  AND message.role = 'assistant'
                  AND session.user_name = %s
                  AND session.status = 'open'
                ON CONFLICT (originating_message_id) DO NOTHING
                RETURNING artifact_id, user_name, project_id, session_id,
                          originating_message_id, kind, title, status,
                          current_revision, created_at, updated_at
                """,
                (
                    str(artifact_id),
                    user_name,
                    project_id,
                    session_id,
                    artifact.kind,
                    artifact.title,
                    artifact.status,
                    message_id,
                    project_id,
                    session_id,
                    user_name,
                ),
            )
            row = await cur.fetchone()
            if row is None:
                row = await self._get_row(
                    cur,
                    artifact_id=None,
                    message_id=message_id,
                    user_name=user_name,
                    project_id=project_id,
                    session_id=session_id,
                )
            if row is None:
                raise ValueError(
                    "assistant message or session is not visible in artifact scope"
                )

            persisted_artifact_id = str(row["artifact_id"])
            await cur.execute(
                """
                INSERT INTO public.project_artifact_revisions (
                    artifact_id, revision, schema_version, kind, title,
                    status, blocks, markdown, content_hash, created_at
                ) VALUES (%s, 1, %s, %s, %s, %s, %s::jsonb, %s, %s,
                          clock_timestamp())
                ON CONFLICT (artifact_id, revision) DO NOTHING
                """,
                (
                    persisted_artifact_id,
                    artifact.schema_version,
                    artifact.kind,
                    artifact.title,
                    artifact.status,
                    json.dumps(artifact_json(artifact)["blocks"]),
                    markdown,
                    content_hash,
                ),
            )
            return self._reference_from_row(row)

    @staticmethod
    async def _get_row(
        cur,
        *,
        artifact_id: str | None,
        message_id: int | None,
        user_name: str,
        project_id: str,
        session_id: str,
    ):
        if artifact_id is not None:
            await cur.execute(
                """
                SELECT artifact_id, user_name, project_id, session_id,
                       originating_message_id, kind, title, status,
                       current_revision, created_at, updated_at
                FROM public.project_artifacts AS artifact
                JOIN public.sessions AS session
                  ON session.session_id = artifact.session_id
                 AND session.project_id = artifact.project_id
                WHERE artifact.artifact_id = %s
                  AND artifact.user_name = %s
                  AND artifact.project_id = %s
                  AND artifact.session_id = %s
                  AND session.user_name = %s
                """,
                (artifact_id, user_name, project_id, session_id, user_name),
            )
        else:
            await cur.execute(
                """
                SELECT artifact_id, user_name, project_id, session_id,
                       originating_message_id, kind, title, status,
                       current_revision, created_at, updated_at
                FROM public.project_artifacts
                WHERE originating_message_id = %s
                  AND user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                """,
                (message_id, user_name, project_id, session_id),
            )
        return await cur.fetchone()

    @staticmethod
    def _reference_from_row(row: dict[str, Any]) -> ArtifactReference:
        return ArtifactReference(
            artifact_id=row["artifact_id"],
            project_id=row["project_id"],
            session_id=row["session_id"],
            originating_message_id=row["originating_message_id"],
            kind=row["kind"],
            title=row["title"],
            status=row["status"],
            current_revision=row["current_revision"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
