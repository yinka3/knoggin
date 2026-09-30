"""Permanently expire retained artifacts, never their canonical source rows."""

from psycopg import Error as PsycopgError

from common.artifact_retention import (
    ARTIFACT_RETENTION_BATCH_SIZE,
    ARTIFACT_RETENTION_PERIOD,
)
from common.exceptions import StorageWriteError
from common.scoping import require_scope_value


class ArtifactRetentionWriter:
    def __init__(self, client) -> None:
        self.client = client

    async def purge_expired_artifacts(
        self, *, user_name: str, limit: int = ARTIFACT_RETENTION_BATCH_SIZE,
    ) -> int:
        """Delete one bounded owned batch; revisions cascade in the same transaction.

        Unknown deletion timestamps are deliberately ineligible. Concurrent
        sweepers skip locked artifacts; a failed/cancelled transaction can retry.
        """
        user_name = require_scope_value(user_name, "user_name", "purge_expired_artifacts")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= ARTIFACT_RETENTION_BATCH_SIZE:
            raise ValueError("Artifact purge limit is out of bounds")
        try:
            async with self.client.transaction() as cursor:
                await cursor.execute(
                    """
                    WITH expired AS (
                        SELECT artifact.artifact_id
                        FROM public.project_artifacts AS artifact
                        JOIN public.sessions AS session
                          ON session.session_id = artifact.session_id
                         AND session.project_id = artifact.project_id
                        JOIN public.projects AS project
                          ON project.project_id = artifact.project_id
                        WHERE artifact.user_name = %(user_name)s
                          AND session.user_name = %(user_name)s
                          AND project.user_name = %(user_name)s
                          AND session.status = 'deleted'
                          AND session.deleted_at <= statement_timestamp() - %(retention)s::interval
                        ORDER BY session.deleted_at, artifact.artifact_id
                        LIMIT %(limit)s
                        FOR UPDATE OF artifact SKIP LOCKED
                    )
                    DELETE FROM public.project_artifacts AS artifact
                    USING expired
                    WHERE artifact.artifact_id = expired.artifact_id
                    """,
                    {"user_name": user_name, "retention": ARTIFACT_RETENTION_PERIOD, "limit": limit},
                )
                deleted_count = cursor.rowcount
            return deleted_count
        except PsycopgError as exc:
            raise StorageWriteError("purge_expired_artifacts") from exc
