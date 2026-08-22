from typing import Optional

from loguru import logger
from psycopg import Error as PsycopgError

from common.exceptions import StorageWriteError
from common.scoping import require_scope_value
from core.knowledge.db.writers.age_projection_writer import AgeProjectionWriter
from infrastructure.postgres_client import PostgresClient


class ProjectDeletionWriter:
    """Delete a project root and the exceptional identity-scoped entities."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client
        self.projection = AgeProjectionWriter(client)

    @staticmethod
    def _raise_storage_write(operation: str, exc: Exception) -> None:
        logger.error("Storage write failed for {}: {}", operation, exc)
        raise StorageWriteError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    async def delete_project(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> Optional[dict[str, int]]:
        user_name = require_scope_value(user_name, "user_name", "delete_project")
        project_id = require_scope_value(project_id, "project_id", "delete_project")

        try:
            async with self.client.transaction() as cur:
                await cur.execute(
                    """
                    SELECT project_id
                    FROM public.projects
                    WHERE user_name = %s AND project_id = %s
                    FOR UPDATE
                    """,
                    (user_name, project_id),
                )
                if await cur.fetchone() is None:
                    return None

                await self.projection.clear_project_projection(cur, project_id)

                # Entities deliberately do not have a project FK: the reserved
                # identity entity shares the table under the non-project
                # ``__identity__`` scope. Every ordinary entity is still deleted
                # explicitly before the project cascade begins.
                await cur.execute(
                    """
                    DELETE FROM public.entities
                    WHERE user_name = %s AND project_id = %s
                    """,
                    (user_name, project_id),
                )
                deleted: dict[str, int] = {"entities": max(cur.rowcount, 0)}

                await cur.execute(
                    """
                    DELETE FROM public.projects
                    WHERE user_name = %s AND project_id = %s
                    RETURNING project_id
                    """,
                    (user_name, project_id),
                )
                if await cur.fetchone() is None:
                    raise RuntimeError("Project disappeared during aggregate deletion")
                deleted["projects"] = 1
                return deleted
        except PsycopgError as exc:
            self._raise_storage_write("delete_project", exc)
