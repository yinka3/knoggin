from typing import Optional

from common.scoping import require_scope_value
from core.knowledge.db.writers.age_projection_writer import AgeProjectionWriter
from infrastructure.postgres_client import PostgresClient


class ProjectDeletionWriter:
    """Atomically remove one project and every PostgreSQL/AGE-owned record."""

    _PROJECT_TABLES = (
        "entity_merge_audits",
        "entity_merge_proposals",
        "relationship_advisory_decisions",
        "relationship_advisories",
        "human_reviews",
        "conflict_evidence_refs",
        "conflict_groups",
        "conflict_discovery_checkpoints",
        "parked_dlq_items",
        "episode_processing_checkpoints",
        "episodes",
        "episode_entities",
        "ingestion_candidate_suggestions",
        "agent_tool_audits",
        "document_content",
        "document_chunks",
        "project_documents",
        "document_folder_uploads",
        "document_workspace_sources",
        "project_document_scan_settings",
        "message_entity_refs",
        "message_search",
        "hierarchy_edges",
        "relationships",
        "entity_aliases",
        "entities",
        "messages",
        "sessions",
        "agents",
        "project_search_revisions",
        "project_read_scopes",
    )

    def __init__(self, client: PostgresClient) -> None:
        self.client = client
        self.projection = AgeProjectionWriter(client)

    async def delete_project(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> Optional[dict[str, int]]:
        user_name = require_scope_value(user_name, "user_name", "delete_project")
        project_id = require_scope_value(project_id, "project_id", "delete_project")

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

            deleted: dict[str, int] = {}
            for table in self._PROJECT_TABLES:
                query, params = self._delete_query(
                    table,
                    user_name=user_name,
                    project_id=project_id,
                )
                await cur.execute(query, params)
                deleted[table] = max(cur.rowcount, 0)

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

    @staticmethod
    def _delete_query(
        table: str,
        *,
        user_name: str,
        project_id: str,
    ) -> tuple[str, tuple]:
        if table == "document_content":
            return (
                """
                DELETE FROM public.document_content
                WHERE document_id IN (
                    SELECT document_id FROM public.project_documents
                    WHERE project_id = %s
                )
                """,
                (project_id,),
            )
        if table == "document_chunks":
            return (
                """
                DELETE FROM public.document_chunks
                WHERE document_id IN (
                    SELECT document_id FROM public.project_documents
                    WHERE project_id = %s
                )
                """,
                (project_id,),
            )
        if table == "conflict_evidence_refs":
            return (
                """
                DELETE FROM public.conflict_evidence_refs
                WHERE conflict_id IN (
                    SELECT conflict_id FROM public.conflict_groups
                    WHERE project_id = %s
                )
                """,
                (project_id,),
            )
        if table == "message_entity_refs":
            return (
                """
                DELETE FROM public.message_entity_refs
                WHERE message_id IN (
                    SELECT message_id FROM public.messages
                    WHERE project_id = %s
                )
                """,
                (project_id,),
            )
        if table == "episode_entities":
            return (
                """
                DELETE FROM public.episode_entities
                WHERE episode_id IN (
                    SELECT episode_id FROM public.episodes
                    WHERE project_id = %s
                )
                """,
                (project_id,),
            )
        if table == "entity_aliases":
            return (
                """
                DELETE FROM public.entity_aliases
                WHERE entity_id IN (
                    SELECT entity_id FROM public.entities
                    WHERE project_id = %s
                )
                """,
                (project_id,),
            )
        if table == "project_read_scopes":
            return (
                """
                DELETE FROM public.project_read_scopes
                WHERE user_name = %s
                  AND (project_id = %s OR readable_project_id = %s)
                """,
                (user_name, project_id, project_id),
            )
        return (
            f"DELETE FROM public.{table} WHERE project_id = %s",  # noqa: S608
            (project_id,),
        )
