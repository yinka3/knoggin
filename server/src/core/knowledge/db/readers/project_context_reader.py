"""Owned reads for canonical project Context revisions and snapshots."""

from __future__ import annotations

from uuid import UUID

from common.schema.context import (
    ContextBlockRecord,
    ContextBlockSupportRecord,
    ContextProjectionState,
    ContextRevisionRecord,
    ContextSnapshot,
)
from common.scoping import require_scope_value
from infrastructure.postgres_client import PostgresClient


class ProjectContextReader:
    """Read immutable Context materializations through project ownership."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    async def get_current_revision(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> ContextRevisionRecord | None:
        """Return the current revision metadata, if Context has been initialized."""

        user_name, project_id = self._scope(user_name, project_id, "get_current_revision")
        row = await self.client.fetch_one(
            """
            SELECT
                revision.revision_id,
                revision.project_id,
                revision.revision_number,
                revision.parent_revision_id,
                revision.window_id,
                revision.origin,
                revision.domain_version,
                revision.edit_summary,
                revision.content_hash
            FROM public.project_contexts AS context
            JOIN public.project_context_revisions AS revision
              ON revision.revision_id = context.current_revision_id
             AND revision.project_id = context.project_id
            WHERE context.user_name = %s AND context.project_id = %s
            """,
            (user_name, project_id),
        )
        return None if row is None else ContextRevisionRecord.model_validate(row)

    async def get_projection_state(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> ContextProjectionState | None:
        """Return the owned projection checkpoint without reading filesystem state."""

        user_name, project_id = self._scope(user_name, project_id, "get_projection_state")
        row = await self.client.fetch_one(
            """
            SELECT
                project_id,
                current_revision_id,
                projection_revision_id,
                projection_hash,
                projection_pending_revision_id,
                projection_pending_hash,
                projection_failure_code,
                projection_failure_summary
            FROM public.project_contexts
            WHERE user_name = %s AND project_id = %s
            """,
            (user_name, project_id),
        )
        return None if row is None else ContextProjectionState.model_validate(row)

    async def get_revision(
        self,
        revision_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> ContextRevisionRecord | None:
        """Return an owned immutable revision without its block materialization."""

        user_name, project_id = self._scope(user_name, project_id, "get_revision")
        revision_id = self._uuid(revision_id, "revision_id")
        row = await self.client.fetch_one(
            """
            SELECT
                revision.revision_id,
                revision.project_id,
                revision.revision_number,
                revision.parent_revision_id,
                revision.window_id,
                revision.origin,
                revision.domain_version,
                revision.edit_summary,
                revision.content_hash
            FROM public.project_context_revisions AS revision
            JOIN public.projects AS project
              ON project.project_id = revision.project_id
            WHERE revision.revision_id = %s
              AND revision.project_id = %s
              AND project.user_name = %s
            """,
            (revision_id, project_id, user_name),
        )
        return None if row is None else ContextRevisionRecord.model_validate(row)

    async def get_revision_blocks(
        self,
        revision_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[ContextBlockRecord]:
        """Reload a revision's exact immutable block versions in stored order."""

        user_name, project_id = self._scope(user_name, project_id, "get_revision_blocks")
        revision_id = self._uuid(revision_id, "revision_id")
        rows = await self.client.fetch_all(
            """
            SELECT
                block.block_id,
                block.project_id,
                block.section_key,
                block.markdown,
                block.content_hash,
                block.assertion_kind,
                block.supersedes_block_id,
                block.source_time_ms
            FROM public.project_context_revision_blocks AS membership
            JOIN public.project_context_revisions AS revision
              ON revision.revision_id = membership.revision_id
             AND revision.project_id = membership.project_id
            JOIN public.projects AS project
              ON project.project_id = revision.project_id
            JOIN public.project_context_blocks AS block
              ON block.block_id = membership.block_id
             AND block.project_id = membership.project_id
            WHERE membership.revision_id = %s
              AND membership.project_id = %s
              AND project.user_name = %s
            ORDER BY membership.ordinal ASC
            """,
            (revision_id, project_id, user_name),
        )
        return [ContextBlockRecord.model_validate(row) for row in rows]

    async def get_snapshot(
        self,
        revision_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> ContextSnapshot | None:
        """Return one complete persisted Context snapshot or ``None`` when absent."""

        revision = await self.get_revision(
            revision_id,
            user_name=user_name,
            project_id=project_id,
        )
        if revision is None:
            return None
        return ContextSnapshot(
            **revision.model_dump(),
            blocks=await self.get_revision_blocks(
                revision.revision_id,
                user_name=user_name,
                project_id=project_id,
            ),
        )

    async def get_revision_impact_block_ids(
        self,
        revision_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> frozenset[UUID]:
        """Reload the persisted Context impact closure for one revision."""

        user_name, project_id = self._scope(
            user_name, project_id, "get_revision_impact_block_ids"
        )
        revision_id = self._uuid(revision_id, "revision_id")
        rows = await self.client.fetch_all(
            """
            SELECT impact.block_id
            FROM public.project_context_revision_impact_blocks AS impact
            JOIN public.project_context_revisions AS revision
              ON revision.revision_id = impact.revision_id
             AND revision.project_id = impact.project_id
            JOIN public.projects AS project
              ON project.project_id = revision.project_id
            WHERE impact.revision_id = %s
              AND impact.project_id = %s
              AND project.user_name = %s
            """,
            (revision_id, project_id, user_name),
        )
        return frozenset(UUID(str(row["block_id"])) for row in rows)

    async def get_block_supports(
        self,
        block_ids: list[UUID | str],
        *,
        user_name: str,
        project_id: str,
    ) -> dict[UUID, tuple[ContextBlockSupportRecord, ...]]:
        """Return typed supports for current Context block versions only."""

        user_name, project_id = self._scope(user_name, project_id, "get_block_supports")
        ids = [self._uuid(block_id, "block_id") for block_id in block_ids]
        if not ids:
            return {}
        rows = await self.client.fetch_all(
            """
            SELECT support.block_id,
                   support.project_id,
                   support.message_id,
                   support.session_id,
                   support.support_kind,
                   support.source_ref_id
            FROM public.project_context_block_supports AS support
            JOIN public.project_context_blocks AS block
              ON block.block_id = support.block_id
             AND block.project_id = support.project_id
            JOIN public.projects AS project
              ON project.project_id = block.project_id
            WHERE support.project_id = %s
              AND project.user_name = %s
              AND support.block_id = ANY(%s)
            ORDER BY support.block_id, support.message_id, support.source_ref_id
            """,
            (project_id, user_name, ids),
        )
        grouped: dict[UUID, list[ContextBlockSupportRecord]] = {}
        for row in rows:
            support = ContextBlockSupportRecord.model_validate(row)
            grouped.setdefault(support.block_id, []).append(support)
        return {
            block_id: tuple(supports) for block_id, supports in grouped.items()
        }

    async def get_window_snapshot(
        self,
        window_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> ContextSnapshot | None:
        """Return a Context revision already committed for one semantic window."""

        user_name, project_id = self._scope(user_name, project_id, "get_window_snapshot")
        window_id = self._uuid(window_id, "window_id")
        row = await self.client.fetch_one(
            """
            SELECT revision_id
            FROM public.project_context_revisions
            WHERE window_id = %s AND project_id = %s
            """,
            (window_id, project_id),
        )
        if row is None:
            return None
        return await self.get_snapshot(
            row["revision_id"],
            user_name=user_name,
            project_id=project_id,
        )

    @staticmethod
    def _scope(user_name: str, project_id: str, operation: str) -> tuple[str, str]:
        return (
            require_scope_value(user_name, "user_name", operation),
            require_scope_value(project_id, "project_id", operation),
        )

    @staticmethod
    def _uuid(value: UUID | str, field: str) -> UUID:
        try:
            return value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError(f"{field} must be a UUID") from exc
