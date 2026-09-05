"""Bounded PostgreSQL reads for Context and relationship evidence paths."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from common.scoping import require_scope_value


class EvidenceTraversalReader:
    """Read project-owned provenance rows without owning evidence mutations."""

    def __init__(self, client) -> None:
        self.client = client

    async def get_relationship_rows(
        self,
        observation_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        row_limit: int,
    ) -> list[dict[str, Any]]:
        user_name, project_id = self._scope(user_name, project_id)
        ids = self._observation_ids(observation_ids)
        row_limit = self._row_limit(row_limit)
        if not ids:
            return []
        return await self.client.fetch_all(
            """
            SELECT
                observation.observation_id,
                observation.observed_relationship_label,
                observation.observed_at_ms,
                observation.retired_at,
                block.block_id,
                block.content_hash AS block_content_hash,
                block.markdown AS block_markdown,
                support.support_kind,
                support.message_id,
                message.role AS message_role,
                message.timestamp_ms AS message_timestamp_ms,
                support.source_ref_id,
                source.source_kind,
                source.content_hash AS source_content_hash,
                source.locator AS source_locator,
                source.excerpt AS source_excerpt
            FROM public.relationship_observations AS observation
            JOIN public.projects AS project
              ON project.project_id = observation.project_id
             AND project.user_name = observation.user_name
            JOIN public.relationship_observation_blocks AS observation_block
              ON observation_block.observation_id = observation.observation_id
             AND observation_block.project_id = observation.project_id
            JOIN public.project_context_blocks AS block
              ON block.block_id = observation_block.block_id
             AND block.project_id = observation_block.project_id
            LEFT JOIN public.project_context_block_supports AS support
              ON support.block_id = block.block_id
             AND support.project_id = block.project_id
            LEFT JOIN public.messages AS message
              ON message.message_id = support.message_id
             AND message.session_id = support.session_id
             AND message.project_id = support.project_id
            LEFT JOIN public.message_source_refs AS source
              ON source.source_ref_id = support.source_ref_id
             AND source.message_id = support.message_id
             AND source.session_id = support.session_id
             AND source.project_id = support.project_id
            WHERE observation.user_name = %s
              AND observation.project_id = %s
              AND observation.observation_id = ANY(%s)
            ORDER BY observation.observation_id, block.block_id,
                     support.support_kind, support.message_id,
                     support.source_ref_id
            LIMIT %s
            """,
            (user_name, project_id, ids, row_limit),
        )

    async def get_context_block_rows(
        self,
        block_ids: list[UUID | str],
        *,
        user_name: str,
        project_id: str,
        row_limit: int,
    ) -> list[dict[str, Any]]:
        user_name, project_id = self._scope(user_name, project_id)
        ids = self._block_ids(block_ids)
        row_limit = self._row_limit(row_limit)
        if not ids:
            return []
        return await self.client.fetch_all(
            """
            SELECT
                NULL::bigint AS observation_id,
                NULL::text AS observed_relationship_label,
                NULL::bigint AS observed_at_ms,
                NULL::timestamptz AS retired_at,
                block.block_id,
                block.content_hash AS block_content_hash,
                block.markdown AS block_markdown,
                support.support_kind,
                support.message_id,
                message.role AS message_role,
                message.timestamp_ms AS message_timestamp_ms,
                support.source_ref_id,
                source.source_kind,
                source.content_hash AS source_content_hash,
                source.locator AS source_locator,
                source.excerpt AS source_excerpt
            FROM public.project_context_blocks AS block
            JOIN public.projects AS project
              ON project.project_id = block.project_id
            LEFT JOIN public.project_context_block_supports AS support
              ON support.block_id = block.block_id
             AND support.project_id = block.project_id
            LEFT JOIN public.messages AS message
              ON message.message_id = support.message_id
             AND message.session_id = support.session_id
             AND message.project_id = support.project_id
            LEFT JOIN public.message_source_refs AS source
              ON source.source_ref_id = support.source_ref_id
             AND source.message_id = support.message_id
             AND source.session_id = support.session_id
             AND source.project_id = support.project_id
            WHERE project.user_name = %s
              AND block.project_id = %s
              AND block.block_id = ANY(%s)
            ORDER BY block.block_id, support.support_kind, support.message_id,
                     support.source_ref_id
            LIMIT %s
            """,
            (user_name, project_id, ids, row_limit),
        )

    @staticmethod
    def _scope(user_name: str, project_id: str) -> tuple[str, str]:
        return (
            require_scope_value(user_name, "user_name", "read_evidence_traversal"),
            require_scope_value(project_id, "project_id", "read_evidence_traversal"),
        )

    @staticmethod
    def _observation_ids(values: list[int]) -> list[int]:
        if len(values) > 128:
            raise ValueError("evidence traversal accepts at most 128 observations")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in values
        ):
            raise ValueError("evidence observation IDs must be positive integers")
        return sorted(set(values))

    @staticmethod
    def _block_ids(values: list[UUID | str]) -> list[UUID]:
        if len(values) > 256:
            raise ValueError("evidence traversal accepts at most 256 Context blocks")
        try:
            return sorted({UUID(str(value)) for value in values}, key=str)
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("evidence Context block IDs must be UUIDs") from exc

    @staticmethod
    def _row_limit(value: int) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 1_025:
            raise ValueError("evidence row_limit must be between 1 and 1025")
        return value
