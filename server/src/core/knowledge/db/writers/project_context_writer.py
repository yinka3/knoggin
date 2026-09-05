"""Atomic durable Context-root, revision, and projection writes."""

from __future__ import annotations

import json
from uuid import UUID, uuid4

from psycopg import Error as PsycopgError

from common.exceptions import StorageWriteError
from common.schema.context import (
    ContextRevisionOrigin,
    ContextRevisionRecord,
    ContextSnapshot,
    ContextSupportKind,
)
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.scoping import require_scope_value
from core.knowledge.context.models import (
    ContextMaterialization,
    ContextProjectionConflictError,
    ContextRevisionConflictError,
)
from infrastructure.postgres_client import PostgresClient

_REVISION_COLUMNS = """
    revision_id,
    project_id,
    revision_number,
    parent_revision_id,
    window_id,
    origin,
    domain_version,
    edit_summary,
    content_hash
"""


class ProjectContextWriter:
    """Owns current-pointer changes and immutable Context revision publication."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    async def ensure_context(self, *, user_name: str, project_id: str) -> None:
        """Ensure a project has its canonical Context root exactly once."""

        user_name = require_scope_value(user_name, "user_name", "ensure_context")
        project_id = require_scope_value(project_id, "project_id", "ensure_context")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO public.project_contexts (project_id, user_name)
                SELECT project_id, user_name
                FROM public.projects
                WHERE project_id = %s AND user_name = %s
                ON CONFLICT (project_id) DO NOTHING
                """,
                (project_id, user_name),
            )
            await cur.execute(
                """
                SELECT 1
                FROM public.project_contexts
                WHERE project_id = %s AND user_name = %s
                """,
                (project_id, user_name),
            )
            if await cur.fetchone() is None:
                raise ValueError("Project is unavailable while initializing Context")

    async def commit_revision(
        self,
        *,
        user_name: str,
        project_id: str,
        expected_parent_revision_id: UUID | str | None,
        window_id: UUID | str | None,
        origin: ContextRevisionOrigin,
        domain_version: int,
        edit_summary: str,
        materialization: ContextMaterialization,
        new_human_edit_window: SemanticWindowRecord | None = None,
    ) -> ContextSnapshot:
        """Publish one full snapshot while holding the project's Context root lock.

        A retry for the same semantic window returns its pre-existing revision.
        A stale parent never writes a sibling revision.  This method intentionally
        does not touch window stages; the semantic coordinator owns that later.
        """

        user_name = require_scope_value(user_name, "user_name", "commit_context_revision")
        project_id = require_scope_value(project_id, "project_id", "commit_context_revision")
        expected_parent_revision_id = self._optional_uuid(
            expected_parent_revision_id, "expected_parent_revision_id"
        )
        window_id = self._optional_uuid(window_id, "window_id")
        if not isinstance(origin, ContextRevisionOrigin):
            raise TypeError("origin must be a ContextRevisionOrigin")
        if not isinstance(domain_version, int) or isinstance(domain_version, bool) or domain_version < 0:
            raise ValueError("domain_version must be a non-negative integer")
        if not isinstance(edit_summary, str) or len(edit_summary) > 2_000:
            raise ValueError("edit_summary must be a string up to 2000 characters")
        if not isinstance(materialization, ContextMaterialization):
            raise TypeError("materialization must be a ContextMaterialization")
        if any(block.project_id != project_id for block in materialization.blocks):
            raise ValueError("Context materialization blocks must belong to its project")
        if new_human_edit_window is not None:
            if (
                not isinstance(new_human_edit_window, SemanticWindowRecord)
                or new_human_edit_window.window_id != window_id
                or new_human_edit_window.user_name != user_name
                or new_human_edit_window.project_id != project_id
                or new_human_edit_window.origin is not SemanticWindowOrigin.HUMAN_EDIT
                or new_human_edit_window.stage is not SemanticWindowStage.CONTEXT_COMMITTED
                or origin is not ContextRevisionOrigin.HUMAN_EDIT
            ):
                raise ValueError("new human edit window must match its Context revision")

        try:
            async with self.client.transaction() as cur:
                await self._ensure_and_lock_root(
                    cur,
                    user_name=user_name,
                    project_id=project_id,
                )
                root = await cur.fetchone()
                if root is None:
                    raise ValueError("Project is unavailable while committing Context")
                current_revision_id = root["current_revision_id"]

                if new_human_edit_window is not None:
                    await self._create_human_edit_window(cur, new_human_edit_window)
                if window_id is not None:
                    await self._require_owned_window(
                        cur,
                        window_id=window_id,
                        user_name=user_name,
                        project_id=project_id,
                    )
                    existing = await self._load_window_revision(
                        cur,
                        window_id=window_id,
                        project_id=project_id,
                    )
                    if existing is not None:
                        if new_human_edit_window is not None:
                            await self._mark_human_edit_context_committed(
                                cur,
                                window_id=window_id,
                                revision_id=existing.revision_id,
                            )
                        return existing

                if current_revision_id != expected_parent_revision_id:
                    raise ContextRevisionConflictError(
                        "Context changed before this revision could be committed"
                    )

                prior_blocks = await self._load_current_blocks(
                    cur,
                    revision_id=current_revision_id,
                    project_id=project_id,
                )
                current_ids = set(prior_blocks)
                desired_ids = {block.block_id for block in materialization.blocks}
                new_ids = desired_ids - current_ids
                if not materialization.impacted_block_ids.issubset(
                    current_ids | desired_ids
                ):
                    raise ValueError(
                        "Context materialization impact closure must reference current or new blocks"
                    )
                if new_ids != set(materialization.new_block_ids):
                    raise ValueError("Context materialization new-block set does not match its snapshot")
                for block in materialization.blocks:
                    existing_block = prior_blocks.get(block.block_id)
                    if existing_block is not None and existing_block != block:
                        raise ValueError("retained Context blocks must remain immutable")
                    if block.block_id in new_ids and (
                        block.supersedes_block_id is not None
                        and block.supersedes_block_id not in current_ids
                    ):
                        raise ValueError("Context replacement must supersede a block from the current snapshot")

                if not new_ids and materialization.content_hash == (
                    await self._current_content_hash(cur, current_revision_id, project_id)
                ):
                    if current_revision_id is None:
                        raise ValueError("an empty initial Context revision must be explicit")
                    return await self._load_revision_snapshot(
                        cur,
                        revision_id=current_revision_id,
                        project_id=project_id,
                    )

                await self._validate_supports(
                    cur,
                    supports=materialization.supports,
                    new_block_ids=new_ids,
                    window_id=window_id,
                    project_id=project_id,
                )
                for block in materialization.blocks:
                    if block.block_id not in new_ids:
                        continue
                    await cur.execute(
                        """
                        INSERT INTO public.project_context_blocks (
                            block_id, project_id, section_key, markdown, content_hash,
                            assertion_kind, supersedes_block_id, source_time_ms
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            block.block_id,
                            project_id,
                            block.section_key,
                            block.markdown,
                            block.content_hash,
                            block.assertion_kind.value,
                            block.supersedes_block_id,
                            block.source_time_ms,
                        ),
                    )
                for support in materialization.supports:
                    await cur.execute(
                        """
                        INSERT INTO public.project_context_block_supports (
                            block_id, project_id, message_id, session_id, source_ref_id,
                            support_kind
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            support.block_id,
                            project_id,
                            support.message_id,
                            support.session_id,
                            support.source_ref_id,
                            support.support_kind.value,
                        ),
                    )

                revision_id = uuid4()
                revision_number = 1 if current_revision_id is None else int(root["revision_number"]) + 1
                await cur.execute(
                    """
                    INSERT INTO public.project_context_revisions (
                        revision_id, project_id, revision_number, parent_revision_id,
                        window_id, origin, domain_version, edit_summary, content_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING """ + _REVISION_COLUMNS,
                    (
                        revision_id,
                        project_id,
                        revision_number,
                        current_revision_id,
                        window_id,
                        origin.value,
                        domain_version,
                        edit_summary.strip(),
                        materialization.content_hash,
                    ),
                )
                revision_row = await cur.fetchone()
                for ordinal, block in enumerate(materialization.blocks):
                    await cur.execute(
                        """
                        INSERT INTO public.project_context_revision_blocks (
                            revision_id, project_id, block_id, ordinal
                        ) VALUES (%s, %s, %s, %s)
                        """,
                        (revision_id, project_id, block.block_id, ordinal),
                    )
                for block_id in sorted(
                    materialization.impacted_block_ids,
                    key=str,
                ):
                    await cur.execute(
                        """
                        INSERT INTO public.project_context_revision_impact_blocks (
                            revision_id, project_id, block_id
                        ) VALUES (%s, %s, %s)
                        """,
                        (revision_id, project_id, block_id),
                    )
                await cur.execute(
                    """
                    UPDATE public.project_contexts
                    SET current_revision_id = %s, updated_at = NOW()
                    WHERE project_id = %s AND user_name = %s
                    """,
                    (revision_id, project_id, user_name),
                )
                if new_human_edit_window is not None:
                    await self._mark_human_edit_context_committed(
                        cur,
                        window_id=window_id,
                        revision_id=revision_id,
                    )
                return ContextSnapshot(
                    **ContextRevisionRecord.model_validate(revision_row).model_dump(),
                    blocks=list(materialization.blocks),
                )
        except (ContextProjectionConflictError, ContextRevisionConflictError, TypeError, ValueError):
            raise
        except PsycopgError as exc:
            raise StorageWriteError(
                "commit_context_revision",
                details={"error_type": type(exc).__name__},
            ) from exc

    async def record_projection(
        self,
        *,
        user_name: str,
        project_id: str,
        revision_id: UUID | str,
        projection_hash: str,
    ) -> bool:
        """Record a successful file projection only while that revision is current."""

        user_name = require_scope_value(user_name, "user_name", "record_context_projection")
        project_id = require_scope_value(project_id, "project_id", "record_context_projection")
        revision_id = self._uuid(revision_id, "revision_id")
        if not isinstance(projection_hash, str) or len(projection_hash) != 64 or any(
            char not in "0123456789abcdef" for char in projection_hash
        ):
            raise ValueError("projection_hash must be a SHA-256 hex digest")
        try:
            async with self.client.transaction() as cur:
                await cur.execute(
                    """
                    UPDATE public.project_contexts
                    SET projection_hash = %s, projection_synced_at = NOW(), updated_at = NOW()
                    WHERE user_name = %s
                      AND project_id = %s
                      AND current_revision_id = %s
                    RETURNING project_id
                    """,
                    (projection_hash, user_name, project_id, revision_id),
                )
                return await cur.fetchone() is not None
        except PsycopgError as exc:
            raise StorageWriteError(
                "record_context_projection",
                details={"error_type": type(exc).__name__},
            ) from exc

    async def _ensure_and_lock_root(self, cur, *, user_name: str, project_id: str) -> None:
        await cur.execute(
            """
            INSERT INTO public.project_contexts (project_id, user_name)
            SELECT project_id, user_name
            FROM public.projects
            WHERE project_id = %s AND user_name = %s
            ON CONFLICT (project_id) DO NOTHING
            """,
            (project_id, user_name),
        )
        await cur.execute(
            """
            SELECT context.current_revision_id, revision.revision_number
            FROM public.project_contexts AS context
            LEFT JOIN public.project_context_revisions AS revision
              ON revision.revision_id = context.current_revision_id
             AND revision.project_id = context.project_id
            WHERE context.project_id = %s AND context.user_name = %s
            FOR UPDATE OF context
            """,
            (project_id, user_name),
        )

    async def _load_window_revision(self, cur, *, window_id: UUID, project_id: str) -> ContextSnapshot | None:
        await cur.execute(
            """
            SELECT """ + _REVISION_COLUMNS + """
            FROM public.project_context_revisions
            WHERE window_id = %s AND project_id = %s
            """,
            (window_id, project_id),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return await self._load_revision_snapshot(
            cur,
            revision_id=row["revision_id"],
            project_id=project_id,
            revision_row=row,
        )

    async def _load_revision_snapshot(
        self,
        cur,
        *,
        revision_id: UUID,
        project_id: str,
        revision_row: dict | None = None,
    ) -> ContextSnapshot:
        if revision_row is None:
            await cur.execute(
                """
                SELECT """ + _REVISION_COLUMNS + """
                FROM public.project_context_revisions
                WHERE revision_id = %s AND project_id = %s
                """,
                (revision_id, project_id),
            )
            revision_row = await cur.fetchone()
        if revision_row is None:
            raise ValueError("Context revision is unavailable")
        blocks = await self._load_current_blocks(
            cur,
            revision_id=revision_id,
            project_id=project_id,
        )
        return ContextSnapshot(
            **ContextRevisionRecord.model_validate(revision_row).model_dump(),
            blocks=list(blocks.values()),
        )

    async def _load_current_blocks(self, cur, *, revision_id: UUID | None, project_id: str):
        if revision_id is None:
            return {}
        await cur.execute(
            """
            SELECT block.block_id, block.project_id, block.section_key, block.markdown,
                   block.content_hash, block.assertion_kind, block.supersedes_block_id,
                   block.source_time_ms
            FROM public.project_context_revision_blocks AS membership
            JOIN public.project_context_blocks AS block
              ON block.block_id = membership.block_id
             AND block.project_id = membership.project_id
            WHERE membership.revision_id = %s AND membership.project_id = %s
            ORDER BY membership.ordinal ASC
            """,
            (revision_id, project_id),
        )
        from common.schema.context import ContextBlockRecord

        return {
            block.block_id: block
            for block in (ContextBlockRecord.model_validate(row) for row in await cur.fetchall())
        }

    async def _current_content_hash(self, cur, revision_id: UUID | None, project_id: str) -> str | None:
        if revision_id is None:
            return None
        await cur.execute(
            """
            SELECT content_hash
            FROM public.project_context_revisions
            WHERE revision_id = %s AND project_id = %s
            """,
            (revision_id, project_id),
        )
        row = await cur.fetchone()
        return None if row is None else str(row["content_hash"])

    async def _require_owned_window(self, cur, *, window_id: UUID, user_name: str, project_id: str) -> None:
        await cur.execute(
            """
            SELECT window_id
            FROM public.project_semantic_windows
            WHERE window_id = %s AND project_id = %s AND user_name = %s
            FOR UPDATE
            """,
            (window_id, project_id, user_name),
        )
        if await cur.fetchone() is None:
            raise ValueError("Context revision window is unavailable for this project")

    async def _create_human_edit_window(
        self,
        cur,
        window: SemanticWindowRecord,
    ) -> None:
        await cur.execute(
            """
            SELECT project_id
            FROM public.projects
            WHERE project_id = %s AND user_name = %s
            FOR UPDATE
            """,
            (window.project_id, window.user_name),
        )
        if await cur.fetchone() is None:
            raise ValueError("Project is unavailable while creating Context reconciliation")
        await cur.execute(
            """
            SELECT window_id
            FROM public.project_semantic_windows
            WHERE project_id = %s AND user_name = %s AND stage <> 'completed'
            FOR UPDATE
            """,
            (window.project_id, window.user_name),
        )
        if await cur.fetchone() is not None:
            raise ContextProjectionConflictError(
                "Context reconciliation is already active for this project"
            )
        await cur.execute(
            """
            INSERT INTO public.project_semantic_windows (
                window_id, user_name, project_id, origin, stage, domain_version,
                policy_snapshot, source_token_count, token_estimator,
                token_estimator_version, overfill_tokens, overfill_ratio,
                episode_result_recorded, context_revision_id, attempt_count,
                last_failure_stage, last_failure_code, last_failure_at_ms,
                last_error_summary, next_retry_at_ms
            ) VALUES (
                %(window_id)s, %(user_name)s, %(project_id)s, %(origin)s, %(stage)s,
                %(domain_version)s, %(policy_snapshot)s, %(source_token_count)s,
                %(token_estimator)s, %(token_estimator_version)s, %(overfill_tokens)s,
                %(overfill_ratio)s, %(episode_result_recorded)s,
                %(context_revision_id)s, %(attempt_count)s, %(last_failure_stage)s,
                %(last_failure_code)s, %(last_failure_at_ms)s, %(last_error_summary)s,
                %(next_retry_at_ms)s
            )
            """,
            {
                **{
                    key: (value.value if hasattr(value, "value") else value)
                    for key, value in window.model_dump().items()
                },
                "policy_snapshot": json.dumps(window.policy_snapshot),
                # The caller's public record describes the post-commit state.
                # Insert a temporary claimed row so this transaction can attach
                # its just-created Context revision before publishing it.
                "stage": SemanticWindowStage.CLAIMED.value,
            },
        )

    async def _mark_human_edit_context_committed(
        self,
        cur,
        *,
        window_id: UUID,
        revision_id: UUID,
    ) -> None:
        await cur.execute(
            """
            UPDATE public.project_semantic_windows
            SET stage = 'context_committed',
                context_revision_id = %s,
                updated_at = NOW()
            WHERE window_id = %s AND stage = 'claimed'
            RETURNING window_id
            """,
            (revision_id, window_id),
        )
        if await cur.fetchone() is None:
            raise ContextProjectionConflictError(
                "Context reconciliation changed while the human edit was committed"
            )

    async def _validate_supports(
        self,
        cur,
        *,
        supports,
        new_block_ids: set[UUID],
        window_id: UUID | None,
        project_id: str,
    ) -> None:
        if any(support.block_id not in new_block_ids for support in supports):
            raise ValueError("Context support may only be written for a new block")
        if not supports:
            return
        if window_id is None:
            raise ValueError("Context support requires a semantic window")
        support_keys = {
            (
                support.block_id,
                support.message_id,
                support.session_id,
                support.source_ref_id,
                support.support_kind,
            )
            for support in supports
        }
        if len(support_keys) != len(supports):
            raise ValueError("Context support cannot be repeated")
        pairs = {(support.message_id, support.session_id) for support in supports}
        await cur.execute(
            """
            SELECT message_id, session_id, role
            FROM public.messages
            WHERE project_id = %s AND message_id = ANY(%s)
            """,
            (project_id, [pair[0] for pair in pairs]),
        )
        message_rows = await cur.fetchall()
        found_pairs = {
            (int(row["message_id"]), str(row["session_id"])) for row in message_rows
        }
        if found_pairs != pairs:
            raise ValueError("Context support references a message outside this project")
        message_roles = {
            (int(row["message_id"]), str(row["session_id"])): str(row["role"])
            for row in message_rows
        }
        if any(
            message_roles[(support.message_id, support.session_id)]
            != (
                "user"
                if support.support_kind is ContextSupportKind.USER_MESSAGE
                else "assistant"
            )
            for support in supports
        ):
            raise ValueError("Context support kind must match its message role")
        source_supports = [support for support in supports if support.source_ref_id is not None]
        if source_supports:
            await cur.execute(
                """
                SELECT source_ref_id, message_id, session_id
                FROM public.message_source_refs
                WHERE project_id = %s AND source_ref_id = ANY(%s)
                """,
                (project_id, [support.source_ref_id for support in source_supports]),
            )
            source_pairs = {
                (row["source_ref_id"], int(row["message_id"]), str(row["session_id"]))
                for row in await cur.fetchall()
            }
            expected_source_pairs = {
                (support.source_ref_id, support.message_id, support.session_id)
                for support in source_supports
            }
            if source_pairs != expected_source_pairs:
                raise ValueError("Context source support is outside its message/project scope")
        if window_id is not None:
            await cur.execute(
                """
                SELECT message_id, session_id
                FROM public.project_semantic_window_messages
                WHERE window_id = %s AND project_id = %s
                """,
                (window_id, project_id),
            )
            window_pairs = {
                (int(row["message_id"]), str(row["session_id"]))
                for row in await cur.fetchall()
            }
            if not pairs.issubset(window_pairs):
                raise ValueError("Context support must belong to its semantic window")

    @staticmethod
    def _uuid(value: UUID | str, field: str) -> UUID:
        try:
            return value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError(f"{field} must be a UUID") from exc

    @classmethod
    def _optional_uuid(cls, value: UUID | str | None, field: str) -> UUID | None:
        return None if value is None else cls._uuid(value, field)
