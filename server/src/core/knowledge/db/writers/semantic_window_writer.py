"""Durable claim/checkpoint writes for project semantic windows."""

from __future__ import annotations

import json
from collections.abc import Sequence
from uuid import UUID

from psycopg import Error as PsycopgError

from common.exceptions import StorageWriteError
from common.schema.semantic_window import (
    SemanticWindowClaimResult,
    SemanticWindowMessage,
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.scoping import require_scope_value
from infrastructure.postgres_client import PostgresClient

_STAGE_SUCCESSOR = {
    SemanticWindowStage.CLAIMED: SemanticWindowStage.CONTEXT_COMMITTED,
    SemanticWindowStage.CONTEXT_COMMITTED: SemanticWindowStage.KNOWLEDGE_COMMITTED,
    SemanticWindowStage.KNOWLEDGE_COMMITTED: SemanticWindowStage.COMPLETED,
}
_WINDOW_COLUMNS = """
    window_id,
    user_name,
    project_id,
    origin,
    stage,
    domain_version,
    policy_snapshot,
    source_token_count,
    token_estimator,
    token_estimator_version,
    overfill_tokens,
    overfill_ratio,
    episode_result_recorded,
    context_revision_id,
    attempt_count,
    last_failure_stage,
    last_failure_code,
    last_failure_at_ms,
    last_error_summary,
    next_retry_at_ms
"""


class SemanticWindowWriter:
    """Claims one project window and advances only its durable checkpoints."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    async def claim_window(
        self,
        window: SemanticWindowRecord,
        messages: Sequence[SemanticWindowMessage],
    ) -> SemanticWindowClaimResult:
        """Create one frozen window or return the current active window atomically."""

        if not isinstance(window, SemanticWindowRecord):
            raise TypeError("window must be a SemanticWindowRecord")
        if window.stage is not SemanticWindowStage.CLAIMED:
            raise ValueError("a newly claimed semantic window must start at claimed")
        members = list(messages)
        self._validate_membership(window, members)
        try:
            async with self.client.transaction() as cur:
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
                    raise ValueError("Project is unavailable while claiming semantic work")
                await cur.execute(
                    f"""
                    SELECT {_WINDOW_COLUMNS}
                    FROM public.project_semantic_windows
                    WHERE project_id = %s AND user_name = %s AND stage <> 'completed'
                    FOR UPDATE
                    """,
                    (window.project_id, window.user_name),
                )
                active = await cur.fetchone()
                if active is not None:
                    return SemanticWindowClaimResult(
                        window=SemanticWindowRecord.model_validate(active),
                        claimed=False,
                    )

                await self._validate_durable_conversation_membership(
                    cur,
                    window=window,
                    members=members,
                )

                await cur.execute(
                    """
                    INSERT INTO public.project_semantic_windows (
                        window_id, user_name, project_id, origin, stage,
                        domain_version, policy_snapshot, source_token_count,
                        token_estimator, token_estimator_version, overfill_tokens,
                        overfill_ratio, episode_result_recorded, context_revision_id,
                        attempt_count, last_failure_stage, last_failure_code,
                        last_failure_at_ms, last_error_summary, next_retry_at_ms
                    ) VALUES (
                        %(window_id)s, %(user_name)s, %(project_id)s, %(origin)s,
                        %(stage)s, %(domain_version)s, %(policy_snapshot)s,
                        %(source_token_count)s, %(token_estimator)s,
                        %(token_estimator_version)s, %(overfill_tokens)s,
                        %(overfill_ratio)s, %(episode_result_recorded)s,
                        %(context_revision_id)s, %(attempt_count)s,
                        %(last_failure_stage)s, %(last_failure_code)s,
                        %(last_failure_at_ms)s, %(last_error_summary)s,
                        %(next_retry_at_ms)s
                    )
                    """,
                    self._record_params(window),
                )
                for member in members:
                    await cur.execute(
                        """
                        INSERT INTO public.project_semantic_window_messages (
                            window_id, project_id, message_id, session_id,
                            exchange_user_message_id, role, ordinal
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            window.window_id,
                            window.project_id,
                            member.message_id,
                            member.session_id,
                            member.exchange_user_message_id,
                            member.role,
                            member.ordinal,
                        ),
                    )
                return SemanticWindowClaimResult(window=window, claimed=True)
        except PsycopgError as exc:
            self._raise_storage_write("claim_window", exc)

    async def advance_stage(
        self,
        *,
        window_id: UUID | str,
        user_name: str,
        project_id: str,
        expected_stage: SemanticWindowStage,
        next_stage: SemanticWindowStage,
        context_revision_id: UUID | str | None = None,
    ) -> bool:
        """Compare-and-set one forward checkpoint; stale writers receive ``False``."""

        user_name = require_scope_value(user_name, "user_name", "advance_stage")
        project_id = require_scope_value(project_id, "project_id", "advance_stage")
        window_id = self._uuid(window_id, "window_id")
        self._validate_transition(expected_stage, next_stage)
        if context_revision_id is not None:
            context_revision_id = self._uuid(
                context_revision_id, "context_revision_id"
            )
        if next_stage is SemanticWindowStage.CONTEXT_COMMITTED and context_revision_id is None:
            raise ValueError("context_committed requires context_revision_id")
        if next_stage is not SemanticWindowStage.CONTEXT_COMMITTED and context_revision_id is not None:
            raise ValueError("context_revision_id may only be set at context_committed")
        try:
            async with self.client.transaction() as cur:
                await cur.execute(
                    """
                    UPDATE public.project_semantic_windows
                    SET stage = %s,
                        context_revision_id = CASE
                            WHEN %s::uuid IS NULL THEN context_revision_id
                            ELSE %s::uuid
                        END,
                        completed_at = CASE
                            WHEN %s = 'completed' THEN NOW()
                            ELSE completed_at
                        END,
                        last_failure_stage = NULL,
                        last_failure_code = NULL,
                        last_failure_at_ms = NULL,
                        last_error_summary = NULL,
                        next_retry_at_ms = NULL,
                        updated_at = NOW()
                    WHERE window_id = %s
                      AND user_name = %s
                      AND project_id = %s
                      AND stage = %s
                    RETURNING window_id
                    """,
                    (
                        next_stage.value,
                        context_revision_id,
                        context_revision_id,
                        next_stage.value,
                        window_id,
                        user_name,
                        project_id,
                        expected_stage.value,
                    ),
                )
                return await cur.fetchone() is not None
        except PsycopgError as exc:
            self._raise_storage_write("advance_stage", exc)

    async def record_failure(
        self,
        *,
        window_id: UUID | str,
        user_name: str,
        project_id: str,
        expected_stage: SemanticWindowStage,
        failure_stage: str,
        failure_code: str,
        error_summary: str,
        failed_at_ms: int,
        next_retry_at_ms: int | None,
    ) -> SemanticWindowRecord | None:
        """Record a retryable failure without mutating the successful checkpoint."""

        user_name = require_scope_value(user_name, "user_name", "record_failure")
        project_id = require_scope_value(project_id, "project_id", "record_failure")
        window_id = self._uuid(window_id, "window_id")
        failure_stage = self._text(failure_stage, "failure_stage")
        failure_code = self._text(failure_code, "failure_code")
        error_summary = self._text(error_summary, "error_summary")
        if not isinstance(failed_at_ms, int) or isinstance(failed_at_ms, bool) or failed_at_ms < 0:
            raise ValueError("failed_at_ms must be a non-negative integer")
        if (
            next_retry_at_ms is not None
            and (
                not isinstance(next_retry_at_ms, int)
                or isinstance(next_retry_at_ms, bool)
                or next_retry_at_ms < failed_at_ms
            )
        ):
            raise ValueError("next_retry_at_ms must be null or >= failed_at_ms")
        try:
            async with self.client.transaction() as cur:
                await cur.execute(
                    f"""
                    UPDATE public.project_semantic_windows
                    SET attempt_count = attempt_count + 1,
                        last_failure_stage = %s,
                        last_failure_code = %s,
                        last_failure_at_ms = %s,
                        last_error_summary = %s,
                        next_retry_at_ms = %s,
                        updated_at = NOW()
                    WHERE window_id = %s
                      AND user_name = %s
                      AND project_id = %s
                      AND stage = %s
                    RETURNING {_WINDOW_COLUMNS}
                    """,
                    (
                        failure_stage,
                        failure_code,
                        failed_at_ms,
                        error_summary,
                        next_retry_at_ms,
                        window_id,
                        user_name,
                        project_id,
                        expected_stage.value,
                    ),
                )
                row = await cur.fetchone()
                return None if row is None else SemanticWindowRecord.model_validate(row)
        except PsycopgError as exc:
            self._raise_storage_write("record_failure", exc)

    async def retry_window(
        self,
        *,
        window_id: UUID | str,
        user_name: str,
        project_id: str,
    ) -> SemanticWindowRecord | None:
        """Reset one failed active window for an explicit operator retry.

        Membership, policy, stage, and every successful checkpoint remain
        untouched.  Resetting the bounded automatic-attempt counter is what
        makes an exhausted window eligible for a new bounded retry cycle.
        """

        user_name = require_scope_value(user_name, "user_name", "retry_window")
        project_id = require_scope_value(project_id, "project_id", "retry_window")
        window_id = self._uuid(window_id, "window_id")
        try:
            async with self.client.transaction() as cur:
                await cur.execute(
                    f"""
                    UPDATE public.project_semantic_windows
                    SET attempt_count = 0,
                        last_failure_stage = NULL,
                        last_failure_code = NULL,
                        last_failure_at_ms = NULL,
                        last_error_summary = NULL,
                        next_retry_at_ms = NULL,
                        updated_at = NOW()
                    WHERE window_id = %s
                      AND user_name = %s
                      AND project_id = %s
                      AND stage <> 'completed'
                      AND last_failure_at_ms IS NOT NULL
                    RETURNING {_WINDOW_COLUMNS}
                    """,
                    (window_id, user_name, project_id),
                )
                row = await cur.fetchone()
                return None if row is None else SemanticWindowRecord.model_validate(row)
        except PsycopgError as exc:
            self._raise_storage_write("retry_window", exc)

    @staticmethod
    def _validate_membership(
        window: SemanticWindowRecord, members: list[SemanticWindowMessage]
    ) -> None:
        if window.origin is SemanticWindowOrigin.CONVERSATION and not members:
            raise ValueError("conversation semantic windows require message membership")
        if window.origin is SemanticWindowOrigin.HUMAN_EDIT and members:
            raise ValueError("human-edit semantic windows must not fabricate message membership")
        message_ids = [member.message_id for member in members]
        ordinals = [member.ordinal for member in members]
        if len(message_ids) != len(set(message_ids)):
            raise ValueError("semantic window membership cannot repeat a message")
        if sorted(ordinals) != list(range(len(members))):
            raise ValueError("semantic window membership ordinals must be contiguous")
        if window.origin is SemanticWindowOrigin.CONVERSATION:
            exchanges: dict[int, list[SemanticWindowMessage]] = {}
            for member in members:
                exchanges.setdefault(member.exchange_user_message_id, []).append(member)
            for exchange_id, exchange_members in exchanges.items():
                users = [
                    member
                    for member in exchange_members
                    if member.role == "user" and member.message_id == exchange_id
                ]
                assistants = [
                    member for member in exchange_members if member.role == "assistant"
                ]
                if len(users) != 1 or len(assistants) > 1:
                    raise ValueError(
                        "conversation membership must contain one user and at most "
                        "one assistant per exchange"
                    )

    @staticmethod
    async def _validate_durable_conversation_membership(
        cur,
        *,
        window: SemanticWindowRecord,
        members: list[SemanticWindowMessage],
    ) -> None:
        """Re-check a selected exchange under the project claim lock."""

        if window.origin is SemanticWindowOrigin.HUMAN_EDIT:
            return
        message_ids = [member.message_id for member in members]
        await cur.execute(
            """
            SELECT message_id, session_id, role, user_msg_id, lifecycle_state,
                   exchange_state, exchange_outcome
            FROM public.messages
            WHERE project_id = %s
              AND user_name = %s
              AND message_id = ANY(%s)
            FOR UPDATE
            """,
            (window.project_id, window.user_name, message_ids),
        )
        rows = {int(row["message_id"]): row for row in await cur.fetchall()}
        if len(rows) != len(members):
            raise ValueError("Semantic window membership contains unavailable messages")
        for member in members:
            row = rows.get(member.message_id)
            if (
                row is None
                or str(row["session_id"]) != member.session_id
                or row["role"] != member.role
            ):
                raise ValueError("Semantic window membership no longer matches messages")
            if member.role == "user":
                if (
                    row["exchange_state"] != "closed"
                    or row["lifecycle_state"] != "sealed"
                    or row["exchange_outcome"] is None
                ):
                    raise ValueError("Only sealed closed user exchanges are admissible")
            elif int(row["user_msg_id"] or 0) != member.exchange_user_message_id:
                raise ValueError("Assistant membership is linked to the wrong exchange")

    @staticmethod
    def _validate_transition(
        expected_stage: SemanticWindowStage, next_stage: SemanticWindowStage
    ) -> None:
        if not isinstance(expected_stage, SemanticWindowStage) or not isinstance(
            next_stage, SemanticWindowStage
        ):
            raise TypeError("semantic window stages must use SemanticWindowStage")
        if _STAGE_SUCCESSOR.get(expected_stage) is not next_stage:
            raise ValueError("semantic window stage transitions must advance exactly once")

    @staticmethod
    def _record_params(window: SemanticWindowRecord) -> dict[str, object]:
        params = {
            key: (value.value if hasattr(value, "value") else value)
            for key, value in window.model_dump().items()
        }
        params["policy_snapshot"] = json.dumps(window.policy_snapshot)
        return params

    @staticmethod
    def _uuid(value: UUID | str, field: str) -> UUID:
        try:
            return value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError(f"{field} must be a UUID") from exc

    @staticmethod
    def _text(value: object, field: str) -> str:
        if not isinstance(value, str) or not (value := value.strip()):
            raise ValueError(f"{field} must be a non-blank string")
        return value

    @staticmethod
    def _raise_storage_write(operation: str, exc: Exception) -> None:
        raise StorageWriteError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc
