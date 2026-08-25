"""Durable lifecycle for user-message revisions and ingestion admission."""

from __future__ import annotations

from dataclasses import dataclass
from time import time
from typing import Any, Dict, List
from uuid import uuid4

from core.knowledge.db.writers.message_writer import MessageWriter
from infrastructure.postgres_client import PostgresClient


@dataclass(frozen=True, slots=True)
class IngestionClaim:
    """One settled FIFO batch claimed from canonical message storage."""

    batch_id: str
    messages: List[Dict[str, Any]]


@dataclass(frozen=True, slots=True)
class MessageAcceptance:
    """The canonical result of accepting a user-message request."""

    message_id: int
    created: bool


class MessageLifecycleWriter:
    """Keep message editing and ingestion eligibility in Postgres.

    Local worker signaling may wake a consumer, but never decides which message
    version is ingested.
    """

    def __init__(self, client: PostgresClient, message_writer: MessageWriter):
        self.client = client
        self.message_writer = message_writer

    @staticmethod
    def _now_ms() -> int:
        return int(time() * 1000)

    async def create_editable_user_message(
        self, message: Dict[str, Any], *, edit_window_seconds: int
    ) -> MessageAcceptance:
        now_ms = self._now_ms()
        row = {
            **message,
            "lifecycle_state": "editable",
            "editable_until_ms": now_ms + (edit_window_seconds * 1000),
            "sealed_at_ms": None,
            "selected_revision": 1,
            "ingestion_state": "waiting_for_seal",
            "ingestion_not_before_ms": None,
            "episode_eligible": False,
        }
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT 1
                FROM public.sessions
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND status = 'open'
                FOR KEY SHARE
                """,
                (row["user_name"], row["project_id"], row["session_id"]),
            )
            if await cur.fetchone() is None:
                raise ValueError("Cannot create a message in a deleted session")
            inserted_id = await self.message_writer.accept_editable_user_message(
                row, cur=cur
            )
            if inserted_id is None:
                await cur.execute(
                    """
                    SELECT message_id
                    FROM public.messages
                    WHERE user_name = %s
                      AND session_id = %s
                      AND acceptance_key = %s
                    """,
                    (
                        row["user_name"],
                        row["session_id"],
                        row["acceptance_key"],
                    ),
                )
                accepted = await cur.fetchone()
                if accepted is None:
                    raise RuntimeError("Accepted user message could not be reloaded")
                return MessageAcceptance(
                    message_id=int(accepted["message_id"]),
                    created=False,
                )
            await cur.execute(
                """
                INSERT INTO public.message_revisions (
                    user_name, session_id, project_id, message_id,
                    revision, content, created_at_ms
                ) VALUES (%s, %s, %s, %s, 1, %s, %s)
                ON CONFLICT (user_name, session_id, message_id, revision) DO NOTHING
                """,
                (
                    row["user_name"],
                    row["session_id"],
                    row["project_id"],
                    inserted_id,
                    row["content"],
                    now_ms,
                ),
            )
        return MessageAcceptance(message_id=inserted_id, created=True)

    async def edit_user_message(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        message_id: int,
        content: str,
    ) -> int:
        """Append and select a revision before the immutable seal deadline."""

        now_ms = self._now_ms()
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.messages AS message
                SET content = %s,
                    selected_revision = selected_revision + 1
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND message_id = %s
                  AND role = 'user'
                  AND lifecycle_state = 'editable'
                  AND editable_until_ms > %s
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.user_name = message.user_name
                        AND session.project_id = message.project_id
                        AND session.session_id = message.session_id
                        AND session.status = 'open'
                  )
                RETURNING selected_revision
                """,
                (
                    content,
                    user_name,
                    project_id,
                    session_id,
                    message_id,
                    now_ms,
                ),
            )
            updated = await cur.fetchone()
            if updated is None:
                raise ValueError("Message is no longer editable")
            revision = int(updated["selected_revision"])
            await cur.execute(
                """
                INSERT INTO public.message_revisions (
                    user_name, session_id, project_id, message_id,
                    revision, content, created_at_ms
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    user_name,
                    session_id,
                    project_id,
                    message_id,
                    revision,
                    content,
                    now_ms,
                ),
            )
        return revision

    async def select_user_message_revision(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        message_id: int,
        revision: int,
    ) -> str:
        """Choose an already-saved draft version without creating another one."""

        now_ms = self._now_ms()
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.messages AS message
                SET content = revision_row.content,
                    selected_revision = revision_row.revision
                FROM public.message_revisions AS revision_row
                WHERE message.user_name = %s
                  AND message.project_id = %s
                  AND message.session_id = %s
                  AND message.message_id = %s
                  AND message.role = 'user'
                  AND message.lifecycle_state = 'editable'
                  AND message.editable_until_ms > %s
                  AND revision_row.user_name = message.user_name
                  AND revision_row.session_id = message.session_id
                  AND revision_row.project_id = message.project_id
                  AND revision_row.message_id = message.message_id
                  AND revision_row.revision = %s
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.user_name = message.user_name
                        AND session.project_id = message.project_id
                        AND session.session_id = message.session_id
                        AND session.status = 'open'
                  )
                RETURNING message.content
                """,
                (
                    user_name,
                    project_id,
                    session_id,
                    message_id,
                    now_ms,
                    revision,
                ),
            )
            updated = await cur.fetchone()
            if updated is None:
                raise ValueError("Message revision is not selectable")
            return str(updated["content"])

    async def seal_due_user_messages(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        settle_delay_seconds: float,
    ) -> List[int]:
        """Seal only expired editable messages and start their settle period."""

        now_ms = self._now_ms()
        not_before_ms = now_ms + int(settle_delay_seconds * 1000)
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.messages AS message
                SET lifecycle_state = 'sealed',
                    sealed_at_ms = %s,
                    editable_until_ms = NULL,
                    ingestion_state = 'ready',
                    ingestion_not_before_ms = %s,
                    episode_eligible = FALSE
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND role = 'user'
                  AND lifecycle_state = 'editable'
                  AND editable_until_ms <= %s
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.user_name = message.user_name
                        AND session.project_id = message.project_id
                        AND session.session_id = message.session_id
                        AND session.status = 'open'
                  )
                RETURNING message_id
                """,
                (
                    now_ms,
                    not_before_ms,
                    user_name,
                    project_id,
                    session_id,
                    now_ms,
                ),
            )
            return [int(row["message_id"]) for row in await cur.fetchall()]

    async def reset_claimed_ingestion(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> List[int]:
        """Release claims left by an earlier runtime for this session only."""

        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.messages AS message
                SET ingestion_state = 'ready',
                    ingestion_claim_id = NULL,
                    ingestion_claimed_at_ms = NULL
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND role = 'user'
                  AND ingestion_state = 'claimed'
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.user_name = message.user_name
                        AND session.project_id = message.project_id
                        AND session.session_id = message.session_id
                        AND session.status = 'open'
                  )
                RETURNING message_id
                """,
                (user_name, project_id, session_id),
            )
            return [int(row["message_id"]) for row in await cur.fetchall()]

    async def claim_next_batch(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        batch_size: int,
    ) -> IngestionClaim | None:
        """Claim the earliest contiguous, settled FIFO batch, or nothing.

        A preceding editable, blocked, or live claim deliberately prevents later
        messages from overtaking it. This preserves the meaning of a session.
        """

        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        now_ms = self._now_ms()
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT message_id, content, timestamp_ms, role, ingestion_state,
                       ingestion_not_before_ms
                FROM public.messages AS message
                JOIN public.sessions AS session
                  ON session.user_name = message.user_name
                 AND session.project_id = message.project_id
                 AND session.session_id = message.session_id
                WHERE message.user_name = %s
                  AND message.project_id = %s
                  AND message.session_id = %s
                  AND session.status = 'open'
                  AND message.role = 'user'
                  AND message.ingestion_state <> 'excluded'
                  AND message.lifecycle_state <> 'superseded'
                  AND message.ingestion_state <> 'processed'
                ORDER BY message.message_id
                LIMIT %s
                FOR UPDATE
                """,
                (user_name, project_id, session_id, batch_size),
            )
            rows = await cur.fetchall()
            if not rows:
                return None
            if any(
                row["ingestion_state"] != "ready"
                or row["ingestion_not_before_ms"] is None
                or int(row["ingestion_not_before_ms"]) > now_ms
                for row in rows
            ):
                return None

            batch_id = str(uuid4())
            message_ids = [int(row["message_id"]) for row in rows]
            await cur.execute(
                """
                UPDATE public.messages AS message
                SET ingestion_state = 'claimed',
                    ingestion_claim_id = %s,
                    ingestion_claimed_at_ms = %s
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND message_id = ANY(%s)
                  AND ingestion_state = 'ready'
                """,
                (
                    batch_id,
                    now_ms,
                    user_name,
                    project_id,
                    session_id,
                    message_ids,
                ),
            )
            if cur.rowcount != len(message_ids):
                raise RuntimeError("Ingestion batch claim was lost")

        return IngestionClaim(
            batch_id=batch_id,
            messages=[
                {
                    "id": int(row["message_id"]),
                    "message": row["content"],
                    "timestamp": row["timestamp_ms"],
                    "role": row["role"],
                }
                for row in rows
            ],
        )

    async def fail_ingestion_claim(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        batch_id: str,
        failure_stage: str,
        failure_code: str,
        error_summary: str,
        retryable: bool,
        max_attempts: int,
    ) -> bool:
        """Record one bounded failure and return whether the batch is blocked."""

        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        stage = str(failure_stage or "ingestion").strip()[:100]
        code = str(failure_code or "ingestion_failure").strip()[:100]
        summary = " ".join(str(error_summary or "").split())[:1_000]
        if not stage or not code or not summary:
            raise ValueError("Ingestion failure metadata must be non-blank")

        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT message_id, ingestion_attempt_count
                FROM public.messages AS message
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND role = 'user'
                  AND ingestion_state = 'claimed'
                  AND ingestion_claim_id = %s
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.user_name = message.user_name
                        AND session.project_id = message.project_id
                        AND session.session_id = message.session_id
                        AND session.status = 'open'
                  )
                FOR UPDATE
                """,
                (user_name, project_id, session_id, batch_id),
            )
            rows = await cur.fetchall()
            if not rows:
                raise RuntimeError("Ingestion claim no longer belongs to this worker")

            blocked = (not retryable) or any(
                int(row["ingestion_attempt_count"]) + 1 >= max_attempts
                for row in rows
            )
            state = "blocked" if blocked else "ready"
            message_ids = [int(row["message_id"]) for row in rows]
            await cur.execute(
                """
                UPDATE public.messages
                SET ingestion_state = %s,
                    ingestion_claim_id = NULL,
                    ingestion_claimed_at_ms = NULL,
                    ingestion_attempt_count = ingestion_attempt_count + 1,
                    ingestion_last_failure_stage = %s,
                    ingestion_last_failure_code = %s,
                    ingestion_last_failure_at_ms = %s,
                    ingestion_last_error_summary = %s
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND message_id = ANY(%s)
                  AND ingestion_state = 'claimed'
                  AND ingestion_claim_id = %s
                """,
                (
                    state,
                    stage,
                    code,
                    self._now_ms(),
                    summary,
                    user_name,
                    project_id,
                    session_id,
                    message_ids,
                    batch_id,
                ),
            )
            if cur.rowcount != len(message_ids):
                raise RuntimeError("Ingestion claim no longer belongs to this worker")
            return blocked

    async def retry_blocked_ingestion(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        message_ids: List[int],
    ) -> List[int]:
        """Explicitly make selected blocked messages eligible for a fresh run."""

        ids = sorted({int(message_id) for message_id in message_ids})
        if not ids or any(message_id <= 0 for message_id in ids):
            raise ValueError("message_ids must contain positive message IDs")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.messages AS message
                SET ingestion_state = 'ready',
                    ingestion_claim_id = NULL,
                    ingestion_claimed_at_ms = NULL,
                    ingestion_not_before_ms = %s
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND role = 'user'
                  AND message_id = ANY(%s)
                  AND ingestion_state = 'blocked'
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.user_name = message.user_name
                        AND session.project_id = message.project_id
                        AND session.session_id = message.session_id
                        AND session.status = 'open'
                  )
                RETURNING message_id
                """,
                (self._now_ms(), user_name, project_id, session_id, ids),
            )
            return [int(row["message_id"]) for row in await cur.fetchall()]

    async def release_ingestion_claim(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        batch_id: str,
        blocked: bool,
    ) -> None:
        await self._set_claim_state(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            batch_id=batch_id,
            state="blocked" if blocked else "ready",
        )

    async def _set_claim_state(
        self, *, user_name: str, project_id: str, session_id: str, batch_id: str, state: str
    ) -> None:
        if state not in {"ready", "blocked"}:
            raise ValueError("Ingestion claims may only be released or blocked")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.messages AS message
                SET ingestion_state = %s,
                    ingestion_claim_id = NULL,
                    ingestion_claimed_at_ms = NULL
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND ingestion_state = 'claimed'
                  AND ingestion_claim_id = %s
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.user_name = message.user_name
                        AND session.project_id = message.project_id
                        AND session.session_id = message.session_id
                        AND session.status = 'open'
                  )
                RETURNING message_id
                """,
                (state, user_name, project_id, session_id, batch_id),
            )
            if not await cur.fetchall():
                raise RuntimeError("Ingestion claim no longer belongs to this worker")
