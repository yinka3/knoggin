"""Durable lifecycle for user-message revisions and ingestion admission."""

from __future__ import annotations

import hashlib
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


@dataclass(frozen=True, slots=True)
class ExchangeClosure:
    """The durable terminal state of one canonical user exchange."""

    user_message_id: int
    outcome: str
    closed_at_ms: int
    assistant_message_id: int | None = None
    already_closed: bool = False


@dataclass(frozen=True, slots=True)
class IngestionFrontier:
    """The last durable message included in a stable maintenance boundary."""

    project_id: str
    message_id: int
    timestamp_ms: int | None
    token: str


class MessageLifecycleWriter:
    """Keep message editing and ingestion eligibility in Postgres.

    Local worker signaling may wake a worker, but never decides which message
    version is ingested.
    """

    def __init__(self, client: PostgresClient, message_writer: MessageWriter):
        self.client = client
        self.message_writer = message_writer

    @staticmethod
    def _now_ms() -> int:
        return int(time() * 1000)

    async def get_stable_ingestion_frontier(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> IngestionFrontier | None:
        """Return a frontier only when no live ingestion work can overtake it.

        ``failed`` is terminal for frontier purposes.  An explicit retry moves
        a message back to ``ready`` and therefore correctly makes the frontier
        unavailable until that retry settles.
        """

        row = await self.client.fetch_one(
            """
            SELECT
                count(*) FILTER (WHERE ingestion_state IN
                    ('waiting_for_seal', 'ready', 'claimed')) AS pending_count,
                COALESCE(max(message_id) FILTER (WHERE ingestion_state IN
                    ('processed', 'failed', 'excluded')), 0) AS frontier_message_id,
                max(timestamp_ms) FILTER (WHERE ingestion_state IN
                    ('processed', 'failed', 'excluded')) AS frontier_timestamp_ms
            FROM public.messages
            WHERE user_name = %s AND project_id = %s
              AND role = 'user' AND lifecycle_state <> 'superseded'
            """,
            (user_name, project_id),
        )
        if row is None or int(row["pending_count"] or 0):
            return None
        message_id = int(row["frontier_message_id"] or 0)
        timestamp_ms = row["frontier_timestamp_ms"]
        token = hashlib.sha256(
            f"{message_id}:{timestamp_ms if timestamp_ms is not None else ''}".encode()
        ).hexdigest()
        return IngestionFrontier(project_id, message_id, timestamp_ms, token)

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

    async def prepare_assistant_exchange_finalization(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        user_message_id: int,
        cur,
    ) -> ExchangeClosure | None:
        """Lock one user exchange before an atomic assistant finalization.

        A previous successful finalization is returned instead of writing a
        second assistant message.  A different terminal outcome is never
        overwritten: callers must preserve the first durable evidence.
        """

        await cur.execute(
            """
            SELECT message_id, exchange_state, exchange_outcome,
                   exchange_closed_at_ms
            FROM public.messages
            WHERE user_name = %s
              AND project_id = %s
              AND session_id = %s
              AND message_id = %s
              AND role = 'user'
            FOR UPDATE
            """,
            (user_name, project_id, session_id, user_message_id),
        )
        user_row = await cur.fetchone()
        if user_row is None:
            raise ValueError("Cannot finalize an unavailable user exchange")
        if user_row["exchange_state"] == "open":
            return None
        if user_row["exchange_outcome"] != "assistant_final":
            raise ValueError(
                "Cannot add an assistant response to an exchange already closed as "
                f"{user_row['exchange_outcome']}"
            )

        await cur.execute(
            """
            SELECT message_id
            FROM public.messages
            WHERE user_name = %s
              AND project_id = %s
              AND session_id = %s
              AND user_msg_id = %s
              AND role = 'assistant'
            """,
            (user_name, project_id, session_id, user_message_id),
        )
        assistant_row = await cur.fetchone()
        if assistant_row is None:
            raise RuntimeError("Closed assistant exchange is missing its assistant row")
        return ExchangeClosure(
            user_message_id=user_message_id,
            outcome="assistant_final",
            closed_at_ms=int(user_row["exchange_closed_at_ms"]),
            assistant_message_id=int(assistant_row["message_id"]),
            already_closed=True,
        )

    async def close_user_exchange(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        user_message_id: int,
        outcome: str,
        closed_at_ms: int | None = None,
        cur=None,
    ) -> ExchangeClosure:
        """Seal and close a user exchange exactly once.

        Closing also makes a formerly editable user revision eligible for the
        legacy ingestion worker during the transition.  It deliberately does
        not alter an already claimed or processed legacy row.
        """

        valid_outcomes = {
            "assistant_final",
            "clarification",
            "failed",
            "cancelled",
            "user_only",
        }
        if outcome not in valid_outcomes:
            raise ValueError("Invalid exchange outcome")
        closed_at_ms = self._now_ms() if closed_at_ms is None else closed_at_ms
        if (
            not isinstance(closed_at_ms, int)
            or isinstance(closed_at_ms, bool)
            or closed_at_ms < 0
        ):
            raise ValueError("closed_at_ms must be a non-negative integer")

        if cur is None:
            async with self.client.transaction() as transaction_cursor:
                return await self.close_user_exchange(
                    user_name=user_name,
                    project_id=project_id,
                    session_id=session_id,
                    user_message_id=user_message_id,
                    outcome=outcome,
                    closed_at_ms=closed_at_ms,
                    cur=transaction_cursor,
                )

        await cur.execute(
            """
            SELECT message_id, exchange_state, exchange_outcome,
                   exchange_closed_at_ms
            FROM public.messages
            WHERE user_name = %s
              AND project_id = %s
              AND session_id = %s
              AND message_id = %s
              AND role = 'user'
            FOR UPDATE
            """,
            (user_name, project_id, session_id, user_message_id),
        )
        row = await cur.fetchone()
        if row is None:
            raise ValueError("Cannot close an unavailable user exchange")
        if row["exchange_state"] == "closed":
            if row["exchange_outcome"] != outcome:
                raise ValueError(
                    "User exchange is already closed as "
                    f"{row['exchange_outcome']}, not {outcome}"
                )
            assistant_message_id = None
            if outcome == "assistant_final":
                await cur.execute(
                    """
                    SELECT message_id
                    FROM public.messages
                    WHERE user_name = %s
                      AND project_id = %s
                      AND session_id = %s
                      AND user_msg_id = %s
                      AND role = 'assistant'
                    """,
                    (user_name, project_id, session_id, user_message_id),
                )
                assistant = await cur.fetchone()
                if assistant is None:
                    raise RuntimeError(
                        "Closed assistant exchange is missing its assistant row"
                    )
                assistant_message_id = int(assistant["message_id"])
            return ExchangeClosure(
                user_message_id=user_message_id,
                outcome=outcome,
                closed_at_ms=int(row["exchange_closed_at_ms"]),
                assistant_message_id=assistant_message_id,
                already_closed=True,
            )

        await cur.execute(
            """
            UPDATE public.messages
            SET lifecycle_state = 'sealed',
                editable_until_ms = NULL,
                sealed_at_ms = COALESCE(sealed_at_ms, %s),
                ingestion_state = CASE
                    WHEN ingestion_state = 'waiting_for_seal' THEN 'ready'
                    ELSE ingestion_state
                END,
                ingestion_not_before_ms = CASE
                    WHEN ingestion_state = 'waiting_for_seal' THEN %s
                    ELSE ingestion_not_before_ms
                END,
                exchange_state = 'closed',
                exchange_outcome = %s,
                exchange_closed_at_ms = %s
            WHERE user_name = %s
              AND project_id = %s
              AND session_id = %s
              AND message_id = %s
              AND role = 'user'
              AND exchange_state = 'open'
            RETURNING message_id
            """,
            (
                closed_at_ms,
                closed_at_ms,
                outcome,
                closed_at_ms,
                user_name,
                project_id,
                session_id,
                user_message_id,
            ),
        )
        if await cur.fetchone() is None:
            raise RuntimeError("User exchange closure was lost")
        return ExchangeClosure(
            user_message_id=user_message_id,
            outcome=outcome,
            closed_at_ms=closed_at_ms,
        )

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
    ) -> List[int]:
        """Seal expired editable messages and make them immediately claimable."""

        now_ms = self._now_ms()
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.messages AS message
                SET lifecycle_state = 'sealed',
                    sealed_at_ms = %s,
                    editable_until_ms = NULL,
                    ingestion_state = 'ready',
                    ingestion_not_before_ms = %s
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
                    now_ms,
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
        """Claim the earliest contiguous ready FIFO batch, or nothing.

        A preceding editable message or live claim prevents later messages from
        overtaking it. Terminally failed messages are excluded so an explicit
        retry does not starve later ready work.
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
                  AND message.ingestion_state IN ('waiting_for_seal', 'ready', 'claimed')
                  AND message.lifecycle_state <> 'superseded'
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
        """Record one bounded failure and return whether it is terminal."""

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

            terminal = (not retryable) or any(
                int(row["ingestion_attempt_count"]) + 1 >= max_attempts
                for row in rows
            )
            state = "failed" if terminal else "ready"
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
            return terminal

    async def retry_failed_ingestion(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        message_ids: List[int],
    ) -> List[int]:
        """Explicitly make selected failed messages eligible for a fresh run."""

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
                  AND ingestion_state = 'failed'
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
    ) -> None:
        await self._set_claim_state(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            batch_id=batch_id,
            state="ready",
        )

    async def _set_claim_state(
        self, *, user_name: str, project_id: str, session_id: str, batch_id: str, state: str
    ) -> None:
        if state != "ready":
            raise ValueError("Ingestion claims may only be released to ready")
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
