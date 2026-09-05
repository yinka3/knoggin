"""Durable lifecycle for user-message revisions and ingestion admission."""

from __future__ import annotations

from dataclasses import dataclass
from time import time
from typing import Any, Dict

from core.knowledge.db.writers.message_writer import MessageWriter
from infrastructure.postgres_client import PostgresClient


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


class MessageLifecycleWriter:
    """Keep message editing and terminal exchange closure in Postgres."""

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

        Closing makes the exchange eligible for project-level semantic-window
        admission. The message itself carries no processing state.
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
