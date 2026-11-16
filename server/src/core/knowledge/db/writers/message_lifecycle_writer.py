"""Durable lifecycle for user-message revisions and ingestion admission."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict
from uuid import UUID, uuid4

from common.exceptions import IdempotencyConflictError
from common.scoping import require_scope_value
from common.utils.time_utils import get_now_ms
from core.knowledge.db.writers.message_writer import MessageWriter
from infrastructure.postgres_client import PostgresClient

_ASSISTANT_EXCHANGE_OUTCOMES = frozenset({"assistant_final", "clarification"})


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
        return get_now_ms()

    async def forget_messages(
        self,
        *,
        user_name: str,
        project_id: str,
        values: tuple[str, ...],
        message_ids: tuple[int, ...],
        block_ids: tuple[UUID, ...],
    ) -> list[int]:
        """Mark every project message containing one of ``values`` (ignoring
        case), the given messages, and every message that supported one of
        ``block_ids`` or a version it replaced, forgotten with their whole
        exchanges; return what was newly forgotten.

        A reply can repeat what its question said, so forgetting either half
        forgets both. Repeating a request changes nothing.
        """

        if not values and not message_ids and not block_ids:
            return []
        if not message_ids:
            raise ValueError("Forgetting requires a request message")
        async with self.client.transaction() as cur:
            await cur.execute(
                """INSERT INTO project_forget_records (
                       forget_id, project_id, user_name, request_message_ids, values, block_ids
                   ) SELECT %s, %s, %s, %s, %s, %s
                   WHERE EXISTS (SELECT 1 FROM projects WHERE project_id = %s AND user_name = %s)
                   ON CONFLICT (project_id, request_message_ids) DO NOTHING
                   RETURNING forget_id""",
                (uuid4(), project_id, user_name, sorted(set(message_ids)), list(values),
                 list(block_ids), project_id, user_name),
            )
            record = await cur.fetchone()
            if record is None:
                return []
            await cur.execute(
                """
                WITH RECURSIVE versions AS (
                    SELECT block.block_id, block.supersedes_block_id
                    FROM public.project_context_blocks AS block
                    WHERE block.project_id = %(project_id)s
                      AND block.block_id = ANY(%(block_ids)s::uuid[])
                    UNION
                    SELECT older.block_id, older.supersedes_block_id
                    FROM public.project_context_blocks AS older
                    JOIN versions ON older.block_id = versions.supersedes_block_id
                    WHERE older.project_id = %(project_id)s
                ),
                named AS (
                    SELECT message.message_id
                    FROM public.messages AS message
                    WHERE message.project_id = %(project_id)s
                      AND message.user_name = %(user_name)s
                      AND message.lifecycle_state = 'sealed'
                      AND EXISTS (
                          SELECT 1 FROM unnest(%(values)s::text[]) AS value
                          WHERE strpos(lower(message.content), lower(value)) > 0
                      )
                    UNION
                    SELECT unnest(%(message_ids)s::bigint[])
                    UNION
                    SELECT support.message_id
                    FROM public.project_context_block_supports AS support
                    JOIN versions USING (block_id)
                    WHERE support.project_id = %(project_id)s
                ),
                exchange AS (
                    SELECT COALESCE(message.user_msg_id, message.message_id) AS user_id
                    FROM public.messages AS message
                    JOIN named USING (message_id)
                    WHERE message.project_id = %(project_id)s
                )
                UPDATE public.messages AS message
                SET lifecycle_state = 'forgotten', forgotten_at = NOW()
                WHERE message.project_id = %(project_id)s
                  AND message.user_name = %(user_name)s
                  AND message.lifecycle_state = 'sealed'
                  AND (
                      message.message_id IN (SELECT message_id FROM named)
                      OR message.message_id IN (SELECT user_id FROM exchange)
                      OR message.user_msg_id IN (SELECT user_id FROM exchange)
                  )
                RETURNING message.message_id
                """,
                {
                    "project_id": project_id,
                    "user_name": user_name,
                    "values": list(values),
                    "message_ids": list(message_ids),
                    "block_ids": list(block_ids),
                },
            )
            forgotten = [int(row["message_id"]) for row in await cur.fetchall()]
            if forgotten:
                await cur.execute(
                    """
                    DELETE FROM public.exchange_embeddings
                    WHERE project_id = %s AND user_message_id = ANY(%s)
                    """,
                    (project_id, forgotten),
                )
            await cur.execute(
                """UPDATE project_forget_records
                   SET message_ids = %s,
                       episode_ids = ARRAY(SELECT DISTINCT episode_id FROM episode_messages
                           WHERE project_id = %s AND message_id = ANY(%s) ORDER BY episode_id)
                   WHERE forget_id = %s""",
                (forgotten, project_id, forgotten, record["forget_id"]),
            )
        return forgotten

    async def complete_forget_cleanup(self, *, user_name: str, project_id: str) -> None:
        await self.client.execute(
            """UPDATE project_forget_records SET cleanup_completed_at = now()
               WHERE project_id = %s AND user_name = %s AND cleanup_completed_at IS NULL
                 AND episode_ids <@ rewritten_episode_ids""", (project_id, user_name),
        )

    async def record_run_steer(
        self, *, user_name: str, project_id: str, session_id: str,
        message_id: int, steer: dict[str, str], expected_state: str | None = None,
    ) -> bool:
        """Attach one steer to its open user exchange or advance its state."""
        scope = (*self._edit_scope(user_name, project_id, session_id, message_id), message_id)
        if expected_state is None:
            assignment = "jsonb_set(metadata, '{run_steer}', %s::jsonb, true)"
            condition = "NOT (metadata ? 'run_steer')"
            values = (json.dumps(steer), *scope)
        else:
            assignment = "jsonb_set(metadata, '{run_steer,state}', to_jsonb(%s::text), true)"
            condition = "metadata -> 'run_steer' ->> 'steer_id' = %s AND metadata -> 'run_steer' ->> 'state' = %s"
            values = (steer["state"], *scope, steer["steer_id"], expected_state)
        exchange_condition = (
            "" if steer["state"] == "undelivered" else
            "AND (exchange_state = 'open' OR "
            "(exchange_state = 'closed' AND exchange_outcome = 'user_only' "
            "AND metadata ->> 'origin' = 'steer'))"
        )
        async with self.client.transaction() as cur:
            await cur.execute(
                f"""
                UPDATE public.messages
                SET metadata = {assignment}
                WHERE user_name = %s AND project_id = %s AND session_id = %s
                  AND message_id = %s AND role = 'user'
                  {exchange_condition} AND {condition}
                RETURNING message_id
                """,
                values,
            )
            return await cur.fetchone() is not None

    async def mark_orphaned_run_steers_undelivered(
        self, *, user_name: str, project_id: str | None = None,
        session_id: str | None = None,
    ) -> int:
        """Close stale pending steers at startup or on session resume."""

        if (project_id is None) != (session_id is None):
            raise ValueError("Project and session scope must be supplied together")
        scope_sql = (
            "AND project_id = %s AND session_id = %s"
            if project_id is not None else ""
        )
        values = (user_name, project_id, session_id) if project_id is not None else (user_name,)
        async with self.client.transaction() as cur:
            await cur.execute(
                f"""UPDATE public.messages
                   SET metadata = jsonb_set(
                       metadata, '{{run_steer,state}}', '"undelivered"'::jsonb, true
                   )
                   WHERE user_name = %s {scope_sql}
                     AND role = 'user' AND exchange_state = 'closed'
                     AND exchange_outcome = 'user_only'
                     AND metadata ->> 'origin' = 'steer'
                     AND metadata -> 'run_steer' ->> 'state' = 'pending'
                   RETURNING message_id""",
                values,
            )
            return len(await cur.fetchall())

    async def create_closed_standalone_message(
        self, *, user_name: str, project_id: str, session_id: str,
        message_id: int, role: str, content: str, timestamp_ms: int,
        acceptance_key: str, metadata: dict[str, Any],
    ) -> MessageAcceptance:
        """Atomically accept one user-only or assistant-only recorded turn."""

        if role not in {"user", "assistant"} or not content.strip():
            raise ValueError("Standalone message needs a role and nonblank content")
        if not isinstance(timestamp_ms, int) or timestamp_ms < 0 or not acceptance_key:
            raise ValueError("Standalone message needs a timestamp and acceptance key")
        outcome = "user_only" if role == "user" else "assistant_only"
        async with self.client.transaction() as cur:
            await cur.execute(
                """SELECT 1 FROM public.sessions
                   WHERE user_name = %s AND project_id = %s AND session_id = %s
                     AND status = 'open' FOR KEY SHARE""",
                (user_name, project_id, session_id),
            )
            if await cur.fetchone() is None:
                raise ValueError("Cannot import a message into a deleted session")
            await cur.execute(
                """INSERT INTO public.messages (
                       user_name, session_id, message_id, project_id, role, content,
                       user_msg_id, metadata, acceptance_key, timestamp_ms,
                       lifecycle_state, sealed_at_ms, exchange_state,
                       exchange_outcome, exchange_closed_at_ms
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                             'sealed', %s, 'closed', %s, %s)
                   ON CONFLICT (user_name, session_id, acceptance_key)
                   WHERE acceptance_key IS NOT NULL DO NOTHING
                   RETURNING message_id""",
                (user_name, session_id, message_id, project_id, role, content,
                 message_id if role == "user" else None, json.dumps(metadata),
                 acceptance_key, timestamp_ms, timestamp_ms, outcome, timestamp_ms),
            )
            inserted = await cur.fetchone()
            if inserted is not None:
                return MessageAcceptance(message_id=int(inserted["message_id"]), created=True)
            await cur.execute(
                """SELECT message_id, project_id, role, content, timestamp_ms
                   FROM public.messages
                   WHERE user_name = %s AND session_id = %s AND acceptance_key = %s""",
                (user_name, session_id, acceptance_key),
            )
            existing = await cur.fetchone()
            if existing is None or (existing["project_id"], existing["role"],
                                    existing["content"], existing["timestamp_ms"]) != (
                                        project_id, role, content, timestamp_ms):
                raise IdempotencyConflictError()
            return MessageAcceptance(message_id=int(existing["message_id"]), created=False)

    async def create_editable_user_message(
        self, message: Dict[str, Any], *, edit_window_seconds: int
    ) -> MessageAcceptance:
        acceptance_key = message.get("acceptance_key")
        request_fingerprint = message.get("request_fingerprint")
        if not isinstance(acceptance_key, str) or not acceptance_key:
            raise ValueError("Editable user message requires acceptance_key")
        if acceptance_key.startswith("request:") and (
            not isinstance(request_fingerprint, str) or not request_fingerprint
        ):
            raise ValueError(
                "Request idempotency acceptance requires a request_fingerprint"
            )
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
                    SELECT message_id,
                           metadata ->> 'request_fingerprint' AS request_fingerprint
                    FROM public.messages
                    WHERE user_name = %s
                      AND project_id = %s
                      AND session_id = %s
                      AND acceptance_key = %s
                    """,
                    (
                        row["user_name"],
                        row["project_id"],
                        row["session_id"],
                        row["acceptance_key"],
                    ),
                )
                accepted = await cur.fetchone()
                if accepted is None:
                    raise RuntimeError("Accepted user message could not be reloaded")
                if (
                    row["acceptance_key"].startswith("request:")
                    and accepted.get("request_fingerprint")
                    != row["request_fingerprint"]
                ):
                    raise IdempotencyConflictError()
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
            await cur.execute(
                """
                UPDATE public.sessions SET last_active_at = now()
                WHERE user_name = %s AND project_id = %s AND session_id = %s
                """,
                (row["user_name"], row["project_id"], row["session_id"]),
            )
        return MessageAcceptance(message_id=inserted_id, created=True)

    async def prepare_assistant_exchange_finalization(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        user_message_id: int,
        outcome: str,
        cur,
    ) -> ExchangeClosure | None:
        """Lock one user exchange before an atomic assistant finalization.

        A previous matching finalization is returned instead of writing a
        second assistant message. A different terminal outcome is never
        overwritten: callers must preserve the first durable evidence.
        """

        if outcome not in _ASSISTANT_EXCHANGE_OUTCOMES:
            raise ValueError("Assistant finalization requires an assistant outcome")

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
        if user_row["exchange_outcome"] != outcome:
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
            outcome=outcome,
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
        terminal_error: dict[str, object] | None = None,
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
        if terminal_error is not None:
            expected_retryability = {
                "llm_budget_exhausted": False,
                "workspace_conflict": False,
                "run_failed": True,
                "run_cancelled": False,
            }
            code = terminal_error.get("code")
            if (
                outcome not in {"failed", "cancelled"}
                or code not in expected_retryability
                or (outcome == "cancelled") != (code == "run_cancelled")
                or terminal_error
                != {
                    "code": code,
                    "retryable": expected_retryability[code],
                }
            ):
                raise ValueError("Invalid terminal error record")
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
                    terminal_error=terminal_error,
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
            if outcome in _ASSISTANT_EXCHANGE_OUTCOMES:
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
                exchange_closed_at_ms = %s,
                metadata = CASE
                    WHEN %s::jsonb IS NULL THEN metadata
                    ELSE jsonb_set(metadata, '{terminal_error}', %s::jsonb, true)
                END
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
                json.dumps(terminal_error) if terminal_error is not None else None,
                json.dumps(terminal_error) if terminal_error is not None else None,
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

    async def close_orphaned_user_exchange(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        user_message_id: int,
        expected_opened_at_ms: int,
        stale_before_ms: int,
    ) -> ExchangeClosure:
        """Close an explicitly selected stale exchange after locking its row."""

        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT timestamp_ms, exchange_state
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
                raise ValueError("Orphaned exchange is unavailable")
            if row["exchange_state"] != "open":
                raise ValueError("Exchange is no longer open")
            if int(row["timestamp_ms"]) != expected_opened_at_ms:
                raise ValueError("Exchange changed since it was inspected")
            if expected_opened_at_ms > stale_before_ms:
                raise ValueError("Exchange is not old enough to repair")
            return await self.close_user_exchange(
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
                user_message_id=user_message_id,
                outcome="failed",
                terminal_error={"code": "run_failed", "retryable": True},
                cur=cur,
            )

    async def close_orphaned_open_exchanges(self, *, user_name: str) -> int:
        """Close every open exchange left behind by a previous engine process.

        Call only at application startup, before any session can run: the
        local single-engine runtime then owns no in-flight run, so an open
        exchange can never complete and would block its session's semantic
        FIFO forever.
        """

        rows = await self.client.fetch_all(
            """
            SELECT project_id, session_id, message_id
            FROM public.messages
            WHERE user_name = %s
              AND role = 'user'
              AND exchange_state = 'open'
            ORDER BY message_id
            """,
            (user_name,),
        )
        for row in rows:
            await self.close_user_exchange(
                user_name=user_name,
                project_id=row["project_id"],
                session_id=row["session_id"],
                user_message_id=int(row["message_id"]),
                outcome="failed",
                terminal_error={"code": "run_failed", "retryable": True},
            )
        return len(rows)

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

        user_name, project_id, session_id = self._edit_scope(
            user_name, project_id, session_id, message_id
        )
        if not isinstance(content, str) or not content.strip():
            raise ValueError("content must not be blank")
        content = content.strip()
        now_ms = self._now_ms()
        async with self.client.transaction() as cur:
            # A separate statement takes the lock before the revision-history
            # snapshot, so concurrent edits see the preceding committed revision.
            await cur.execute(
                """
                SELECT message_id FROM public.messages
                WHERE user_name = %s AND project_id = %s AND session_id = %s
                  AND message_id = %s AND role = 'user'
                FOR UPDATE
                """,
                (user_name, project_id, session_id, message_id),
            )
            if await cur.fetchone() is None:
                raise ValueError("Message is no longer editable")
            await cur.execute(
                """
                UPDATE public.messages AS message
                SET content = %s,
                    selected_revision = (
                        SELECT COALESCE(MAX(saved.revision), 0) + 1
                        FROM public.message_revisions AS saved
                        WHERE saved.user_name = message.user_name
                          AND saved.project_id = message.project_id
                          AND saved.session_id = message.session_id
                          AND saved.message_id = message.message_id
                    )
                WHERE user_name = %s
                  AND project_id = %s
                  AND session_id = %s
                  AND message_id = %s
                  AND role = 'user'
                  AND lifecycle_state = 'editable'
                  AND exchange_state = 'open'
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

        user_name, project_id, session_id = self._edit_scope(
            user_name, project_id, session_id, message_id
        )
        if not isinstance(revision, int) or isinstance(revision, bool) or revision <= 0:
            raise ValueError("revision must be a positive integer")
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
                  AND message.exchange_state = 'open'
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

    @staticmethod
    def _edit_scope(user_name, project_id, session_id, message_id):
        if (
            not isinstance(message_id, int)
            or isinstance(message_id, bool)
            or message_id <= 0
        ):
            raise ValueError("message_id must be a positive integer")
        return tuple(
            require_scope_value(value, name, "edit_user_message")
            for name, value in (
                ("user_name", user_name),
                ("project_id", project_id),
                ("session_id", session_id),
            )
        )
