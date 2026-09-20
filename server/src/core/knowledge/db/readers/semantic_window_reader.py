"""Owned reads for durable project semantic-window state."""

from __future__ import annotations

from uuid import UUID

from common.schema.semantic_window import SemanticWindowMessage, SemanticWindowRecord
from common.scoping import require_scope_value
from infrastructure.postgres_client import PostgresClient

_WINDOW_COLUMNS = """
    semantic_window.window_id,
    semantic_window.user_name,
    semantic_window.project_id,
    semantic_window.origin,
    semantic_window.stage,
    semantic_window.domain_version,
    semantic_window.policy_snapshot,
    semantic_window.source_token_count,
    semantic_window.token_estimator,
    semantic_window.token_estimator_version,
    semantic_window.overfill_tokens,
    semantic_window.overfill_ratio,
    semantic_window.episode_result_recorded,
    semantic_window.context_revision_id,
    semantic_window.attempt_count,
    semantic_window.last_failure_stage,
    semantic_window.last_failure_code,
    semantic_window.last_failure_at_ms,
    semantic_window.last_error_summary,
    semantic_window.next_retry_at_ms
"""


_ELIGIBLE_CONVERSATION_EXCHANGES_CTE = """
    eligible_conversation_exchanges AS (
        SELECT
            user_message.message_id AS user_message_id,
            user_message.session_id,
            user_message.project_id,
            user_message.content AS user_content,
            user_message.timestamp_ms AS user_timestamp_ms,
            user_message.lifecycle_state AS user_lifecycle_state,
            user_message.exchange_state AS user_exchange_state,
            user_message.exchange_outcome AS user_exchange_outcome,
            user_message.exchange_closed_at_ms,
            assistant_message.message_id AS assistant_message_id,
            assistant_message.content AS assistant_content,
            assistant_message.timestamp_ms AS assistant_timestamp_ms,
            assistant_message.lifecycle_state AS assistant_lifecycle_state
        FROM public.messages AS user_message
        JOIN public.sessions AS session
          ON session.session_id = user_message.session_id
         AND session.project_id = user_message.project_id
         AND session.user_name = user_message.user_name
        LEFT JOIN public.messages AS assistant_message
          ON assistant_message.user_name = user_message.user_name
         AND assistant_message.project_id = user_message.project_id
         AND assistant_message.session_id = user_message.session_id
         AND assistant_message.user_msg_id = user_message.message_id
         AND assistant_message.role = 'assistant'
        WHERE user_message.user_name = %s
          AND user_message.project_id = %s
          AND user_message.role = 'user'
          AND user_message.lifecycle_state <> 'superseded'
          AND session.status = 'open'
          AND session.semantic_participation_enabled
          AND user_message.message_id > session.semantic_participation_after_message_id
    )
"""


class SemanticWindowReader:
    """Read frozen window identity/order only through owned project scope."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    async def get_window(
        self,
        window_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> SemanticWindowRecord | None:
        user_name, project_id = self._scope(user_name, project_id, "get_window")
        window_id = self._uuid(window_id, "window_id")
        row = await self.client.fetch_one(
            f"""
            SELECT {_WINDOW_COLUMNS}
            FROM public.project_semantic_windows AS semantic_window
            WHERE semantic_window.window_id = %s
              AND semantic_window.user_name = %s
              AND semantic_window.project_id = %s
            """,
            (window_id, user_name, project_id),
        )
        return None if row is None else SemanticWindowRecord.model_validate(row)

    async def get_active_window(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> SemanticWindowRecord | None:
        user_name, project_id = self._scope(user_name, project_id, "get_active_window")
        row = await self.client.fetch_one(
            f"""
            SELECT {_WINDOW_COLUMNS}
            FROM public.project_semantic_windows AS semantic_window
            WHERE semantic_window.user_name = %s
              AND semantic_window.project_id = %s
              AND semantic_window.stage <> 'completed'
            """,
            (user_name, project_id),
        )
        return None if row is None else SemanticWindowRecord.model_validate(row)

    async def get_window_messages(
        self,
        window_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[SemanticWindowMessage]:
        """Reload exact persisted membership; message content is never copied here."""

        user_name, project_id = self._scope(
            user_name, project_id, "get_window_messages"
        )
        window_id = self._uuid(window_id, "window_id")
        rows = await self.client.fetch_all(
            """
            SELECT
                membership.message_id,
                membership.session_id,
                membership.exchange_user_message_id,
                membership.role,
                membership.ordinal
            FROM public.project_semantic_window_messages AS membership
            JOIN public.project_semantic_windows AS semantic_window
              ON semantic_window.window_id = membership.window_id
             AND semantic_window.project_id = membership.project_id
            WHERE membership.window_id = %s
              AND membership.project_id = %s
              AND semantic_window.user_name = %s
            ORDER BY membership.ordinal ASC
            """,
            (window_id, project_id, user_name),
        )
        return [SemanticWindowMessage.model_validate(row) for row in rows]

    async def get_window_evidence_messages(
        self,
        window_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[dict]:
        """Reload canonical message content in the frozen member order."""

        user_name, project_id = self._scope(
            user_name, project_id, "get_window_evidence_messages"
        )
        window_id = self._uuid(window_id, "window_id")
        return await self.client.fetch_all(
            """
            SELECT membership.message_id,
                   membership.session_id,
                   membership.exchange_user_message_id,
                   membership.role,
                   membership.ordinal,
                   message.content,
                   message.timestamp_ms,
                   message.user_msg_id,
                   exchange_user.exchange_outcome
            FROM public.project_semantic_window_messages AS membership
            JOIN public.project_semantic_windows AS semantic_window
              ON semantic_window.window_id = membership.window_id
             AND semantic_window.project_id = membership.project_id
            JOIN public.messages AS message
              ON message.message_id = membership.message_id
             AND message.project_id = membership.project_id
             AND message.session_id = membership.session_id
            JOIN public.messages AS exchange_user
              ON exchange_user.message_id = membership.exchange_user_message_id
             AND exchange_user.project_id = membership.project_id
             AND exchange_user.session_id = membership.session_id
             AND exchange_user.user_name = semantic_window.user_name
             AND exchange_user.role = 'user'
            WHERE membership.window_id = %s
              AND membership.project_id = %s
              AND semantic_window.user_name = %s
            ORDER BY membership.ordinal ASC
            """,
            (window_id, project_id, user_name),
        )

    async def get_window_assistant_source_refs(
        self,
        window_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[dict]:
        """Return source refs owned by assistant messages in one frozen window.

        This is intentionally a narrow Context-stage catalog rather than a
        second source store. The updater receives only canonical refs whose
        owning assistant message is itself frozen window evidence.
        """

        user_name, project_id = self._scope(
            user_name, project_id, "get_window_assistant_source_refs"
        )
        window_id = self._uuid(window_id, "window_id")
        return await self.client.fetch_all(
            """
            SELECT ref.source_ref_id,
                   ref.message_id,
                   ref.session_id,
                   ref.source_kind,
                   ref.canonical_url,
                   ref.locator,
                   ref.excerpt,
                   membership.ordinal AS message_ordinal
            FROM public.project_semantic_window_messages AS membership
            JOIN public.project_semantic_windows AS semantic_window
              ON semantic_window.window_id = membership.window_id
             AND semantic_window.project_id = membership.project_id
            JOIN public.messages AS message
              ON message.message_id = membership.message_id
             AND message.project_id = membership.project_id
             AND message.session_id = membership.session_id
            JOIN public.message_source_refs AS ref
              ON ref.message_id = membership.message_id
             AND ref.project_id = membership.project_id
             AND ref.session_id = membership.session_id
            WHERE membership.window_id = %s
              AND membership.project_id = %s
              AND semantic_window.user_name = %s
              AND message.role = 'assistant'
            ORDER BY membership.ordinal ASC, ref.created_at ASC,
                     ref.result_position ASC, ref.source_ref_id ASC
            """,
            (window_id, project_id, user_name),
        )

    async def get_window_episode_ids(
        self,
        window_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[str] | None:
        """Return ``None`` before Episode runs and ``[]`` for zero output."""

        user_name, project_id = self._scope(
            user_name, project_id, "get_window_episode_ids"
        )
        window_id = self._uuid(window_id, "window_id")
        window = await self.client.fetch_one(
            """
            SELECT episode_result_recorded
            FROM public.project_semantic_windows
            WHERE window_id = %s AND user_name = %s AND project_id = %s
            """,
            (window_id, user_name, project_id),
        )
        if window is None or not bool(window["episode_result_recorded"]):
            return None
        rows = await self.client.fetch_all(
            """
            SELECT episode_id
            FROM public.project_semantic_window_episodes
            WHERE window_id = %s AND project_id = %s
            ORDER BY ordinal ASC
            """,
            (window_id, project_id),
        )
        return [str(row["episode_id"]) for row in rows]

    async def get_unclaimed_project_exchange_rows(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> list[dict]:
        """Return participation-eligible exchanges needed for FIFO admission.

        Excluded sessions and pre-frontier exchanges must be absent before
        per-session FIFO is evaluated. Within an eligible session, incomplete
        exchanges remain in the stream so they still block later exchanges
        from overtaking them.
        """

        user_name, project_id = self._scope(
            user_name, project_id, "get_unclaimed_project_exchange_rows"
        )
        return await self.client.fetch_all(
            f"""
            WITH {_ELIGIBLE_CONVERSATION_EXCHANGES_CTE}
            SELECT
                eligible.user_message_id,
                eligible.session_id,
                eligible.user_content,
                eligible.user_timestamp_ms,
                eligible.user_lifecycle_state,
                eligible.user_exchange_state,
                eligible.user_exchange_outcome,
                eligible.exchange_closed_at_ms,
                eligible.assistant_message_id,
                eligible.assistant_content,
                eligible.assistant_timestamp_ms,
                eligible.assistant_lifecycle_state,
                EXISTS (
                    SELECT 1
                    FROM public.project_semantic_window_messages AS membership
                    WHERE membership.project_id = eligible.project_id
                      AND membership.message_id IN (
                          eligible.user_message_id,
                          COALESCE(eligible.assistant_message_id, -1)
                      )
                ) AS already_claimed
            FROM eligible_conversation_exchanges AS eligible
            ORDER BY eligible.session_id,
                     eligible.user_timestamp_ms ASC NULLS LAST,
                     eligible.user_message_id
            """,
            (user_name, project_id),
        )

    async def get_maintenance_quiescence(
        self,
        *,
        user_name: str,
        project_id: str,
        cur=None,
    ) -> dict:
        """Read one Project's semantic quiescence and completed-window boundary.

        The conversation CTE is shared with admission so maintenance uses the
        same Session status, participation flag, and participation frontier.
        A claimed conversation is not pending independently, while every
        non-completed window remains an all-origin maintenance barrier.
        """

        user_name, project_id = self._scope(
            user_name, project_id, "get_maintenance_quiescence"
        )
        query = f"""
            WITH {_ELIGIBLE_CONVERSATION_EXCHANGES_CTE},
            pending_exchanges AS (
                SELECT count(*) AS pending_exchange_count
                FROM eligible_conversation_exchanges AS eligible
                WHERE eligible.user_exchange_state = 'closed'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM public.project_semantic_window_messages AS membership
                      WHERE membership.project_id = eligible.project_id
                        AND membership.exchange_user_message_id = eligible.user_message_id
                  )
            ),
            eligible_frontier AS (
                SELECT
                    COALESCE(max(eligible.user_message_id) FILTER (
                        WHERE eligible.user_exchange_state = 'closed'
                    ), 0) AS frontier_message_id,
                    max(eligible.user_timestamp_ms) FILTER (
                        WHERE eligible.user_exchange_state = 'closed'
                    ) AS frontier_timestamp_ms
                FROM eligible_conversation_exchanges AS eligible
            ),
            semantic_windows AS (
                SELECT
                    count(*) FILTER (
                        WHERE semantic_window.stage <> 'completed'
                    ) AS active_window_count,
                    COALESCE(
                        string_agg(
                            concat_ws(
                                ':',
                                semantic_window.window_id::text,
                                semantic_window.origin,
                                COALESCE(
                                    semantic_window.context_revision_id::text,
                                    ''
                                )
                            ),
                            '|' ORDER BY semantic_window.window_id
                        ) FILTER (WHERE semantic_window.stage = 'completed'),
                        ''
                    ) AS completed_window_boundary
                FROM public.project_semantic_windows AS semantic_window
                WHERE semantic_window.user_name = %s
                  AND semantic_window.project_id = %s
            )
            SELECT
                pending_exchanges.pending_exchange_count,
                semantic_windows.active_window_count,
                eligible_frontier.frontier_message_id,
                eligible_frontier.frontier_timestamp_ms,
                semantic_windows.completed_window_boundary
            FROM pending_exchanges
            CROSS JOIN eligible_frontier
            CROSS JOIN semantic_windows
        """
        params = (user_name, project_id, user_name, project_id)
        if cur is None:
            row = await self.client.fetch_one(query, params)
        else:
            await cur.execute(query, params)
            row = await cur.fetchone()
        return {} if row is None else dict(row)

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
