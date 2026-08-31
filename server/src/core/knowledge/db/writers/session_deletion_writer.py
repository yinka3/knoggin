from infrastructure.postgres_client import PostgresClient


class SessionDeletionWriter:
    """Tombstone one session without changing project-library documents.

    Sessions own their immutable messages and runtime state. Project documents
    belong to the project, so closing a session must not hide, delete, detach,
    or otherwise mutate the library that other sessions still use.
    """

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    async def delete_session(self, *, user_name: str, session_id: str) -> None:
        async with self.client.transaction() as cur:
            params = {"user_name": user_name, "session_id": session_id}
            await cur.execute(
                """
                UPDATE public.sessions
                SET status = 'deleted',
                    deleted_at = COALESCE(deleted_at, now()),
                    last_active_at = now(),
                    model = NULL,
                    agent_id = NULL,
                    enabled_tools = NULL,
                    document_focus = NULL,
                    episode_participation_enabled = FALSE
                WHERE user_name = %(user_name)s
                  AND session_id = %(session_id)s
                  AND status <> 'deleted'
                """,
                params,
            )
            # Do not delete or mutate canonical messages (or their AGE
            # projections). The session tombstone makes them read-only
            # historical evidence and mutation paths require an open session.
