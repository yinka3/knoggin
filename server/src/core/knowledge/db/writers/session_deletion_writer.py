from infrastructure.postgres_client import PostgresClient


class SessionDeletionWriter:
    """Tombstone one session while purging its session-owned documents.

    A deleted session remains as the durable owner of its immutable messages.
    That keeps project graph and episode provenance inspectable while making the
    session unavailable for any future runtime or mutation work.
    """

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    async def delete_session(self, *, user_name: str, session_id: str) -> None:
        async with self.client.transaction() as cur:
            params = {"user_name": user_name, "session_id": session_id}
            await cur.execute(
                """
                UPDATE public.project_documents AS document
                SET status = 'deleted',
                    deleted_at = COALESCE(document.deleted_at, now()),
                    indexed_at = NULL,
                    error_message = NULL,
                    updated_at = now()
                WHERE document.session_id = %(session_id)s
                  AND document.status <> 'deleted'
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.session_id = %(session_id)s
                        AND session.user_name = %(user_name)s
                  )
                """,
                params,
            )
            await cur.execute(
                """
                DELETE FROM public.document_chunks AS chunk
                WHERE chunk.document_id IN (
                    SELECT document.document_id
                    FROM public.project_documents AS document
                    WHERE document.session_id = %(session_id)s
                      AND EXISTS (
                          SELECT 1
                          FROM public.sessions AS session
                          WHERE session.session_id = %(session_id)s
                            AND session.user_name = %(user_name)s
                      )
                )
                """,
                params,
            )
            await cur.execute(
                """
                DELETE FROM public.document_content AS content
                WHERE content.document_id IN (
                    SELECT document.document_id
                    FROM public.project_documents AS document
                    WHERE document.session_id = %(session_id)s
                      AND EXISTS (
                          SELECT 1
                          FROM public.sessions AS session
                          WHERE session.session_id = %(session_id)s
                            AND session.user_name = %(user_name)s
                      )
                )
                """,
                params,
            )
            await cur.execute(
                """
                UPDATE public.project_documents AS document
                SET source_id = NULL,
                    folder_root_id = NULL
                WHERE document.session_id = %(session_id)s
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.session_id = %(session_id)s
                        AND session.user_name = %(user_name)s
                  )
                """,
                params,
            )
            await cur.execute(
                """
                DELETE FROM public.document_folder_uploads AS folder
                WHERE folder.session_id = %(session_id)s
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.session_id = %(session_id)s
                        AND session.user_name = %(user_name)s
                  )
                """,
                params,
            )
            await cur.execute(
                """
                DELETE FROM public.document_workspace_sources AS source
                WHERE source.session_id = %(session_id)s
                  AND EXISTS (
                      SELECT 1
                      FROM public.sessions AS session
                      WHERE session.session_id = %(session_id)s
                        AND session.user_name = %(user_name)s
                  )
                """,
                params,
            )
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
