from core.knowledge.db.writers.age_projection_writer import AgeProjectionWriter
from infrastructure.postgres_client import PostgresClient


class SessionDeletionWriter:
    """Atomically remove one session's messages and session-owned documents."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client
        self.projection = AgeProjectionWriter(client)

    async def delete_session(self, *, user_name: str, session_id: str) -> None:
        async with self.client.transaction() as cur:
            await self.projection.delete_session_message_projection(
                cur,
                user_name,
                session_id,
            )
            params = {"user_name": user_name, "session_id": session_id}
            await cur.execute(
                """
                DELETE FROM public.project_documents AS document
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
                DELETE FROM public.messages
                WHERE user_name = %(user_name)s
                  AND session_id = %(session_id)s
                """,
                params,
            )
            await cur.execute(
                """
                DELETE FROM public.sessions
                WHERE user_name = %(user_name)s
                  AND session_id = %(session_id)s
                """,
                params,
            )
