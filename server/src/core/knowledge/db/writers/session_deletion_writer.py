from core.knowledge.db.writers.age_projection_writer import AgeProjectionWriter
from infrastructure.postgres_client import PostgresClient


class SessionDeletionWriter:
    """Atomically remove one session's canonical and AGE message state."""

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
            await cur.execute(
                """
                DELETE FROM public.messages
                WHERE user_name = %(user_name)s
                  AND session_id = %(session_id)s
                """,
                {"user_name": user_name, "session_id": session_id},
            )
            await cur.execute(
                """
                DELETE FROM public.sessions
                WHERE user_name = %(user_name)s
                  AND session_id = %(session_id)s
                """,
                {"user_name": user_name, "session_id": session_id},
            )
