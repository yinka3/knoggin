from infrastructure.postgres_client import PostgresClient


class IdAllocator:
    """Allocate durable canonical IDs from PostgreSQL sequences."""

    _ENTITY_ID_QUERY = "SELECT nextval('public.entity_id_seq') AS id"
    _MESSAGE_ID_QUERY = "SELECT nextval('public.message_id_seq') AS id"

    def __init__(self, client: PostgresClient):
        self.client = client

    async def allocate_entity_id(self) -> int:
        return await self._next_value(self._ENTITY_ID_QUERY, "public.entity_id_seq")

    async def allocate_message_id(self) -> int:
        return await self._next_value(self._MESSAGE_ID_QUERY, "public.message_id_seq")

    async def _next_value(self, query: str, sequence_name: str) -> int:
        rows = await self.client.execute_read(query)
        if len(rows) != 1 or rows[0].get("id") is None:
            raise RuntimeError(f"Failed to allocate ID from {sequence_name}")
        return int(rows[0]["id"])
