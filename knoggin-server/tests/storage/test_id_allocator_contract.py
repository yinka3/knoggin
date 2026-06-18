import pytest

from knoggin_server.knowledge.db.id_allocator import IdAllocator
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_id_allocator_uses_postgres_sequences():
    client = RecordingPostgresClient(
        execute_read_results=[[{"id": 2}], [{"id": 1}]]
    )
    allocator = IdAllocator(client)

    assert await allocator.allocate_entity_id() == 2
    assert await allocator.allocate_message_id() == 1
    assert client.calls == [
        (
            "execute_read",
            "SELECT nextval('public.entity_id_seq') AS id",
            None,
        ),
        (
            "execute_read",
            "SELECT nextval('public.message_id_seq') AS id",
            None,
        ),
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_id_allocator_rejects_missing_sequence_result():
    allocator = IdAllocator(RecordingPostgresClient(execute_read_results=[[]]))

    with pytest.raises(RuntimeError, match="public.entity_id_seq"):
        await allocator.allocate_entity_id()
