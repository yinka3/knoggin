import pytest

from core.knowledge.db.id_allocator import IdAllocator
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_id_allocator_uses_postgres_sequences():
    client = RecordingPostgresClient(
        fetch_one_results=[{"id": 2}, {"id": 1}]
    )
    allocator = IdAllocator(client)

    assert await allocator.allocate_entity_id() == 2
    assert await allocator.allocate_message_id() == 1
    assert client.calls == [
        (
            "fetch_one",
            "SELECT nextval('public.entity_id_seq') AS id",
            None,
        ),
        (
            "fetch_one",
            "SELECT nextval('public.message_id_seq') AS id",
            None,
        ),
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_id_allocator_rejects_missing_sequence_result():
    allocator = IdAllocator(RecordingPostgresClient(fetch_one_results=[None]))

    with pytest.raises(RuntimeError, match="public.entity_id_seq"):
        await allocator.allocate_entity_id()
