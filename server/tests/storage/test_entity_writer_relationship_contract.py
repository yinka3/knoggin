import pytest

from core.knowledge.db.writers.entity_writer import EntityWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_relationship_write_rejects_missing_or_out_of_scope_endpoints():
    client = RecordingPostgresClient()
    writer = EntityWriter(client)

    with pytest.raises(
        ValueError,
        match="Relationship endpoints must exist in the project scope",
    ):
        await writer.write_batch(
            [],
            [
                {
                    "entity_a_id": 2,
                    "entity_b_id": 3,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "message_id": 7,
                }
            ],
        )

    relationship_insert = next(
        call for call in client.calls if "INSERT INTO relationships" in call[1]
    )
    assert "RETURNING relationship_id" in relationship_insert[1]
    assert not any(
        "INSERT INTO relationship_evidence_refs" in call[1]
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_relationship_write_rolls_back_when_endpoints_are_not_in_scope(
    real_postgres_client,
):
    writer = EntityWriter(real_postgres_client)

    with pytest.raises(
        ValueError,
        match="Relationship endpoints must exist in the project scope",
    ):
        await writer.write_batch(
            [],
            [
                {
                    "entity_a_id": 2,
                    "entity_b_id": 3,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "message_id": 7,
                }
            ],
        )

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationships"
    ) == {"count": 0}
