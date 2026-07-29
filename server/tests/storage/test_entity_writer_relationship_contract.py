import pytest

from common.schema.contracts import EngineScope, RelationshipWrite
from core.knowledge.db.writers.entity_writer import EntityWriter
from tests.fixtures.fakes import RecordingPostgresClient


SCOPE = EngineScope(
    user_name="ada",
    session_id="session-1",
    project_id="project-1",
)


def relationship_write(**overrides) -> RelationshipWrite:
    payload = {
        "entity_a_id": 2,
        "entity_b_id": 3,
        "relationship_type": "works_with",
        "message_id": 7,
        "confidence": 1.0,
    }
    payload.update(overrides)
    return RelationshipWrite(**payload)


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
                relationship_write()
            ],
            scope=SCOPE,
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
@pytest.mark.no_network
def test_relationship_write_rejects_blank_relationship_type_at_command_boundary():
    with pytest.raises(ValueError, match="relationship_type must not be blank"):
        relationship_write(relationship_type="  ")


@pytest.mark.storage
@pytest.mark.no_network
async def test_relationship_write_persists_relationship_type_in_sql_and_projection():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {"relationship_id": "project-1:2:3:works_with"},
            {"message_id": 7},
        ]
    )
    writer = EntityWriter(client)

    await writer.write_batch(
        [],
        [relationship_write(relationship_type="works_with")],
        scope=SCOPE,
    )

    relationship_insert = next(
        call for call in client.calls if "INSERT INTO relationships" in call[1]
    )
    assert relationship_insert[2][5] == "works_with"
    assert relationship_insert[2][0] == "project-1:2:3:works_with"

    projection_write = next(
        call for call in client.calls if "UNWIND $batch AS rel" in call[1]
    )
    assert '"relationship_type": "works_with"' in projection_write[2][0]


@pytest.mark.storage
@pytest.mark.no_network
async def test_relationship_types_create_distinct_pair_identities():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {"relationship_id": "project-1:2:3:works_with"},
            {"message_id": 7},
            {"relationship_id": "project-1:2:3:mentors"},
            {"message_id": 8},
        ]
    )
    writer = EntityWriter(client)

    await writer.write_batch(
        [],
        [
            relationship_write(relationship_type="WORKS_WITH"),
            relationship_write(
                entity_a_id=3,
                entity_b_id=2,
                relationship_type="Mentors",
                message_id=8,
            ),
        ],
        scope=SCOPE,
    )

    relationship_inserts = [
        call for call in client.calls if "INSERT INTO relationships" in call[1]
    ]
    assert [call[2][0] for call in relationship_inserts] == [
        "project-1:2:3:works_with",
        "project-1:2:3:mentors",
    ]
    assert [call[2][5] for call in relationship_inserts] == [
        "works_with",
        "mentors",
    ]


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
                relationship_write()
            ],
            scope=SCOPE,
        )

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationships"
    ) == {"count": 0}
