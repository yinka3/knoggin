import pytest
from psycopg.errors import ForeignKeyViolation


async def _seed_search_projection_rows(client) -> None:
    await client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1');

        INSERT INTO entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Original entity');
        INSERT INTO project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-1', 2, 'ada', 'person', 'People');

        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 101, 'project-1', 'user', 'Original message');

        """
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_entities_reject_canonical_name_mutation(
    real_postgres_client,
):
    await _seed_search_projection_rows(real_postgres_client)

    with pytest.raises(Exception, match="immutable"):
        await real_postgres_client.execute(
            "UPDATE entities SET canonical_name = 'Renamed entity' WHERE entity_id = 2"
        )
    assert await real_postgres_client.fetch_one(
        "SELECT canonical_name FROM entities WHERE entity_id = 2"
    ) == {"canonical_name": "Original entity"}
    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            UPDATE messages
            SET project_id = 'project-2'
            WHERE message_id = 101
            """
        )
