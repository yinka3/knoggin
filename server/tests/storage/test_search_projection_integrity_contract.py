import pytest
from psycopg.errors import CheckViolation, ForeignKeyViolation


async def _seed_search_projection_rows(client) -> None:
    await client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1');

        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, topic
        )
        VALUES (2, 'ada', 'project-1', 'Original entity', 'People');

        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 101, 'project-1', 'user', 'Original message');

        INSERT INTO entity_search (
            entity_id, canonical_name, user_name, project_id
        )
        VALUES (2, 'Original entity', 'ada', 'project-1');

        INSERT INTO message_search (
            message_id, user_name, session_id, project_id, content_tsvector
        )
        VALUES (
            101,
            'ada',
            'session-1',
            'project-1',
            to_tsvector('english', 'Original message')
        );
        """
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_entity_search_rejects_a_canonical_name_that_does_not_match_source(
    real_postgres_client,
):
    await _seed_search_projection_rows(real_postgres_client)

    with pytest.raises(CheckViolation, match="entity search canonical name"):
        await real_postgres_client.execute(
            """
            UPDATE entity_search
            SET canonical_name = 'Drifted entity'
            WHERE entity_id = 2
            """
        )

    assert await real_postgres_client.fetch_one(
        "SELECT canonical_name FROM entity_search WHERE entity_id = 2"
    ) == {"canonical_name": "Original entity"}

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            "UPDATE entity_search SET project_id = 'project-2' WHERE entity_id = 2"
        )
    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            UPDATE message_search
            SET project_id = 'project-2'
            WHERE message_id = 101
            """
        )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_canonical_scope_and_name_changes_keep_search_projections_aligned(
    real_postgres_client,
):
    await _seed_search_projection_rows(real_postgres_client)

    await real_postgres_client.execute(
        """
        UPDATE entities
        SET canonical_name = 'Renamed entity', project_id = 'project-2'
        WHERE entity_id = 2;

        UPDATE messages
        SET project_id = 'project-2'
        WHERE message_id = 101;
        """
    )

    assert await real_postgres_client.fetch_one(
        """
        SELECT canonical_name, user_name, project_id
        FROM entity_search
        WHERE entity_id = 2
        """
    ) == {
        "canonical_name": "Renamed entity",
        "user_name": "ada",
        "project_id": "project-2",
    }
    assert await real_postgres_client.fetch_one(
        """
        SELECT user_name, session_id, project_id
        FROM message_search
        WHERE message_id = 101
        """
    ) == {
        "user_name": "ada",
        "session_id": "session-1",
        "project_id": "project-2",
    }
