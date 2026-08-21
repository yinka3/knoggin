import pytest
from psycopg.errors import ForeignKeyViolation


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_relationship_observation_requires_a_scoped_message_and_cascades(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES
            ('session-1', 'ada', 'project-1'),
            ('session-2', 'ada', 'project-2')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 101, 'project-1', 'user', 'Evidence.')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-2', 102, 'project-2', 'user', 'Other project.')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, type, topic
        )
        VALUES
            (2, 'ada', 'project-1', 'Ada', 'person', 'People'),
            (3, 'ada', 'project-1', 'Knoggin', 'organization', 'Work')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO relationships (
            relationship_id, user_name, project_id,
            entity_a_id, entity_b_id, relationship_type
        )
        VALUES (
            'project-1:2:3:related', 'ada', 'project-1', 2, 3,
            'related'
        )
        """
    )

    await real_postgres_client.execute(
        """
        INSERT INTO relationship_observations (
            relationship_id, project_id, user_name, session_id, message_id,
            source_entity_id, target_entity_id, observed_relationship_label,
            observed_at_ms
        )
        VALUES (
            'project-1:2:3:related', 'project-1', 'ada', 'session-1', 101,
            2, 3, 'related to', 1
        )
        """
    )
    await real_postgres_client.execute("DELETE FROM messages WHERE message_id = 101")
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationship_observations"
    ) == {"count": 0}

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO relationship_observations (
                relationship_id, project_id, user_name, session_id, message_id,
                source_entity_id, target_entity_id, observed_relationship_label,
                observed_at_ms
            )
            VALUES (
                'project-1:2:3:related', 'project-1', 'ada', 'session-1', 999,
                2, 3, 'related to', 2
            )
            """
        )

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO relationship_observations (
                relationship_id, project_id, user_name, session_id, message_id,
                source_entity_id, target_entity_id, observed_relationship_label,
                observed_at_ms
            )
            VALUES (
                'project-1:2:3:related', 'project-2', 'ada', 'session-2', 102,
                2, 3, 'related to', 3
            )
            """
        )
