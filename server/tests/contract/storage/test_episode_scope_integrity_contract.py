import pytest
from psycopg.errors import ForeignKeyViolation


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_messages_must_belong_to_an_existing_session_in_the_same_project(
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

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO messages (
                user_name, session_id, message_id, project_id, role, content
            ) VALUES ('ada', 'missing-session', 101, 'project-1', 'user', 'Invalid.')
            """
        )

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO messages (
                user_name, session_id, message_id, project_id, role, content
            ) VALUES ('ada', 'session-2', 102, 'project-1', 'user', 'Wrong project.')
            """
        )

    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        ) VALUES ('ada', 'session-1', 103, 'project-1', 'user', 'Valid.')
        """
    )
    await real_postgres_client.execute(
        "DELETE FROM sessions WHERE session_id = 'session-1'"
    )

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM messages WHERE message_id = 103"
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_episode_and_source_messages_must_share_project_session_scope(
    real_postgres_client,
):
    """Direct SQL cannot join an episode to another scope's session or message."""

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
        VALUES
            ('ada', 'session-1', 101, 'project-1', 'user', 'In scope.'),
            ('ada', 'session-2', 102, 'project-2', 'user', 'Out of scope.')
        """
    )

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO episodes (episode_id, project_id, session_id, summary)
            VALUES ('episode-invalid', 'project-1', 'session-2', 'Invalid scope.')
            """
        )

    await real_postgres_client.execute(
        """
        INSERT INTO episodes (episode_id, project_id, session_id, summary)
        VALUES ('episode-1', 'project-1', 'session-1', 'Valid scope.')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO episode_messages (
            episode_id, project_id, session_id, message_id, message_position
        )
        VALUES ('episode-1', 'project-1', 'session-1', 101, 0)
        """
    )

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO episode_messages (
                episode_id, project_id, session_id, message_id, message_position
            )
            VALUES ('episode-1', 'project-2', 'session-2', 102, 1)
            """
        )

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO episode_messages (
                episode_id, project_id, session_id, message_id, message_position
            )
            VALUES ('episode-1', 'project-1', 'session-1', 102, 1)
            """
        )

    await real_postgres_client.execute("DELETE FROM messages WHERE message_id = 101")
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM episode_messages"
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_episode_derived_attachments_must_share_project_scope(
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
        INSERT INTO episodes (episode_id, project_id, session_id, summary)
        VALUES ('episode-1', 'project-1', 'session-1', 'Valid scope.')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, topic
        )
        VALUES
            (2, 'ada', 'project-1', 'In project one', 'General'),
            (3, 'ada', 'project-2', 'In project two', 'General'),
            (4, 'ada', 'project-2', 'Also in project two', 'General')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO relationships (
            relationship_id, user_name, project_id,
            entity_a_id, entity_b_id, relationship_type
        )
        VALUES (
            'project-2:3:4:related', 'ada', 'project-2', 3, 4,
            'related'
        )
        """
    )

    await real_postgres_client.execute(
        """
        INSERT INTO episode_entities (episode_id, project_id, entity_id)
        VALUES ('episode-1', 'project-1', 2)
        """
    )
    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO episode_entities (episode_id, project_id, entity_id)
            VALUES ('episode-1', 'project-1', 3)
            """
        )

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO episode_relationships (
                episode_id, project_id, relationship_id
            )
            VALUES ('episode-1', 'project-1', 'project-2:3:4:related')
            """
        )

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO episode_relationships (
                episode_id, project_id, relationship_id
            )
            VALUES ('episode-1', 'project-2', 'project-2:3:4:related')
            """
        )
