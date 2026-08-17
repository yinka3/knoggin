import pytest
from psycopg.errors import ForeignKeyViolation


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_relationship_evidence_requires_a_scoped_message_and_cascades(
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
            entity_a_id, entity_b_id, relationship_type,
            observed_relationship_label
        )
        VALUES (
            'project-1:2:3:related', 'ada', 'project-1', 2, 3,
            'related', 'related'
        )
        """
    )

    await real_postgres_client.execute(
        """
        INSERT INTO relationship_evidence_refs (
            relationship_id, project_id, user_name, session_id, message_id
        )
        VALUES ('project-1:2:3:related', 'project-1', 'ada', 'session-1', 101)
        """
    )
    await real_postgres_client.execute("DELETE FROM messages WHERE message_id = 101")
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationship_evidence_refs"
    ) == {"count": 0}

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO relationship_evidence_refs (
                relationship_id, project_id, user_name, session_id, message_id
            )
            VALUES (
                'project-1:2:3:related', 'project-1', 'ada', 'session-1', 999
            )
            """
        )

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO relationship_evidence_refs (
                relationship_id, project_id, user_name, session_id, message_id
            )
            VALUES (
                'project-1:2:3:related', 'project-2', 'ada', 'session-2', 102
            )
            """
        )
