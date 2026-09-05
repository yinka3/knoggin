import pytest
from psycopg.errors import ForeignKeyViolation


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_relationship_observation_requires_a_scoped_semantic_window(
    real_postgres_client,
):
    """Observations are project-window evidence, not message-owned rows."""

    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
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
        INSERT INTO entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Ada'), (3, 'ada', 'Knoggin');
        INSERT INTO project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'person', 'People'),
            ('project-1', 3, 'ada', 'organization', 'Work');
        INSERT INTO relationships (
            relationship_id, user_name, project_id,
            entity_a_id, entity_b_id, relationship_type
        ) VALUES (
            'project-1:2:3:related', 'ada', 'project-1', 2, 3, 'related'
        );
        INSERT INTO project_semantic_windows (
            window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, source_token_count, token_estimator,
            token_estimator_version, completed_at
        ) VALUES (
            '11111111-1111-4111-8111-111111111111', 'ada', 'project-1',
            'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now()
        )
        """
    )

    await real_postgres_client.execute(
        """
        INSERT INTO relationship_observations (
            relationship_id, project_id, user_name, semantic_window_id,
            source_entity_id, target_entity_id, observed_relationship_label,
            observed_at_ms
        ) VALUES (
            'project-1:2:3:related', 'project-1', 'ada',
            '11111111-1111-4111-8111-111111111111', 2, 3, 'related to', 1
        )
        """
    )
    await real_postgres_client.execute("DELETE FROM messages WHERE message_id = 101")
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationship_observations"
    ) == {"count": 1}

    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO relationship_observations (
                relationship_id, project_id, user_name, semantic_window_id,
                source_entity_id, target_entity_id, observed_relationship_label,
                observed_at_ms
            ) VALUES (
                'project-1:2:3:related', 'project-1', 'ada',
                '99999999-9999-4999-8999-999999999999', 2, 3, 'other evidence', 2
            )
            """
        )
