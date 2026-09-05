import pytest
from psycopg.errors import CheckViolation


async def _seed_scoped_graph(client) -> None:
    await client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1');

        INSERT INTO entities (entity_id, user_name, canonical_name)
        VALUES
            (1, 'ada', 'Ada'),
            (2, 'ada', 'Primary'),
            (3, 'ada', 'Secondary'),
            (4, 'ada', 'Other Project');
        INSERT INTO project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'person', 'People'),
            ('project-1', 3, 'ada', 'person', 'People'),
            ('project-2', 4, 'ada', 'person', 'People');

        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 101, 'project-1', 'user', 'Scoped message');
        """
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_domain_constraints_reject_invalid_relationship_values_and_scope(
    real_postgres_client,
):
    await _seed_scoped_graph(real_postgres_client)

    with pytest.raises(
        CheckViolation,
        match="relationship endpoints must belong to the relationship user and project scope",
    ):
        await real_postgres_client.execute(
            """
            INSERT INTO relationships (
                relationship_id, user_name, project_id,
                entity_a_id, entity_b_id, relationship_type
            )
            VALUES ('project-1:2:4:scope', 'ada', 'project-1', 2, 4, 'scope')
            """
        )

    await real_postgres_client.execute(
        """
        INSERT INTO relationships (
            relationship_id, user_name, project_id,
            entity_a_id, entity_b_id, relationship_type
        )
        VALUES (
            'project-1:1:2:knows', 'ada', 'project-1', 1, 2,
            'knows'
        )
        """
    )
    with pytest.raises(
        CheckViolation,
        match="relationship_observations_interpretation_source_check",
    ):
        await real_postgres_client.execute(
            """
            INSERT INTO relationship_observations (
                relationship_id, project_id, user_name, semantic_window_id,
                source_entity_id, target_entity_id, observed_relationship_label,
                interpretation_source, observed_at_ms
            )
            VALUES (
                'project-1:1:2:knows', 'project-1', 'ada',
                '11111111-1111-4111-8111-111111111111',
                1, 2, 'knows', 'invalid', 1
            )
            """
        )
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationships"
    ) == {"count": 1}

    with pytest.raises(
        CheckViolation,
        match="relationships_identity_matches_fields",
    ):
        await real_postgres_client.execute(
            """
            INSERT INTO relationships (
                relationship_id, user_name, project_id,
                entity_a_id, entity_b_id, relationship_type
            )
            VALUES (
                'project-1:1:2:knows', 'ada', 'project-1', 2, 3,
                'mentors'
            )
            """
        )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_domain_constraints_enforce_message_entity_attachment_scope(
    real_postgres_client,
):
    await _seed_scoped_graph(real_postgres_client)

    await real_postgres_client.execute(
        """
        INSERT INTO message_entity_refs (message_id, entity_id)
        VALUES (101, 1)
        """
    )
    with pytest.raises(CheckViolation, match="message entity reference"):
        await real_postgres_client.execute(
            """
            INSERT INTO message_entity_refs (message_id, entity_id)
            VALUES (101, 4)
            """
        )

@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_additive_constraints_reject_invalid_graph_and_episode_values(
    real_postgres_client,
):
    await _seed_scoped_graph(real_postgres_client)

    with pytest.raises(CheckViolation, match="entities_canonical_name_nonblank_check"):
        await real_postgres_client.execute(
            """
            INSERT INTO entities (entity_id, user_name, canonical_name)
            VALUES (5, 'ada', '   ')
            """
        )
    with pytest.raises(CheckViolation, match="messages_id_positive_check"):
        await real_postgres_client.execute(
            """
            INSERT INTO messages (
                user_name, session_id, message_id, project_id, role, content
            )
            VALUES ('ada', 'session-1', 0, 'project-1', 'user', 'Invalid ID')
            """
        )

    await real_postgres_client.execute(
        """
        INSERT INTO entities (entity_id, user_name, canonical_name)
        VALUES (5, 'ada', 'Third');
        INSERT INTO project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-1', 5, 'ada', 'person', 'People');
        INSERT INTO episodes (episode_id, project_id, summary)
        VALUES ('episode-1', 'project-1', 'Memberships are unranked');
        INSERT INTO episode_entities (
            episode_id, project_id, entity_id
        )
        VALUES
            ('episode-1', 'project-1', 2),
            ('episode-1', 'project-1', 3);
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO episode_entities (episode_id, project_id, entity_id)
        VALUES ('episode-1', 'project-1', 5)
        """
    )
