import pytest
from psycopg.errors import CheckViolation, ForeignKeyViolation


async def _seed_scoped_graph(client) -> None:
    await client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1');

        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, topic
        )
        VALUES
            (1, 'ada', '__identity__', 'Ada', 'Identity'),
            (2, 'ada', 'project-1', 'Primary', 'People'),
            (3, 'ada', 'project-1', 'Secondary', 'People'),
            (4, 'ada', 'project-2', 'Other Project', 'People');

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

    with pytest.raises(CheckViolation, match="relationships_weight_positive_check"):
        await real_postgres_client.execute(
            """
            INSERT INTO relationships (
                relationship_id, user_name, project_id,
                entity_a_id, entity_b_id, relationship_type,
                observed_relationship_label, weight
            )
            VALUES (
                'project-1:2:3:zero', 'ada', 'project-1', 2, 3,
                'zero', 'zero', 0
            )
            """
        )
    with pytest.raises(CheckViolation, match="relationships_confidence_range_check"):
        await real_postgres_client.execute(
            """
            INSERT INTO relationships (
                relationship_id, user_name, project_id,
                entity_a_id, entity_b_id, relationship_type,
                observed_relationship_label, confidence
            )
            VALUES (
                'project-1:2:3:confidence', 'ada', 'project-1', 2, 3,
                'confidence', 'confidence', 1.1
            )
            """
        )
    with pytest.raises(CheckViolation, match="relationships_identity_matches_fields"):
        await real_postgres_client.execute(
            """
            INSERT INTO relationships (
                relationship_id, user_name, project_id,
                entity_a_id, entity_b_id, relationship_type,
                observed_relationship_label
            )
            VALUES ('project-1:4:2:scope', 'ada', 'project-1', 2, 4, 'scope', 'scope')
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
            'project-1:1:2:knows', 'ada', 'project-1', 1, 2,
            'knows', 'knows'
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
                entity_a_id, entity_b_id, relationship_type,
                observed_relationship_label
            )
            VALUES (
                'project-1:1:2:knows', 'ada', 'project-1', 2, 3,
                'mentors', 'mentors'
            )
            """
        )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_domain_constraints_enforce_attachment_checkpoint_and_hierarchy_scope(
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
    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO episode_processing_checkpoints (project_id, session_id)
            VALUES ('project-2', 'session-1')
            """
        )
    with pytest.raises(CheckViolation, match="hierarchy endpoints"):
        await real_postgres_client.execute(
            """
            INSERT INTO hierarchy_edges (project_id, parent_id, child_id)
            VALUES ('project-1', 2, 4)
            """
        )

    await real_postgres_client.execute(
        """
        INSERT INTO hierarchy_edges (project_id, parent_id, child_id)
        VALUES ('project-1', 2, 3)
        """
    )
    with pytest.raises(CheckViolation, match="hierarchy edge would create a cycle"):
        await real_postgres_client.execute(
            """
            INSERT INTO hierarchy_edges (project_id, parent_id, child_id)
            VALUES ('project-1', 3, 2)
            """
        )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_domain_constraints_reject_invalid_entity_and_audit_lifecycle_values(
    real_postgres_client,
):
    with pytest.raises(CheckViolation, match="entities_confidence_range_check"):
        await real_postgres_client.execute(
            """
            INSERT INTO entities (
                entity_id, user_name, project_id,
                canonical_name, topic, confidence
            )
            VALUES (2, 'ada', 'project-1', 'Invalid', 'People', -0.1)
            """
        )

    await real_postgres_client.execute(
        """
        INSERT INTO entity_merge_proposals (
            proposal_id, user_name, project_id,
            primary_entity_id, duplicate_entity_id, reasoning,
            reviewed_state_hash, reviewed_state, confirmation_token_hash
        )
        VALUES (
            'proposal-1', 'ada', 'project-1', 2, 3, 'Test lifecycle',
            'hash', '{}'::jsonb, 'token'
        );
        INSERT INTO entity_merge_audits (
            audit_id, proposal_id, user_name, project_id,
            primary_entity_id, duplicate_entity_id, reasoning, confirmed_by
        )
        VALUES (
            'audit-1', 'proposal-1', 'ada', 'project-1',
            2, 3, 'Test lifecycle', 'ada'
        );
        """
    )
    with pytest.raises(CheckViolation, match="entity_merge_audits_status_check"):
        await real_postgres_client.execute(
            """
            UPDATE entity_merge_audits
            SET status = 'unknown'
            WHERE audit_id = 'audit-1'
            """
        )
    with pytest.raises(
        CheckViolation,
        match="entity_merge_audits_rollback_status_check",
    ):
        await real_postgres_client.execute(
            """
            UPDATE entity_merge_audits
            SET rollback_status = 'unknown'
            WHERE audit_id = 'audit-1'
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
            INSERT INTO entities (
                entity_id, user_name, project_id, canonical_name, topic
            )
            VALUES (5, 'ada', 'project-1', '   ', 'People')
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
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, topic
        )
        VALUES (5, 'ada', 'project-1', 'Third', 'People');
        INSERT INTO episodes (episode_id, project_id, session_id, summary)
        VALUES ('episode-1', 'project-1', 'session-1', 'Focus limit');
        INSERT INTO episode_entities (
            episode_id, project_id, entity_id, is_focus_entity
        )
        VALUES
            ('episode-1', 'project-1', 2, TRUE),
            ('episode-1', 'project-1', 3, TRUE);
        """
    )
    with pytest.raises(CheckViolation, match="at most two focus entities"):
        await real_postgres_client.execute(
            """
            INSERT INTO episode_entities (
                episode_id, project_id, entity_id, is_focus_entity
            )
            VALUES ('episode-1', 'project-1', 5, TRUE)
            """
        )
