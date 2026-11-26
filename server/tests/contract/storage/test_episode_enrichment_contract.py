from uuid import uuid4

import pytest

from core.knowledge.db.writers.episode_writer import EpisodeWriter
from core.knowledge.db.writers.relationship_interpretation_writer import (
    RelationshipInterpretationWriter,
)
from core.knowledge.maintenance.maintenance_reviews import (
    RelationshipInterpretationChange,
    RelationshipInterpretationPlan,
)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_episodes_link_the_entities_and_relationships_their_messages_carry(
    real_postgres_client,
):
    """An Episode links a relationship or entity only through its own messages."""
    window_id = uuid4()
    relationship_id = "project-1:2:3:uses"
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('episode-session', 'ada', 'project-1');
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, exchange_state, exchange_outcome, exchange_closed_at_ms
        ) VALUES
            ('ada', 'episode-session', 101, 'project-1', 'user',
             'Knoggin is new.', 101, 'closed', 'user_only', 101),
            ('ada', 'episode-session', 102, 'project-1', 'user',
             'Ada started using it.', 102, 'closed', 'user_only', 102),
            ('ada', 'episode-session', 103, 'project-1', 'user',
             'Ada uses Knoggin daily.', 103, 'closed', 'user_only', 103);
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Ada'), (3, 'ada', 'Knoggin');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'Identity', 'Identity'),
            ('project-1', 3, 'ada', 'Concept', 'General');
        INSERT INTO public.relationships (
            relationship_id, user_name, project_id,
            entity_a_id, entity_b_id, relationship_type
        ) VALUES ('project-1:2:3:uses', 'ada', 'project-1', 2, 3, 'uses');
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_semantic_windows (
            window_id, user_name, project_id, stage, domain_version,
            policy_snapshot, token_estimator, token_estimator_version
        ) VALUES (
            %s, 'ada', 'project-1', 'knowledge_committed', 1,
            '{}'::jsonb, 'test', '1'
        )
        """,
        (window_id,),
    )
    observation = await real_postgres_client.fetch_one(
        """
        INSERT INTO public.relationship_observations (
            relationship_id, project_id, user_name, semantic_window_id,
            source_entity_id, target_entity_id, observed_relationship_label,
            support_key, observed_at_ms
        ) VALUES (%s, 'project-1', 'ada', %s, 2, 3, 'uses', %s, 103)
        RETURNING observation_id
        """,
        (relationship_id, window_id, "d" * 32),
    )
    assert observation is not None
    await real_postgres_client.execute(
        """
        INSERT INTO public.relationship_observation_messages (
            observation_id, project_id, message_id
        ) VALUES (%s, 'project-1', 102), (%s, 'project-1', 103)
        """,
        (observation["observation_id"], observation["observation_id"]),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.message_entity_occurrences (
            window_id, project_id, pass_number, occurrence_index, message_id,
            resolved_entity_id, mention_text
        ) VALUES (%s, 'project-1', 1, 0, 101, 3, 'Knoggin'),
                 (%s, 'project-1', 1, 0, 103, 3, 'Knoggin')
        """,
        (window_id, window_id),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.episodes (episode_id, project_id, summary)
        VALUES
            ('partial-episode', 'project-1', 'Partial evidence.'),
            ('complete-episode', 'project-1', 'Complete evidence.');
        INSERT INTO public.episode_messages (
            episode_id, project_id, session_id, message_id, message_position
        ) VALUES
            ('partial-episode', 'project-1', 'episode-session', 101, 0),
            ('complete-episode', 'project-1', 'episode-session', 102, 0),
            ('complete-episode', 'project-1', 'episode-session', 103, 1)
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_semantic_window_episodes (
            window_id, project_id, episode_id, ordinal
        ) VALUES
            (%s, 'project-1', 'partial-episode', 0),
            (%s, 'project-1', 'complete-episode', 1)
        """,
        (window_id, window_id),
    )
    # A stale link from a message that does not support the relationship.
    await real_postgres_client.execute(
        """
        INSERT INTO public.episode_relationships (
            episode_id, project_id, relationship_id, source_message_count
        ) VALUES ('partial-episode', 'project-1', %s, 1)
        """,
        (relationship_id,),
    )

    writer = EpisodeWriter(real_postgres_client)
    for _ in range(2):
        await writer.enrich_project_semantic_window_episodes(
            window_id=str(window_id), user_name="ada", project_id="project-1"
        )
        assert await real_postgres_client.fetch_all(
            """
            SELECT episode_id, entity_id, source_message_count
            FROM public.episode_entities
            ORDER BY episode_id
            """
        ) == [
            {"episode_id": "complete-episode", "entity_id": 3, "source_message_count": 1},
            {"episode_id": "partial-episode", "entity_id": 3, "source_message_count": 1},
        ]
        assert await real_postgres_client.fetch_all(
            """
            SELECT episode_id, relationship_id, source_message_count
            FROM public.episode_relationships
            WHERE project_id = 'project-1'
            ORDER BY episode_id
            """
        ) == [
            {
                "episode_id": "complete-episode",
                "relationship_id": relationship_id,
                "source_message_count": 2,
            }
        ]

    # Maintenance must not restore a stale partial link while changing the
    # interpretation of the same observation.
    await real_postgres_client.execute(
        """
        INSERT INTO public.episode_relationships (
            episode_id, project_id, relationship_id, source_message_count
        ) VALUES ('partial-episode', 'project-1', %s, 1)
        """,
        (relationship_id,),
    )
    result = await RelationshipInterpretationWriter(real_postgres_client).apply_plan(
        user_name="ada",
        project_id="project-1",
        plan=RelationshipInterpretationPlan(
            changes=[
                RelationshipInterpretationChange(
                    observation_id=observation["observation_id"],
                    expected_relationship_id=relationship_id,
                    target_relationship_type="CONNECTS",
                    interpretation_source="review",
                )
            ]
        ),
    )
    assert result.updated == 1
    new_relationship_id = "project-1:2:3:connects"
    assert await real_postgres_client.fetch_all(
        """
        SELECT episode_id, relationship_id, source_message_count
        FROM public.episode_relationships
        WHERE project_id = 'project-1'
        """
    ) == [
        {
            "episode_id": "complete-episode",
            "relationship_id": new_relationship_id,
            "source_message_count": 2,
        }
    ]

    await real_postgres_client.execute(
        """
        UPDATE public.relationship_observations
        SET retired_at = NOW(), retired_reason = 'test retirement'
        WHERE observation_id = %s
        """,
        (observation["observation_id"],),
    )
    await writer.enrich_project_semantic_window_episodes(
        window_id=str(window_id), user_name="ada", project_id="project-1"
    )
    assert await real_postgres_client.fetch_all(
        "SELECT episode_id FROM public.episode_relationships "
        "WHERE project_id = 'project-1'"
    ) == []

    # The maintenance reconciler must also leave retired evidence detached.
    await real_postgres_client.execute(
        """
        INSERT INTO public.episode_relationships (
            episode_id, project_id, relationship_id, source_message_count
        ) VALUES ('complete-episode', 'project-1', %s, 2)
        """,
        (new_relationship_id,),
    )
    async with real_postgres_client.transaction() as cur:
        await RelationshipInterpretationWriter._reconcile_episodes(
            cur, "project-1", [observation["observation_id"]]
        )
    assert await real_postgres_client.fetch_all(
        "SELECT episode_id FROM public.episode_relationships "
        "WHERE project_id = 'project-1'"
    ) == []
