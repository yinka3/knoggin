"""Episode provenance counts must survive a global entity merge and rollback."""

from uuid import uuid4

import pytest

from common.schema.evidence import EvidencePointer, EvidenceSnapshot
from core.knowledge.db.writers.conflict_writer import ConflictWriter
from core.knowledge.db.writers.global_entity_merge_writer import GlobalEntityMergeWriter


async def _seed_episode_merge_collision(client):
    await client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Survivor'), (3, 'ada', 'Retired'), (4, 'ada', 'Target');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'Concept', 'General'),
            ('project-1', 3, 'ada', 'Concept', 'General'),
            ('project-1', 4, 'ada', 'Concept', 'General');
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('merge-episode-session', 'ada', 'project-1');
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            lifecycle_state, exchange_state, exchange_outcome, exchange_closed_at_ms
        ) VALUES
            ('ada', 'merge-episode-session', 101, 'project-1', 'user',
             'Both relationships.', 'sealed', 'closed', 'user_only', 101),
            ('ada', 'merge-episode-session', 102, 'project-1', 'user',
             'Survivor relationship.', 'sealed', 'closed', 'user_only', 102),
            ('ada', 'merge-episode-session', 103, 'project-1', 'user',
             'Retired relationship.', 'sealed', 'closed', 'user_only', 103);
        INSERT INTO public.episodes (
            episode_id, project_id, summary, source_message_count
        ) VALUES
            ('overlap-episode', 'project-1', 'Both from one message.', 1),
            ('distinct-episode', 'project-1', 'Each from another message.', 2);
        INSERT INTO public.episode_messages (
            episode_id, project_id, session_id, message_id, message_position
        ) VALUES
            ('overlap-episode', 'project-1', 'merge-episode-session', 101, 0),
            ('distinct-episode', 'project-1', 'merge-episode-session', 102, 0),
            ('distinct-episode', 'project-1', 'merge-episode-session', 103, 1);
        INSERT INTO public.relationships (
            relationship_id, user_name, project_id, entity_a_id, entity_b_id,
            relationship_type
        ) VALUES
            ('project-1:2:4:uses', 'ada', 'project-1', 2, 4, 'uses'),
            ('project-1:3:4:uses', 'ada', 'project-1', 3, 4, 'uses');
        INSERT INTO public.episode_relationships (
            episode_id, project_id, relationship_id, source_message_count
        ) VALUES
            ('overlap-episode', 'project-1', 'project-1:2:4:uses', 1),
            ('overlap-episode', 'project-1', 'project-1:3:4:uses', 1),
            ('distinct-episode', 'project-1', 'project-1:2:4:uses', 1),
            ('distinct-episode', 'project-1', 'project-1:3:4:uses', 1);
        INSERT INTO public.episode_entities (
            episode_id, project_id, entity_id, source_message_count
        ) VALUES
            ('overlap-episode', 'project-1', 2, 1),
            ('overlap-episode', 'project-1', 3, 1),
            ('distinct-episode', 'project-1', 2, 1),
            ('distinct-episode', 'project-1', 3, 1)
        """
    )
    window_id = uuid4()
    await client.execute(
        """
        INSERT INTO public.project_semantic_windows (
            window_id, user_name, project_id, stage, domain_version,
            policy_snapshot, token_estimator, token_estimator_version,
            completed_at
        ) VALUES (%s, 'ada', 'project-1', 'completed', 1,
                  '{}'::jsonb, 'test', '1', NOW())
        """,
        (window_id,),
    )
    await client.execute(
        """
        INSERT INTO public.project_semantic_window_episodes (
            window_id, project_id, episode_id, ordinal
        ) VALUES
            (%s, 'project-1', 'overlap-episode', 0),
            (%s, 'project-1', 'distinct-episode', 1)
        """,
        (window_id, window_id),
    )
    # Survivor is named in 101 and 102; Retired in 101 and 103.
    await client.execute(
        """
        INSERT INTO public.message_entity_refs (message_id, entity_id) VALUES
            (101, 2), (102, 2), (101, 3), (103, 3)
        """
    )
    observations = await client.fetch_all(
        """
        INSERT INTO public.relationship_observations (
            relationship_id, project_id, user_name, semantic_window_id,
            source_entity_id, target_entity_id, observed_relationship_label,
            support_key, observed_at_ms
        ) VALUES
            ('project-1:2:4:uses', 'project-1', 'ada', %s, 2, 4,
             'uses', %s, 101),
            ('project-1:3:4:uses', 'project-1', 'ada', %s, 3, 4,
             'uses', %s, 103)
        RETURNING observation_id, source_entity_id
        """,
        (window_id, "c" * 32, window_id, "d" * 32),
    )
    by_source = {row["source_entity_id"]: row["observation_id"] for row in observations}
    await _support(client, by_source[2], 101, 102)
    await _support(client, by_source[3], *RETIRED_MESSAGES)
    return window_id


RETIRED_MESSAGES = (101, 103)


async def _support(client, observation_id, *message_ids):
    for message_id in message_ids:
        await client.execute(
            """INSERT INTO public.relationship_observation_messages
               (observation_id, project_id, message_id) VALUES (%s, 'project-1', %s)""",
            (observation_id, message_id),
        )


async def _relationship_links(client):
    return await client.fetch_all(
        """
        SELECT episode_id, relationship_id, source_message_count
        FROM public.episode_relationships
        WHERE project_id = 'project-1'
        ORDER BY episode_id, relationship_id
        """
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_merge_stales_reviews_citing_rewritten_relationship_observations(
    real_postgres_client,
):
    await _seed_episode_merge_collision(real_postgres_client)
    rows = await real_postgres_client.fetch_all(
        "SELECT observation_id FROM public.relationship_observations ORDER BY observation_id"
    )
    observation_ids = tuple(int(row["observation_id"]) for row in rows)
    conflict = await ConflictWriter(real_postgres_client).record_detection(
        user_name="ada",
        project_id="project-1",
        origin="user_created",
        kind="possible_contradiction",
        rationale="The two identities may refer to the same relationship.",
        confidence=0.8,
        evidence_ids=observation_ids,
        evidence_snapshot=EvidenceSnapshot(
            pointers=tuple(
                EvidencePointer.for_observation(value) for value in observation_ids
            ),
            total_nodes=len(observation_ids),
            state_token="a" * 64,
        ),
    )

    await GlobalEntityMergeWriter(real_postgres_client).merge(
        user_name="ada", survivor_id=2, retired_id=3, merge_id=str(uuid4())
    )

    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.maintenance_reviews WHERE review_id = %s",
        (conflict.group.conflict_id,),
    ) == {"status": "stale"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_global_merge_preserves_distinct_episode_support_and_rolls_back(
    real_postgres_client,
):
    await _seed_episode_merge_collision(real_postgres_client)
    before_links = await _relationship_links(real_postgres_client)
    before_entities = await real_postgres_client.fetch_all(
        """SELECT episode_id, entity_id, source_message_count,
                  first_seen_at, last_seen_at
           FROM public.episode_entities WHERE project_id = 'project-1'
           ORDER BY episode_id, entity_id"""
    )
    writer = GlobalEntityMergeWriter(real_postgres_client)
    merge_id = str(uuid4())

    await writer.merge(
        user_name="ada", survivor_id=2, retired_id=3, merge_id=merge_id
    )

    assert await _relationship_links(real_postgres_client) == [
        {
            "episode_id": "distinct-episode",
            "relationship_id": "project-1:2:4:uses",
            "source_message_count": 2,
        },
        {
            "episode_id": "overlap-episode",
            "relationship_id": "project-1:2:4:uses",
            "source_message_count": 1,
        },
    ]
    assert await real_postgres_client.fetch_all(
        """
        SELECT episode_id, entity_id, source_message_count
        FROM public.episode_entities WHERE project_id = 'project-1'
        ORDER BY episode_id, entity_id
        """
    ) == [
        {"episode_id": "distinct-episode", "entity_id": 2, "source_message_count": 2},
        {"episode_id": "overlap-episode", "entity_id": 2, "source_message_count": 1},
    ]

    rollback = await writer.plan_rollback(merge_id=merge_id, user_name="ada")
    assert rollback["conflicting_mutations"] == []
    result = await writer.rollback_safe(
        merge_id=merge_id,
        user_name="ada",
        safe_mutation_ids=rollback["safe_mutation_ids"],
    )
    assert result["rolled_back"] is True
    assert await _relationship_links(real_postgres_client) == before_links
    assert await real_postgres_client.fetch_all(
        """SELECT episode_id, entity_id, source_message_count,
                  first_seen_at, last_seen_at
           FROM public.episode_entities WHERE project_id = 'project-1'
           ORDER BY episode_id, entity_id"""
    ) == before_entities


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_merged_references_no_longer_claim_exact_occurrence_provenance(
    real_postgres_client,
):
    await _seed_episode_merge_collision(real_postgres_client)
    await real_postgres_client.execute(
        """UPDATE public.message_entity_refs SET occurrence_provenance_complete = true;
           UPDATE public.episode_entities SET occurrence_provenance_complete = true"""
    )

    await GlobalEntityMergeWriter(real_postgres_client).merge(
        user_name="ada", survivor_id=2, retired_id=3, merge_id=str(uuid4())
    )

    assert await real_postgres_client.fetch_one(
        """SELECT occurrence_provenance_complete FROM public.message_entity_refs
           WHERE message_id = 103 AND entity_id = 2"""
    ) == {"occurrence_provenance_complete": False}
    assert await real_postgres_client.fetch_one(
        """SELECT occurrence_provenance_complete FROM public.episode_entities
           WHERE episode_id = 'distinct-episode' AND entity_id = 2"""
    ) == {"occurrence_provenance_complete": False}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_global_merge_restores_a_collapsed_self_relationship_link(
    real_postgres_client,
):
    await _seed_episode_merge_collision(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.relationships (
            relationship_id, user_name, project_id, entity_a_id, entity_b_id,
            relationship_type
        ) VALUES ('project-1:2:3:knows', 'ada', 'project-1', 2, 3, 'knows');
        INSERT INTO public.episode_relationships (
            episode_id, project_id, relationship_id, source_message_count
        ) VALUES ('overlap-episode', 'project-1', 'project-1:2:3:knows', 1)
        """
    )
    before_links = await _relationship_links(real_postgres_client)
    writer = GlobalEntityMergeWriter(real_postgres_client)
    merge_id = str(uuid4())

    await writer.merge(user_name="ada", survivor_id=2, retired_id=3, merge_id=merge_id)

    assert not any(
        link["relationship_id"] == "project-1:2:3:knows"
        for link in await _relationship_links(real_postgres_client)
    )
    rollback = await writer.plan_rollback(merge_id=merge_id, user_name="ada")
    assert rollback["conflicting_mutations"] == []
    result = await writer.rollback_safe(
        merge_id=merge_id,
        user_name="ada",
        safe_mutation_ids=rollback["safe_mutation_ids"],
    )
    assert result["rolled_back"] is True
    assert await _relationship_links(real_postgres_client) == before_links


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_partial_rollback_keeps_changed_and_new_episode_links(
    real_postgres_client,
):
    window_id = await _seed_episode_merge_collision(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.relationships (
            relationship_id, user_name, project_id, entity_a_id, entity_b_id,
            relationship_type
        ) VALUES ('project-1:3:4:likes', 'ada', 'project-1', 3, 4, 'likes');
        INSERT INTO public.episode_relationships (
            episode_id, project_id, relationship_id, source_message_count
        ) VALUES ('overlap-episode', 'project-1', 'project-1:3:4:likes', 1)
        """
    )
    observation = await real_postgres_client.fetch_one(
        """
        INSERT INTO public.relationship_observations (
            relationship_id, project_id, user_name, semantic_window_id,
            source_entity_id, target_entity_id, observed_relationship_label,
            support_key, observed_at_ms
        ) VALUES ('project-1:3:4:likes', 'project-1', 'ada', %s, 3, 4,
                  'likes', %s, 101)
        RETURNING observation_id
        """,
        (window_id, "e" * 32),
    )
    await _support(real_postgres_client, observation["observation_id"], *RETIRED_MESSAGES)
    writer = GlobalEntityMergeWriter(real_postgres_client)
    merge_id = str(uuid4())
    await writer.merge(user_name="ada", survivor_id=2, retired_id=3, merge_id=merge_id)

    initial_plan = await writer.plan_rollback(merge_id=merge_id, user_name="ada")
    link_mutation = next(
        mutation for mutation in initial_plan["mutations"]
        if mutation["object_kind"] == "episode_relationship"
        and mutation["object_key"].endswith(":likes")
    )
    incomplete = await writer.rollback_safe(
        merge_id=merge_id,
        user_name="ada",
        safe_mutation_ids=[link_mutation["mutation_id"]],
    )
    assert incomplete["applied_mutation_ids"] == []

    new_observation = await real_postgres_client.fetch_one(
        """INSERT INTO public.relationship_observations (
               relationship_id, project_id, user_name, semantic_window_id,
               source_entity_id, target_entity_id,
               observed_relationship_label, support_key, observed_at_ms
           ) VALUES ('project-1:2:4:likes', 'project-1', 'ada', %s,
                     2, 4, 'likes', %s, 104)
           RETURNING observation_id""",
        (window_id, "9" * 32),
    )
    await _support(real_postgres_client, new_observation["observation_id"], *RETIRED_MESSAGES)
    untracked_plan = await writer.plan_rollback(
        merge_id=merge_id, user_name="ada"
    )
    assert any(
        mutation["object_kind"] == "relationship"
        and mutation["object_key"] == "project-1:3:4:likes"
        for mutation in untracked_plan["conflicting_mutations"]
    )

    await real_postgres_client.execute(
        """
        UPDATE public.episode_relationships
        SET source_message_count = 2
        WHERE episode_id = 'overlap-episode'
          AND relationship_id = 'project-1:2:4:likes';
        INSERT INTO public.episode_relationships (
            episode_id, project_id, relationship_id, source_message_count
        ) VALUES ('distinct-episode', 'project-1', 'project-1:2:4:likes', 1)
        """
    )
    plan = await writer.plan_rollback(merge_id=merge_id, user_name="ada")
    assert any(
        mutation["object_kind"] == "relationship"
        and mutation["object_key"] == "project-1:3:4:likes"
        for mutation in plan["conflicting_mutations"]
    )
    result = await writer.rollback_safe(
        merge_id=merge_id,
        user_name="ada",
        safe_mutation_ids=plan["safe_mutation_ids"],
    )
    assert result["rolled_back"] is False
    assert await real_postgres_client.fetch_one(
        """SELECT relationship_id FROM public.relationships
           WHERE relationship_id = 'project-1:2:4:likes'"""
    ) == {"relationship_id": "project-1:2:4:likes"}
    assert {
        "episode_id": "overlap-episode",
        "relationship_id": "project-1:2:4:likes",
        "source_message_count": 2,
    } in await _relationship_links(real_postgres_client)
    assert {
        "episode_id": "distinct-episode",
        "relationship_id": "project-1:2:4:likes",
        "source_message_count": 1,
    } in await _relationship_links(real_postgres_client)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_global_merge_restores_a_new_relationship_and_its_episode_link(
    real_postgres_client,
):
    window_id = await _seed_episode_merge_collision(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.relationships (
            relationship_id, user_name, project_id, entity_a_id, entity_b_id,
            relationship_type
        ) VALUES ('project-1:3:4:likes', 'ada', 'project-1', 3, 4, 'likes');
        INSERT INTO public.episode_relationships (
            episode_id, project_id, relationship_id, source_message_count
        ) VALUES ('overlap-episode', 'project-1', 'project-1:3:4:likes', 1)
        """
    )
    observation = await real_postgres_client.fetch_one(
        """
        INSERT INTO public.relationship_observations (
            relationship_id, project_id, user_name, semantic_window_id,
            source_entity_id, target_entity_id, observed_relationship_label,
            support_key, observed_at_ms
        ) VALUES ('project-1:3:4:likes', 'project-1', 'ada', %s, 3, 4,
                  'likes', %s, 101)
        RETURNING observation_id
        """,
        (window_id, "e" * 32),
    )
    await _support(real_postgres_client, observation["observation_id"], *RETIRED_MESSAGES)
    before_links = await _relationship_links(real_postgres_client)
    writer = GlobalEntityMergeWriter(real_postgres_client)
    merge_id = str(uuid4())

    await writer.merge(user_name="ada", survivor_id=2, retired_id=3, merge_id=merge_id)

    assert {
        "episode_id": "overlap-episode",
        "relationship_id": "project-1:2:4:likes",
        "source_message_count": 1,
    } in await _relationship_links(real_postgres_client)
    rollback = await writer.plan_rollback(merge_id=merge_id, user_name="ada")
    assert rollback["conflicting_mutations"] == []
    result = await writer.rollback_safe(
        merge_id=merge_id,
        user_name="ada",
        safe_mutation_ids=rollback["safe_mutation_ids"],
    )
    assert result["rolled_back"] is True
    assert await _relationship_links(real_postgres_client) == before_links
