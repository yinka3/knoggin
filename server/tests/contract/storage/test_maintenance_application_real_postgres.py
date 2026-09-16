import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.writers.episode_writer import EpisodeWriter
from core.knowledge.db.writers.graph_writer import GraphWriter
from core.knowledge.entity.maintenance_service import EntityMaintenanceService
from core.knowledge.maintenance.maintenance_reviews import (
    RelationshipInterpretationChange,
    RelationshipInterpretationPlan,
)
from core.project.maintenance_service import ProjectMaintenanceService


async def _project(_project_id):
    return {"project_id": "project-1", "status": "active"}


async def _seed_entities(client):
    await client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Ada Lovelace'), (3, 'ada', 'Augusta Ada King')
        """
    )
    await client.execute(
        """
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        )
        VALUES
            ('project-1', 2, 'ada', 'Concept', 'General'),
            ('project-1', 3, 'ada', 'Concept', 'General')
        """
    )


async def _seed_context_block(client, associations, *, project_id="project-1"):
    block_id = uuid4()
    await client.execute(
        """
        INSERT INTO public.project_context_blocks (
            block_id, project_id, section_key, markdown, content_hash,
            assertion_kind
        ) VALUES (%s, %s, 'current_state', 'Ada identity evidence.',
                  %s, 'source_grounded')
        """,
        (block_id, project_id, "a" * 64),
    )
    for entity_id, mention_text in associations:
        await client.execute(
            """
            INSERT INTO public.context_block_entities (
                block_id, project_id, entity_id, mention_text
            ) VALUES (%s, %s, %s, %s)
            """,
            (block_id, project_id, entity_id, mention_text),
        )
    return block_id


async def _seed_semantic_window(
    client,
    *,
    origin,
    stage,
    project_id="project-1",
):
    window_id = uuid4()
    await client.execute(
        """
        INSERT INTO public.project_semantic_windows (
            window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, source_token_count, token_estimator,
            token_estimator_version, completed_at
        ) VALUES (
            %s, 'ada', %s, %s, %s, 1, '{}'::jsonb, 0, 'test', 'v1',
            CASE WHEN %s = 'completed' THEN NOW() ELSE NULL END
        )
        """,
        (window_id, project_id, origin, stage, stage),
    )
    return window_id


async def _complete_semantic_window(client, window_id):
    await client.execute(
        """
        UPDATE public.project_semantic_windows
        SET stage = 'completed', completed_at = NOW(), updated_at = NOW()
        WHERE window_id = %s
        """,
        (window_id,),
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_confirmed_global_merge_and_rollback_repair_durable_state(
    real_postgres_client,
):
    await _seed_entities(real_postgres_client)
    block_id = await _seed_context_block(
        real_postgres_client,
        [(3, "Augusta Ada King")],
    )
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )
    review = await service.review_writer.open(
        user_name="ada",
        scope="user-global",
        project_id=None,
        kind="entity_merge",
        reasoning="Both names identify the same person.",
        proposed_plan=preview["plan"],
        expected_state={
            "state_hash": preview["state_hash"],
            "frontiers": preview["frontiers"],
            "definition_versions": preview["definition_versions"],
        },
    )

    merged = await service.apply_merge_review(
        review.review_id,
        expected_state=review.expected_state,
    )

    assert merged["projection_errors"] == []
    assert await real_postgres_client.fetch_one(
        """
        SELECT status, redirect_entity_id
        FROM public.entities
        WHERE entity_id = 3
        """
    ) == {"status": "redirected", "redirect_entity_id": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.maintenance_reviews WHERE review_id = %s",
        (review.review_id,),
    ) == {"status": "applied"}
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id
        FROM public.project_entity_contexts
        WHERE project_id = 'project-1'
        ORDER BY entity_id
        """
    ) == [{"entity_id": 2}]
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, mention_text
        FROM public.context_block_entities
        WHERE block_id = %s
        """,
        (block_id,),
    ) == [{"entity_id": 2, "mention_text": "Augusta Ada King"}]

    rolled_back = await service.rollback(merged["merge_id"])

    assert rolled_back["rolled_back"] is True, rolled_back
    assert rolled_back["projection_errors"] == []
    assert await real_postgres_client.fetch_one(
        """
        SELECT status, redirect_entity_id
        FROM public.entities
        WHERE entity_id = 3
        """
    ) == {"status": "active", "redirect_entity_id": None}
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id
        FROM public.project_entity_contexts
        WHERE project_id = 'project-1'
        ORDER BY entity_id
        """
    ) == [{"entity_id": 2}, {"entity_id": 3}]
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, mention_text
        FROM public.context_block_entities
        WHERE block_id = %s
        """,
        (block_id,),
    ) == [{"entity_id": 3, "mention_text": "Augusta Ada King"}]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_cancelled_merge_projection_repair_is_durable_and_retries_from_audit(
    real_postgres_client,
    monkeypatch,
):
    """A post-commit interruption must not lose the projection-repair obligation."""

    await _seed_entities(real_postgres_client)
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )

    async def cancel_before_rebuild(*_args, **_kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(
        service.projection_rebuilder,
        "rebuild_project_projection",
        cancel_before_rebuild,
    )

    with pytest.raises(asyncio.CancelledError):
        await service.merge(preview["plan"])

    audit = await real_postgres_client.fetch_one(
        """
        SELECT merge_id, status, failure_reason
        FROM public.entity_global_merge_audits
        WHERE user_name = 'ada' AND survivor_entity_id = 2 AND retired_entity_id = 3
        """
    )
    assert audit is not None
    assert audit["status"] == "executed"
    assert audit["failure_reason"] == "projection_repair_pending"
    assert await service.projection_repair_health() == {
        "pending_count": 1,
        "truncated": False,
    }
    mutation_count = await real_postgres_client.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.entity_global_merge_mutations
        WHERE merge_id = %s
        """,
        (audit["merge_id"],),
    )
    assert mutation_count is not None

    restarted_service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    repaired = await restarted_service.repair_merge_projections(audit["merge_id"])

    assert repaired["canonical_status"] == "executed"
    assert repaired["projection_repaired"] is True
    assert await restarted_service.projection_repair_health() == {
        "pending_count": 0,
        "truncated": False,
    }
    assert await real_postgres_client.fetch_one(
        """
        SELECT status, redirect_entity_id
        FROM public.entities
        WHERE entity_id = 3
        """
    ) == {"status": "redirected", "redirect_entity_id": 2}
    assert await real_postgres_client.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.entity_global_merge_mutations
        WHERE merge_id = %s
        """,
        (audit["merge_id"],),
    ) == mutation_count


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_cancellation_after_merge_projection_rebuild_keeps_durable_marker(
    real_postgres_client,
    monkeypatch,
):
    """Cancellation between successful rebuild and marker clearing is recoverable."""

    await _seed_entities(real_postgres_client)
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )
    rebuilt = False
    rebuild = service.projection_rebuilder.rebuild_project_projection

    async def rebuild_then_continue(*args, **kwargs):
        nonlocal rebuilt
        result = await rebuild(*args, **kwargs)
        rebuilt = True
        return result

    async def cancel_before_clearing_marker(*_args, **_kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(
        service.projection_rebuilder,
        "rebuild_project_projection",
        rebuild_then_continue,
    )
    monkeypatch.setattr(
        service,
        "_record_projection_repair_state",
        cancel_before_clearing_marker,
    )

    with pytest.raises(asyncio.CancelledError):
        await service.merge(preview["plan"])

    assert rebuilt is True
    audit = await real_postgres_client.fetch_one(
        """
        SELECT merge_id, failure_reason
        FROM public.entity_global_merge_audits
        WHERE user_name = 'ada' AND survivor_entity_id = 2 AND retired_entity_id = 3
        """
    )
    assert audit is not None
    assert audit["failure_reason"] == "projection_repair_pending"

    repaired = await EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    ).repair_merge_projections(audit["merge_id"])

    assert repaired["projection_repaired"] is True
    assert await real_postgres_client.fetch_one(
        "SELECT failure_reason FROM public.entity_global_merge_audits WHERE merge_id = %s",
        (audit["merge_id"],),
    ) == {"failure_reason": None}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_cancelled_rollback_projection_repair_is_durable_and_retries_from_audit(
    real_postgres_client,
    monkeypatch,
):
    """Rollback records the same durable repair obligation before rebuilding."""

    await _seed_entities(real_postgres_client)
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )
    merged = await service.merge(preview["plan"])

    async def cancel_before_rebuild(*_args, **_kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(
        service.projection_rebuilder,
        "rebuild_project_projection",
        cancel_before_rebuild,
    )

    with pytest.raises(asyncio.CancelledError):
        await service.rollback(merged["merge_id"])

    audit = await real_postgres_client.fetch_one(
        """
        SELECT status, failure_reason
        FROM public.entity_global_merge_audits
        WHERE merge_id = %s
        """,
        (merged["merge_id"],),
    )
    assert audit == {
        "status": "rolled_back",
        "failure_reason": "projection_repair_pending",
    }
    assert await real_postgres_client.fetch_one(
        "SELECT status, redirect_entity_id FROM public.entities WHERE entity_id = 3"
    ) == {"status": "active", "redirect_entity_id": None}

    restarted_service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    repaired = await restarted_service.repair_merge_projections(merged["merge_id"])

    assert repaired["canonical_status"] == "rolled_back"
    assert repaired["projection_repaired"] is True
    assert await restarted_service.projection_repair_health() == {
        "pending_count": 0,
        "truncated": False,
    }


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_partial_multi_project_projection_repair_stays_pending_until_retry(
    real_postgres_client,
    monkeypatch,
):
    await _seed_entities(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-2', 2, 'ada', 'Concept', 'General'),
            ('project-2', 3, 'ada', 'Concept', 'General')
        """
    )
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )
    rebuild = service.projection_rebuilder.rebuild_project_projection

    async def fail_project_two(project_id, user_name):
        if project_id == "project-2":
            raise RuntimeError("project-2 projection unavailable")
        return await rebuild(project_id, user_name)

    monkeypatch.setattr(
        service.projection_rebuilder,
        "rebuild_project_projection",
        fail_project_two,
    )

    merged = await service.merge(preview["plan"])

    assert merged["projection_errors"] == [
        {"project_id": "project-2", "error": "project-2 projection unavailable"}
    ]
    assert await service.projection_repair_health() == {
        "pending_count": 1,
        "truncated": False,
    }

    restarted_service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    repaired = await restarted_service.repair_merge_projections(merged["merge_id"])

    assert repaired["affected_project_ids"] == ["project-1", "project-2"]
    assert repaired["projection_repaired"] is True
    assert await restarted_service.projection_repair_health() == {
        "pending_count": 0,
        "truncated": False,
    }


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_global_merge_deduplicates_context_associations_and_preserves_changed_residue(
    real_postgres_client,
):
    await _seed_entities(real_postgres_client)
    moved_block_id = await _seed_context_block(
        real_postgres_client,
        [(3, "Augusta")],
    )
    collision_block_id = await _seed_context_block(
        real_postgres_client,
        [(2, "Ada Lovelace"), (3, "Ada")],
    )
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )

    assert preview["plan"].context_block_association_counts == {"project-1": 2}
    merged = await service.merge(preview["plan"])

    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, mention_text
        FROM public.context_block_entities
        WHERE block_id = %s
        ORDER BY entity_id
        """,
        (moved_block_id,),
    ) == [{"entity_id": 2, "mention_text": "Augusta"}]
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, mention_text
        FROM public.context_block_entities
        WHERE block_id = %s
        ORDER BY entity_id
        """,
        (collision_block_id,),
    ) == [{"entity_id": 2, "mention_text": "Ada Lovelace"}]
    mutations = await real_postgres_client.fetch_all(
        """
        SELECT object_kind, object_key
        FROM public.entity_global_merge_mutations
        WHERE merge_id = %s AND object_kind = 'context_block_entity'
        ORDER BY object_key
        """,
        (merged["merge_id"],),
    )
    assert {item["object_key"] for item in mutations} == {
        str(moved_block_id),
        str(collision_block_id),
    }

    await real_postgres_client.execute(
        """
        UPDATE public.context_block_entities
        SET mention_text = 'Changed after merge'
        WHERE block_id = %s AND entity_id = 2
        """,
        (moved_block_id,),
    )
    rollback_plan = await service.plan_rollback(merged["merge_id"])
    association_conflicts = [
        item
        for item in rollback_plan["conflicting_mutations"]
        if item["object_kind"] == "context_block_entity"
    ]
    assert [item["object_key"] for item in association_conflicts] == [
        str(moved_block_id)
    ]

    rolled_back = await service.rollback(merged["merge_id"])

    assert rolled_back["rolled_back"] is False
    assert any(
        item["object_kind"] == "context_block_entity"
        and item["object_key"] == str(moved_block_id)
        for item in rolled_back["conflicts"]
    )
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, mention_text
        FROM public.context_block_entities
        WHERE block_id = %s
        ORDER BY entity_id
        """,
        (moved_block_id,),
    ) == [{"entity_id": 2, "mention_text": "Changed after merge"}]
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, mention_text
        FROM public.context_block_entities
        WHERE block_id = %s
        ORDER BY entity_id
        """,
        (collision_block_id,),
    ) == [
        {"entity_id": 2, "mention_text": "Ada Lovelace"},
        {"entity_id": 3, "mention_text": "Ada"},
    ]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_global_merge_rejects_a_preview_when_context_association_changes(
    real_postgres_client,
):
    await _seed_entities(real_postgres_client)
    block_id = await _seed_context_block(real_postgres_client, [(3, "Augusta")])
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )
    await real_postgres_client.execute(
        """
        UPDATE public.context_block_entities
        SET mention_text = 'Updated before merge'
        WHERE block_id = %s AND entity_id = 3
        """,
        (block_id,),
    )

    with pytest.raises(ValueError, match="entity evidence changed"):
        await service.merge(preview["plan"])

    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.entities WHERE entity_id = 3"
    ) == {"status": "active"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_global_merge_frontier_excludes_disabled_deleted_and_prefrontier_exchanges(
    real_postgres_client,
):
    await _seed_entities(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (
            session_id, user_name, project_id, status,
            semantic_participation_enabled,
            semantic_participation_after_message_id
        ) VALUES
            ('maintenance-disabled', 'ada', 'project-1', 'open', FALSE, 0),
            ('maintenance-deleted', 'ada', 'project-1', 'deleted', TRUE, 0),
            ('maintenance-reenabled', 'ada', 'project-1', 'open', TRUE, 403)
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES
            ('ada', 'maintenance-disabled', 401, 'project-1', 'user', 'Disabled.',
             401, 'sealed', 'closed', 'user_only', 401),
            ('ada', 'maintenance-deleted', 402, 'project-1', 'user', 'Deleted.',
             402, 'sealed', 'closed', 'user_only', 402),
            ('ada', 'maintenance-reenabled', 403, 'project-1', 'user', 'Pre-frontier.',
             403, 'sealed', 'closed', 'user_only', 403)
        """
    )
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )

    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )

    assert preview["frontiers"]["project-1"]["message_id"] == 0
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES (
            'ada', 'maintenance-reenabled', 404, 'project-1', 'user',
            'Post-frontier.', 404, 'sealed', 'closed', 'user_only', 404
        )
        """
    )

    with pytest.raises(ValueError, match="ingestion advanced after review"):
        await service.merge(preview["plan"])

    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.entities WHERE entity_id = 3"
    ) == {"status": "active"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_maintenance_frontier_blocks_all_active_window_origins_and_tracks_completion(
    real_postgres_client,
):
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    human_window_id = await _seed_semantic_window(
        real_postgres_client,
        origin="human_edit",
        stage="context_committed",
    )

    with pytest.raises(RuntimeError, match="active semantic windows"):
        await service.capture_frontier(["project-1"])

    await _complete_semantic_window(real_postgres_client, human_window_id)
    human_frontier = await service.capture_frontier(["project-1"])
    assert (
        str(human_window_id) in human_frontier["project-1"]["completed_window_boundary"]
    )

    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('maintenance-conversation', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES (
            'ada', 'maintenance-conversation', 501, 'project-1', 'user',
            'Conversation semantic work.', 501, 'sealed', 'closed',
            'user_only', 501
        )
        """
    )
    conversation_window_id = await _seed_semantic_window(
        real_postgres_client,
        origin="conversation",
        stage="claimed",
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_semantic_window_messages (
            window_id, project_id, message_id, session_id,
            exchange_user_message_id, role, ordinal
        ) VALUES (%s, 'project-1', 501, 'maintenance-conversation', 501, 'user', 0)
        """,
        (conversation_window_id,),
    )

    with pytest.raises(RuntimeError, match="active semantic windows"):
        await service.capture_frontier(["project-1"])

    await _complete_semantic_window(real_postgres_client, conversation_window_id)
    completed_frontier = await service.capture_frontier(["project-1"])
    assert (
        completed_frontier["project-1"]["token"] != human_frontier["project-1"]["token"]
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_cleanup_removes_context_associations_without_reintroducing_episode_links(
    real_postgres_client,
):
    await _seed_entities(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-2', 2, 'ada', 'Concept', 'General')
        """
    )
    block_id = await _seed_context_block(real_postgres_client, [(2, "Ada")])
    other_project_block_id = await _seed_context_block(
        real_postgres_client,
        [(2, "Ada")],
        project_id="project-2",
    )
    preview = await EntityReader(
        real_postgres_client
    ).preview_project_entity_cleanup(
        user_name="ada",
        project_id="project-1",
    )
    assert next(
        item["context_block_association_count"]
        for item in preview
        if item["entity_id"] == 2
    ) == 1

    revision_id = uuid4()
    window_id = uuid4()
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('cleanup-session', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms
        ) VALUES (
            'ada', 'cleanup-session', 901, 'project-1', 'user',
            'Ada is part of the current project.', 1000
        )
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.episodes (episode_id, project_id, summary)
        VALUES ('cleanup-episode', 'project-1', 'Ada is current project context.')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.episode_messages (
            episode_id, project_id, session_id, message_id, message_position
        ) VALUES ('cleanup-episode', 'project-1', 'cleanup-session', 901, 0)
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_context_revisions (
            revision_id, project_id, revision_number, origin, domain_version,
            content_hash
        ) VALUES (%s, 'project-1', 1, 'conversation', 1, %s)
        """,
        (revision_id, "a" * 64),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_context_revision_blocks (
            revision_id, project_id, block_id, ordinal
        ) VALUES (%s, 'project-1', %s, 0)
        """,
        (revision_id, block_id),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_semantic_windows (
            window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, source_token_count, token_estimator,
            token_estimator_version, context_revision_id
        ) VALUES (
            %s, 'ada', 'project-1', 'conversation', 'knowledge_committed', 1,
            '{}'::jsonb, 0, 'test', 'v1', %s
        )
        """,
        (window_id, revision_id),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_semantic_window_episodes (
            window_id, project_id, episode_id, ordinal
        ) VALUES (%s, 'project-1', 'cleanup-episode', 0)
        """,
        (window_id,),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_context_block_supports (
            block_id, project_id, message_id, session_id, support_kind
        ) VALUES (%s, 'project-1', 901, 'cleanup-session', 'user_message')
        """,
        (block_id,),
    )

    episode_writer = EpisodeWriter(real_postgres_client)
    assert await episode_writer.enrich_project_semantic_window_episodes(
        window_id=str(window_id),
        user_name="ada",
        project_id="project-1",
    ) == {"entities": 1, "relationships": 0}
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id
        FROM public.episode_entities
        WHERE project_id = 'project-1' AND episode_id = 'cleanup-episode'
        """
    ) == [{"entity_id": 2}]

    assert await GraphWriter(real_postgres_client).delete_selected_project_entities(
        [2],
        user_name="ada",
        project_id="project-1",
    ) == [2]

    assert await real_postgres_client.fetch_all(
        """
        SELECT block_id::text AS block_id, project_id, entity_id
        FROM public.context_block_entities
        WHERE entity_id = 2
        ORDER BY project_id, block_id
        """
    ) == [
        {
            "block_id": str(other_project_block_id),
            "project_id": "project-2",
            "entity_id": 2,
        }
    ]
    assert await real_postgres_client.fetch_all(
        """
        SELECT project_id, entity_id
        FROM public.project_entity_contexts
        WHERE entity_id = 2
        ORDER BY project_id
        """
    ) == [{"project_id": "project-2", "entity_id": 2}]
    assert await real_postgres_client.fetch_one(
        "SELECT entity_id FROM public.entities WHERE entity_id = 2"
    ) == {"entity_id": 2}
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id
        FROM public.episode_entities
        WHERE project_id = 'project-1' AND episode_id = 'cleanup-episode'
        """
    ) == []
    assert await episode_writer.enrich_project_semantic_window_episodes(
        window_id=str(window_id),
        user_name="ada",
        project_id="project-1",
    ) == {"entities": 0, "relationships": 0}
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id
        FROM public.episode_entities
        WHERE project_id = 'project-1' AND episode_id = 'cleanup-episode'
        """
    ) == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_review_application_reinterprets_before_marking_applied(
    real_postgres_client,
):
    await _seed_entities(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state
        )
        VALUES (
            'ada', 'session-1', 101, 'project-1', 'user', 'Ada met Augusta.',
            1000, 'sealed'
        )
        """
    )
    relationship_id = "project-1:2:3:related_to"
    await real_postgres_client.execute(
        """
        INSERT INTO public.relationships (
            relationship_id, user_name, project_id, entity_a_id, entity_b_id,
            relationship_type, "symmetric"
        )
        VALUES (%s, 'ada', 'project-1', 2, 3, 'RELATED_TO', false)
        """,
        (relationship_id,),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_semantic_windows (
            window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, source_token_count, token_estimator,
            token_estimator_version, completed_at
        )
        VALUES (
            '11111111-1111-4111-8111-111111111111', 'ada', 'project-1',
            'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now()
        )
        """
    )
    observation = await real_postgres_client.fetch_one(
        """
        INSERT INTO public.relationship_observations (
            relationship_id, project_id, user_name, semantic_window_id,
            source_entity_id, target_entity_id, observed_relationship_label,
            observed_at_ms
        )
        VALUES (
            %s, 'project-1', 'ada', '11111111-1111-4111-8111-111111111111',
            2, 3, 'related to', 1000
        )
        RETURNING observation_id
        """,
        (relationship_id,),
    )
    resources = SimpleNamespace(
        postgres=real_postgres_client,
        knowledge_store=object(),
    )
    service = ProjectMaintenanceService(
        resources=resources,
        user_name="ada",
        project_lookup=_project,
        active_projects={},
        project_leases={},
    )
    review = await service._maintenance_reviews.open(
        user_name="ada",
        scope="project",
        project_id="project-1",
        kind="relationship_interpretation",
        reasoning="This extracted edge should remain evidence but leave the graph.",
        proposed_plan=RelationshipInterpretationPlan(
            changes=[
                RelationshipInterpretationChange(
                    observation_id=observation["observation_id"],
                    expected_relationship_id=relationship_id,
                    target_relationship_type=None,
                    interpretation_source="review",
                )
            ]
        ),
        expected_state={"domain_version": 1},
    )

    applied = await service.transition_maintenance_review(
        "project-1",
        review.review_id,
        status="applied",
        expected_state=review.expected_state,
    )

    assert applied.status == "applied"
    assert await real_postgres_client.fetch_one(
        """
        SELECT relationship_id, interpretation_source
        FROM public.relationship_observations
        WHERE observation_id = %s
        """,
        (observation["observation_id"],),
    ) == {"relationship_id": None, "interpretation_source": "review"}
    assert await real_postgres_client.fetch_one(
        "SELECT relationship_id FROM public.relationships WHERE relationship_id = %s",
        (relationship_id,),
    ) is None
