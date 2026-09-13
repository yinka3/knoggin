"""Durable conflict-review lifecycle contracts against fresh PostgreSQL schema."""

import pytest

from common.schema.evidence import EvidencePointer, EvidenceSnapshot
from core.knowledge.db.readers.conflict_reader import ConflictReader
from core.knowledge.db.writers.conflict_writer import ConflictWriter


async def _seed_conflict_observations(client) -> tuple[int, int]:
    await client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Ada'), (3, 'ada', 'Acme')
        """
    )
    await client.execute(
        """
        INSERT INTO public.project_semantic_windows (
            window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, source_token_count, token_estimator,
            token_estimator_version, completed_at
        )
        VALUES
            ('11111111-1111-4111-8111-111111111111', 'ada', 'project-1',
             'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now()),
            ('22222222-2222-4222-8222-222222222222', 'ada', 'project-1',
             'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now())
        """
    )
    rows = await client.fetch_all(
        """
        INSERT INTO public.relationship_observations (
            project_id, user_name, semantic_window_id, source_entity_id,
            target_entity_id, observed_relationship_label, observed_at_ms
        )
        VALUES
            ('project-1', 'ada', '11111111-1111-4111-8111-111111111111',
             2, 3, 'works at', 100),
            ('project-1', 'ada', '22222222-2222-4222-8222-222222222222',
             2, 3, 'does not work at', 200)
        RETURNING observation_id
        """
    )
    return tuple(int(row["observation_id"]) for row in rows)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_conflict_resolution_is_durable_and_keeps_original_proposal_immutable(
    real_postgres_client,
):
    observation_ids = await _seed_conflict_observations(real_postgres_client)
    snapshot = EvidenceSnapshot(
        pointers=tuple(EvidencePointer.for_observation(value) for value in observation_ids),
        total_nodes=2,
        state_token="a" * 64,
    )
    writer = ConflictWriter(real_postgres_client)
    created = await writer.record_detection(
        user_name="ada",
        project_id="project-1",
        origin="background_discovery",
        kind="possible_contradiction",
        rationale="The two observations may describe incompatible employment states.",
        confidence=0.8,
        evidence_ids=observation_ids,
        evidence_snapshot=snapshot,
    )

    resolved = await writer.resolve(
        conflict_id=created.group.conflict_id,
        user_name="ada",
        project_id="project-1",
        resolution_kind="not_a_conflict",
        resolved_by="ada",
        resolution_note="The observations describe different dates.",
    )
    detail = await ConflictReader(real_postgres_client).get_detail(
        conflict_id=created.group.conflict_id,
        user_name="ada",
        project_id="project-1",
    )
    repeated = await writer.resolve(
        conflict_id=created.group.conflict_id,
        user_name="ada",
        project_id="project-1",
        resolution_kind="custom",
        resolved_by="other-actor",
        resolution_note="This must not replace the recorded classification.",
    )

    assert resolved.resolution_kind == "not_a_conflict"
    assert resolved.resolution_note == "The observations describe different dates."
    assert detail is not None
    assert detail["status"] == "applied"
    assert "resolution" not in detail["proposed_plan"]
    assert "note" not in detail["proposed_plan"]
    assert detail["resolution"] == {
        "resolution_kind": "not_a_conflict",
        "resolution_note": "The observations describe different dates.",
        "resolved_by": "ada",
        "resolved_at": detail["resolution"]["resolved_at"],
    }
    assert detail["resolution"]["resolved_at"] is not None
    assert repeated.resolution_kind == "not_a_conflict"
    assert repeated.resolution_note == "The observations describe different dates."
    assert await real_postgres_client.fetch_one(
        """
        SELECT resolution_kind, resolution_note, resolved_by, resolved_at IS NOT NULL AS has_time
        FROM public.maintenance_review_resolutions
        WHERE review_id = %s
        """,
        (created.group.conflict_id,),
    ) == {
        "resolution_kind": "not_a_conflict",
        "resolution_note": "The observations describe different dates.",
        "resolved_by": "ada",
        "has_time": True,
    }


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_conflict_review_reuses_equivalent_evidence_and_stales_only_changed_state(
    real_postgres_client,
):
    observation_ids = await _seed_conflict_observations(real_postgres_client)
    pointers = tuple(EvidencePointer.for_observation(value) for value in observation_ids)
    first_snapshot = EvidenceSnapshot(
        pointers=pointers,
        total_nodes=2,
        state_token="a" * 64,
    )
    changed_snapshot = EvidenceSnapshot(
        pointers=pointers,
        total_nodes=2,
        state_token="b" * 64,
    )
    writer = ConflictWriter(real_postgres_client)
    first = await writer.record_detection(
        user_name="ada",
        project_id="project-1",
        origin="background_discovery",
        kind="possible_contradiction",
        rationale="The first evidence state needs review.",
        confidence=0.8,
        evidence_ids=observation_ids,
        evidence_snapshot=first_snapshot,
    )
    successor = await writer.record_detection(
        user_name="ada",
        project_id="project-1",
        origin="background_discovery",
        kind="possible_contradiction",
        rationale="The cited evidence changed.",
        confidence=0.9,
        evidence_ids=observation_ids,
        evidence_snapshot=changed_snapshot,
    )
    reaffirmed = await writer.record_detection(
        user_name="ada",
        project_id="project-1",
        origin="agent_discovery",
        kind="possible_contradiction",
        rationale="Different model wording must not reopen the same evidence.",
        confidence=0.1,
        evidence_ids=tuple(reversed(observation_ids)),
        evidence_snapshot=changed_snapshot,
    )
    await writer.reviews.transition(
        successor.group.conflict_id,
        user_name="ada",
        project_id="project-1",
        status="dismissed",
        actor="ada",
        reason="The user dismissed this evidence state.",
    )
    dismissed_reaffirmed = await writer.record_detection(
        user_name="ada",
        project_id="project-1",
        origin="user_created",
        kind="possible_contradiction",
        rationale="Equivalent evidence must preserve dismissal.",
        confidence=0.5,
        evidence_ids=observation_ids,
        evidence_snapshot=changed_snapshot,
    )
    restored_state = await writer.record_detection(
        user_name="ada",
        project_id="project-1",
        origin="background_discovery",
        kind="possible_contradiction",
        rationale="The evidence later returned to the earlier state.",
        confidence=0.6,
        evidence_ids=observation_ids,
        evidence_snapshot=first_snapshot,
    )

    assert successor.created
    assert successor.group.conflict_id != first.group.conflict_id
    successor_detail = await ConflictReader(real_postgres_client).get_detail(
        conflict_id=successor.group.conflict_id,
        user_name="ada",
        project_id="project-1",
    )
    assert successor_detail is not None
    assert successor_detail["proposed_plan"]["supersedes_review_id"] == (
        first.group.conflict_id
    )
    assert not reaffirmed.created
    assert not reaffirmed.should_notify
    assert reaffirmed.group.conflict_id == successor.group.conflict_id
    assert not dismissed_reaffirmed.created
    assert not dismissed_reaffirmed.should_notify
    assert dismissed_reaffirmed.group.conflict_id == successor.group.conflict_id
    assert dismissed_reaffirmed.group.status == "resolved"
    assert restored_state.created
    assert restored_state.group.conflict_id not in {
        first.group.conflict_id,
        successor.group.conflict_id,
    }
    restored_detail = await ConflictReader(real_postgres_client).get_detail(
        conflict_id=restored_state.group.conflict_id,
        user_name="ada",
        project_id="project-1",
    )
    assert restored_detail is not None
    assert restored_detail["proposed_plan"]["supersedes_review_id"] == (
        successor.group.conflict_id
    )
    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.maintenance_reviews WHERE review_id = %s",
        (first.group.conflict_id,),
    ) == {"status": "stale"}
    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.maintenance_reviews WHERE review_id = %s",
        (successor.group.conflict_id,),
    ) == {"status": "dismissed"}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.maintenance_reviews",
    ) == {"count": 3}
