"""Historical maintenance review probes, superseded by Stage 5 contracts.

MC1–MC5 were repaired in Stage 5. These assertions intentionally describe the
former defects and are retained as skipped historical records; normal contract
tests now assert the desired behavior.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from common.schema.ingestion.contracts import (
    ContextBlockEntityAssociation,
    ContextRelationshipWrite,
)
from common.schema.semantic_window import SemanticWindowStage
from core.knowledge.conflict.conflict_discovery import ConflictPacketBuilder
from core.knowledge.conflict.conflicts import (
    ConflictDiscoveryCursor,
    ConflictDiscoveryPackage,
    LLMConflictCandidate,
)
from core.knowledge.db.readers.conflict_discovery_reader import ConflictDiscoveryReader
from core.knowledge.db.writers.conflict_writer import ConflictWriter
from core.knowledge.db.writers.semantic_commit_writer import SemanticCommitWriter
from core.knowledge.db.writers.semantic_window_writer import SemanticWindowWriter
from core.knowledge.entity.maintenance_service import EntityMaintenanceService
from core.knowledge.evidence_service import EvidenceService
from core.knowledge.store import KnowledgeStore
from core.project.maintenance_service import ProjectMaintenanceService
from tests.contract.storage.test_maintenance_application_real_postgres import (
    _seed_entities,
)
from tests.contract.storage.test_semantic_commit_contract import (
    _block,
    _build,
    _commit_context,
    _entity,
    _membership,
    _seed_message,
    _window,
    _WindowContext,
)

pytest_plugins = ["tests.contract.storage.conftest"]
pytestmark = [
    pytest.mark.storage,
    pytest.mark.requires_postgres,
    pytest.mark.no_network,
    pytest.mark.skip(
        reason=(
            "Historical MC1–MC5 reproductions resolved by Core Stage 5; normal "
            "conflict, maintenance, and semantic-commit contracts assert the "
            "desired behavior"
        )
    ),
]


async def seed_observations(client):
    """Use the actual Context and semantic commit writers for the first observation."""
    await _seed_message(client)
    window = _window()
    windows = SemanticWindowWriter(client)
    await windows.claim_window(window, _membership())
    block = _block("Sarah owns Delta.")
    snapshot = await _commit_context(client, window, (block,))
    await windows.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=snapshot.revision_id,
    )
    build = _build(
        _WindowContext(window.window_id, snapshot),
        impact=(block.block_id,),
        entity_ids=(10, 11),
        entities={
            10: _entity(10, "Sarah", "Person"),
            11: _entity(11, "Delta", "Company"),
        },
        associations=tuple(
            ContextBlockEntityAssociation(
                block_id=block.block_id, entity_id=i, mention_text=name
            )
            for i, name in [(10, "Sarah"), (11, "Delta")]
        ),
        relationship=ContextRelationshipWrite(
            support_block_ids=(block.block_id,),
            entity_a_id=10,
            entity_b_id=11,
            relationship_type="owns",
            canonical_type="OWNS",
            source_type="Person",
            target_type="Company",
            domain_version=1,
        ),
    )
    await SemanticCommitWriter(client).commit(build)
    first = await client.fetch_one(
        "SELECT observation_id FROM public.relationship_observations"
    )
    ids = [first["observation_id"]]
    # Additional observations only supply distinct evidence IDs for workflow tests.
    for label in ["disputed ownership", "ownership uncertain"]:
        row = await client.fetch_one(
            """INSERT INTO public.relationship_observations (
                relationship_id, project_id, user_name, semantic_window_id,
                source_entity_id, target_entity_id, observed_relationship_label, observed_at_ms
            ) SELECT relationship_id, project_id, user_name, semantic_window_id,
                source_entity_id, target_entity_id, %s, observed_at_ms
              FROM public.relationship_observations WHERE observation_id = %s
              RETURNING observation_id""",
            (label, ids[0]),
        )
        ids.append(row["observation_id"])
        await client.execute(
            """INSERT INTO public.relationship_observation_blocks (observation_id, project_id, block_id)
               VALUES (%s, 'project-1', %s)""",
            (ids[-1], block.block_id),
        )
    return ids


def service(client, store):
    return ProjectMaintenanceService(
        resources=SimpleNamespace(postgres=client, knowledge_store=store),
        user_name="ada",
        project_lookup=AsyncMock(return_value={"status": "active"}),
        active_projects={},
        project_leases={},
    )


def knowledge(client):
    from core.knowledge.db.readers.evidence_traversal_reader import (
        EvidenceTraversalReader,
    )

    store = object.__new__(KnowledgeStore)
    store._evidence_service = EvidenceService(EvidenceTraversalReader(client))
    return store


async def test_current_ingestion_evidence_is_skipped_by_discovery(real_postgres_client):
    ids = await seed_observations(real_postgres_client)
    reader = ConflictDiscoveryReader(real_postgres_client)
    cursor = ConflictDiscoveryCursor("ada", "project-1", 0)
    seeds = await reader.get_seed_observations(cursor, max_span_days=60)
    assert {row["evidence_origin"] for row in seeds} == {"context"}
    store = knowledge(real_postgres_client)

    async def load_evidence(observation_ids):
        return await store.get_relationship_observations_evidence(
            observation_ids,
            user_name="ada",
            project_id="project-1",
        )

    package = await ConflictPacketBuilder(reader, evidence_loader=load_evidence).build(
        cursor, max_span_days=60, max_tokens=5000
    )
    assert package.observations == ()
    assert package.next_observation_id == max(ids)


async def test_agent_conflict_is_immediately_changed_on_detail(real_postgres_client):
    ids = await seed_observations(real_postgres_client)
    owner = service(real_postgres_client, knowledge(real_postgres_client))
    owner._conflict_service.notify_detection = AsyncMock()
    result = await owner.record_conflict_detection(
        "project-1",
        origin="agent_discovery",
        kind="possible_contradiction",
        rationale="The ownership observations disagree.",
        confidence=0.8,
        evidence_ids=ids[:2],
    )
    detail = await owner.get_maintenance_review_detail(
        "project-1", result.group.conflict_id
    )
    assert detail.evidence_state == "changed"
    assert detail.stored_snapshot.total_nodes == 0
    assert detail.current_evidence[0].total_nodes > 0


async def test_background_snapshot_includes_uncited_packet_evidence(
    real_postgres_client,
):
    ids = await seed_observations(real_postgres_client)
    store = knowledge(real_postgres_client)
    bundles = await store.get_relationship_observations_evidence(
        ids, user_name="ada", project_id="project-1"
    )
    owner = service(real_postgres_client, store)
    owner._conflict_service.notify_detection = AsyncMock()
    package = ConflictDiscoveryPackage(
        cursor=ConflictDiscoveryCursor("ada", "project-1", 0),
        observations=tuple({"observation_id": i} for i in ids),
        next_observation_id=max(ids),
        prompt="packet",
        estimated_tokens=100,
        evidence_bundles=bundles,
    )
    await owner.complete_conflict_discovery(
        package,
        candidates=[
            LLMConflictCandidate(
                evidence_ids=ids[:2],
                kind="possible_contradiction",
                rationale="Two observations disagree.",
                confidence=0.8,
            )
        ],
    )
    reviews = await owner.list_maintenance_reviews("project-1")
    detail = await owner.get_maintenance_review_detail(
        "project-1", reviews[0].review_id
    )
    assert detail.evidence_state == "changed"
    assert len(detail.review.evidence_refs) == 2
    assert any(
        p.identifier == str(ids[2]) and p.kind == "relationship_observation"
        for p in detail.stored_snapshot.pointers
    )


@pytest.mark.parametrize("dismiss_first", [False, True])
async def test_confidence_change_duplicates_conflict_without_notification(
    real_postgres_client, dismiss_first
):
    ids = await seed_observations(real_postgres_client)
    writer = ConflictWriter(real_postgres_client)
    kwargs = dict(
        user_name="ada",
        project_id="project-1",
        origin="agent_discovery",
        kind="possible_contradiction",
        rationale="The same two observations disagree.",
        evidence_ids=ids[:2],
    )
    first = await writer.record_detection(**kwargs, confidence=0.8)
    if dismiss_first:
        await writer.reviews.transition(
            first.group.conflict_id,
            user_name="ada",
            project_id="project-1",
            status="dismissed",
        )
    second = await writer.record_detection(**kwargs, confidence=0.9)
    assert first.group.conflict_id != second.group.conflict_id
    assert not second.created and not second.should_notify
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.maintenance_reviews WHERE status = 'open'"
    ) == {"count": 1 if dismiss_first else 2}


async def test_resolution_kind_is_not_preserved_in_review_plan(real_postgres_client):
    ids = await seed_observations(real_postgres_client)
    writer = ConflictWriter(real_postgres_client)
    result = await writer.record_detection(
        user_name="ada",
        project_id="project-1",
        origin="agent_discovery",
        kind="possible_contradiction",
        rationale="The observations disagree.",
        confidence=0.8,
        evidence_ids=ids[:2],
    )
    resolved = await writer.resolve(
        conflict_id=result.group.conflict_id,
        user_name="ada",
        project_id="project-1",
        resolution_kind="normal_temporal_change",
        resolved_by="ada",
        resolution_note="The move happened in June.",
    )
    assert resolved.resolution_kind == "normal_temporal_change"
    review = await writer.reviews.get(
        result.group.conflict_id, user_name="ada", project_id="project-1"
    )
    assert review.status == "applied"
    assert review.proposed_plan.resolution is None and review.proposed_plan.note is None
    events = await real_postgres_client.fetch_all(
        "SELECT reason FROM public.maintenance_review_events WHERE review_id = %s",
        (review.review_id,),
    )
    assert any(row["reason"] == "The move happened in June." for row in events)
    assert all(row["reason"] != "normal_temporal_change" for row in events)


async def test_cancel_after_merge_commit_leaves_no_pending_repair_marker(
    real_postgres_client, monkeypatch
):
    await _seed_entities(real_postgres_client)
    owner = EntityMaintenanceService(real_postgres_client, "ada")
    preview = await owner.preview_merge(survivor_entity_id=2, retired_entity_id=3)

    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(owner, "_rebuild_projections", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await owner.merge(preview["plan"])
    row = await real_postgres_client.fetch_one(
        "SELECT status, failure_reason FROM public.entity_global_merge_audits"
    )
    assert row == {"status": "executed", "failure_reason": None}
    assert (await owner.projection_repair_health())["pending_count"] == 0
    entity = await real_postgres_client.fetch_one(
        "SELECT status FROM public.entities WHERE entity_id = 3"
    )
    assert entity["status"] == "redirected"
