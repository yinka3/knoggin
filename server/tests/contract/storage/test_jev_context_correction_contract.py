"""Reviewed JEV-linked type/topic correction is scoped and transactional."""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from core.knowledge.db.readers.evidence_traversal_reader import EvidenceTraversalReader
from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.db.writers.entity_reclassification_writer import (
    EntityReclassificationWriter,
)
from core.knowledge.entity.maintenance_service import EntityMaintenanceService
from core.knowledge.evidence_service import EvidenceService
from core.project.maintenance_service import ProjectMaintenanceService


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_jev_context_correction_review_applies_and_protects_user_choice(real_postgres_client):
    client = real_postgres_client
    block_id, window_id = uuid4(), uuid4()
    await client.execute(
        "INSERT INTO public.entities (entity_id, user_name, canonical_name) VALUES (2, 'ada', 'Orion')"
    )
    await client.execute(
        """INSERT INTO public.project_entity_contexts
           (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-1', 2, 'ada', 'Concept', 'General')"""
    )
    await client.execute(
        """INSERT INTO public.project_context_blocks
           (block_id, project_id, section_key, markdown, content_hash, assertion_kind)
           VALUES (%s, 'project-1', 'current_state', 'Orion is a person.', %s, 'source_grounded')""",
        (block_id, "a" * 64),
    )
    await client.execute(
        """INSERT INTO public.project_semantic_windows
           (window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, source_token_count, token_estimator, token_estimator_version,
            completed_at)
           VALUES (%s, 'ada', 'project-1', 'conversation', 'completed', 1,
                   '{}'::jsonb, 0, 'test', 'v1', now())""",
        (window_id,),
    )
    decision = {
        "entity_id": 2, "baseline_topic": "General",
        "suggested_topic": "General", "operational_topic": "General",
        "record": {"evidence_block_ids": [str(block_id)]},
    }
    await client.execute(
        """INSERT INTO public.project_jev_classification_decisions
           (window_id, project_id, pass_number, occurrence_key, decision, entity_id,
            entity_type, baseline_topic, suggested_topic, accepted_topic,
            aggregation_status, operational_topic, option_set_truncated)
           VALUES (%s, 'project-1', 1, '0:topic', %s::jsonb, 2,
                   'Concept', 'General', 'General', NULL,
                   'consistent_proposal', 'General', false)""",
        (window_id, json.dumps(decision)),
    )

    class Store:
        async def get_jev_decision(self, *args, **kwargs):
            return await SemanticWindowReader(client).get_jev_decision(*args, **kwargs)

        async def get_context_block_evidence(self, block, **kwargs):
            return await EvidenceService(EvidenceTraversalReader(client)).for_context_block(
                block, **kwargs,
            )

    async def project_lookup(project_id):
        return {"status": "active"} if project_id == "project-1" else None

    service = ProjectMaintenanceService(
        resources=SimpleNamespace(postgres=client, knowledge_store=Store()),
        user_name="ada", project_lookup=project_lookup,
        active_projects={}, project_leases={},
    )
    review = await service.flag_jev_entity_context_decision(
        "project-1", capability="classification", window_id=str(window_id),
        pass_number=1, occurrence_key="0:topic", entity_id=2,
        entity_type="Identity", topic="Identity",
        reasoning="The source describes a person, not a concept.",
    )
    assert review.status == "open"
    detail, impact = await service.preview_maintenance_review("project-1", review.review_id)
    assert detail.evidence_state == "current"
    assert impact.impacts
    await client.execute(
        """UPDATE public.project_entity_contexts SET topic = 'Identity'
           WHERE project_id = 'project-1' AND entity_id = 2"""
    )
    with pytest.raises(ValueError, match="changed"):
        await service.transition_maintenance_review(
            "project-1", review.review_id, status="applied",
            expected_state=review.expected_state,
        )
    assert (await service.get_maintenance_review_detail(
        "project-1", review.review_id,
    )).review.status == "open"
    await client.execute(
        """UPDATE public.project_entity_contexts SET topic = 'General'
           WHERE project_id = 'project-1' AND entity_id = 2"""
    )
    applied = await service.transition_maintenance_review(
        "project-1", review.review_id, status="applied",
        expected_state=review.expected_state,
    )
    assert applied.status == "applied"
    context = await client.fetch_one(
        """SELECT entity_type, topic, human_correction_review_id
           FROM public.project_entity_contexts
           WHERE project_id = 'project-1' AND entity_id = 2"""
    )
    assert context == {
        "entity_type": "Identity", "topic": "Identity",
        "human_correction_review_id": review.review_id,
    }
    assert await EntityReclassificationWriter(client).fetch_entities(
        user_name="ada", project_id="project-1",
    ) == []
    await client.execute(
        "INSERT INTO public.entities (entity_id, user_name, canonical_name) VALUES (3, 'ada', 'Orion twin')"
    )
    with pytest.raises(ValueError, match="Reopen active human"):
        await EntityMaintenanceService(client, "ada").preview_merge(
            survivor_entity_id=2, retired_entity_id=3,
        )
    reopened = await service.reopen_jev_entity_context_correction(
        "project-1", 2, expected_review_id=review.review_id,
    )
    assert reopened["human_correction_active"] is False
    assert len(await EntityReclassificationWriter(client).fetch_entities(
        user_name="ada", project_id="project-1",
    )) == 1
