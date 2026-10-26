"""Exact JEV identity corrections start from current, proved occurrences."""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.exceptions import MaintenanceConflictError
from core.knowledge.db.readers.evidence_traversal_reader import EvidenceTraversalReader
from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.evidence_service import EvidenceService
from core.project.maintenance_service import ProjectMaintenanceService


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_identity_review_requires_complete_current_occurrence(real_postgres_client):
    client = real_postgres_client
    block_id, window_id, revision_id = uuid4(), uuid4(), uuid4()
    key = f"0:{block_id}"
    await client.execute(
        """INSERT INTO public.entities (entity_id, user_name, canonical_name)
           VALUES (2, 'ada', 'Orion'), (3, 'ada', 'Other Orion')"""
    )
    await client.execute(
        """INSERT INTO public.project_entity_contexts
           (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-1', 2, 'ada', 'Concept', 'General'),
                  ('project-1', 3, 'ada', 'Concept', 'General')"""
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
    await client.execute(
        """INSERT INTO public.project_context_revisions
           (revision_id, project_id, revision_number, window_id, origin,
            domain_version, content_hash)
           VALUES (%s, 'project-1', 1, %s, 'conversation', 1, %s)""",
        (revision_id, window_id, "a" * 64),
    )
    await client.execute(
        """INSERT INTO public.project_context_blocks
           (block_id, project_id, section_key, markdown, content_hash, assertion_kind)
           VALUES (%s, 'project-1', 'current_state', 'Orion is a concept.', %s,
                   'source_grounded')""",
        (block_id, "b" * 64),
    )
    await client.execute(
        """INSERT INTO public.project_context_revision_blocks
           (revision_id, project_id, block_id, ordinal)
           VALUES (%s, 'project-1', %s, 0)""",
        (revision_id, block_id),
    )
    await client.execute(
        """INSERT INTO public.project_contexts
           (project_id, user_name, current_revision_id)
           VALUES ('project-1', 'ada', %s)""",
        (revision_id,),
    )
    await client.execute(
        """INSERT INTO public.context_block_entities
           (block_id, project_id, entity_id, mention_text, occurrence_provenance_complete)
           VALUES (%s, 'project-1', 2, 'Orion', true)""",
        (block_id,),
    )
    await client.execute(
        """INSERT INTO public.context_block_entity_occurrences
           (window_id, project_id, pass_number, occurrence_index, block_id,
            resolved_entity_id, mention_text, identity_decision_key)
           VALUES (%s, 'project-1', 1, 0, %s, 2, 'Orion', %s)""",
        (window_id, block_id, key),
    )
    observation = {
        "operational_entity_id": 2, "mention_text": "Orion",
        "record": {"evidence_block_ids": [str(block_id)]},
    }
    await client.execute(
        """INSERT INTO public.project_jev_identity_decisions
           (window_id, project_id, pass_number, occurrence_key, decision,
            eligible_candidate_count, offered_candidate_count,
            candidate_set_truncated)
           VALUES (%s, 'project-1', 1, %s, %s::jsonb, 1, 1, false)""",
        (window_id, key, json.dumps(observation)),
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
    inputs = dict(
        window_id=str(window_id), pass_number=1, occurrence_key=key,
        source_entity_id=2, target_entity_id=3, new_entity_name=None,
        entity_type=None, topic=None, reasoning="This mention is the other Orion.",
    )
    review = await service.flag_jev_identity_decision("project-1", **inputs)
    assert review.status == "open"
    assert review.proposed_plan.target_entity_id == 3
    assert review.proposed_plan.block_ids == (block_id,)
    detail, impact = await service.preview_maintenance_review("project-1", review.review_id)
    assert detail.evidence_state == "current"
    assert impact.impacts[0].kind == "context_block_entity"
    assert impact.impacts[0].identifiers == (str(block_id),)
    new_review = await service.flag_jev_identity_decision("project-1", **{
        **inputs, "target_entity_id": None, "new_entity_name": "Orion Two",
        "entity_type": "Concept", "topic": "General",
    })
    assert new_review.review_id != review.review_id
    assert new_review.proposed_plan.new_entity_name == "Orion Two"
    await client.execute(
        """UPDATE public.context_block_entities
           SET occurrence_provenance_complete = false
           WHERE block_id = %s AND entity_id = 2""",
        (block_id,),
    )
    with pytest.raises(MaintenanceConflictError):
        await service.flag_jev_identity_decision("project-1", **inputs)
    with pytest.raises(MaintenanceConflictError):
        await service.preview_maintenance_review("project-1", review.review_id)
