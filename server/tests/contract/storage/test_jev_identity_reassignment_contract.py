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
async def test_identity_review_requires_complete_current_occurrence(real_postgres_client, monkeypatch):
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
    await client.execute(
        """INSERT INTO public.sessions (session_id, project_id, user_name)
           VALUES ('identity-session', 'project-1', 'ada')"""
    )
    await client.execute(
        """INSERT INTO public.messages
           (user_name, session_id, project_id, message_id, role, content,
            timestamp_ms, exchange_state, exchange_outcome, exchange_closed_at_ms)
           VALUES ('ada', 'identity-session', 'project-1', 101, 'user',
                   'Orion is a concept.', 1000, 'closed', 'user_only', 1000)"""
    )
    await client.execute(
        """INSERT INTO public.project_context_block_supports
           (block_id, project_id, message_id, session_id, support_kind)
           VALUES (%s, 'project-1', 101, 'identity-session', 'user_message')""",
        (block_id,),
    )
    await client.execute(
        """INSERT INTO public.message_entity_refs
           (message_id, entity_id, occurrence_provenance_complete)
           VALUES (101, 2, true)"""
    )
    await client.execute(
        """INSERT INTO public.message_entity_occurrences
           (window_id, project_id, pass_number, occurrence_index,
            block_id, message_id, resolved_entity_id)
           VALUES (%s, 'project-1', 1, 0, %s, 101, 2)""",
        (window_id, block_id),
    )
    await client.execute(
        """INSERT INTO public.episodes (episode_id, project_id, summary)
           VALUES ('identity-episode', 'project-1', 'Orion is a concept.')"""
    )
    await client.execute(
        """INSERT INTO public.episode_messages
           (episode_id, project_id, session_id, message_id, message_position)
           VALUES ('identity-episode', 'project-1', 'identity-session', 101, 0)"""
    )
    await client.execute(
        """INSERT INTO public.episode_entities
           (episode_id, project_id, entity_id, source_message_count,
            occurrence_provenance_complete)
           VALUES ('identity-episode', 'project-1', 2, 1, true)"""
    )
    await client.execute(
        """INSERT INTO public.episode_entity_occurrences
           (episode_id, project_id, window_id, pass_number, occurrence_index,
            block_id, message_id, resolved_entity_id)
           VALUES ('identity-episode', 'project-1', %s, 1, 0, %s, 101, 2)""",
        (window_id, block_id),
    )
    await client.execute(
        """INSERT INTO public.entities (entity_id, user_name, canonical_name)
           VALUES (4, 'ada', 'Atlas')"""
    )
    await client.execute(
        """INSERT INTO public.project_entity_contexts
           (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-1', 4, 'ada', 'Concept', 'General')"""
    )
    await client.execute(
        """INSERT INTO public.relationships
           (relationship_id, user_name, project_id, entity_a_id, entity_b_id,
            relationship_type)
           VALUES ('project-1:2:4:links', 'ada', 'project-1', 2, 4, 'LINKS')"""
    )
    observation_row = await client.fetch_one(
        """INSERT INTO public.relationship_observations
           (relationship_id, project_id, user_name, semantic_window_id,
            source_entity_id, target_entity_id, observed_relationship_label,
            support_key, observed_at_ms)
           VALUES ('project-1:2:4:links', 'project-1', 'ada', %s,
                   2, 4, 'LINKS', %s, 1000)
           RETURNING observation_id""",
        (window_id, "f" * 32),
    )
    await client.execute(
        """INSERT INTO public.relationship_observation_blocks
           (observation_id, project_id, block_id)
           VALUES (%s, 'project-1', %s)""",
        (observation_row["observation_id"], block_id),
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
    assert next(item for item in impact.impacts if item.kind == "message_entity_ref").identifiers == ("101",)
    assert next(item for item in impact.impacts if item.kind == "episode_entity_link").identifiers == ("identity-episode",)
    assert next(item for item in impact.impacts if item.kind == "relationship_observation").total_count == 1
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
    await client.execute(
        """UPDATE public.context_block_entities
           SET occurrence_provenance_complete = true
           WHERE block_id = %s AND entity_id = 2""",
        (block_id,),
    )
    async def fail_projection(*_args, **_kwargs):
        raise RuntimeError("projection unavailable")

    with monkeypatch.context() as patcher:
        patcher.setattr(
            "core.knowledge.entity.identity_reassignment_writer.GraphBuilder.rebuild_project_projection",
            fail_projection,
        )
        with pytest.raises(RuntimeError, match="projection unavailable"):
            await service.apply_jev_identity_review(
                "project-1", review.review_id, expected_state=review.expected_state,
            )
    assert (await client.fetch_one(
        "SELECT status FROM public.maintenance_reviews WHERE review_id = %s",
        (review.review_id,),
    ))["status"] == "open"
    assert await client.fetch_all(
        "SELECT entity_id FROM public.context_block_entities WHERE block_id = %s",
        (block_id,),
    ) == [{"entity_id": 2}]
    applied = await service.apply_jev_identity_review(
        "project-1", review.review_id, expected_state=review.expected_state,
    )
    assert applied["review"].status == "applied"
    assert applied["target_entity_id"] == 3
    assert await client.fetch_all(
        """SELECT entity_id FROM public.context_block_entities
           WHERE block_id = %s ORDER BY entity_id""",
        (block_id,),
    ) == [{"entity_id": 3}]
    assert await client.fetch_one(
        """SELECT corrected_entity_id, human_correction_review_id
           FROM public.context_block_entity_occurrences WHERE block_id = %s""",
        (block_id,),
    ) == {"corrected_entity_id": 3, "human_correction_review_id": review.review_id}
    assert await client.fetch_all(
        "SELECT entity_id FROM public.message_entity_refs WHERE message_id = 101"
    ) == [{"entity_id": 3}]
    assert await client.fetch_all(
        "SELECT entity_id FROM public.episode_entities WHERE episode_id = 'identity-episode'"
    ) == [{"entity_id": 3}]
    assert (await client.fetch_one(
        "SELECT retired_reason FROM public.relationship_observations WHERE observation_id = %s",
        (observation_row["observation_id"],),
    ))["retired_reason"] == "human_identity_correction"
    assert await client.fetch_one(
        "SELECT relationship_id FROM public.relationships WHERE relationship_id = 'project-1:2:4:links'"
    ) is None
    with pytest.raises(ValueError):
        await service.apply_jev_identity_review(
            "project-1", review.review_id, expected_state=review.expected_state,
        )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_new_identity_keeps_another_mention_on_the_old_entity(real_postgres_client):
    client = real_postgres_client
    block_id, window_id, revision_id = uuid4(), uuid4(), uuid4()
    key = f"0:{block_id}"
    await client.execute(
        "INSERT INTO public.entities (entity_id, user_name, canonical_name) VALUES (2, 'ada', 'Orion')"
    )
    await client.execute(
        """INSERT INTO public.project_entity_contexts
           (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-1', 2, 'ada', 'Concept', 'General')"""
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
           VALUES (%s, 'project-1', 'current_state', 'Orion met Orion.', %s,
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
            resolved_entity_id, mention_text, source_start, source_end,
            identity_decision_key)
           VALUES (%s, 'project-1', 1, 0, %s, 2, 'Orion', 0, 5, %s),
                  (%s, 'project-1', 1, 1, %s, 2, 'Orion', 10, 15, NULL)""",
        (window_id, block_id, key, window_id, block_id),
    )
    await client.execute(
        """INSERT INTO public.project_jev_identity_decisions
           (window_id, project_id, pass_number, occurrence_key, decision,
            eligible_candidate_count, offered_candidate_count,
            candidate_set_truncated)
           VALUES (%s, 'project-1', 1, %s, %s::jsonb, 1, 1, false)""",
        (window_id, key, json.dumps({
            "operational_entity_id": 2, "mention_text": "Orion",
            "record": {"evidence_block_ids": [str(block_id)]},
        })),
    )
    await client.execute("SELECT setval('public.entity_id_seq', 2, true)")

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
    review = await service.flag_jev_identity_decision(
        "project-1", window_id=str(window_id), pass_number=1,
        occurrence_key=key, source_entity_id=2, target_entity_id=None,
        new_entity_name="Another Orion", entity_type="Concept", topic="General",
        reasoning="The first Orion is a different concept.",
    )
    applied = await service.apply_jev_identity_review(
        "project-1", review.review_id, expected_state=review.expected_state,
    )
    target_id = applied["target_entity_id"]
    assert target_id != 2
    assert await client.fetch_all(
        "SELECT entity_id FROM public.context_block_entities WHERE block_id = %s ORDER BY entity_id",
        (block_id,),
    ) == [{"entity_id": 2}, {"entity_id": target_id}]
    assert await client.fetch_all(
        """SELECT occurrence_index, corrected_entity_id
           FROM public.context_block_entity_occurrences
           WHERE block_id = %s ORDER BY occurrence_index""",
        (block_id,),
    ) == [
        {"occurrence_index": 0, "corrected_entity_id": target_id},
        {"occurrence_index": 1, "corrected_entity_id": None},
    ]
