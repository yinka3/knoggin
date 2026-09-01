import pytest

from core.knowledge.db.readers.merge_audit_reader import MergeAuditReader
from core.knowledge.db.writers.merge_audit_writer import MergeAuditWriter
from core.knowledge.entity.merge_service import EntityMergeService


class FailingMergeStore:
    async def merge_entities(self, *args, **kwargs):
        raise RuntimeError("injected merge failure")


class FailingProjectionRebuilder:
    async def rebuild_project_projection(self, *args, **kwargs):
        raise RuntimeError("injected projection failure")


async def _insert_entities(client, entity_ids):
    for entity_id in entity_ids:
        await client.execute(
            """
            INSERT INTO entities (entity_id, user_name, canonical_name)
            VALUES (%s, 'ada', %s);
            INSERT INTO project_entity_contexts (
                project_id, entity_id, user_name, entity_type, topic
            ) VALUES ('project-1', %s, 'ada', 'person', 'People')
            """,
            (entity_id, f"Entity {entity_id}", entity_id),
        )


async def _create_proposal(client, *, before_state, token="confirm-token"):
    writer = MergeAuditWriter(client)
    await writer.create_proposal(
        proposal_id="proposal-1",
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_message_ids=[],
        evidence_episode_ids=[],
        reasoning="The records refer to the same person.",
        model_confidence=0.9,
        reviewed_state_hash=EntityMergeService._state_hash(before_state),
        reviewed_state=before_state,
        policy_checks={},
        confirmation_token_hash=EntityMergeService._token_hash(token),
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_merge_confirmation_failure_rolls_back_claim_and_audit(
    real_postgres_client,
):
    await _insert_entities(real_postgres_client, [2, 3])
    reader = MergeAuditReader(real_postgres_client)
    before_state = await reader.snapshot("ada", "project-1", 2, 3)
    await _create_proposal(real_postgres_client, before_state=before_state)
    service = EntityMergeService(real_postgres_client, FailingMergeStore())

    result = await service.confirm(
        proposal_id="proposal-1",
        confirmation_token="confirm-token",
        confirmed_by="ada",
    )

    assert result == {
        "policy_result": "rejected",
        "reason": "The canonical merge transaction failed without committing.",
    }
    proposal = await real_postgres_client.fetch_one(
        "SELECT status, confirmed_at, confirmed_by FROM entity_merge_proposals "
        "WHERE proposal_id = 'proposal-1'"
    )
    audit_count = await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entity_merge_audits"
    )
    assert proposal == {
        "status": "confirmation_required",
        "confirmed_at": None,
        "confirmed_by": None,
    }
    assert audit_count["count"] == 0


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_rollback_projection_failure_rolls_back_relational_restore(
    real_postgres_client,
):
    await _insert_entities(real_postgres_client, [2, 3])
    reader = MergeAuditReader(real_postgres_client)
    before_state = await reader.snapshot("ada", "project-1", 2, 3)
    await _create_proposal(real_postgres_client, before_state=before_state)
    await real_postgres_client.execute("DELETE FROM entities WHERE entity_id = 3")
    after_state = await reader.snapshot("ada", "project-1", 2, 3)
    writer = MergeAuditWriter(real_postgres_client)
    await writer.create_audit(
        audit_id="audit-1",
        proposal={
            "proposal_id": "proposal-1",
            "user_name": "ada",
            "project_id": "project-1",
            "primary_entity_id": 2,
            "duplicate_entity_id": 3,
            "reasoning": "The records refer to the same person.",
        },
        evidence_message_ids=[],
        evidence_episode_ids=[],
        before_state=before_state,
        confirmed_by="ada",
    )
    await writer.mark_audit_executed(
        audit_id="audit-1",
        after_state=after_state,
        rollback_retention_hours=5,
    )
    service = EntityMergeService(real_postgres_client, object())
    service.projection_rebuilder = FailingProjectionRebuilder()

    result = await service.rollback("audit-1", "ada")

    assert result == {
        "policy_result": "rejected",
        "reason": "The merge rollback failed without committing.",
    }
    duplicate = await real_postgres_client.fetch_one(
        "SELECT entity_id FROM entities WHERE entity_id = 3"
    )
    audit = await real_postgres_client.fetch_one(
        "SELECT rollback_status, rolled_back_at FROM entity_merge_audits "
        "WHERE audit_id = 'audit-1'"
    )
    assert duplicate is None
    assert audit == {"rollback_status": "available", "rolled_back_at": None}
