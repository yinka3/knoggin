import pytest

from core.knowledge.db.writers.merge_audit_writer import MergeAuditWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_writer_creates_merge_proposal():
    client = RecordingPostgresClient()
    writer = MergeAuditWriter(client)

    await writer.create_proposal(
        proposal_id="proposal-1",
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_message_ids=[7],
        evidence_episode_ids=["episode-1"],
        reasoning="same",
        model_confidence=0.9,
        reviewed_state_hash="hash",
        reviewed_state={"entities": []},
        policy_checks={"ok": True},
        confirmation_token_hash="token-hash",
    )

    call = client.calls[0]
    assert call[0] == "execute_command"
    assert "INSERT INTO entity_merge_proposals" in call[1]
    assert call[2][0] == "proposal-1"
    assert call[2][5] == "[7]"
    assert call[2][6] == '["episode-1"]'
    assert call[2][11] == '{"ok": true}'


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_writer_updates_proposal_lifecycle():
    client = RecordingPostgresClient()
    writer = MergeAuditWriter(client)

    await writer.set_proposal_failure("p1", "failed", "reason")
    await writer.claim_proposal_for_execution("p1", "ada")
    await writer.mark_proposal_executed("p1")

    executed_sql = "\n".join(call[1] for call in client.calls)
    assert "SET status = %s" in executed_sql
    assert "SET status = 'executing'" in executed_sql
    assert "SET status = 'executed'" in executed_sql


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_writer_updates_audit_lifecycle():
    client = RecordingPostgresClient()
    writer = MergeAuditWriter(client)
    proposal = {
        "proposal_id": "p1",
        "user_name": "ada",
        "project_id": "project-1",
        "primary_entity_id": 2,
        "duplicate_entity_id": 3,
        "reasoning": "same",
    }

    await writer.create_audit(
        audit_id="audit-1",
        proposal=proposal,
        evidence_message_ids=[7],
        evidence_episode_ids=["episode-1"],
        before_state={"entities": []},
        confirmed_by="ada",
    )
    await writer.mark_audit_failed("audit-1", "failed")
    await writer.mark_audit_executed(
        audit_id="audit-1",
        after_state={"entities": []},
        rollback_retention_hours=5,
    )

    executed_sql = "\n".join(call[1] for call in client.calls)
    assert "INSERT INTO entity_merge_audits" in executed_sql
    assert "SET status = 'failed'" in executed_sql
    assert "SET status = 'executed'" in executed_sql
    assert "rollback_status = 'available'" in executed_sql


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_writer_marks_rollback_failure():
    client = RecordingPostgresClient()
    writer = MergeAuditWriter(client)

    await writer.mark_rollback_failure("audit-1", "manual repair required")

    call = client.calls[0]
    assert call[0] == "execute_command"
    assert "rollback_status = 'failed'" in call[1]
    assert "rollback_failure_reason = %s" in call[1]
    assert call[2] == ("manual repair required", "audit-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_writer_restores_before_state_transactionally():
    client = RecordingPostgresClient()
    writer = MergeAuditWriter(client)
    before_state = {
        "entities": [
            {
                "entity_id": 2,
                "user_name": "ada",
                "project_id": "project-1",
                "session_id": "session-1",
                "canonical_name": "Ada Lovelace",
                "type": "person",
                "topic": "People",
                "confidence": 0.9,
                "last_mentioned_ms": 100,
                "last_updated_ms": 110,
                "last_profiled_msg_id": 7,
                "aliases": ["Ada"],
            }
        ],
        "message_refs": [
            {
                "message_id": 1,
                "entity_id": 2,
            }
        ],
        "episode_entities": [
            {
                "episode_id": "episode-1",
                "entity_id": 2,
                "prominence_weight": 0.8,
                "role": "subject",
                "is_focus_entity": True,
                "source_message_count": 1,
            }
        ],
        "relationships": [
            {
                "relationship_id": "project-1:2:9",
                "user_name": "ada",
                "project_id": "project-1",
                "entity_a_id": 2,
                "entity_b_id": 9,
                "weight": 1,
                "confidence": 0.8,
                "context": "worked with",
                "last_seen_ms": 120,
                "evidence_refs": [
                    {
                        "user_name": "ada",
                        "session_id": "session-1",
                        "message_id": 1,
                    }
                ],
            }
        ],
        "relationship_observations": [
            {
                "relationship_id": "project-1:2:9",
                "project_id": "project-1",
                "user_name": "ada",
                "session_id": "session-1",
                "message_id": 1,
                "source_entity_id": 2,
                "target_entity_id": 9,
                "source_type": "Person",
                "target_type": "Project",
                "observed_relationship_label": "worked with",
                "canonical_relationship_type": None,
                "domain_status": "unrecognized",
                "confidence": 0.8,
                "context": "context",
                "observed_at_ms": 120,
            }
        ],
        "episode_relationships": [
            {
                "episode_id": "episode-1",
                "relationship_id": "project-1:2:9",
                "prominence_weight": 0.7,
                "is_central_relationship": True,
                "source_message_count": 1,
            }
        ],
        "hierarchy": [
            {
                "project_id": "project-1",
                "parent_id": 9,
                "child_id": 2,
                "created_at_ms": 100,
            }
        ],
    }

    await writer.restore_before_state(
        before_state,
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        audit_id="audit-1",
        actor="admin",
    )

    executed_sql = "\n".join(call[1] for call in client.calls)
    assert "DELETE FROM relationship_evidence_refs" in executed_sql
    assert "INSERT INTO entities" in executed_sql
    assert "INSERT INTO entity_aliases" in executed_sql
    assert "INSERT INTO message_entity_refs" in executed_sql
    assert "INSERT INTO episode_entities" in executed_sql
    assert "INSERT INTO relationships" in executed_sql
    assert "INSERT INTO relationship_evidence_refs" in executed_sql
    assert "INSERT INTO relationship_observations" in executed_sql
    assert "INSERT INTO episode_relationships" in executed_sql
    assert "INSERT INTO hierarchy_edges" in executed_sql
    assert "rollback_status = 'rolled_back'" in executed_sql


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_writer_expires_available_rollback_states():
    client = RecordingPostgresClient()
    writer = MergeAuditWriter(client)

    result = await writer.expire_rollback_states(
        "2026-01-01T05:00:00+00:00",
        user_name="ada",
        project_id="project-1",
    )

    assert result is None
    call = client.calls[0]
    assert call[0] == "execute_command"
    assert "UPDATE entity_merge_audits" in call[1]
    assert "before_state = NULL" in call[1]
    assert "after_state = NULL" in call[1]
    assert "rollback_status = 'expired'" in call[1]
    assert call[2] == (
        "project-1",
        "ada",
        "2026-01-01T05:00:00+00:00",
    )
