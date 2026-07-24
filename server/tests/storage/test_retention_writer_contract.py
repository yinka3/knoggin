from datetime import timedelta
from uuid import uuid4

import pytest

from common.utils.time_utils import get_now
from core.knowledge.db.writers.retention_writer import RetentionWriter


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_retention_writer_purges_only_expired_terminal_operational_records(
    real_postgres_client,
):
    writer = RetentionWriter(real_postgres_client)
    user_name = "retention-test"
    project_id = "retention-project"
    proposal_id = f"proposal-{uuid4()}"
    audit_id = f"audit-{uuid4()}"
    protected_proposal_id = f"proposal-{uuid4()}"
    protected_audit_id = f"audit-{uuid4()}"
    old = get_now() - timedelta(days=200)

    await real_postgres_client.execute(
        """
        INSERT INTO ingestion_candidate_suggestions (
            suggestion_id, user_name, project_id, session_id, msg_id, mention,
            mention_type, mention_topic, candidate_id, candidate_name, base_score,
            created_at
        ) VALUES (%s, %s, %s, 'session-1', 1, 'notes', 'tool', 'General', 1,
                  'Notes', 0.5, %s)
        """,
        (f"suggestion-{uuid4()}", user_name, project_id, old),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO agent_tool_audits (
            audit_id, user_name, agent_id, project_id, session_id, run_id,
            tool_name, capability, status, created_at
        ) VALUES (%s, %s, 'agent-1', %s, 'session-1', 'run-1', 'rename_project',
                  'configuration_write', 'succeeded', %s)
        """,
        (uuid4(), user_name, project_id, old),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entity_merge_proposals (
            proposal_id, user_name, project_id, primary_entity_id,
            duplicate_entity_id, evidence_message_ids, evidence_episode_ids,
            reasoning, reviewed_state_hash, reviewed_state, policy_checks,
            confirmation_token_hash, status, created_at
        ) VALUES (%s, %s, %s, 1, 2, '[]'::jsonb, '[]'::jsonb, 'duplicate',
                  'hash', '{}'::jsonb, '{}'::jsonb, 'token', 'executed', %s)
        """,
        (proposal_id, user_name, project_id, old),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entity_merge_audits (
            audit_id, proposal_id, user_name, project_id, primary_entity_id,
            duplicate_entity_id, reasoning, confirmed_by, status,
            rollback_status, created_at
        ) VALUES (%s, %s, %s, %s, 1, 2, 'duplicate', 'ada', 'executed',
                  'expired', %s)
        """,
        (audit_id, proposal_id, user_name, project_id, old),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entity_merge_proposals (
            proposal_id, user_name, project_id, primary_entity_id,
            duplicate_entity_id, evidence_message_ids, evidence_episode_ids,
            reasoning, reviewed_state_hash, reviewed_state, policy_checks,
            confirmation_token_hash, status, created_at
        ) VALUES (%s, %s, %s, 3, 4, '[]'::jsonb, '[]'::jsonb, 'duplicate',
                  'hash', '{}'::jsonb, '{}'::jsonb, 'token', 'executed', %s)
        """,
        (protected_proposal_id, user_name, project_id, old),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entity_merge_audits (
            audit_id, proposal_id, user_name, project_id, primary_entity_id,
            duplicate_entity_id, reasoning, confirmed_by, status,
            rollback_status, created_at
        ) VALUES (%s, %s, %s, %s, 3, 4, 'duplicate', 'ada', 'executed',
                  'available', %s)
        """,
        (protected_audit_id, protected_proposal_id, user_name, project_id, old),
    )

    counts = await writer.purge_expired_records(
        user_name=user_name,
        project_id=project_id,
        candidate_suggestion_cutoff=get_now() - timedelta(days=30),
        tool_audit_cutoff=get_now() - timedelta(days=180),
        merge_history_cutoff=get_now() - timedelta(days=180),
    )

    assert counts == {
        "candidate_suggestions": 1,
        "tool_audits": 1,
        "merge_audits": 1,
        "merge_proposals": 1,
    }
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entity_merge_proposals WHERE proposal_id = %s",
        (proposal_id,),
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entity_merge_proposals WHERE proposal_id = %s",
        (protected_proposal_id,),
    ) == {"count": 1}
