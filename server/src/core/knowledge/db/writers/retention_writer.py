from datetime import datetime

from infrastructure.postgres_client import PostgresClient


class RetentionWriter:
    """Purges expired advisory and terminal operational records."""

    def __init__(self, client: PostgresClient):
        self.client = client

    async def purge_expired_records(
        self,
        *,
        user_name: str,
        project_id: str,
        tool_audit_cutoff: datetime,
        merge_history_cutoff: datetime,
    ) -> dict[str, int]:
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                DELETE FROM agent_tool_audits
                WHERE user_name = %s
                  AND project_id = %s
                  AND created_at < %s
                """,
                (user_name, project_id, tool_audit_cutoff),
            )
            tool_audits = cur.rowcount

            await cur.execute(
                """
                DELETE FROM entity_merge_audits audit
                USING entity_merge_proposals proposal
                WHERE audit.proposal_id = proposal.proposal_id
                  AND audit.user_name = %s
                  AND audit.project_id = %s
                  AND audit.created_at < %s
                  AND audit.rollback_status <> 'available'
                  AND proposal.status IN ('executed', 'rejected', 'failed')
                """,
                (user_name, project_id, merge_history_cutoff),
            )
            merge_audits = cur.rowcount

            await cur.execute(
                """
                DELETE FROM entity_merge_proposals proposal
                WHERE proposal.user_name = %s
                  AND proposal.project_id = %s
                  AND proposal.created_at < %s
                  AND proposal.status IN ('executed', 'rejected', 'failed')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM entity_merge_audits audit
                      WHERE audit.proposal_id = proposal.proposal_id
                  )
                """,
                (user_name, project_id, merge_history_cutoff),
            )
            merge_proposals = cur.rowcount

        return {
            "tool_audits": tool_audits,
            "merge_audits": merge_audits,
            "merge_proposals": merge_proposals,
        }
