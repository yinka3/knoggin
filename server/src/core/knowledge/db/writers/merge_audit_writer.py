import json
from datetime import datetime
from typing import Any, Dict, Optional

from infrastructure.postgres_client import PostgresClient


class MergeAuditWriter:
    """Writes durable merge audit maintenance state."""

    def __init__(self, client: PostgresClient):
        self.client = client

    async def create_proposal(
        self,
        *,
        proposal_id: str,
        user_name: str,
        project_id: str,
        primary_id: int,
        duplicate_id: int,
        evidence_message_ids: list[int],
        evidence_episode_ids: list[str],
        reasoning: str,
        model_confidence: Optional[float],
        reviewed_state_hash: str,
        reviewed_state: Dict[str, Any],
        policy_checks: Dict[str, Any],
        confirmation_token_hash: str,
    ) -> None:
        await self.client.execute(
            """
            INSERT INTO entity_merge_proposals (
                proposal_id,
                user_name,
                project_id,
                primary_entity_id,
                duplicate_entity_id,
                evidence_message_ids,
                evidence_episode_ids,
                reasoning,
                model_confidence,
                reviewed_state_hash,
                reviewed_state,
                policy_checks,
                confirmation_token_hash
            )
            VALUES (
                %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s,
                %s, %s::jsonb, %s::jsonb, %s
            )
            """,
            (
                proposal_id,
                user_name,
                project_id,
                primary_id,
                duplicate_id,
                json.dumps(evidence_message_ids),
                json.dumps(evidence_episode_ids),
                reasoning.strip(),
                model_confidence,
                reviewed_state_hash,
                json.dumps(reviewed_state, default=str),
                json.dumps(policy_checks),
                confirmation_token_hash,
            ),
        )

    async def set_proposal_failure(
        self,
        proposal_id: str,
        status: str,
        reason: str,
    ) -> None:
        await self.client.execute(
            """
            UPDATE entity_merge_proposals
            SET status = %s,
                failure_reason = %s
            WHERE proposal_id = %s
            """,
            (status, reason, proposal_id),
        )

    async def claim_proposal_for_execution(
        self,
        proposal_id: str,
        confirmed_by: str,
    ) -> int:
        return await self.client.execute(
            """
            UPDATE entity_merge_proposals
            SET status = 'executing',
                confirmed_at = NOW(),
                confirmed_by = %s
            WHERE proposal_id = %s
              AND status = 'confirmation_required'
            """,
            (confirmed_by, proposal_id),
        )

    async def create_audit(
        self,
        *,
        audit_id: str,
        proposal: Dict[str, Any],
        evidence_message_ids: list[int],
        evidence_episode_ids: list[str],
        before_state: Dict[str, Any],
        confirmed_by: str,
    ) -> None:
        await self.client.execute(
            """
            INSERT INTO entity_merge_audits (
                audit_id,
                proposal_id,
                user_name,
                project_id,
                primary_entity_id,
                duplicate_entity_id,
                evidence_message_ids,
                evidence_episode_ids,
                reasoning,
                confirmed_by,
                before_state
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s::jsonb
            )
            """,
            (
                audit_id,
                proposal["proposal_id"],
                proposal["user_name"],
                proposal["project_id"],
                int(proposal["primary_entity_id"]),
                int(proposal["duplicate_entity_id"]),
                json.dumps(evidence_message_ids),
                json.dumps(evidence_episode_ids),
                proposal["reasoning"],
                confirmed_by,
                json.dumps(before_state, default=str),
            ),
        )

    async def mark_audit_failed(self, audit_id: str, reason: str) -> None:
        await self.client.execute(
            """
            UPDATE entity_merge_audits
            SET status = 'failed',
                failure_reason = %s
            WHERE audit_id = %s
            """,
            (reason, audit_id),
        )

    async def mark_audit_executed(
        self,
        *,
        audit_id: str,
        after_state: Dict[str, Any],
        rollback_retention_hours: float,
    ) -> None:
        await self.client.execute(
            """
            UPDATE entity_merge_audits
            SET status = 'executed',
                after_state = %s::jsonb,
                rollback_status = 'available',
                rollback_expires_at = NOW() + (%s * INTERVAL '1 hour')
            WHERE audit_id = %s
            """,
            (
                json.dumps(after_state, default=str),
                rollback_retention_hours,
                audit_id,
            ),
        )

    async def mark_proposal_executed(self, proposal_id: str) -> None:
        await self.client.execute(
            """
            UPDATE entity_merge_proposals
            SET status = 'executed',
                executed_at = NOW()
            WHERE proposal_id = %s
            """,
            (proposal_id,),
        )

    async def mark_rollback_failure(self, audit_id: str, reason: str) -> None:
        await self.client.execute(
            """
            UPDATE entity_merge_audits
            SET rollback_status = 'failed',
                rollback_failure_reason = %s
            WHERE audit_id = %s
            """,
            (reason, audit_id),
        )

    async def restore_before_state(
        self,
        before_state: Dict[str, Any],
        *,
        project_id: str,
        primary_id: int,
        duplicate_id: int,
        audit_id: str,
        actor: str,
    ) -> None:
        ids = [primary_id, duplicate_id]
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                DELETE FROM relationship_evidence_refs
                WHERE relationship_id IN (
                    SELECT relationship_id
                    FROM relationships
                    WHERE project_id = %s
                      AND (entity_a_id = ANY(%s) OR entity_b_id = ANY(%s))
                )
                """,
                (project_id, ids, ids),
            )
            await cur.execute(
                """
                DELETE FROM relationships
                WHERE project_id = %s
                  AND (entity_a_id = ANY(%s) OR entity_b_id = ANY(%s))
                """,
                (project_id, ids, ids),
            )
            await cur.execute(
                """
                DELETE FROM hierarchy_edges
                WHERE project_id = %s
                  AND (parent_id = ANY(%s) OR child_id = ANY(%s))
                """,
                (project_id, ids, ids),
            )
            await cur.execute(
                """
                DELETE FROM message_entity_refs
                WHERE entity_id = ANY(%s)
                """,
                (ids,),
            )
            await cur.execute(
                """
                DELETE FROM episode_entities
                WHERE entity_id = ANY(%s)
                """,
                (ids,),
            )
            await cur.execute(
                """
                DELETE FROM entity_aliases
                WHERE entity_id = ANY(%s)
                """,
                (ids,),
            )

            for entity in before_state["entities"]:
                await cur.execute(
                    """
                    INSERT INTO entities (
                        entity_id,
                        user_name,
                        project_id,
                        session_id,
                        canonical_name,
                        type,
                        topic,
                        confidence,
                        last_mentioned_ms,
                        last_updated_ms,
                        last_profiled_msg_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (entity_id) DO UPDATE SET
                        user_name = EXCLUDED.user_name,
                        project_id = EXCLUDED.project_id,
                        session_id = EXCLUDED.session_id,
                        canonical_name = EXCLUDED.canonical_name,
                        type = EXCLUDED.type,
                        topic = EXCLUDED.topic,
                        confidence = EXCLUDED.confidence,
                        last_mentioned_ms = EXCLUDED.last_mentioned_ms,
                        last_updated_ms = EXCLUDED.last_updated_ms,
                        last_profiled_msg_id = EXCLUDED.last_profiled_msg_id
                    """,
                    (
                        int(entity["entity_id"]),
                        entity["user_name"],
                        entity["project_id"],
                        entity.get("session_id"),
                        entity["canonical_name"],
                        entity.get("type"),
                        entity.get("topic") or "General",
                        float(entity.get("confidence") or 1.0),
                        entity.get("last_mentioned_ms"),
                        entity.get("last_updated_ms"),
                        entity.get("last_profiled_msg_id"),
                    ),
                )
                for alias in entity.get("aliases") or []:
                    await cur.execute(
                        """
                        INSERT INTO entity_aliases (entity_id, alias)
                        VALUES (%s, %s)
                        ON CONFLICT (entity_id, alias) DO NOTHING
                        """,
                        (int(entity["entity_id"]), alias),
                    )

            for message_ref in before_state["message_refs"]:
                await cur.execute(
                    """
                    INSERT INTO message_entity_refs (message_id, entity_id)
                    VALUES (%s, %s)
                    ON CONFLICT (message_id, entity_id) DO NOTHING
                    """,
                    (
                        int(message_ref["message_id"]),
                        int(message_ref["entity_id"]),
                    ),
                )

            for episode_entity in before_state["episode_entities"]:
                await cur.execute(
                    """
                    INSERT INTO episode_entities (
                        episode_id,
                        entity_id,
                        prominence_weight,
                        role,
                        is_focus_entity,
                        source_message_count,
                        first_seen_at,
                        last_seen_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (episode_id, entity_id) DO UPDATE SET
                        prominence_weight = EXCLUDED.prominence_weight,
                        role = EXCLUDED.role,
                        is_focus_entity = EXCLUDED.is_focus_entity,
                        source_message_count = EXCLUDED.source_message_count,
                        first_seen_at = EXCLUDED.first_seen_at,
                        last_seen_at = EXCLUDED.last_seen_at
                    """,
                    (
                        episode_entity["episode_id"],
                        int(episode_entity["entity_id"]),
                        float(episode_entity.get("prominence_weight") or 0.0),
                        episode_entity.get("role"),
                        bool(episode_entity.get("is_focus_entity")),
                        int(episode_entity.get("source_message_count") or 0),
                        episode_entity.get("first_seen_at"),
                        episode_entity.get("last_seen_at"),
                    ),
                )

            for relationship in before_state["relationships"]:
                await cur.execute(
                    """
                    INSERT INTO relationships (
                        relationship_id,
                        user_name,
                        project_id,
                        entity_a_id,
                        entity_b_id,
                        relationship_type,
                        weight,
                        confidence,
                        context,
                        last_seen_ms
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        relationship["relationship_id"],
                        relationship["user_name"],
                        relationship["project_id"],
                        int(relationship["entity_a_id"]),
                        int(relationship["entity_b_id"]),
                        relationship.get("relationship_type"),
                        int(relationship.get("weight") or 1),
                        float(relationship.get("confidence") or 1.0),
                        relationship.get("context"),
                        relationship.get("last_seen_ms"),
                    ),
                )
                for ref in self._json_value(relationship.get("evidence_refs") or []):
                    await cur.execute(
                        """
                        INSERT INTO relationship_evidence_refs (
                            relationship_id,
                            user_name,
                            session_id,
                            message_id
                        )
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (
                            relationship_id,
                            user_name,
                            session_id,
                            message_id
                        ) DO NOTHING
                        """,
                        (
                            relationship["relationship_id"],
                            ref["user_name"],
                            ref["session_id"],
                            int(ref["message_id"]),
                        ),
                    )

            for edge in before_state["hierarchy"]:
                await cur.execute(
                    """
                    INSERT INTO hierarchy_edges (
                        project_id,
                        parent_id,
                        child_id,
                        created_at_ms
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (project_id, parent_id, child_id)
                    DO UPDATE SET created_at_ms = EXCLUDED.created_at_ms
                    """,
                    (
                        edge["project_id"],
                        int(edge["parent_id"]),
                        int(edge["child_id"]),
                        edge.get("created_at_ms"),
                    ),
                )

            await cur.execute(
                """
                UPDATE entity_merge_audits
                SET rollback_status = 'rolled_back',
                    rolled_back_at = NOW(),
                    rolled_back_by = %s,
                    rollback_failure_reason = NULL
                WHERE audit_id = %s
                """,
                (actor, audit_id),
            )

    async def expire_rollback_states(
        self,
        cutoff: datetime,
        *,
        user_name: str,
        project_id: str,
    ) -> int:
        return await self.client.execute(
            """
            UPDATE entity_merge_audits
            SET before_state = NULL,
                after_state = NULL,
                rollback_status = 'expired'
            WHERE project_id = %s
              AND user_name = %s
              AND rollback_status = 'available'
              AND rollback_expires_at < %s
            """,
            (project_id, user_name, cutoff),
        )

    @staticmethod
    def _json_value(value: Any) -> Any:
        if isinstance(value, str):
            return json.loads(value)
        return value
