from typing import Any, Dict, Optional

from infrastructure.postgres_client import PostgresClient


class MergeAuditReader:
    """Reads merge proposal, audit, and candidate snapshot state."""

    def __init__(self, client: PostgresClient):
        self.client = client

    async def snapshot(
        self,
        user_name: str,
        project_id: str,
        primary_id: int,
        duplicate_id: int,
    ) -> Dict[str, Any]:
        ids = (primary_id, duplicate_id)
        entities = await self.client.fetch_all(
            """
            SELECT
                e.entity_id,
                e.user_name,
                e.project_id,
                e.session_id,
                e.canonical_name,
                e.type,
                e.topic,
                e.confidence,
                e.last_mentioned_ms,
                e.last_updated_ms,
                e.last_profiled_msg_id,
                COALESCE(
                    array_agg(DISTINCT a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                    ARRAY[]::text[]
                ) AS aliases
            FROM entities e
            LEFT JOIN entity_aliases a ON a.entity_id = e.entity_id
            WHERE e.user_name = %s
              AND e.project_id = %s
              AND e.entity_id = ANY(%s)
            GROUP BY e.entity_id
            ORDER BY e.entity_id
            """,
            (user_name, project_id, list(ids)),
        )
        message_refs = await self.client.fetch_all(
            """
            SELECT
                ref.message_id,
                ref.entity_id,
                message.user_name,
                message.project_id,
                message.session_id,
                message.content
            FROM message_entity_refs ref
            JOIN messages message ON message.message_id = ref.message_id
            WHERE message.user_name = %s
              AND message.project_id = %s
              AND ref.entity_id = ANY(%s)
            ORDER BY ref.message_id, ref.entity_id
            """,
            (user_name, project_id, list(ids)),
        )
        episode_entities = await self.client.fetch_all(
            """
            SELECT episode_entity.*
            FROM episode_entities episode_entity
            JOIN episodes episode ON episode.episode_id = episode_entity.episode_id
            WHERE episode.project_id = %s
              AND episode_entity.entity_id = ANY(%s)
            ORDER BY episode_entity.episode_id, episode_entity.entity_id
            """,
            (project_id, list(ids)),
        )
        relationships = await self.client.fetch_all(
            """
            SELECT
                r.*,
                COALESCE(
                    json_agg(
                        json_build_object(
                            'user_name', ref.user_name,
                            'session_id', ref.session_id,
                            'message_id', ref.message_id
                        )
                        ORDER BY
                            ref.user_name,
                            ref.session_id,
                            ref.message_id
                    ) FILTER (WHERE ref.relationship_id IS NOT NULL),
                    '[]'
                ) AS evidence_refs
            FROM relationships r
            LEFT JOIN relationship_evidence_refs ref
              ON ref.relationship_id = r.relationship_id
             AND ref.user_name = %s
            WHERE r.user_name = %s
              AND r.project_id = %s
              AND (r.entity_a_id = ANY(%s) OR r.entity_b_id = ANY(%s))
            GROUP BY r.relationship_id
            ORDER BY r.relationship_id
            """,
            (user_name, user_name, project_id, list(ids), list(ids)),
        )
        hierarchy = await self.client.fetch_all(
            """
            SELECT h.*
            FROM hierarchy_edges h
            JOIN entities parent_entity
              ON parent_entity.entity_id = h.parent_id
             AND parent_entity.user_name = %s
             AND parent_entity.project_id = h.project_id
            JOIN entities child_entity
              ON child_entity.entity_id = h.child_id
             AND child_entity.user_name = %s
             AND child_entity.project_id = h.project_id
            WHERE h.project_id = %s
              AND (h.parent_id = ANY(%s) OR h.child_id = ANY(%s))
            ORDER BY h.parent_id, h.child_id
            """,
            (user_name, user_name, project_id, list(ids), list(ids)),
        )
        return {
            "entities": entities,
            "message_refs": message_refs,
            "episode_entities": episode_entities,
            "relationships": relationships,
            "hierarchy": hierarchy,
        }

    async def get_proposal(self, proposal_id: str) -> Optional[Dict[str, Any]]:
        rows = await self.client.fetch_all(
            """
            SELECT *
            FROM entity_merge_proposals
            WHERE proposal_id = %s
            """,
            (proposal_id,),
        )
        return rows[0] if rows else None

    async def get_audit(self, audit_id: str) -> Optional[Dict[str, Any]]:
        rows = await self.client.fetch_all(
            """
            SELECT *
            FROM entity_merge_audits
            WHERE audit_id = %s
            """,
            (audit_id,),
        )
        return rows[0] if rows else None
