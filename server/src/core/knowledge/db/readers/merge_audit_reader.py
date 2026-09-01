from typing import Any, Dict, Optional

from infrastructure.postgres_client import PostgresClient


class MergeAuditReader:
    """Reads merge proposal, audit, and candidate snapshot state."""

    def __init__(self, client: PostgresClient):
        self.client = client

    async def _fetch_all(self, cur, query: str, params):
        if cur is None:
            return await self.client.fetch_all(query, params)
        await cur.execute(query, params)
        return await cur.fetchall()

    async def snapshot(
        self,
        user_name: str,
        project_id: str,
        primary_id: int,
        duplicate_id: int,
        *,
        cur=None,
    ) -> Dict[str, Any]:
        if cur is None:
            async with self.client.transaction() as snapshot_cursor:
                await snapshot_cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                return await self.snapshot(
                    user_name,
                    project_id,
                    primary_id,
                    duplicate_id,
                    cur=snapshot_cursor,
                )

        ids = (primary_id, duplicate_id)
        entities = await self._fetch_all(
            cur,
            """
            SELECT
                e.entity_id,
                e.user_name,
                e.canonical_name,
                context.project_id,
                context.entity_type AS type,
                context.topic,
                context.last_mentioned_ms,
                COALESCE(
                    array_agg(DISTINCT a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                    ARRAY[]::text[]
                ) AS aliases
            FROM entities e
            JOIN project_entity_contexts context
              ON context.entity_id = e.entity_id
            LEFT JOIN entity_aliases a ON a.entity_id = e.entity_id
            WHERE e.user_name = %s
              AND context.project_id = %s
              AND e.entity_id = ANY(%s)
            GROUP BY
                e.entity_id,
                context.project_id,
                context.entity_type,
                context.topic,
                context.last_mentioned_ms
            ORDER BY e.entity_id
            """,
            (user_name, project_id, list(ids)),
        )
        message_refs = await self._fetch_all(
            cur,
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
        episode_entities = await self._fetch_all(
            cur,
            """
            SELECT episode_entity.*
            FROM episode_entities episode_entity
            JOIN episodes episode
              ON episode.episode_id = episode_entity.episode_id
             AND episode.project_id = episode_entity.project_id
            WHERE episode.project_id = %s
              AND episode_entity.entity_id = ANY(%s)
            ORDER BY episode_entity.episode_id, episode_entity.entity_id
            """,
            (project_id, list(ids)),
        )
        relationships = await self._fetch_all(
            cur,
            """
            SELECT
                r.*,
                COALESCE(
                    json_agg(
                        json_build_object(
                            'project_id', ref.project_id,
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
            LEFT JOIN relationship_observations ref
              ON ref.relationship_id = r.relationship_id
             AND ref.project_id = r.project_id
             AND ref.user_name = %s
            WHERE r.user_name = %s
              AND r.project_id = %s
              AND (r.entity_a_id = ANY(%s) OR r.entity_b_id = ANY(%s))
            GROUP BY r.relationship_id
            ORDER BY r.relationship_id
            """,
            (user_name, user_name, project_id, list(ids), list(ids)),
        )
        relationship_observations = await self._fetch_all(
            cur,
            """
            SELECT observation.*
            FROM relationship_observations observation
            JOIN relationships relationship
              ON relationship.relationship_id = observation.relationship_id
             AND relationship.project_id = observation.project_id
            WHERE relationship.user_name = %s
              AND relationship.project_id = %s
              AND (
                  relationship.entity_a_id = ANY(%s)
                  OR relationship.entity_b_id = ANY(%s)
              )
            ORDER BY observation.observation_id
            """,
            (user_name, project_id, list(ids), list(ids)),
        )
        episode_relationships = await self._fetch_all(
            cur,
            """
            SELECT episode_relationship.*
            FROM episode_relationships episode_relationship
            JOIN episodes episode
              ON episode.episode_id = episode_relationship.episode_id
             AND episode.project_id = episode_relationship.project_id
            JOIN relationships relationship
              ON relationship.relationship_id = episode_relationship.relationship_id
             AND relationship.project_id = episode_relationship.project_id
            WHERE episode.project_id = %s
              AND relationship.project_id = %s
              AND (
                  relationship.entity_a_id = ANY(%s)
                  OR relationship.entity_b_id = ANY(%s)
              )
            ORDER BY
                episode_relationship.episode_id,
                episode_relationship.relationship_id
            """,
            (project_id, project_id, list(ids), list(ids)),
        )
        return {
            "entities": entities,
            "message_refs": message_refs,
            "episode_entities": episode_entities,
            "relationships": relationships,
            "relationship_observations": relationship_observations,
            "episode_relationships": episode_relationships,
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

    async def get_proposal_for_update(self, cur, proposal_id: str) -> Optional[Dict]:
        await cur.execute(
            """
            SELECT *
            FROM entity_merge_proposals
            WHERE proposal_id = %s
            FOR UPDATE
            """,
            (proposal_id,),
        )
        return await cur.fetchone()

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

    async def get_audit_for_update(self, cur, audit_id: str) -> Optional[Dict]:
        await cur.execute(
            """
            SELECT *
            FROM entity_merge_audits
            WHERE audit_id = %s
            FOR UPDATE
            """,
            (audit_id,),
        )
        return await cur.fetchone()
