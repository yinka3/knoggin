import json
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Optional

from loguru import logger

from common.scoping import IDENTITY_ENTITY_ID
from common.scoping import require_scope_value
from infrastructure.postgres_client import PostgresClient
from knoggin_server.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)


class ProjectionRebuilder:
    """Rebuilds AGE traversal projection from canonical Postgres tables."""

    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

    @staticmethod
    def _to_iso(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return value

    @staticmethod
    def _normalize_evidence_refs(value) -> List[Dict]:
        if not value:
            return []
        refs = json.loads(value) if isinstance(value, str) else value
        if isinstance(refs, dict):
            refs = [refs]
        return [ref for ref in refs if ref]

    @classmethod
    def _relationship_projection_params(cls, rows: List[Dict]) -> List[Dict]:
        params = []
        for row in rows:
            evidence_refs = [
                json.dumps(ref, sort_keys=True)
                for ref in cls._normalize_evidence_refs(row.get("evidence_refs"))
            ]
            params.append(
                {
                    "project_id": row["project_id"],
                    "entity_a_id": int(row["entity_a_id"]),
                    "entity_b_id": int(row["entity_b_id"]),
                    "weight": int(row.get("weight") or 1),
                    "confidence": float(row.get("confidence") or 0),
                    "context": row.get("context"),
                    "last_seen": int(row.get("last_seen_ms") or 0),
                    "message_ids": evidence_refs,
                }
            )
        return params

    @classmethod
    def _fact_projection_params(cls, row: Dict) -> Dict:
        return {
            "id": row["fact_id"],
            "content": row["content"],
            "valid_at": cls._to_iso(row["valid_at"]),
            "invalid_at": cls._to_iso(row["invalid_at"]),
            "confidence": float(row["confidence"] or 0),
            "source_msg_id": row["source_msg_id"],
            "source_user_name": row["source_user_name"],
            "source_session_id": row["source_session_id"],
            "source": row["source"],
        }

    @staticmethod
    def _hierarchy_projection_params(rows: List[Dict]) -> List[Dict]:
        return [
            {
                "project_id": row["project_id"],
                "parent_id": int(row["parent_id"]),
                "child_id": int(row["child_id"]),
                "created_at": int(row.get("created_at_ms") or 0),
            }
            for row in rows
        ]

    async def rebuild_project_projection(
        self,
        project_id: str,
        user_name: str,
    ) -> Dict[str, int]:
        project_id = require_scope_value(
            project_id,
            "project_id",
            "rebuild_project_projection",
        )
        user_name = require_scope_value(
            user_name,
            "user_name",
            "rebuild_project_projection",
        )
        try:
            async with self.client.transaction() as cur:
                await self.projection.clear_project_projection(cur, project_id)

                messages = await self._fetch_messages(
                    cur,
                    project_id,
                    user_name,
                )
                entities = await self._fetch_entities(
                    cur,
                    project_id,
                    user_name,
                )
                relationships = await self._fetch_relationships(
                    cur,
                    project_id,
                    user_name,
                )
                facts = await self._fetch_facts(cur, project_id, user_name)
                hierarchy_edges = await self._fetch_hierarchy_edges(
                    cur,
                    project_id,
                )

                await self.projection.project_messages(cur, messages)
                await self.projection.project_entities(cur, entities)
                await self.projection.project_entity_topics(
                    cur,
                    [
                        {"id": entity["id"], "topic": entity["topic"]}
                        for entity in entities
                        if entity.get("topic")
                    ],
                )
                await self.projection.replace_relationships_for_entities(
                    cur,
                    project_id,
                    [entity["id"] for entity in entities],
                    self._relationship_projection_params(relationships),
                )

                fact_params_by_entity = defaultdict(list)
                fact_user_by_entity = {}
                for fact in facts:
                    entity_id = int(fact["entity_id"])
                    fact_params_by_entity[entity_id].append(
                        self._fact_projection_params(fact)
                    )
                    fact_user_by_entity[entity_id] = fact["user_name"]

                all_fact_params = []
                for entity_id, fact_params in fact_params_by_entity.items():
                    all_fact_params.extend(fact_params)
                    await self.projection.project_facts(
                        cur,
                        entity_id,
                        fact_params,
                        fact_user_by_entity[entity_id],
                        None,
                        project_id,
                    )
                await self.projection.project_fact_message_links(
                    cur,
                    all_fact_params,
                )

                await self.projection.replace_hierarchy_edges_for_entities(
                    cur,
                    project_id,
                    [entity["id"] for entity in entities],
                    self._hierarchy_projection_params(hierarchy_edges),
                )

                summary = {
                    "messages": len(messages),
                    "entities": len(entities),
                    "relationships": len(relationships),
                    "facts": len(facts),
                    "hierarchy_edges": len(hierarchy_edges),
                }
                logger.info(
                    "Rebuilt AGE projection for project "
                    f"{project_id}: {summary}"
                )
                return summary
        except Exception as e:
            logger.error(
                f"Failed to rebuild AGE projection for project {project_id}: {e}"
            )
            return {
                "messages": 0,
                "entities": 0,
                "relationships": 0,
                "facts": 0,
                "hierarchy_edges": 0,
            }

    async def _fetch_messages(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        filters = ["project_id = %s"]
        params = [project_id]
        filters.append("user_name = %s")
        params.append(user_name)

        await cur.execute(
            f"""
            SELECT
                message_id AS id,
                content,
                role,
                user_name,
                session_id,
                project_id,
                timestamp_ms AS timestamp
            FROM messages
            WHERE {" AND ".join(filters)}
            ORDER BY user_name, session_id, message_id
            """,
            tuple(params),
        )
        return list(await cur.fetchall())

    async def _fetch_entities(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        filters = ["(e.project_id = %s OR e.entity_id = %s)"]
        params = [project_id, IDENTITY_ENTITY_ID]
        filters.append("(e.user_name = %s OR e.entity_id = %s)")
        params.extend([user_name, IDENTITY_ENTITY_ID])

        await cur.execute(
            f"""
            SELECT
                e.entity_id AS id,
                e.user_name,
                e.session_id,
                e.project_id,
                e.canonical_name,
                e.type,
                e.topic,
                e.confidence,
                e.last_updated_ms AS last_updated,
                e.last_mentioned_ms AS last_mentioned,
                e.last_profiled_msg_id,
                COALESCE(
                    array_agg(DISTINCT a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                    ARRAY[]::text[]
                ) AS aliases
            FROM entities e
            LEFT JOIN entity_aliases a
              ON a.entity_id = e.entity_id
            WHERE {" AND ".join(filters)}
            GROUP BY e.entity_id
            ORDER BY e.entity_id
            """,
            tuple(params),
        )
        return list(await cur.fetchall())

    async def _fetch_relationships(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        filters = ["rel.project_id = %s"]
        params = [project_id]
        filters.append("rel.user_name = %s")
        params.append(user_name)

        await cur.execute(
            f"""
            SELECT
                rel.relationship_id,
                rel.user_name,
                rel.project_id,
                rel.entity_a_id,
                rel.entity_b_id,
                rel.weight,
                rel.confidence,
                rel.context,
                rel.last_seen_ms,
                COALESCE(
                    json_agg(
                        json_build_object(
                            'user_name', ref.user_name,
                            'session_id', ref.session_id,
                            'message_id', ref.message_id
                        )
                    )
                    FILTER (WHERE ref.relationship_id IS NOT NULL),
                    '[]'
                ) AS evidence_refs
            FROM relationships rel
            LEFT JOIN relationship_evidence_refs ref
              ON ref.relationship_id = rel.relationship_id
            WHERE {" AND ".join(filters)}
            GROUP BY rel.relationship_id
            ORDER BY rel.relationship_id
            """,
            tuple(params),
        )
        return list(await cur.fetchall())

    async def _fetch_facts(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        filters = ["project_id = %s"]
        params = [project_id]
        filters.append("user_name = %s")
        params.append(user_name)

        await cur.execute(
            f"""
            SELECT
                fact_id,
                entity_id,
                user_name,
                project_id,
                content,
                valid_at,
                invalid_at,
                confidence,
                source_msg_id,
                source_user_name,
                source_session_id,
                source
            FROM facts
            WHERE {" AND ".join(filters)}
            ORDER BY entity_id, fact_id
            """,
            tuple(params),
        )
        return list(await cur.fetchall())

    async def _fetch_hierarchy_edges(self, cur, project_id: str) -> List[Dict]:
        await cur.execute(
            """
            SELECT project_id, parent_id, child_id, created_at_ms
            FROM hierarchy_edges
            WHERE project_id = %s
            ORDER BY parent_id, child_id
            """,
            (project_id,),
        )
        return list(await cur.fetchall())
