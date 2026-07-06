import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

from loguru import logger

from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from common.utils.time_utils import get_now_ms
from infrastructure.postgres_client import PostgresClient
from core.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)


class GraphWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

    def _current_time_ms(self) -> int:
        return get_now_ms()

    def _normalize_timestamp_ms(self, value) -> int:
        if value is None or value == "":
            return self._current_time_ms()
        if isinstance(value, datetime):
            return int(value.timestamp() * 1000)
        if isinstance(value, str):
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        return int(value)

    @staticmethod
    def _require_project_id(project_id: str, operation: str) -> str:
        return require_scope_value(project_id, "project_id", operation)

    @staticmethod
    def _merge_evidence_refs(existing: List, incoming: List) -> List:
        merged = []
        seen = set()
        for ref in (existing or []) + (incoming or []):
            key = json.dumps(ref, sort_keys=True) if isinstance(ref, dict) else str(ref)
            if key in seen:
                continue
            seen.add(key)
            merged.append(ref)
        return merged

    @staticmethod
    def _relationship_id(project_id: str, entity_a_id: int, entity_b_id: int) -> str:
        a_id, b_id = sorted((entity_a_id, entity_b_id))
        return f"{project_id}:{a_id}:{b_id}"

    @staticmethod
    def _clean_string(value):
        if isinstance(value, str):
            return value.strip('"')
        return value

    @classmethod
    def _dedupe_aliases(cls, *alias_groups) -> List[str]:
        aliases = []
        seen = set()
        for group in alias_groups:
            if group is None:
                continue
            values = group if isinstance(group, list) else [group]
            for value in values:
                alias = cls._clean_string(value)
                if not alias or alias in seen:
                    continue
                seen.add(alias)
                aliases.append(alias)
        return aliases

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

    async def save_message_logs(self, messages: List[Dict]) -> bool:
        if not messages:
            return True

        async with self.client.transaction() as cur:
            batch_params = []
            for msg in messages:
                missing = [
                    key
                    for key in ("user_name", "session_id", "project_id")
                    if not msg.get(key)
                ]
                if missing:
                    raise ValueError(
                        f"Message {msg.get('id')} missing required scope "
                        f"fields: {missing}"
                    )

                batch_params.append(
                    {
                        "id": msg["id"],
                        "content": msg["content"],
                        "role": msg["role"],
                        "user_name": msg["user_name"],
                        "session_id": msg["session_id"],
                        "project_id": msg["project_id"],
                        "user_msg_id": (
                            msg.get("user_msg_id")
                            or (msg["id"] if msg["role"] == "user" else None)
                        ),
                        "metadata": json.dumps(msg.get("metadata") or {}),
                        "timestamp": self._normalize_timestamp_ms(
                            msg.get("timestamp")
                        ),
                    }
                )

            # Write canonical messages first.
            for msg in batch_params:
                await cur.execute(
                    """
                    INSERT INTO messages (
                        user_name,
                        session_id,
                        message_id,
                        project_id,
                        role,
                        content,
                        user_msg_id,
                        metadata,
                        timestamp_ms
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (user_name, session_id, message_id)
                    DO UPDATE SET
                        message_id = EXCLUDED.message_id,
                        user_msg_id = EXCLUDED.user_msg_id,
                        metadata = EXCLUDED.metadata
                    WHERE messages.project_id = EXCLUDED.project_id
                      AND messages.role = EXCLUDED.role
                      AND messages.content = EXCLUDED.content
                      AND messages.timestamp_ms = EXCLUDED.timestamp_ms
                    RETURNING message_id
                    """,
                    (
                        msg["user_name"],
                        msg["session_id"],
                        msg["id"],
                        msg["project_id"],
                        msg["role"],
                        msg["content"],
                        msg["user_msg_id"],
                        msg["metadata"],
                        msg["timestamp"],
                    ),
                )
                persisted = await cur.fetchone()
                if not persisted:
                    raise RuntimeError(
                        "Canonical message ID collision for "
                        f"{msg['user_name']}/{msg['session_id']}/"
                        f"{msg['id']}"
                    )

            # Temporarily keep AGE Message nodes for Fact EXTRACTED_FROM
            # links until facts are moved to canonical Postgres tables.
            await self.projection.project_messages(cur, batch_params)

            # Write to Hybrid Full Text Search Table
            for msg in batch_params:
                await cur.execute(
                    """
                    INSERT INTO message_search (
                        message_id,
                        user_name,
                        session_id,
                        project_id,
                        content_tsvector
                    )
                    VALUES (%s, %s, %s, %s, to_tsvector('english', %s))
                    ON CONFLICT (message_id)
                    DO UPDATE SET
                        user_name = EXCLUDED.user_name,
                        session_id = EXCLUDED.session_id,
                        project_id = EXCLUDED.project_id,
                        content_tsvector = EXCLUDED.content_tsvector
                    """,
                    (
                        msg["id"],
                        msg["user_name"],
                        msg["session_id"],
                        msg["project_id"],
                        msg["content"],
                    ),
                )

        logger.info(f"Saved {len(messages)} message logs to Postgres/AGE.")
        return True

    async def create_hierarchy_edge(
        self, parent_id: int, child_id: int, *, project_id: str
    ) -> bool:
        project_id = self._require_project_id(project_id, "create_hierarchy_edge")
        if parent_id == child_id:
            logger.warning(f"Self hierarchy edge rejected: {parent_id}")
            return False
        try:
            now_ms = self._current_time_ms()
            async with self.client.transaction() as cur:
                await cur.execute(
                    """
                    SELECT pg_advisory_xact_lock(
                        hashtextextended(%s, 0)
                    )
                    """,
                    (project_id,),
                )
                await cur.execute(
                    """
                    WITH RECURSIVE ancestors(entity_id) AS (
                        SELECT parent_id
                        FROM hierarchy_edges
                        WHERE project_id = %s
                          AND child_id = %s

                        UNION

                        SELECT edge.parent_id
                        FROM hierarchy_edges edge
                        JOIN ancestors
                          ON edge.child_id = ancestors.entity_id
                        WHERE edge.project_id = %s
                    )
                    INSERT INTO hierarchy_edges (
                        project_id,
                        parent_id,
                        child_id,
                        created_at_ms
                    )
                    SELECT %s, %s, %s, %s
                    WHERE EXISTS (
                        SELECT 1
                        FROM entities
                        WHERE entity_id = %s
                          AND project_id = %s
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM entities
                        WHERE entity_id = %s
                          AND project_id = %s
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM ancestors
                        WHERE entity_id = %s
                    )
                    ON CONFLICT (project_id, parent_id, child_id)
                    DO NOTHING
                    RETURNING parent_id
                    """,
                    (
                        project_id,
                        parent_id,
                        project_id,
                        project_id,
                        parent_id,
                        child_id,
                        now_ms,
                        parent_id,
                        project_id,
                        child_id,
                        project_id,
                        child_id,
                    ),
                )
                canonical_record = await cur.fetchone()
                if not canonical_record:
                    return False

                projected = await self.projection.create_hierarchy_edge(
                    cur,
                    parent_id,
                    child_id,
                    project_id,
                    now_ms,
                )
                return projected
        except Exception as e:
            logger.error(
                "Failed to create hierarchy edge "
                f"({child_id})-[:PART_OF]->({parent_id}): {e}"
            )
            return False

    async def delete_relationship(
        self, entity_a_id: int, entity_b_id: int, *, project_id: str
    ) -> bool:
        project_id = self._require_project_id(project_id, "delete_relationship")
        try:
            relationship_id = self._relationship_id(
                project_id,
                entity_a_id,
                entity_b_id,
            )
            async with self.client.transaction() as cur:
                await cur.execute(
                    """
                    DELETE FROM relationship_evidence_refs
                    WHERE relationship_id = %s
                    """,
                    (relationship_id,),
                )
                await cur.execute(
                    """
                    DELETE FROM relationships
                    WHERE relationship_id = %s
                    RETURNING relationship_id
                    """,
                    (relationship_id,),
                )
                canonical_record = await cur.fetchone()
                projected_deleted = await self.projection.delete_relationship(
                    cur,
                    entity_a_id,
                    entity_b_id,
                    project_id,
                )

            return bool(canonical_record or projected_deleted)
        except Exception as e:
            logger.error(
                f"Failed to delete relationship ({entity_a_id}, {entity_b_id}): {e}"
            )
            return False

    async def merge_entities(
        self,
        primary_id: int,
        secondary_id: int,
        *,
        project_id: str,
        final_topic: Optional[str] = None,
    ) -> bool:
        project_id = self._require_project_id(project_id, "merge_entities")
        if primary_id == secondary_id:
            logger.warning(f"Self-merge rejected: {primary_id}")
            return False
        if primary_id == IDENTITY_ENTITY_ID or secondary_id == IDENTITY_ENTITY_ID:
            logger.warning(
                f"Identity entity merge rejected: {primary_id} <- {secondary_id}"
            )
            return False

        try:
            async with self.client.transaction() as cur:
                await cur.execute(
                    """
                    SELECT pg_advisory_xact_lock(
                        hashtextextended(%s, 0)
                    )
                    """,
                    (project_id,),
                )
                await cur.execute(
                    """
                    SELECT
                        p.canonical_name AS p_name,
                        p.topic AS p_topic,
                        p.confidence AS p_conf,
                        p.last_mentioned_ms AS p_last,
                        s.canonical_name AS s_name,
                        s.confidence AS s_conf,
                        s.last_mentioned_ms AS s_last,
                        COALESCE(
                            array_agg(DISTINCT p_alias.alias)
                            FILTER (WHERE p_alias.alias IS NOT NULL),
                            ARRAY[]::text[]
                        ) AS p_aliases,
                        COALESCE(
                            array_agg(DISTINCT s_alias.alias)
                            FILTER (WHERE s_alias.alias IS NOT NULL),
                            ARRAY[]::text[]
                        ) AS s_aliases
                    FROM entities p
                    JOIN entities s
                      ON s.entity_id = %s
                     AND s.project_id = %s
                    LEFT JOIN entity_aliases p_alias
                      ON p_alias.entity_id = p.entity_id
                    LEFT JOIN entity_aliases s_alias
                      ON s_alias.entity_id = s.entity_id
                    WHERE p.entity_id = %s
                      AND p.project_id = %s
                    GROUP BY
                        p.entity_id,
                        p.canonical_name,
                        p.topic,
                        p.confidence,
                        p.last_mentioned_ms,
                        s.entity_id,
                        s.canonical_name,
                        s.confidence,
                        s.last_mentioned_ms
                    """,
                    (
                        secondary_id,
                        project_id,
                        primary_id,
                        project_id,
                    ),
                )
                check = await cur.fetchone()

                if not check:
                    logger.error(
                        "Merge failed: one or both entities not found "
                        f"({primary_id}, {secondary_id})"
                    )
                    return False

                await cur.execute(
                    """
                    WITH RECURSIVE
                    primary_ancestors(entity_id) AS (
                        SELECT parent_id
                        FROM hierarchy_edges
                        WHERE project_id = %s
                          AND child_id = %s

                        UNION

                        SELECT edge.parent_id
                        FROM hierarchy_edges edge
                        JOIN primary_ancestors ancestor
                          ON edge.child_id = ancestor.entity_id
                        WHERE edge.project_id = %s
                    ),
                    secondary_ancestors(entity_id) AS (
                        SELECT parent_id
                        FROM hierarchy_edges
                        WHERE project_id = %s
                          AND child_id = %s

                        UNION

                        SELECT edge.parent_id
                        FROM hierarchy_edges edge
                        JOIN secondary_ancestors ancestor
                          ON edge.child_id = ancestor.entity_id
                        WHERE edge.project_id = %s
                    )
                    SELECT EXISTS (
                        SELECT 1
                        FROM primary_ancestors
                        WHERE entity_id = %s

                        UNION ALL

                        SELECT 1
                        FROM secondary_ancestors
                        WHERE entity_id = %s
                    ) AS creates_cycle
                    """,
                    (
                        project_id,
                        primary_id,
                        project_id,
                        project_id,
                        secondary_id,
                        project_id,
                        secondary_id,
                        primary_id,
                    ),
                )
                cycle_check = await cur.fetchone() or {}
                if cycle_check.get("creates_cycle"):
                    logger.warning(
                        "Merge rejected because hierarchy connects "
                        f"{primary_id} and {secondary_id}"
                    )
                    return False

                s_name_raw = self._clean_string(check["s_name"])
                primary_topic = (
                    self._clean_string(check["p_topic"]) or "General"
                )
                final_topic = self._clean_string(final_topic) or primary_topic
                p_conf = float(check["p_conf"] or 0)
                s_conf = float(check["s_conf"] or 0)
                p_last = int(check["p_last"] or 0)
                s_last = int(check["s_last"] or 0)

                combined_aliases = self._dedupe_aliases(
                    check.get("p_aliases"),
                    check.get("s_aliases"),
                    [s_name_raw],
                )
                new_conf = s_conf if s_conf > p_conf else p_conf
                new_last = s_last if s_last > p_last else p_last
                now_ms = self._current_time_ms()
                await cur.execute(
                    """
                    UPDATE entities
                    SET confidence = %s,
                        topic = %s,
                        last_mentioned_ms = %s,
                        last_updated_ms = %s
                    WHERE entity_id = %s
                      AND project_id = %s
                    """,
                    (
                        new_conf,
                        final_topic,
                        new_last,
                        now_ms,
                        primary_id,
                        project_id,
                    ),
                )
                for alias in combined_aliases:
                    if not alias:
                        continue
                    await cur.execute(
                        """
                        INSERT INTO entity_aliases (entity_id, alias)
                        VALUES (%s, %s)
                        ON CONFLICT (entity_id, alias) DO NOTHING
                        """,
                        (primary_id, alias),
                    )

                await cur.execute(
                    """
                    UPDATE facts
                    SET entity_id = %s
                    WHERE entity_id = %s
                      AND project_id = %s
                    """,
                    (primary_id, secondary_id, project_id),
                )

                await cur.execute(
                    """
                    SELECT *
                    FROM relationships
                    WHERE project_id = %s
                      AND (
                          entity_a_id = %s
                          OR entity_b_id = %s
                      )
                    """,
                    (project_id, secondary_id, secondary_id),
                )
                canonical_relationships = await cur.fetchall()
                for rel in canonical_relationships:
                    old_relationship_id = rel["relationship_id"]
                    rel_a = int(rel["entity_a_id"])
                    rel_b = int(rel["entity_b_id"])
                    target_id = rel_b if rel_a == secondary_id else rel_a

                    if target_id == primary_id:
                        await cur.execute(
                            """
                            DELETE FROM relationship_evidence_refs
                            WHERE relationship_id = %s
                            """,
                            (old_relationship_id,),
                        )
                        await cur.execute(
                            """
                            DELETE FROM relationships
                            WHERE relationship_id = %s
                            """,
                            (old_relationship_id,),
                        )
                        continue

                    new_a, new_b = sorted((primary_id, target_id))
                    new_relationship_id = self._relationship_id(
                        project_id,
                        new_a,
                        new_b,
                    )
                    await cur.execute(
                        """
                        INSERT INTO relationships (
                            relationship_id,
                            user_name,
                            project_id,
                            entity_a_id,
                            entity_b_id,
                            weight,
                            confidence,
                            context,
                            last_seen_ms
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (relationship_id) DO UPDATE SET
                            weight = relationships.weight + EXCLUDED.weight,
                            confidence = GREATEST(
                                relationships.confidence,
                                EXCLUDED.confidence
                            ),
                            context = COALESCE(
                                EXCLUDED.context,
                                relationships.context
                            ),
                            last_seen_ms = GREATEST(
                                COALESCE(relationships.last_seen_ms, 0),
                                COALESCE(EXCLUDED.last_seen_ms, 0)
                            )
                        """,
                        (
                            new_relationship_id,
                            rel["user_name"],
                            project_id,
                            new_a,
                            new_b,
                            rel["weight"],
                            rel["confidence"],
                            rel["context"],
                            rel["last_seen_ms"],
                        ),
                    )
                    await cur.execute(
                        """
                        INSERT INTO relationship_evidence_refs (
                            relationship_id,
                            user_name,
                            session_id,
                            message_id
                        )
                        SELECT %s, user_name, session_id, message_id
                        FROM relationship_evidence_refs
                        WHERE relationship_id = %s
                        ON CONFLICT (
                            relationship_id,
                            user_name,
                            session_id,
                            message_id
                        ) DO NOTHING
                        """,
                        (new_relationship_id, old_relationship_id),
                    )
                    if new_relationship_id != old_relationship_id:
                        await cur.execute(
                            """
                            DELETE FROM relationship_evidence_refs
                            WHERE relationship_id = %s
                            """,
                            (old_relationship_id,),
                        )
                        await cur.execute(
                            """
                            DELETE FROM relationships
                            WHERE relationship_id = %s
                            """,
                            (old_relationship_id,),
                        )

                await cur.execute(
                    """
                    WITH rewritten AS (
                        SELECT
                            project_id,
                            CASE
                                WHEN parent_id = %s THEN %s
                                ELSE parent_id
                            END AS parent_id,
                            CASE
                                WHEN child_id = %s THEN %s
                                ELSE child_id
                            END AS child_id,
                            created_at_ms
                        FROM hierarchy_edges
                        WHERE project_id = %s
                          AND (
                              parent_id = %s
                              OR child_id = %s
                          )
                    ),
                    deduplicated AS (
                        SELECT
                            project_id,
                            parent_id,
                            child_id,
                            MIN(created_at_ms) AS created_at_ms
                        FROM rewritten
                        WHERE parent_id <> child_id
                        GROUP BY project_id, parent_id, child_id
                    )
                    INSERT INTO hierarchy_edges (
                        project_id,
                        parent_id,
                        child_id,
                        created_at_ms
                    )
                    SELECT
                        project_id,
                        parent_id,
                        child_id,
                        created_at_ms
                    FROM deduplicated
                    ON CONFLICT (project_id, parent_id, child_id)
                    DO NOTHING
                    """,
                    (
                        secondary_id,
                        primary_id,
                        secondary_id,
                        primary_id,
                        project_id,
                        secondary_id,
                        secondary_id,
                    ),
                )

                await cur.execute(
                    """
                    DELETE FROM hierarchy_edges
                    WHERE project_id = %s
                      AND (
                          parent_id = %s
                          OR child_id = %s
                      )
                    """,
                    (project_id, secondary_id, secondary_id),
                )

                await cur.execute(
                    """
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
                            FILTER (
                                WHERE ref.relationship_id IS NOT NULL
                            ),
                            '[]'
                        ) AS evidence_refs
                    FROM relationships rel
                    LEFT JOIN relationship_evidence_refs ref
                      ON ref.relationship_id = rel.relationship_id
                    WHERE rel.project_id = %s
                      AND (
                          rel.entity_a_id = %s
                          OR rel.entity_b_id = %s
                      )
                    GROUP BY rel.relationship_id
                    """,
                    (project_id, primary_id, primary_id),
                )
                relationship_projection_rows = await cur.fetchall()
                relationship_projection = (
                    self._relationship_projection_params(
                        relationship_projection_rows
                    )
                )

                await cur.execute(
                    """
                    SELECT project_id, parent_id, child_id, created_at_ms
                    FROM hierarchy_edges
                    WHERE project_id = %s
                      AND (
                          parent_id = %s
                          OR child_id = %s
                      )
                    """,
                    (project_id, primary_id, primary_id),
                )
                hierarchy_projection_rows = await cur.fetchall()
                hierarchy_projection = self._hierarchy_projection_params(
                    hierarchy_projection_rows
                )

                await cur.execute(
                    """
                    UPDATE fact_search
                    SET entity_id = %s
                    WHERE entity_id = %s
                    """,
                    (primary_id, secondary_id),
                )
                await cur.execute(
                    """
                    SELECT
                        (
                            SELECT count(*)
                            FROM facts
                            WHERE entity_id = %s
                        ) AS fact_count,
                        (
                            SELECT count(*)
                            FROM relationships
                            WHERE entity_a_id = %s
                               OR entity_b_id = %s
                        ) AS relationship_count,
                        (
                            SELECT count(*)
                            FROM hierarchy_edges
                            WHERE parent_id = %s
                               OR child_id = %s
                        ) AS hierarchy_count
                    """,
                    (
                        secondary_id,
                        secondary_id,
                        secondary_id,
                        secondary_id,
                        secondary_id,
                    ),
                )
                remaining = await cur.fetchone() or {}
                if any(
                    int(remaining.get(field, 0))
                    for field in (
                        "fact_count",
                        "relationship_count",
                        "hierarchy_count",
                    )
                ):
                    raise RuntimeError(
                        "Merge left canonical dependencies on secondary "
                        f"entity {secondary_id}"
                    )

                await self.projection.update_merged_entity(
                    cur,
                    primary_id,
                    project_id,
                    combined_aliases,
                    new_conf,
                    new_last,
                    now_ms,
                )
                await self.projection.transfer_merged_entity_facts(
                    cur,
                    primary_id,
                    secondary_id,
                )
                await self.projection.project_entity_topics(
                    cur,
                    [{"id": primary_id, "topic": final_topic}],
                )
                await self.projection.replace_relationships_for_entities(
                    cur,
                    project_id,
                    [primary_id, secondary_id],
                    relationship_projection,
                )
                await self.projection.replace_hierarchy_edges_for_entities(
                    cur,
                    project_id,
                    [primary_id, secondary_id],
                    hierarchy_projection,
                )
                await self.projection.delete_entity_projection(
                    cur,
                    secondary_id,
                    project_id,
                )

                await cur.execute(
                    """
                    DELETE FROM entities
                    WHERE entity_id = %s
                      AND project_id = %s
                    RETURNING entity_id
                    """,
                    (secondary_id, project_id),
                )
                deleted_secondary = await cur.fetchone()
                if not deleted_secondary:
                    raise RuntimeError(
                        f"Secondary entity {secondary_id} disappeared "
                        "before merge deletion"
                    )

                logger.info(f"Merged entity {secondary_id} into {primary_id}")
                return True
        except Exception as e:
            logger.error(f"Merge transaction failed: {e}")
            return False
