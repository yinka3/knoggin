import json
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from loguru import logger
from psycopg import Error as PsycopgError

from common.exceptions import StorageWriteError
from common.schema.ingestion.contracts import (
    normalize_relationship_type,
    relationship_identity,
)
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from common.utils.time_utils import get_now_ms
from core.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)
from infrastructure.postgres_client import PostgresClient


class EntityMergeWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

    def _current_time_ms(self) -> int:
        return get_now_ms()

    @staticmethod
    def _raise_storage_write(operation: str, exc: Exception) -> None:
        logger.error("Storage write failed for {}: {}", operation, exc)
        raise StorageWriteError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    @asynccontextmanager
    async def _merge_cursor(self, cur):
        if cur is not None:
            yield cur
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

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
    def _relationship_id(
        project_id: str,
        entity_a_id: int,
        entity_b_id: int,
        relationship_type: str,
        *,
        symmetric: bool = True,
    ) -> str:
        return relationship_identity(
            project_id,
            entity_a_id,
            entity_b_id,
            relationship_type,
            symmetric=symmetric,
        )

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
            params.append(
                {
                    "relationship_id": row["relationship_id"],
                    "project_id": row["project_id"],
                    "entity_a_id": int(row["entity_a_id"]),
                    "entity_b_id": int(row["entity_b_id"]),
                    "relationship_type": normalize_relationship_type(
                        row["relationship_type"]
                    ),
                }
            )
            params[-1]["symmetric"] = bool(row.get("symmetric", False))
        return params

    async def delete_relationship(
        self,
        entity_a_id: int,
        entity_b_id: int,
        *,
        relationship_type: str,
        project_id: str,
    ) -> bool:
        project_id = self._require_project_id(project_id, "delete_relationship")
        relationship_id = self._relationship_id(
            project_id,
            entity_a_id,
            entity_b_id,
            relationship_type,
        )
        try:
            async with self.client.transaction() as cur:
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
                    relationship_id,
                    project_id,
                )

            return bool(canonical_record or projected_deleted)
        except PsycopgError as exc:
            self._raise_storage_write("delete_relationship", exc)

    async def merge_entities(
        self,
        primary_id: int,
        secondary_id: int,
        *,
        project_id: str,
        final_topic: Optional[str] = None,
        cur=None,
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
            async with self._merge_cursor(cur) as cur:
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
                        p_context.topic AS p_topic,
                        p_context.last_mentioned_ms AS p_last,
                        s.canonical_name AS s_name,
                        s_context.last_mentioned_ms AS s_last,
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
                    JOIN project_entity_contexts p_context
                      ON p_context.entity_id = p.entity_id
                     AND p_context.project_id = %s
                    JOIN project_entity_contexts s_context
                      ON s_context.entity_id = s.entity_id
                     AND s_context.project_id = %s
                    LEFT JOIN entity_aliases p_alias
                      ON p_alias.entity_id = p.entity_id
                    LEFT JOIN entity_aliases s_alias
                      ON s_alias.entity_id = s.entity_id
                    WHERE p.entity_id = %s
                    GROUP BY
                        p.entity_id,
                        p.canonical_name,
                        p_context.topic,
                        p_context.last_mentioned_ms,
                        s.entity_id,
                        s.canonical_name,
                        s_context.last_mentioned_ms
                    """,
                    (
                        secondary_id,
                        project_id,
                        project_id,
                        primary_id,
                    ),
                )
                check = await cur.fetchone()

                if not check:
                    logger.error(
                        "Merge failed: one or both entities not found "
                        f"({primary_id}, {secondary_id})"
                    )
                    return False

                s_name_raw = self._clean_string(check["s_name"])
                primary_topic = self._clean_string(check["p_topic"]) or "General"
                final_topic = self._clean_string(final_topic) or primary_topic
                p_last = int(check["p_last"] or 0)
                s_last = int(check["s_last"] or 0)

                combined_aliases = self._dedupe_aliases(
                    check.get("p_aliases"),
                    check.get("s_aliases"),
                    [s_name_raw],
                )
                primary_aliases = self._dedupe_aliases(check.get("p_aliases"))
                new_last = s_last if s_last > p_last else p_last
                await cur.execute(
                    """
                    UPDATE project_entity_contexts
                    SET topic = %s,
                        last_mentioned_ms = %s
                    WHERE entity_id = %s
                      AND project_id = %s
                    """,
                    (
                        final_topic,
                        new_last,
                        primary_id,
                        project_id,
                    ),
                )
                await cur.execute(
                    """
                    DELETE FROM message_entity_refs secondary_ref
                    USING messages message
                    WHERE secondary_ref.message_id = message.message_id
                      AND secondary_ref.entity_id = %s
                      AND message.project_id = %s
                      AND EXISTS (
                          SELECT 1
                          FROM message_entity_refs primary_ref
                          WHERE primary_ref.message_id = secondary_ref.message_id
                            AND primary_ref.entity_id = %s
                      )
                    """,
                    (secondary_id, project_id, primary_id),
                )
                await cur.execute(
                    """
                    UPDATE message_entity_refs
                    SET entity_id = %s
                    FROM messages message
                    WHERE message_entity_refs.message_id = message.message_id
                      AND message_entity_refs.entity_id = %s
                      AND message.project_id = %s
                    """,
                    (primary_id, secondary_id, project_id),
                )
                await cur.execute(
                    """
                    INSERT INTO episode_entities (
                        episode_id,
                        project_id,
                        entity_id,
                        prominence_weight,
                        role,
                        is_focus_entity,
                        source_message_count
                    )
                    SELECT
                        ee.episode_id,
                        ee.project_id,
                        %s,
                        ee.prominence_weight,
                        ee.role,
                        ee.is_focus_entity,
                        ee.source_message_count
                    FROM episode_entities ee
                    JOIN episodes episode
                      ON episode.episode_id = ee.episode_id
                     AND episode.project_id = ee.project_id
                    WHERE ee.entity_id = %s
                      AND episode.project_id = %s
                    ON CONFLICT (episode_id, entity_id) DO UPDATE SET
                        prominence_weight = GREATEST(
                            episode_entities.prominence_weight,
                            EXCLUDED.prominence_weight
                        ),
                        role = COALESCE(
                            episode_entities.role,
                            EXCLUDED.role
                        ),
                        is_focus_entity = (
                            episode_entities.is_focus_entity
                            OR EXCLUDED.is_focus_entity
                        ),
                        source_message_count = GREATEST(
                            episode_entities.source_message_count,
                            EXCLUDED.source_message_count
                        )
                    """,
                    (primary_id, secondary_id, project_id),
                )
                await cur.execute(
                    """
                    DELETE FROM episode_entities ee
                    USING episodes episode
                    WHERE ee.episode_id = episode.episode_id
                      AND ee.project_id = episode.project_id
                      AND ee.entity_id = %s
                      AND episode.project_id = %s
                    """,
                    (secondary_id, project_id),
                )
                await cur.execute(
                    """
                    WITH source_stats AS (
                        SELECT
                            em.episode_id,
                            COUNT(DISTINCT em.message_id) AS source_message_count,
                            COALESCE(
                                SUM(em.influence_weight),
                                0.0
                            ) AS prominence_weight,
                            MIN(m.timestamp_ms) AS first_seen_at_ms,
                            MAX(m.timestamp_ms) AS last_seen_at_ms
                        FROM episode_messages em
                        JOIN episodes episode
                          ON episode.episode_id = em.episode_id
                         AND episode.project_id = em.project_id
                        JOIN messages m
                          ON m.message_id = em.message_id
                         AND m.project_id = em.project_id
                         AND m.session_id = em.session_id
                        JOIN message_entity_refs mer
                          ON mer.message_id = em.message_id
                        WHERE mer.entity_id = %s
                          AND episode.project_id = %s
                        GROUP BY em.episode_id
                    )
                    UPDATE episode_entities ee
                    SET
                        source_message_count = source_stats.source_message_count,
                        prominence_weight = GREATEST(
                            ee.prominence_weight,
                            source_stats.prominence_weight
                        ),
                        first_seen_at = CASE
                            WHEN source_stats.first_seen_at_ms IS NULL THEN NULL
                            ELSE to_timestamp(
                                source_stats.first_seen_at_ms / 1000.0
                            )
                        END,
                        last_seen_at = CASE
                            WHEN source_stats.last_seen_at_ms IS NULL THEN NULL
                            ELSE to_timestamp(
                                source_stats.last_seen_at_ms / 1000.0
                            )
                        END
                    FROM source_stats
                    WHERE ee.episode_id = source_stats.episode_id
                      AND ee.entity_id = %s
                    """,
                    (primary_id, project_id, primary_id),
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
                            DELETE FROM relationship_observations
                            WHERE relationship_id = %s
                              AND project_id = %s
                            """,
                            (old_relationship_id, project_id),
                        )
                        await cur.execute(
                            """
                            DELETE FROM relationships
                            WHERE relationship_id = %s
                            """,
                            (old_relationship_id,),
                        )
                        continue

                    symmetric = bool(rel.get("symmetric", False))
                    if symmetric:
                        new_a, new_b = sorted((primary_id, target_id))
                    elif rel["entity_a_id"] == secondary_id:
                        new_a, new_b = primary_id, target_id
                    else:
                        new_a, new_b = target_id, primary_id
                    new_relationship_id = self._relationship_id(
                        project_id,
                        new_a,
                        new_b,
                        rel["relationship_type"],
                        symmetric=symmetric,
                    )
                    await cur.execute(
                        """
                        INSERT INTO relationships (
                            relationship_id,
                            user_name,
                            project_id,
                            entity_a_id,
                            entity_b_id,
                            relationship_type,
                            "symmetric"
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (relationship_id) DO NOTHING
                        """,
                        (
                            new_relationship_id,
                            rel["user_name"],
                            project_id,
                            new_a,
                            new_b,
                            rel["relationship_type"],
                            bool(rel.get("symmetric", False)),
                        ),
                    )
                    await cur.execute(
                        """
                        WITH rewritten AS (
                            SELECT
                                project_id,
                                user_name,
                                session_id,
                                message_id,
                                CASE
                                    WHEN source_entity_id = %s THEN %s
                                    ELSE source_entity_id
                                END AS rewritten_source_id,
                                CASE
                                    WHEN target_entity_id = %s THEN %s
                                    ELSE target_entity_id
                                END AS rewritten_target_id,
                                source_type,
                                target_type,
                                observed_relationship_label,
                                canonical_relationship_type,
                                domain_status,
                                domain_version,
                                "symmetric",
                                confidence,
                                context,
                                observed_at_ms
                            FROM relationship_observations
                            WHERE relationship_id = %s
                              AND project_id = %s
                        )
                        INSERT INTO relationship_observations (
                            relationship_id,
                            project_id,
                            user_name,
                            session_id,
                            message_id,
                            source_entity_id,
                            target_entity_id,
                            source_type,
                            target_type,
                            observed_relationship_label,
                            canonical_relationship_type,
                            domain_status,
                            domain_version,
                            "symmetric",
                            confidence,
                            context,
                            observed_at_ms
                        )
                        SELECT
                            %s,
                            project_id,
                            user_name,
                            session_id,
                            message_id,
                            CASE WHEN %s
                                THEN LEAST(rewritten_source_id, rewritten_target_id)
                                ELSE rewritten_source_id
                            END,
                            CASE WHEN %s
                                THEN GREATEST(rewritten_source_id, rewritten_target_id)
                                ELSE rewritten_target_id
                            END,
                            source_type,
                            target_type,
                            observed_relationship_label,
                            canonical_relationship_type,
                            domain_status,
                            domain_version,
                            "symmetric",
                            confidence,
                            context,
                            observed_at_ms
                        FROM rewritten
                        ON CONFLICT (
                            project_id,
                            user_name,
                            session_id,
                            message_id,
                            source_entity_id,
                            target_entity_id,
                            observed_relationship_label
                        ) DO UPDATE SET
                            relationship_id = EXCLUDED.relationship_id,
                            canonical_relationship_type = COALESCE(
                                EXCLUDED.canonical_relationship_type,
                                relationship_observations.canonical_relationship_type
                            ),
                            domain_status = EXCLUDED.domain_status,
                            domain_version = EXCLUDED.domain_version,
                            "symmetric" = EXCLUDED."symmetric",
                            confidence = GREATEST(
                                relationship_observations.confidence,
                                EXCLUDED.confidence
                            ),
                            context = COALESCE(
                                EXCLUDED.context,
                                relationship_observations.context
                            ),
                            observed_at_ms = GREATEST(
                                relationship_observations.observed_at_ms,
                                EXCLUDED.observed_at_ms
                            )
                        """,
                        (
                            secondary_id,
                            primary_id,
                            secondary_id,
                            primary_id,
                            old_relationship_id,
                            project_id,
                            new_relationship_id,
                            bool(rel.get("symmetric", False)),
                            bool(rel.get("symmetric", False)),
                        ),
                    )
                    await cur.execute(
                        """
                        INSERT INTO episode_relationships (
                            episode_id,
                            project_id,
                            relationship_id,
                            prominence_weight,
                            is_central_relationship,
                            source_message_count
                        )
                        SELECT
                            episode_id,
                            project_id,
                            %s,
                            prominence_weight,
                            is_central_relationship,
                            source_message_count
                        FROM episode_relationships
                        WHERE relationship_id = %s
                          AND project_id = %s
                        ON CONFLICT (episode_id, relationship_id) DO UPDATE SET
                            prominence_weight = GREATEST(
                                episode_relationships.prominence_weight,
                                EXCLUDED.prominence_weight
                            ),
                            is_central_relationship = (
                                episode_relationships.is_central_relationship
                                OR EXCLUDED.is_central_relationship
                            ),
                            source_message_count = GREATEST(
                                episode_relationships.source_message_count,
                                EXCLUDED.source_message_count
                            )
                        """,
                        (new_relationship_id, old_relationship_id, project_id),
                    )
                    await cur.execute(
                        """
                        WITH source_stats AS (
                            SELECT
                                em.episode_id,
                                COUNT(DISTINCT em.message_id) AS source_message_count,
                                COALESCE(
                                    SUM(em.influence_weight),
                                    0.0
                                ) AS prominence_weight
                            FROM episode_messages em
                            JOIN relationship_observations rer
                              ON rer.message_id = em.message_id
                             AND rer.project_id = em.project_id
                             AND rer.session_id = em.session_id
                             AND rer.relationship_id = %s
                             AND rer.project_id = %s
                            GROUP BY em.episode_id
                        )
                        UPDATE episode_relationships er
                        SET
                            source_message_count = source_stats.source_message_count,
                            prominence_weight = GREATEST(
                                er.prominence_weight,
                                source_stats.prominence_weight
                            )
                        FROM source_stats
                        WHERE er.episode_id = source_stats.episode_id
                          AND er.relationship_id = %s
                        """,
                        (new_relationship_id, project_id, new_relationship_id),
                    )
                    if new_relationship_id != old_relationship_id:
                        await cur.execute(
                            """
                            DELETE FROM relationship_observations
                            WHERE relationship_id = %s
                              AND project_id = %s
                            """,
                            (old_relationship_id, project_id),
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
                    SELECT
                        rel.relationship_id,
                        rel.user_name,
                        rel.project_id,
                        rel.entity_a_id,
                        rel.entity_b_id,
                        rel.relationship_type,
                        rel."symmetric" AS symmetric
                    FROM relationships rel
                    WHERE rel.project_id = %s
                      AND (
                          rel.entity_a_id = %s
                          OR rel.entity_b_id = %s
                      )
                    """,
                    (project_id, primary_id, primary_id),
                )
                relationship_projection_rows = await cur.fetchall()
                relationship_projection = self._relationship_projection_params(
                    relationship_projection_rows
                )

                await cur.execute(
                    """
                    SELECT
                        (
                            SELECT count(*)
                            FROM message_entity_refs ref
                            JOIN messages message ON message.message_id = ref.message_id
                            WHERE ref.entity_id = %s
                              AND message.project_id = %s
                        ) AS message_ref_count,
                        (
                            SELECT count(*)
                            FROM episode_entities
                            WHERE entity_id = %s AND project_id = %s
                        ) AS episode_entity_count,
                        (
                            SELECT count(*)
                            FROM relationships
                            WHERE project_id = %s
                              AND (entity_a_id = %s
                               OR entity_b_id = %s
                              )
                        ) AS relationship_count
                    """,
                    (
                        secondary_id,
                        project_id,
                        secondary_id,
                        project_id,
                        project_id,
                        secondary_id,
                        secondary_id,
                    ),
                )
                remaining = await cur.fetchone() or {}
                if any(
                    int(remaining.get(field, 0))
                    for field in (
                        "message_ref_count",
                        "episode_entity_count",
                        "relationship_count",
                    )
                ):
                    raise RuntimeError(
                        "Merge left canonical dependencies on secondary "
                        f"entity {secondary_id}"
                    )

                await self.projection.replace_relationships_for_entities(
                    cur,
                    project_id,
                    [primary_id, secondary_id],
                    relationship_projection,
                )
                await cur.execute(
                    """
                    DELETE FROM project_entity_contexts
                    WHERE entity_id = %s AND project_id = %s
                    """,
                    (secondary_id, project_id),
                )
                await cur.execute(
                    """
                    DELETE FROM entities
                    WHERE entity_id = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM project_entity_contexts
                          WHERE entity_id = %s
                      )
                    RETURNING entity_id
                    """,
                    (secondary_id, secondary_id),
                )
                deleted_secondary = await cur.fetchone()
                if deleted_secondary:
                    await self.projection.delete_entity_projection(
                        cur,
                        secondary_id,
                        project_id,
                    )
                    for alias in combined_aliases:
                        await cur.execute(
                            """
                            INSERT INTO entity_aliases (entity_id, alias)
                            VALUES (%s, %s)
                            ON CONFLICT (entity_id, alias) DO NOTHING
                            """,
                            (primary_id, alias),
                        )
                    projected_aliases = combined_aliases
                else:
                    # The secondary identity is still active in another
                    # project. Do not transfer its globally owned aliases.
                    projected_aliases = primary_aliases

                await self.projection.update_merged_entity(
                    cur,
                    primary_id,
                    projected_aliases,
                )

                logger.info(f"Merged entity {secondary_id} into {primary_id}")
                return True
        except PsycopgError as exc:
            self._raise_storage_write("merge_entities", exc)
