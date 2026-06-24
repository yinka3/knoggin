import json
from typing import Dict, List, Optional

from loguru import logger

from common.scoping import (
    IDENTITY_ENTITY_ID,
    IDENTITY_SCOPE,
    require_scope_value,
)
from common.utils.time_utils import get_now_ms
from infrastructure.postgres_client import PostgresClient
from knoggin_server.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)


class EntityWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

    def _current_time_ms(self) -> int:
        return get_now_ms()

    @staticmethod
    def _require_scope(data: Dict, fields: List[str], label: str) -> None:
        missing = [field for field in fields if not data.get(field)]
        if missing:
            raise ValueError(f"{label} missing required scope fields: {missing}")

    @staticmethod
    def _require_project_id(project_id: str, operation: str) -> str:
        return require_scope_value(project_id, "project_id", operation)

    @staticmethod
    def _parse_message_id(message_id) -> int:
        if isinstance(message_id, str):
            if message_id.startswith("msg_"):
                return int(message_id.split("_", 1)[1])
            if message_id.startswith("turn_"):
                raise ValueError(
                    "Conversation turn IDs are not canonical message IDs"
                )
        return int(message_id)

    @classmethod
    def _build_evidence_ref(cls, rel: Dict) -> Dict:
        evidence_ref = rel.get("evidence_ref")
        if isinstance(evidence_ref, dict):
            return {
                "user_name": evidence_ref["user_name"],
                "session_id": evidence_ref["session_id"],
                "message_id": cls._parse_message_id(evidence_ref["message_id"]),
            }

        if not rel.get("user_name") or not rel.get("session_id"):
            raise ValueError(
                "Relationship evidence requires user_name and session_id scope"
            )

        return {
            "user_name": rel["user_name"],
            "session_id": rel["session_id"],
            "message_id": cls._parse_message_id(rel["message_id"]),
        }

    @staticmethod
    def _relationship_id(project_id: str, entity_a_id: int, entity_b_id: int) -> str:
        return f"{project_id}:{entity_a_id}:{entity_b_id}"

    @staticmethod
    def _normalized_identity_value(value: object) -> str:
        return str(value or "").strip().strip('"').casefold()

    async def ensure_identity_entity(
        self, user_name: str, aliases: Optional[List[str]] = None
    ) -> Dict:
        """Persist and validate the identity-scoped entity reserved at ID 1."""
        user_name = str(user_name or "").strip()
        if not user_name:
            raise ValueError("Identity requires a non-empty configured user name")
        canonical_key = self._normalized_identity_value(user_name)
        clean_aliases = []
        seen_aliases = {canonical_key}
        for alias in aliases or []:
            clean_alias = str(alias or "").strip()
            alias_key = self._normalized_identity_value(clean_alias)
            if clean_alias and alias_key not in seen_aliases:
                clean_aliases.append(clean_alias)
                seen_aliases.add(alias_key)

        now_ms = self._current_time_ms()
        identity = {
            "id": IDENTITY_ENTITY_ID,
            "user_name": user_name,
            "session_id": None,
            "project_id": IDENTITY_SCOPE,
            "canonical_name": user_name,
            "aliases": clean_aliases,
            "type": "person",
            "topic": "Identity",
            "confidence": 1.0,
            "now": now_ms,
        }

        async with self.client.transaction() as cur:
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (IDENTITY_ENTITY_ID,),
            )
            await cur.execute(
                """
                SELECT entity_id, user_name, project_id, canonical_name
                FROM entities
                WHERE entity_id = %s
                FOR UPDATE
                """,
                (IDENTITY_ENTITY_ID,),
            )
            existing = await cur.fetchone()
            if existing and (
                existing["project_id"] != IDENTITY_SCOPE
                or self._normalized_identity_value(existing["user_name"])
                != canonical_key
                or self._normalized_identity_value(
                    existing["canonical_name"]
                )
                != canonical_key
            ):
                raise RuntimeError(
                    "Entity ID 1 is occupied by a non-identity entity; "
                    "reset the development database before startup"
                )

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
                VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, NULL)
                ON CONFLICT (entity_id) DO UPDATE SET
                    user_name = EXCLUDED.user_name,
                    project_id = EXCLUDED.project_id,
                    session_id = NULL,
                    canonical_name = EXCLUDED.canonical_name,
                    type = EXCLUDED.type,
                    topic = EXCLUDED.topic,
                    confidence = EXCLUDED.confidence,
                    last_updated_ms = EXCLUDED.last_updated_ms
                """,
                (
                    IDENTITY_ENTITY_ID,
                    user_name,
                    IDENTITY_SCOPE,
                    user_name,
                    "person",
                    "Identity",
                    1.0,
                    now_ms,
                    now_ms,
                ),
            )
            await cur.execute(
                "DELETE FROM entity_aliases WHERE entity_id = %s",
                (IDENTITY_ENTITY_ID,),
            )
            for alias in clean_aliases:
                await cur.execute(
                    """
                    INSERT INTO entity_aliases (entity_id, alias)
                    VALUES (%s, %s)
                    ON CONFLICT (entity_id, alias) DO NOTHING
                    """,
                    (IDENTITY_ENTITY_ID, alias),
                )
            await cur.execute(
                """
                INSERT INTO entity_search (
                    entity_id, canonical_name, user_name, project_id, embedding
                )
                VALUES (%s, %s, %s, %s, NULL)
                ON CONFLICT (entity_id) DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    user_name = EXCLUDED.user_name,
                    project_id = EXCLUDED.project_id
                """,
                (
                    IDENTITY_ENTITY_ID,
                    user_name,
                    user_name,
                    IDENTITY_SCOPE,
                ),
            )
            await self.projection.project_identity(cur, identity)

        return identity

    async def write_batch(self, entities: List[Dict], relationships: List[Dict]):
        # We need a transaction for both Graph and Hybrid tables
        now_ms = self._current_time_ms()

        async with self.client.transaction() as cur:
            # Write Entities to Graph
            if entities:
                entity_params = []
                for e in entities:
                    self._require_scope(
                        e,
                        ["user_name", "session_id", "project_id"],
                        f"Entity {e.get('id')}",
                    )
                    if "is_new" not in e:
                        raise ValueError(
                            f"Entity {e.get('id')} missing is_new write intent"
                        )
                    if e.get("id") == IDENTITY_ENTITY_ID:
                        raise ValueError(
                            "Identity entity writes must use "
                            "ensure_identity_entity"
                        )

                    e_clean = e.copy()
                    is_new = bool(e_clean.pop("is_new"))
                    e_clean["aliases"] = e.get("aliases") or []
                    e_clean["now"] = now_ms
                    entity_params.append(e_clean)

                    if is_new:
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
                            VALUES (
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s
                            )
                            """,
                            (
                                e_clean["id"],
                                e_clean["user_name"],
                                e_clean["project_id"],
                                e_clean["session_id"],
                                e_clean["canonical_name"],
                                e_clean.get("type"),
                                e_clean.get("topic", "General"),
                                e_clean.get("confidence", 1.0),
                                now_ms,
                                now_ms,
                                e_clean.get("last_profiled_msg_id"),
                            ),
                        )
                    else:
                        await cur.execute(
                            """
                            UPDATE entities
                            SET session_id = %s,
                                canonical_name = %s,
                                type = COALESCE(type, %s),
                                topic = %s,
                                confidence = %s,
                                last_mentioned_ms = %s,
                                last_updated_ms = %s,
                                last_profiled_msg_id = COALESCE(
                                    last_profiled_msg_id,
                                    %s
                                )
                            WHERE entity_id = %s
                              AND (
                                  project_id = %s
                                  OR entity_id = %s
                              )
                            RETURNING entity_id
                            """,
                            (
                                e_clean["session_id"],
                                e_clean["canonical_name"],
                                e_clean.get("type"),
                                e_clean.get("topic", "General"),
                                e_clean.get("confidence", 1.0),
                                now_ms,
                                now_ms,
                                e_clean.get("last_profiled_msg_id"),
                                e_clean["id"],
                                e_clean["project_id"],
                                IDENTITY_ENTITY_ID,
                            ),
                        )

                        persisted = await cur.fetchone()
                        if not persisted:
                            raise RuntimeError(
                                f"Existing entity {e_clean['id']} was not "
                                f"found in project {e_clean['project_id']}"
                            )

                    for alias in e_clean["aliases"]:
                        if not alias:
                            continue
                        await cur.execute(
                            """
                            INSERT INTO entity_aliases (entity_id, alias)
                            VALUES (%s, %s)
                            ON CONFLICT (entity_id, alias) DO NOTHING
                            """,
                            (e_clean["id"], alias),
                        )

                await self.projection.project_entities(cur, entity_params)

                # Handle topics in AGE without FOREACH
                topic_params = [
                    {"id": e["id"], "topic": e["topic"]}
                    for e in entity_params
                    if e.get("topic")
                ]
                await self.projection.project_entity_topics(cur, topic_params)

                # 2. Write Hybrid Search Data (Vectors)
                for original, e in zip(entities, entity_params):
                    if "embedding" in e and e["embedding"]:
                        if original["is_new"]:
                            await cur.execute(
                                """
                                INSERT INTO entity_search (
                                    entity_id,
                                    canonical_name,
                                    user_name,
                                    project_id,
                                    embedding
                                )
                                VALUES (%s, %s, %s, %s, %s::vector)
                                """,
                                (
                                    e["id"],
                                    e["canonical_name"],
                                    e["user_name"],
                                    e["project_id"],
                                    json.dumps(e["embedding"]),
                                ),
                            )
                        else:
                            await cur.execute(
                                """
                                INSERT INTO entity_search (
                                    entity_id,
                                    canonical_name,
                                    user_name,
                                    project_id,
                                    embedding
                                )
                                VALUES (%s, %s, %s, %s, %s::vector)
                                ON CONFLICT (entity_id) DO UPDATE SET
                                    canonical_name = EXCLUDED.canonical_name,
                                    user_name = EXCLUDED.user_name,
                                    project_id = EXCLUDED.project_id,
                                    embedding = EXCLUDED.embedding
                                """,
                                (
                                    e["id"],
                                    e["canonical_name"],
                                    e["user_name"],
                                    e["project_id"],
                                    json.dumps(e["embedding"]),
                                ),
                            )

            # 3. Write Relationships to Graph
            if relationships:
                rel_params = []
                for r in relationships:
                    label = (
                        f"Relationship {r.get('entity_a_id')}:"
                        f"{r.get('entity_b_id')}"
                    )
                    self._require_scope(
                        r,
                        ["user_name", "session_id", "project_id"],
                        label,
                    )
                    r_clean = r.copy()
                    # AGE MERGE can be touchy when sorting IDs in Cypher.
                    a_id, b_id = r["entity_a_id"], r["entity_b_id"]
                    if a_id > b_id:
                        a_id, b_id = b_id, a_id
                    r_clean["entity_a_id"] = a_id
                    r_clean["entity_b_id"] = b_id

                    evidence_ref = self._build_evidence_ref(r)
                    relationship_id = self._relationship_id(
                        r["project_id"],
                        a_id,
                        b_id,
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
                        SELECT
                            %s, %s, %s, %s, %s, 1, %s, %s, %s
                        WHERE EXISTS (
                            SELECT 1
                            FROM entities
                            WHERE entity_id = %s
                              AND (
                                  project_id = %s
                                  OR entity_id = %s
                              )
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM entities
                            WHERE entity_id = %s
                              AND (
                                  project_id = %s
                                  OR entity_id = %s
                              )
                        )
                        ON CONFLICT (relationship_id) DO UPDATE SET
                            user_name = EXCLUDED.user_name,
                            project_id = EXCLUDED.project_id,
                            entity_a_id = EXCLUDED.entity_a_id,
                            entity_b_id = EXCLUDED.entity_b_id,
                            weight = relationships.weight + 1,
                            confidence = GREATEST(
                                relationships.confidence,
                                EXCLUDED.confidence
                            ),
                            context = COALESCE(
                                EXCLUDED.context,
                                relationships.context
                            ),
                            last_seen_ms = EXCLUDED.last_seen_ms
                        RETURNING relationship_id
                        """,
                        (
                            relationship_id,
                            r["user_name"],
                            r["project_id"],
                            a_id,
                            b_id,
                            r.get("confidence", 1.0),
                            r.get("context"),
                            now_ms,
                            a_id,
                            r["project_id"],
                            IDENTITY_ENTITY_ID,
                            b_id,
                            r["project_id"],
                            IDENTITY_ENTITY_ID,
                        ),
                    )
                    record = await cur.fetchone()
                    if not record:
                        continue

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
                            relationship_id,
                            evidence_ref["user_name"],
                            evidence_ref["session_id"],
                            evidence_ref["message_id"],
                        ),
                    )

                    r_clean["evidence_ref"] = json.dumps(evidence_ref)
                    r_clean["confidence"] = r.get("confidence", 1.0)
                    r_clean["now"] = now_ms
                    rel_params.append(r_clean)

                await self.projection.project_relationships(cur, rel_params)

        return True

    async def update_entity_profile(
        self,
        entity_id: int,
        canonical_name: str,
        embedding: List[float],
        last_msg_id: int,
        *,
        project_id: str,
    ):
        project_id = self._require_project_id(project_id, "update_entity_profile")
        now_ms = self._current_time_ms()
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE entities
                SET canonical_name = %s,
                    last_updated_ms = %s,
                    last_profiled_msg_id = %s
                WHERE entity_id = %s
                  AND (project_id = %s OR entity_id = %s)
                """,
                (
                    canonical_name,
                    now_ms,
                    last_msg_id,
                    entity_id,
                    project_id,
                    IDENTITY_ENTITY_ID,
                ),
            )

            cypher = """
            MATCH (e:Entity {id: $id})
            WHERE e.project_id = $project_id OR e.id = $identity_entity_id
            SET e.canonical_name = $canonical_name,
                e.last_updated = $now,
                e.last_profiled_msg_id = $last_msg_id
            RETURN e.id
            """
            await cur.execute(
                self.client.build_cypher(cypher),
                (
                    json.dumps(
                        {
                            "id": entity_id,
                            "canonical_name": canonical_name,
                            "now": now_ms,
                            "last_msg_id": last_msg_id,
                            "project_id": project_id,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                        }
                    ),
                ),
            )

            # Update Vector Table
            await cur.execute(
                """
                UPDATE entity_search
                SET canonical_name = %s, embedding = %s::vector
                WHERE entity_id = %s
                  AND (project_id = %s OR entity_id = %s)
                """,
                (
                    canonical_name,
                    json.dumps(embedding),
                    entity_id,
                    project_id,
                    IDENTITY_ENTITY_ID,
                ),
            )
        logger.info(f"Updated entity {entity_id} (checkpoint: msg_{last_msg_id})")

    async def update_entity_canonical_name(
        self, entity_id: int, canonical_name: str, *, project_id: str
    ) -> None:
        project_id = self._require_project_id(
            project_id,
            "update_entity_canonical_name",
        )
        now_ms = self._current_time_ms()
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE entities
                SET canonical_name = %s,
                    last_updated_ms = %s
                WHERE entity_id = %s
                  AND (project_id = %s OR entity_id = %s)
                """,
                (
                    canonical_name,
                    now_ms,
                    entity_id,
                    project_id,
                    IDENTITY_ENTITY_ID,
                ),
            )

            cypher = """
            MATCH (e:Entity {id: $id})
            WHERE e.project_id = $project_id OR e.id = $identity_entity_id
            SET e.canonical_name = $canonical_name, e.last_updated = $now
            RETURN e.id
            """
            await cur.execute(
                self.client.build_cypher(cypher),
                (
                    json.dumps(
                        {
                            "id": entity_id,
                            "canonical_name": canonical_name,
                            "now": now_ms,
                            "project_id": project_id,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                        }
                    ),
                ),
            )

            # Update Vector Table
            await cur.execute(
                """
                UPDATE entity_search
                SET canonical_name = %s
                WHERE entity_id = %s
                  AND (project_id = %s OR entity_id = %s)
                """,
                (canonical_name, entity_id, project_id, IDENTITY_ENTITY_ID),
            )

    async def update_entity_embedding(
        self, entity_id: int, embedding: List[float], *, project_id: str
    ) -> None:
        project_id = self._require_project_id(project_id, "update_entity_embedding")
        now_ms = self._current_time_ms()
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE entities
                SET last_updated_ms = %s
                WHERE entity_id = %s
                  AND (project_id = %s OR entity_id = %s)
                """,
                (now_ms, entity_id, project_id, IDENTITY_ENTITY_ID),
            )

            cypher = """
            MATCH (e:Entity {id: $id})
            WHERE e.project_id = $project_id OR e.id = $identity_entity_id
            SET e.last_updated = $now
            RETURN e.id
            """
            await cur.execute(
                self.client.build_cypher(cypher),
                (
                    json.dumps(
                        {
                            "id": entity_id,
                            "now": now_ms,
                            "project_id": project_id,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                        }
                    ),
                ),
            )

            # Update Vector Table
            await cur.execute(
                """
                UPDATE entity_search
                SET embedding = %s::vector
                WHERE entity_id = %s
                  AND (project_id = %s OR entity_id = %s)
                """,
                (
                    json.dumps(embedding),
                    entity_id,
                    project_id,
                    IDENTITY_ENTITY_ID,
                ),
            )

    async def update_entity_checkpoint(
        self, entity_id: int, last_msg_id: int, *, project_id: str
    ) -> None:
        project_id = self._require_project_id(project_id, "update_entity_checkpoint")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE entities
                SET last_profiled_msg_id = %s
                WHERE entity_id = %s
                  AND (project_id = %s OR entity_id = %s)
                """,
                (last_msg_id, entity_id, project_id, IDENTITY_ENTITY_ID),
            )
            cypher = """
            MATCH (e:Entity {id: $id})
            WHERE e.project_id = $project_id OR e.id = $identity_entity_id
            SET e.last_profiled_msg_id = $last_msg_id
            RETURN e.id
            """
            await cur.execute(
                self.client.build_cypher(cypher),
                (
                    json.dumps(
                        {
                            "id": entity_id,
                            "last_msg_id": last_msg_id,
                            "project_id": project_id,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                        }
                    ),
                ),
            )

    async def update_entity_aliases(
        self, alias_updates: Dict[int, List[str]], *, project_id: str
    ) -> None:
        project_id = self._require_project_id(project_id, "update_entity_aliases")
        if not alias_updates:
            return

        params = [
            {"id": entity_id, "aliases": aliases}
            for entity_id, aliases in alias_updates.items()
            if aliases
        ]
        if not params:
            return

        now_ms = self._current_time_ms()
        async with self.client.transaction() as cur:
            for item in params:
                await cur.execute(
                    """
                    UPDATE entities
                    SET last_updated_ms = %s
                    WHERE entity_id = %s
                      AND (project_id = %s OR entity_id = %s)
                    """,
                    (
                        now_ms,
                        item["id"],
                        project_id,
                        IDENTITY_ENTITY_ID,
                    ),
                )
                for alias in item["aliases"]:
                    await cur.execute(
                        """
                        INSERT INTO entity_aliases (entity_id, alias)
                        SELECT %s, %s
                        WHERE EXISTS (
                            SELECT 1
                            FROM entities
                            WHERE entity_id = %s
                              AND (
                                  project_id = %s
                                  OR entity_id = %s
                              )
                        )
                        ON CONFLICT (entity_id, alias) DO NOTHING
                        """,
                        (
                            item["id"],
                            alias,
                            item["id"],
                            project_id,
                            IDENTITY_ENTITY_ID,
                        ),
                    )

            cypher = """
            UNWIND $batch AS data
            MATCH (e:Entity {id: data.id})
            WHERE e.project_id = $project_id OR e.id = $identity_entity_id
            WITH e,
                coalesce(e.aliases, []) + coalesce(data.aliases, [])
                AS all_aliases
            WITH e,
                CASE WHEN size(all_aliases) = 0
                     THEN [null]
                     ELSE all_aliases
                END AS safe_aliases
            UNWIND safe_aliases AS alias
            WITH e, collect(DISTINCT alias) AS merged_aliases
            WITH e,
                [x IN merged_aliases WHERE x IS NOT NULL] AS final_aliases
            SET e.aliases = final_aliases,
                e.last_updated = $now
            RETURN count(e)
            """
            await cur.execute(
                self.client.build_cypher(cypher),
                (
                    json.dumps(
                        {
                            "batch": params,
                            "project_id": project_id,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                            "now": now_ms,
                        }
                    ),
                ),
            )

    async def _delete_entity_aggregate(
        self,
        entity_ids: List[int],
        project_id: str,
    ) -> List[int]:
        unique_ids = sorted(
            {
                int(entity_id)
                for entity_id in entity_ids
                if int(entity_id) != IDENTITY_ENTITY_ID
            }
        )
        if not unique_ids:
            return []

        async with self.client.transaction() as cur:
            return await self._delete_entity_aggregate_with_cursor(
                cur,
                unique_ids,
                project_id,
            )

    async def _delete_entity_aggregate_with_cursor(
        self,
        cur,
        entity_ids: List[int],
        project_id: str,
    ) -> List[int]:
        if not entity_ids:
            return []

        await cur.execute(
            """
            DELETE FROM entities
            WHERE entity_id = ANY(%s)
              AND project_id = %s
              AND entity_id <> %s
            RETURNING entity_id
            """,
            (entity_ids, project_id, IDENTITY_ENTITY_ID),
        )
        deleted_ids = sorted(int(row["entity_id"]) for row in await cur.fetchall())
        await self.projection.delete_entities_projection(
            cur,
            deleted_ids,
            project_id,
        )
        return deleted_ids

    async def cleanup_null_entities(self, *, project_id: str) -> List[int]:
        project_id = self._require_project_id(project_id, "cleanup_null_entities")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT entity_id
                FROM entities
                WHERE type IS NULL
                  AND project_id = %s
                """,
                (project_id,),
            )
            rows = await cur.fetchall()
            entity_ids = [row["entity_id"] for row in rows]
            return await self._delete_entity_aggregate_with_cursor(
                cur,
                entity_ids,
                project_id,
            )

    async def delete_entity(
        self,
        entity_id: int,
        *,
        project_id: str,
    ) -> bool:
        project_id = self._require_project_id(project_id, "delete_entity")
        if entity_id == IDENTITY_ENTITY_ID:
            logger.warning("Identity entity deletion rejected")
            return False

        deleted_ids = await self._delete_entity_aggregate([entity_id], project_id)
        return entity_id in deleted_ids

    async def bulk_delete_entities(
        self, entity_ids: List[int], *, project_id: str
    ) -> List[int]:
        project_id = self._require_project_id(project_id, "bulk_delete_entities")
        if not entity_ids:
            return []
        return await self._delete_entity_aggregate(entity_ids, project_id)
