import json
from typing import Dict, List, Optional, Sequence

from loguru import logger

from common.schema.ingestion.contracts import (
    EntityWrite,
    EpisodeEligibility,
    ExecutionScope,
    MessageEntityRef,
    RelationshipWrite,
    relationship_identity,
)
from common.scoping import (
    IDENTITY_ENTITY_ID,
    IDENTITY_SCOPE,
    require_scope_value,
)
from common.utils.time_utils import get_now_ms
from core.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)
from infrastructure.postgres_client import PostgresClient


class EntityWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

    def _current_time_ms(self) -> int:
        return get_now_ms()

    @staticmethod
    def _require_project_id(project_id: str, operation: str) -> str:
        return require_scope_value(project_id, "project_id", operation)

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
            "project_id": IDENTITY_SCOPE,
            "canonical_name": user_name,
            "aliases": clean_aliases,
            "type": "person",
            "topic": "Identity",
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
                or self._normalized_identity_value(existing["canonical_name"])
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
                    canonical_name,
                    type,
                    topic,
                    last_mentioned_ms,
                    embedding
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
                ON CONFLICT (entity_id) DO UPDATE SET
                    user_name = EXCLUDED.user_name,
                    project_id = EXCLUDED.project_id,
                    canonical_name = EXCLUDED.canonical_name,
                    type = EXCLUDED.type,
                    topic = EXCLUDED.topic,
                    last_mentioned_ms = EXCLUDED.last_mentioned_ms
                """,
                (
                    IDENTITY_ENTITY_ID,
                    user_name,
                    IDENTITY_SCOPE,
                    user_name,
                    "person",
                    "Identity",
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
            await self.projection.project_identity(cur, identity)

        return identity

    async def write_batch(
        self,
        entities: Sequence[EntityWrite],
        relationships: Sequence[RelationshipWrite],
        *,
        message_entity_refs: Sequence[MessageEntityRef] = (),
        eligible_messages: Sequence[EpisodeEligibility] = (),
        scope: ExecutionScope,
    ) -> bool:
        """Persist typed graph commands inside one explicit execution scope."""

        if not isinstance(scope, ExecutionScope):
            raise TypeError("write_batch requires an ExecutionScope")
        user_name = require_scope_value(scope.user_name, "user_name", "graph write")
        session_id = require_scope_value(scope.session_id, "session_id", "graph write")
        project_id = self._require_project_id(scope.project_id, "graph write")
        now_ms = self._current_time_ms()

        async with self.client.transaction() as cur:
            if entities:
                entity_params = []
                for entity in entities:
                    if not isinstance(entity, EntityWrite):
                        raise TypeError("entities must be EntityWrite instances")
                    if entity.entity_id == IDENTITY_ENTITY_ID:
                        raise ValueError(
                            "Identity entity writes must use ensure_identity_entity"
                        )

                    entity_params.append(
                        {
                            "id": entity.entity_id,
                            "user_name": user_name,
                            "project_id": project_id,
                            "canonical_name": entity.canonical_name,
                            "aliases": list(entity.aliases),
                            "type": entity.entity_type,
                            "topic": entity.topic,
                            "embedding": entity.embedding,
                            "now": now_ms,
                        }
                    )

                    if entity.is_new:
                        await cur.execute(
                            """
                            INSERT INTO entities (
                                entity_id,
                                user_name,
                                project_id,
                                canonical_name,
                                type,
                                topic,
                                last_mentioned_ms,
                                embedding
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s::vector
                            )
                            ON CONFLICT (entity_id) DO UPDATE SET
                                canonical_name = EXCLUDED.canonical_name,
                                type = EXCLUDED.type,
                                topic = EXCLUDED.topic,
                                last_mentioned_ms = EXCLUDED.last_mentioned_ms,
                                embedding = EXCLUDED.embedding
                            WHERE entities.project_id = EXCLUDED.project_id
                            RETURNING entity_id
                            """,
                            (
                                entity.entity_id,
                                user_name,
                                project_id,
                                entity.canonical_name,
                                entity.entity_type,
                                entity.topic,
                                now_ms,
                                json.dumps(entity.embedding) if entity.embedding else None,
                            ),
                        )
                        if await cur.fetchone() is None:
                            raise RuntimeError(
                                f"Entity {entity.entity_id} already exists "
                                f"outside project {project_id}"
                            )
                    else:
                        await cur.execute(
                            """
                            UPDATE entities
                            SET canonical_name = %s,
                                type = COALESCE(type, %s),
                                topic = %s,
                                last_mentioned_ms = %s,
                                embedding = COALESCE(%s::vector, embedding)
                            WHERE entity_id = %s
                              AND (
                                  project_id = %s
                                  OR entity_id = %s
                              )
                            RETURNING entity_id
                            """,
                            (
                                entity.canonical_name,
                                entity.entity_type,
                                entity.topic,
                                now_ms,
                                json.dumps(entity.embedding) if entity.embedding else None,
                                entity.entity_id,
                                project_id,
                                IDENTITY_ENTITY_ID,
                            ),
                        )

                        persisted = await cur.fetchone()
                        if not persisted:
                            raise RuntimeError(
                                f"Existing entity {entity.entity_id} was not "
                                f"found in project {project_id}"
                            )

                    for alias in entity.aliases:
                        if not alias:
                            continue
                        await cur.execute(
                            """
                            INSERT INTO entity_aliases (entity_id, alias)
                            VALUES (%s, %s)
                            ON CONFLICT (entity_id, alias) DO NOTHING
                            """,
                            (entity.entity_id, alias),
                        )

                await self.projection.project_entities(cur, entity_params)

                topic_params = [
                    {"id": e["id"], "topic": e["topic"]}
                    for e in entity_params
                    if e.get("topic")
                ]
                await self.projection.project_entity_topics(cur, topic_params)

            if message_entity_refs:
                await self._write_message_entity_refs(
                    cur,
                    message_entity_refs,
                    scope,
                )

            # Graph writes are part of an ingestion claim, not its durable
            # completion boundary.  MessageLifecycleWriter marks the claimed
            # user turns episode-ready only after the claim finishes.

            if relationships:
                rel_params = []
                for relationship in relationships:
                    if not isinstance(relationship, RelationshipWrite):
                        raise TypeError(
                            "relationships must be RelationshipWrite instances"
                        )
                    relationship_id = relationship_identity(
                        project_id,
                        relationship.entity_a_id,
                        relationship.entity_b_id,
                        relationship.relationship_type,
                        symmetric=relationship.symmetric,
                    )
                    evidence_ref = {
                        "user_name": user_name,
                        "session_id": session_id,
                        "message_id": relationship.message_id,
                    }
                    await cur.execute(
                        """
                        INSERT INTO relationships (
                            relationship_id,
                            user_name,
                            project_id,
                            entity_a_id,
                            entity_b_id,
                            relationship_type,
                            canonical_relationship_type,
                            observed_relationship_label,
                            domain_status,
                            "symmetric",
                            weight,
                            confidence,
                            context,
                            last_seen_ms
                        )
                        SELECT
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s
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
                            relationship_type = COALESCE(
                                EXCLUDED.relationship_type,
                                relationships.relationship_type
                            ),
                            canonical_relationship_type = COALESCE(
                                EXCLUDED.canonical_relationship_type,
                                relationships.canonical_relationship_type
                            ),
                            observed_relationship_label = COALESCE(
                                EXCLUDED.observed_relationship_label,
                                relationships.observed_relationship_label
                            ),
                            domain_status = EXCLUDED.domain_status,
                            "symmetric" = EXCLUDED."symmetric",
                            weight = relationships.weight + CASE
                                WHEN EXISTS (
                                    SELECT 1
                                    FROM relationship_evidence_refs
                                        AS existing_evidence
                                    WHERE existing_evidence.relationship_id =
                                        relationships.relationship_id
                                      AND existing_evidence.project_id = %s
                                      AND existing_evidence.user_name = %s
                                      AND existing_evidence.session_id = %s
                                      AND existing_evidence.message_id = %s
                                ) THEN 0
                                ELSE 1
                            END,
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
                            user_name,
                            project_id,
                            relationship.entity_a_id,
                            relationship.entity_b_id,
                            relationship.relationship_type,
                            relationship.canonical_type,
                            relationship.observed_label,
                            relationship.domain_status,
                            relationship.symmetric,
                            relationship.confidence,
                            relationship.context,
                            now_ms,
                            relationship.entity_a_id,
                            project_id,
                            IDENTITY_ENTITY_ID,
                            relationship.entity_b_id,
                            project_id,
                            IDENTITY_ENTITY_ID,
                            project_id,
                            user_name,
                            session_id,
                            relationship.message_id,
                        ),
                    )
                    record = await cur.fetchone()
                    if not record:
                        raise ValueError(
                            "Relationship endpoints must exist in the "
                            f"project scope: {project_id}/"
                            f"{relationship.entity_a_id}/"
                            f"{relationship.entity_b_id}"
                        )

                    await cur.execute(
                        """
                        SELECT message_id
                        FROM messages
                        WHERE message_id = %s
                          AND user_name = %s
                          AND session_id = %s
                          AND project_id = %s
                        """,
                        (
                            evidence_ref["message_id"],
                            user_name,
                            session_id,
                            project_id,
                        ),
                    )
                    if await cur.fetchone() is None:
                        raise ValueError(
                            "Relationship evidence message must exist in the "
                            "relationship project scope"
                        )

                    await cur.execute(
                        """
                        INSERT INTO relationship_evidence_refs (
                            relationship_id,
                            project_id,
                            user_name,
                            session_id,
                            message_id
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (
                            relationship_id,
                            user_name,
                            session_id,
                            message_id
                        ) DO NOTHING
                        """,
                        (
                            relationship_id,
                            project_id,
                            user_name,
                            session_id,
                            evidence_ref["message_id"],
                        ),
                    )

                    await cur.execute(
                        """
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
                            confidence,
                            context,
                            observed_at_ms
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (
                            project_id,
                            user_name,
                            session_id,
                            message_id,
                            source_entity_id,
                            target_entity_id,
                            observed_relationship_label
                        ) DO UPDATE SET
                            canonical_relationship_type = COALESCE(
                                EXCLUDED.canonical_relationship_type,
                                relationship_observations.canonical_relationship_type
                            ),
                            domain_status = EXCLUDED.domain_status,
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
                            relationship_id,
                            project_id,
                            user_name,
                            session_id,
                            evidence_ref["message_id"],
                            relationship.source_entity_id,
                            relationship.target_entity_id,
                            relationship.source_type,
                            relationship.target_type,
                            relationship.observed_label,
                            relationship.canonical_type,
                            relationship.domain_status,
                            relationship.confidence,
                            relationship.context,
                            now_ms,
                        ),
                    )

                    rel_params.append(
                        {
                            "relationship_id": relationship_id,
                            "project_id": project_id,
                            "entity_a_id": relationship.entity_a_id,
                            "entity_b_id": relationship.entity_b_id,
                            "relationship_type": relationship.relationship_type,
                            "canonical_relationship_type": relationship.canonical_type,
                            "observed_relationship_label": relationship.observed_label,
                            "domain_status": relationship.domain_status,
                            "symmetric": relationship.symmetric,
                            "evidence_ref": json.dumps(evidence_ref),
                            "confidence": relationship.confidence,
                            "context": relationship.context,
                            "now": now_ms,
                        }
                    )

                await self.projection.project_relationships(cur, rel_params)

        return True

    async def _write_message_entity_refs(
        self,
        cur,
        references: Sequence[MessageEntityRef],
        scope: ExecutionScope,
    ) -> None:
        user_name = require_scope_value(
            scope.user_name, "user_name", "message-entity write"
        )
        session_id = require_scope_value(
            scope.session_id, "session_id", "message-entity write"
        )
        project_id = self._require_project_id(scope.project_id, "message-entity write")
        if any(not isinstance(reference, MessageEntityRef) for reference in references):
            raise TypeError("message_entity_refs must be MessageEntityRef instances")
        normalized_references = {
            (reference.message_id, reference.entity_id) for reference in references
        }
        if any(
            message_id <= 0 or entity_id <= 0
            for message_id, entity_id in normalized_references
        ):
            raise ValueError("Message-entity references require positive IDs")

        message_ids = sorted({message_id for message_id, _ in normalized_references})
        entity_ids = sorted({entity_id for _, entity_id in normalized_references})
        await cur.execute(
            """
            SELECT message_id
            FROM messages
            WHERE message_id = ANY(%s)
              AND user_name = %s
              AND session_id = %s
              AND project_id = %s
            """,
            (message_ids, user_name, session_id, project_id),
        )
        scoped_message_ids = {int(row["message_id"]) for row in await cur.fetchall()}
        if scoped_message_ids != set(message_ids):
            raise ValueError("Message-entity references include messages outside scope")

        await cur.execute(
            """
            SELECT entity_id
            FROM entities
            WHERE entity_id = ANY(%s)
              AND (project_id = %s OR entity_id = %s)
            """,
            (entity_ids, project_id, IDENTITY_ENTITY_ID),
        )
        scoped_entity_ids = {int(row["entity_id"]) for row in await cur.fetchall()}
        if scoped_entity_ids != set(entity_ids):
            raise ValueError("Message-entity references include entities outside scope")

        for message_id, entity_id in sorted(normalized_references):
            await cur.execute(
                """
                INSERT INTO message_entity_refs (message_id, entity_id)
                VALUES (%s, %s)
                ON CONFLICT (message_id, entity_id) DO NOTHING
                """,
                (message_id, entity_id),
            )

    async def _mark_episode_eligible_messages(
        self,
        cur,
        eligible_messages: Sequence[EpisodeEligibility],
        scope: ExecutionScope,
    ) -> None:
        user_name = require_scope_value(
            scope.user_name, "user_name", "episode eligibility write"
        )
        session_id = require_scope_value(
            scope.session_id, "session_id", "episode eligibility write"
        )
        project_id = self._require_project_id(
            scope.project_id, "episode eligibility write"
        )
        eligibility_by_message_id: Dict[int, EpisodeEligibility] = {}
        for eligibility in eligible_messages:
            if not isinstance(eligibility, EpisodeEligibility):
                raise TypeError(
                    "eligible_messages must be EpisodeEligibility instances"
                )
            message_id = int(eligibility.message_id)
            prior = eligibility_by_message_id.get(message_id)
            if (
                prior
                and prior.episode_type
                and eligibility.episode_type
                and prior.episode_type != eligibility.episode_type
            ):
                raise ValueError(
                    "Episode eligibility has conflicting types for one message"
                )
            eligibility_by_message_id[message_id] = EpisodeEligibility(
                message_id=message_id,
                episode_type=eligibility.episode_type
                or (prior.episode_type if prior else None),
            )
        normalized_message_ids = sorted(eligibility_by_message_id)
        if any(message_id <= 0 for message_id in normalized_message_ids):
            raise ValueError("Episode-eligible messages require positive IDs")

        await cur.execute(
            """
            SELECT message_id
            FROM messages
            WHERE message_id = ANY(%s)
              AND user_name = %s
              AND session_id = %s
              AND project_id = %s
            """,
            (normalized_message_ids, user_name, session_id, project_id),
        )
        scoped_message_ids = {int(row["message_id"]) for row in await cur.fetchall()}
        if scoped_message_ids != set(normalized_message_ids):
            raise ValueError("Episode-eligible messages include messages outside scope")

        for message_id in normalized_message_ids:
            eligibility = eligibility_by_message_id[message_id]
            await cur.execute(
                """
                UPDATE messages
                SET episode_eligible = TRUE,
                    episode_type = COALESCE(%s, episode_type)
                WHERE message_id = %s
                  AND user_name = %s
                  AND session_id = %s
                  AND project_id = %s
                """,
                (
                    eligibility.episode_type,
                    message_id,
                    user_name,
                    session_id,
                    project_id,
                ),
            )

    async def update_entity_canonical_name(
        self, entity_id: int, canonical_name: str, *, project_id: str
    ) -> None:
        project_id = self._require_project_id(
            project_id,
            "update_entity_canonical_name",
        )
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE entities
                SET canonical_name = %s
                WHERE entity_id = %s
                  AND (project_id = %s OR entity_id = %s)
                """,
                (
                    canonical_name,
                    entity_id,
                    project_id,
                    IDENTITY_ENTITY_ID,
                ),
            )

            cypher = """
            MATCH (e:Entity {id: $id})
            WHERE e.project_id = $project_id OR e.id = $identity_entity_id
            SET e.canonical_name = $canonical_name
            RETURN e.id
            """
            await cur.execute(
                self.client.build_cypher(cypher),
                (
                    json.dumps(
                        {
                            "id": entity_id,
                            "canonical_name": canonical_name,
                            "project_id": project_id,
                            "identity_entity_id": IDENTITY_ENTITY_ID,
                        }
                    ),
                ),
            )

    async def update_entity_embedding(
        self, entity_id: int, embedding: List[float], *, project_id: str
    ) -> None:
        project_id = self._require_project_id(project_id, "update_entity_embedding")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE entities
                SET embedding = %s::vector
                WHERE entity_id = %s
                  AND (project_id = %s OR entity_id = %s)
                """,
                (json.dumps(embedding), entity_id, project_id, IDENTITY_ENTITY_ID),
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

        async with self.client.transaction() as cur:
            for item in params:
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
            SET e.aliases = final_aliases
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
