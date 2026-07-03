import json
from collections import defaultdict
from typing import Dict, List

from loguru import logger

from common.scoping import IDENTITY_ENTITY_ID
from infrastructure.postgres_client import PostgresClient
from knoggin_server.knowledge.services.embedding_service import EmbeddingService
from knoggin_server.knowledge.services.entity_embedding import (
    build_entity_embedding_text,
)


class SearchIndexer:
    """Rebuilds relational search indexes from canonical PostgreSQL rows."""

    def __init__(
        self,
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
    ):
        self.client = postgres_client
        self.embedding_service = embedding_service

    @staticmethod
    def _validate_scope(
        project_id: str,
        user_name: str,
        identity_project_ids: List[str],
    ) -> List[str]:
        if not project_id:
            raise ValueError("rebuild_project_indexes requires project_id scope")
        if not user_name:
            raise ValueError("rebuild_project_indexes requires user_name scope")
        return list(dict.fromkeys(pid for pid in identity_project_ids if pid))

    def _validate_embeddings(
        self,
        embeddings: List[List[float]],
        expected_count: int,
        label: str,
    ) -> List[List[float]]:
        if embeddings is None:
            raise RuntimeError(f"{label} embedding result is missing")
        if len(embeddings) != expected_count:
            raise RuntimeError(
                f"{label} embedding count mismatch: "
                f"expected {expected_count}, got {len(embeddings)}"
            )
        normalized = []
        for index, embedding in enumerate(embeddings):
            vector = list(embedding) if embedding is not None else []
            if len(vector) != 1024:
                raise RuntimeError(
                    f"{label} embedding {index} has dimension {len(vector)}; "
                    "expected 1024"
                )
            normalized.append(vector)
        return normalized

    async def rebuild_project_indexes(
        self,
        project_id: str,
        user_name: str,
        identity_project_ids: List[str],
    ) -> Dict[str, int]:
        identity_project_ids = self._validate_scope(
            project_id,
            user_name,
            identity_project_ids,
        )
        if self.embedding_service.embedding_dim != 1024:
            raise RuntimeError(
                "Search indexes require 1024-dimensional embeddings; "
                f"configured model reports {self.embedding_service.embedding_dim}"
            )
        messages = await self._fetch_messages(project_id, user_name)
        entities = await self._fetch_entities(project_id, user_name)
        facts = await self._fetch_facts(project_id, user_name)
        identity = await self._fetch_identity(user_name)
        if not identity:
            raise RuntimeError("Canonical identity entity is missing")
        identity_facts = await self._fetch_identity_facts(
            user_name,
            identity_project_ids,
        )

        active_facts_by_entity = defaultdict(list)
        for fact in facts:
            if fact.get("invalid_at") is None:
                active_facts_by_entity[int(fact["entity_id"])].append(fact)

        entity_inputs = [
            build_entity_embedding_text(
                entity["canonical_name"],
                entity.get("type"),
                active_facts_by_entity[int(entity["entity_id"])],
            )
            for entity in entities
        ]
        identity_input = build_entity_embedding_text(
            identity["canonical_name"],
            identity.get("type"),
            identity_facts,
        )
        entity_vectors = self._validate_embeddings(
            await self.embedding_service.encode(entity_inputs + [identity_input]),
            len(entity_inputs) + 1,
            "entity",
        )
        identity_vector = entity_vectors.pop()

        fact_vectors = self._validate_embeddings(
            await self.embedding_service.encode(
                [str(fact["content"]) for fact in facts]
            ),
            len(facts),
            "fact",
        )

        async with self.client.transaction() as cur:
            await cur.execute(
                """
                DELETE FROM message_search
                WHERE project_id = %s
                  AND user_name = %s
                """,
                (project_id, user_name),
            )
            await cur.execute(
                """
                DELETE FROM fact_search
                WHERE project_id = %s
                  AND user_name = %s
                """,
                (project_id, user_name),
            )
            await cur.execute(
                """
                DELETE FROM entity_search
                WHERE project_id = %s
                  AND user_name = %s
                  AND entity_id <> %s
                """,
                (project_id, user_name, IDENTITY_ENTITY_ID),
            )

            for message in messages:
                await cur.execute(
                    """
                    INSERT INTO message_search (
                        message_id,
                        user_name,
                        session_id,
                        project_id,
                        content_tsvector
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        to_tsvector('english', %s)
                    )
                    """,
                    (
                        message["message_id"],
                        message["user_name"],
                        message["session_id"],
                        message["project_id"],
                        message["content"],
                    ),
                )

            for entity, embedding in zip(entities, entity_vectors):
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
                        entity["entity_id"],
                        entity["canonical_name"],
                        entity["user_name"],
                        entity["project_id"],
                        json.dumps(embedding),
                    ),
                )

            for fact, embedding in zip(facts, fact_vectors):
                await cur.execute(
                    """
                    INSERT INTO fact_search (
                        fact_id,
                        entity_id,
                        user_name,
                        project_id,
                        embedding,
                        invalid_at
                    )
                    VALUES (%s, %s, %s, %s, %s::vector, %s)
                    """,
                    (
                        fact["fact_id"],
                        fact["entity_id"],
                        fact["user_name"],
                        fact["project_id"],
                        json.dumps(embedding),
                        fact["invalid_at"],
                    ),
                )

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
                    identity["entity_id"],
                    identity["canonical_name"],
                    identity["user_name"],
                    identity["project_id"],
                    json.dumps(identity_vector),
                ),
            )

        summary = {
            "messages": len(messages),
            "entities": len(entities),
            "facts": len(facts),
            "identity": 1,
        }
        logger.info(f"Rebuilt search indexes for project {project_id}: {summary}")
        return summary

    async def _fetch_messages(
        self,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        return list(
            await self.client.fetch_all(
                """
                SELECT message_id, user_name, session_id, project_id, content
                FROM messages
                WHERE project_id = %s
                  AND user_name = %s
                ORDER BY message_id
                """,
                (project_id, user_name),
            )
        )

    async def _fetch_entities(
        self,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        return list(
            await self.client.fetch_all(
                """
                SELECT
                    entity_id,
                    canonical_name,
                    type,
                    user_name,
                    project_id
                FROM entities
                WHERE project_id = %s
                  AND user_name = %s
                  AND entity_id <> %s
                ORDER BY entity_id
                """,
                (project_id, user_name, IDENTITY_ENTITY_ID),
            )
        )

    async def _fetch_facts(
        self,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        return list(
            await self.client.fetch_all(
                """
                SELECT
                    fact_id,
                    entity_id,
                    user_name,
                    project_id,
                    content,
                    valid_at,
                    invalid_at
                FROM facts
                WHERE project_id = %s
                  AND user_name = %s
                ORDER BY entity_id, valid_at NULLS FIRST, fact_id
                """,
                (project_id, user_name),
            )
        )

    async def _fetch_identity(self, user_name: str) -> Dict:
        row = await self.client.fetch_one(
            """
            SELECT entity_id, canonical_name, type, user_name, project_id
            FROM entities
            WHERE entity_id = %s
              AND user_name = %s
            """,
            (IDENTITY_ENTITY_ID, user_name),
        )
        return row or {}

    async def _fetch_identity_facts(
        self,
        user_name: str,
        identity_project_ids: List[str],
    ) -> List[Dict]:
        return list(
            await self.client.fetch_all(
                """
                SELECT DISTINCT
                    f.fact_id,
                    f.content,
                    f.valid_at,
                    f.invalid_at
                FROM facts f
                LEFT JOIN messages m
                  ON m.message_id = f.source_msg_id
                 AND m.user_name = f.source_user_name
                 AND m.session_id = f.source_session_id
                WHERE f.entity_id = %s
                  AND f.user_name = %s
                  AND f.invalid_at IS NULL
                  AND (
                      f.source_msg_id IS NULL
                      OR m.project_id = ANY(%s)
                  )
                ORDER BY f.valid_at NULLS FIRST, f.fact_id
                """,
                (IDENTITY_ENTITY_ID, user_name, identity_project_ids),
            )
        )
