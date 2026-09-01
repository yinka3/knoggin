"""Quiescent rebuilds of canonical entity and episode embeddings."""

from __future__ import annotations

import json
from typing import Dict, List

from loguru import logger

from common.scoping import IDENTITY_ENTITY_ID
from core.knowledge.entity.embedding import build_entity_embedding_text
from core.knowledge.episodes.embedding import build_episode_embedding_text_from_fields
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.postgres_client import PostgresClient


class EmbeddingRebuilder:
    """Regenerate durable embeddings while project runtimes are quiescent."""

    def __init__(
        self,
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
    ) -> None:
        self.client = postgres_client
        self.embedding_service = embedding_service

    @staticmethod
    def _validate_scope(project_id: str, user_name: str) -> None:
        if not project_id:
            raise ValueError("rebuild_project_embeddings requires project_id scope")
        if not user_name:
            raise ValueError("rebuild_project_embeddings requires user_name scope")

    @staticmethod
    def _validate_embeddings(
        embeddings: List[List[float]] | None,
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
        normalized: list[list[float]] = []
        for index, embedding in enumerate(embeddings):
            vector = list(embedding) if embedding is not None else []
            if len(vector) != 1024:
                raise RuntimeError(
                    f"{label} embedding {index} has dimension {len(vector)}; "
                    "expected 1024"
                )
            normalized.append(vector)
        return normalized

    async def rebuild_project_embeddings(
        self,
        project_id: str,
        user_name: str,
    ) -> Dict[str, int]:
        """Read canonical rows, encode them, then atomically replace vectors.

        Project maintenance already excludes active runtimes.  The rebuild does
        not therefore publish conditionally or maintain a second revision
        subsystem solely to race concurrent canonical writes.
        """

        self._validate_scope(project_id, user_name)
        if self.embedding_service.embedding_dim != 1024:
            raise RuntimeError(
                "Embeddings require 1024-dimensional vectors; configured model reports "
                f"{self.embedding_service.embedding_dim}"
            )

        entities, episodes, identity = await self._snapshot(project_id, user_name)
        if not identity:
            raise RuntimeError("Canonical identity entity is missing")

        # An entity embedding describes its user-global identity.  Project
        # classification belongs to project_entity_contexts and must not cause
        # the same identity to receive different vectors in different projects.
        entity_inputs = [
            build_entity_embedding_text(entity["canonical_name"], None)
            for entity in entities
        ]
        identity_input = build_entity_embedding_text(identity["canonical_name"], None)
        entity_vectors = self._validate_embeddings(
            await self.embedding_service.encode(entity_inputs + [identity_input]),
            len(entity_inputs) + 1,
            "entity",
        )
        identity_vector = entity_vectors.pop()

        episode_inputs = [
            build_episode_embedding_text_from_fields(
                str(episode["summary"]),
                self._json_list(episode.get("new_developments")),
                self._json_list(episode.get("updates")),
                self._json_list(episode.get("unresolved")),
            )
            for episode in episodes
        ]
        episode_vectors: list[list[float]] = []
        if episode_inputs:
            episode_vectors = self._validate_embeddings(
                await self.embedding_service.encode(episode_inputs),
                len(episode_inputs),
                "episode",
            )

        await self._replace_embeddings(
            project_id,
            user_name,
            entities,
            identity,
            episodes,
            entity_vectors,
            identity_vector,
            episode_vectors,
        )
        summary = {"entities": len(entities), "identity": 1, "episodes": len(episodes)}
        logger.info("Rebuilt embeddings for project {}: {}", project_id, summary)
        return summary

    async def _snapshot(
        self,
        project_id: str,
        user_name: str,
    ) -> tuple[List[Dict], List[Dict], Dict]:
        async with self.client.transaction() as cur:
            await cur.execute("SET TRANSACTION READ ONLY")
            entities = await self._fetch_entities(cur, project_id, user_name)
            episodes = await self._fetch_episodes(cur, project_id, user_name)
            identity = await self._fetch_identity(cur, user_name)
        return entities, episodes, identity

    async def _replace_embeddings(
        self,
        project_id: str,
        user_name: str,
        entities: List[Dict],
        identity: Dict,
        episodes: List[Dict],
        entity_vectors: List[List[float]],
        identity_vector: List[float],
        episode_vectors: List[List[float]],
    ) -> None:
        async with self.client.transaction() as cur:
            for entity, embedding in zip(entities, entity_vectors):
                await cur.execute(
                    """
                    UPDATE entities
                    SET embedding = %s::vector
                    WHERE entity_id = %s
                      AND user_name = %s
                    """,
                    (
                        json.dumps(embedding),
                        entity["entity_id"],
                        entity["user_name"],
                    ),
                )
            await cur.execute(
                """
                UPDATE entities
                SET embedding = %s::vector
                WHERE entity_id = %s
                  AND user_name = %s
                """,
                (
                    json.dumps(identity_vector),
                    identity["entity_id"],
                    identity["user_name"],
                ),
            )
            await cur.execute(
                """
                UPDATE episodes
                SET embedding = NULL
                WHERE project_id = %s
                """,
                (project_id,),
            )
            for episode, embedding in zip(episodes, episode_vectors):
                await cur.execute(
                    """
                    UPDATE episodes
                    SET embedding = %s::vector
                    WHERE episode_id = %s
                      AND project_id = %s
                    """,
                    (json.dumps(embedding), episode["episode_id"], project_id),
                )

    @staticmethod
    async def _fetch_entities(cur, project_id: str, user_name: str) -> List[Dict]:
        await cur.execute(
            """
            SELECT e.entity_id, e.canonical_name, e.user_name
            FROM entities e
            JOIN project_entity_contexts context
              ON context.entity_id = e.entity_id
            WHERE context.project_id = %s
              AND e.user_name = %s
            ORDER BY e.entity_id
            """,
            (project_id, user_name),
        )
        return list(await cur.fetchall())

    @staticmethod
    async def _fetch_episodes(cur, project_id: str, user_name: str) -> List[Dict]:
        await cur.execute(
            """
            SELECT
                episode.episode_id,
                episode.summary,
                episode.new_developments,
                episode.updates,
                episode.unresolved
            FROM episodes episode
            JOIN projects project ON project.project_id = episode.project_id
            WHERE episode.project_id = %s
              AND project.user_name = %s
            ORDER BY episode.episode_id
            """,
            (project_id, user_name),
        )
        return list(await cur.fetchall())

    @staticmethod
    async def _fetch_identity(cur, user_name: str) -> Dict:
        await cur.execute(
            """
            SELECT entity_id, canonical_name, user_name
            FROM entities
            WHERE entity_id = %s
              AND user_name = %s
            """,
            (IDENTITY_ENTITY_ID, user_name),
        )
        return await cur.fetchone() or {}

    @staticmethod
    def _json_list(value) -> List[str]:
        if isinstance(value, str):
            value = json.loads(value)
        return [str(item) for item in value or []]
