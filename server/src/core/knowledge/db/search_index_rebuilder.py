import json
from dataclasses import dataclass
from typing import Dict, List

from loguru import logger

from common.scoping import IDENTITY_ENTITY_ID
from core.knowledge.entity.embedding import (
    build_entity_embedding_text,
)
from core.knowledge.episodes.embedding import build_episode_embedding_text_from_fields
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.postgres_client import PostgresClient


@dataclass(frozen=True)
class SearchIndexRevision:
    """Canonical-data revisions that a rebuilt search snapshot depends on."""

    project: int
    identity: int


class SearchIndexer:
    """Rebuilds relational search indexes from canonical PostgreSQL rows."""

    _MAX_PUBLICATION_ATTEMPTS = 3

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
        await self._ensure_revision_rows(project_id, user_name)

        for attempt in range(1, self._MAX_PUBLICATION_ATTEMPTS + 1):
            entities, episodes, identity, revision = await self._snapshot(
                project_id,
                user_name,
            )
            if not identity:
                raise RuntimeError("Canonical identity entity is missing")

            entity_inputs = [
                build_entity_embedding_text(
                    entity["canonical_name"],
                    entity.get("type"),
                )
                for entity in entities
            ]
            identity_input = build_entity_embedding_text(
                identity["canonical_name"],
                identity.get("type"),
            )
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
            episode_vectors = []
            if episode_inputs:
                episode_vectors = self._validate_embeddings(
                    await self.embedding_service.encode(episode_inputs),
                    len(episode_inputs),
                    "episode",
                )

            published = await self._publish_if_current(
                project_id,
                user_name,
                revision,
                entities,
                identity,
                episodes,
                entity_vectors,
                identity_vector,
                episode_vectors,
            )
            if not published:
                logger.info(
                    "Search index snapshot for project {} was superseded; "
                    "retrying ({}/{})",
                    project_id,
                    attempt,
                    self._MAX_PUBLICATION_ATTEMPTS,
                )
                continue

            summary = {
                "entities": len(entities),
                "identity": 1,
                "episodes": len(episodes),
            }
            logger.info(
                "Rebuilt search indexes for project {}: {}",
                project_id,
                summary,
            )
            return summary

        raise RuntimeError(
            "Search index rebuild was superseded by canonical writes; retry later"
        )

    async def _ensure_revision_rows(self, project_id: str, user_name: str) -> None:
        """Create revision rows before taking the read-only snapshot."""
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO project_search_revisions (project_id)
                VALUES (%s)
                ON CONFLICT (project_id) DO NOTHING
                """,
                (project_id,),
            )
            await cur.execute(
                """
                INSERT INTO identity_search_revisions (user_name)
                VALUES (%s)
                ON CONFLICT (user_name) DO NOTHING
                """,
                (user_name,),
            )

    async def _snapshot(
        self,
        project_id: str,
        user_name: str,
    ) -> tuple[List[Dict], List[Dict], Dict, SearchIndexRevision]:
        """Read canonical rows and their versions from one stable snapshot."""
        async with self.client.transaction() as cur:
            await cur.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            )
            entities = await self._fetch_entities(cur, project_id, user_name)
            episodes = await self._fetch_episodes(cur, project_id, user_name)
            identity = await self._fetch_identity(cur, user_name)
            revision = await self._fetch_revision(cur, project_id, user_name)
        return entities, episodes, identity, revision

    async def _fetch_revision(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> SearchIndexRevision:
        await cur.execute(
            """
            SELECT revision
            FROM project_search_revisions
            WHERE project_id = %s
            """,
            (project_id,),
        )
        project_row = await cur.fetchone()
        await cur.execute(
            """
            SELECT revision
            FROM identity_search_revisions
            WHERE user_name = %s
            """,
            (user_name,),
        )
        identity_row = await cur.fetchone()
        return SearchIndexRevision(
            project=int((project_row or {}).get("revision", 0)),
            identity=int((identity_row or {}).get("revision", 0)),
        )

    async def _publish_if_current(
        self,
        project_id: str,
        user_name: str,
        expected_revision: SearchIndexRevision,
        entities: List[Dict],
        identity: Dict,
        episodes: List[Dict],
        entity_vectors: List[List[float]],
        identity_vector: List[float],
        episode_vectors: List[List[float]],
    ) -> bool:
        """Publish only when no canonical row changed since ``_snapshot``.

        The revision rows are locked for this short write transaction. A
        concurrent canonical writer cannot commit its trigger-driven revision
        bump until this derived index publication has committed or rolled back.
        """
        async with self.client.transaction() as cur:
            current_revision = await self._lock_revision(cur, project_id, user_name)
            if current_revision != expected_revision:
                return False

            await cur.execute(
                """
                UPDATE entities
                SET embedding = NULL
                WHERE project_id = %s
                  AND user_name = %s
                  AND entity_id <> %s
                """,
                (project_id, user_name, IDENTITY_ENTITY_ID),
            )

            for entity, embedding in zip(entities, entity_vectors):
                await cur.execute(
                    """
                    UPDATE entities
                    SET embedding = %s::vector
                    WHERE entity_id = %s
                      AND user_name = %s
                      AND project_id = %s
                    """,
                    (
                        json.dumps(embedding),
                        entity["entity_id"],
                        entity["user_name"],
                        entity["project_id"],
                    ),
                )

            await cur.execute(
                """
                UPDATE entities
                SET embedding = %s::vector
                WHERE entity_id = %s
                  AND user_name = %s
                  AND project_id = %s
                """,
                (
                    json.dumps(identity_vector),
                    identity["entity_id"],
                    identity["user_name"],
                    identity["project_id"],
                ),
            )

            for episode, embedding in zip(episodes, episode_vectors):
                await cur.execute(
                    """
                    UPDATE episodes
                    SET embedding = %s::vector
                    WHERE episode_id = %s
                      AND project_id = %s
                    """,
                    (
                        json.dumps(embedding),
                        episode["episode_id"],
                        project_id,
                    ),
                )
        return True

    async def _lock_revision(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> SearchIndexRevision:
        await cur.execute(
            """
            SELECT revision
            FROM project_search_revisions
            WHERE project_id = %s
            FOR UPDATE
            """,
            (project_id,),
        )
        project_row = await cur.fetchone()
        await cur.execute(
            """
            SELECT revision
            FROM identity_search_revisions
            WHERE user_name = %s
            FOR UPDATE
            """,
            (user_name,),
        )
        identity_row = await cur.fetchone()
        return SearchIndexRevision(
            project=int((project_row or {}).get("revision", 0)),
            identity=int((identity_row or {}).get("revision", 0)),
        )

    async def _fetch_entities(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        await cur.execute(
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
        return list(await cur.fetchall())

    async def _fetch_episodes(
        self,
        cur,
        project_id: str,
        user_name: str,
    ) -> List[Dict]:
        await cur.execute(
            """
            SELECT
                e.episode_id,
                e.summary,
                e.new_developments,
                e.updates,
                e.unresolved
            FROM episodes e
            JOIN projects p ON p.project_id = e.project_id
            WHERE e.project_id = %s
              AND p.user_name = %s
            ORDER BY e.episode_id
            """,
            (project_id, user_name),
        )
        return list(await cur.fetchall())

    async def _fetch_identity(self, cur, user_name: str) -> Dict:
        await cur.execute(
            """
            SELECT entity_id, canonical_name, type, user_name, project_id
            FROM entities
            WHERE entity_id = %s
              AND user_name = %s
            """,
            (IDENTITY_ENTITY_ID, user_name),
        )
        row = await cur.fetchone()
        return row or {}

    @staticmethod
    def _json_list(value) -> List[str]:
        if isinstance(value, str):
            value = json.loads(value)
        return [str(item) for item in value or []]
