"""Quiescent rebuilds of episode and document retrieval embeddings."""

from __future__ import annotations

import json
import math

from common.embedding_contract import EMBEDDING_DIMENSION
from core.knowledge.documents.extraction import DocumentChunk, embedding_text
from core.knowledge.embedding_service import EmbeddingService
from core.knowledge.episodes.embedding import (
    build_episode_embedding_text_from_fields,
    encode_episode_embedding_texts,
    exchange_embedding_text,
)
from infrastructure.postgres_client import PostgresClient


class EmbeddingRebuilder:
    """Replace derived vectors while the local engine is stopped or quiescent."""

    def __init__(
        self, postgres_client: PostgresClient, embedding_service: EmbeddingService
    ):
        self.client = postgres_client
        self.embedding_service = embedding_service

    async def ensure_configuration(self) -> None:
        """Refuse to compare vectors from an unknown or different configuration."""
        if self.embedding_service.embedding_dim != EMBEDDING_DIMENSION:
            raise RuntimeError(
                "Loaded embedding model produces "
                f"{self.embedding_service.embedding_dim} dimensions; "
                f"the database requires {EMBEDDING_DIMENSION}"
            )
        async with self.client.transaction() as cur:
            await cur.execute(
                "LOCK TABLE public.embedding_configuration IN EXCLUSIVE MODE"
            )
            await cur.execute(
                "SELECT fingerprint FROM public.embedding_configuration WHERE singleton = TRUE"
            )
            existing = await cur.fetchone()
            fingerprint = self.embedding_service.configuration_fingerprint
            if existing is not None and existing["fingerprint"] == fingerprint:
                return
            await cur.execute(
                "SELECT EXISTS (SELECT 1 FROM public.episodes WHERE embedding IS NOT NULL) "
                "OR EXISTS (SELECT 1 FROM public.document_chunks) "
                "OR EXISTS (SELECT 1 FROM public.exchange_embeddings) AS has_vectors"
            )
            if (await cur.fetchone())["has_vectors"]:
                raise RuntimeError(
                    "Stored embedding configuration is unknown or changed. Stop the engine "
                    "and run scripts/rebuild_embeddings.py to rebuild all episode, exchange, and document vectors."
                )
            await self._record_configuration(cur, fingerprint)

    @staticmethod
    def _validate_embeddings(
        embeddings, expected_count: int, label: str
    ) -> list[list[float]]:
        if embeddings is None or len(embeddings) != expected_count:
            raise RuntimeError(
                f"{label} embedding count mismatch: expected {expected_count}"
            )
        normalized = []
        for embedding in embeddings:
            vector = list(embedding) if embedding is not None else []
            if len(vector) != EMBEDDING_DIMENSION or any(
                not math.isfinite(value) for value in vector
            ):
                raise RuntimeError(
                    f"{label} embeddings require {EMBEDDING_DIMENSION} finite values"
                )
            normalized.append(vector)
        return normalized

    async def rebuild_project_embeddings(
        self, project_id: str, user_name: str
    ) -> dict[str, int]:
        if not project_id or not user_name:
            raise ValueError(
                "rebuild_project_embeddings requires project_id and user_name scope"
            )
        # A project-only rebuild cannot migrate an engine-wide model change.
        await self.ensure_configuration()
        return await self._rebuild(project_id=project_id, user_name=user_name)

    async def rebuild_all_embeddings(self) -> dict[str, int]:
        """Offline model migration: replace every corpus and fingerprint atomically."""
        return await self._rebuild()

    async def _rebuild(
        self, *, project_id: str | None = None, user_name: str | None = None
    ) -> dict[str, int]:
        if self.embedding_service.embedding_dim != EMBEDDING_DIMENSION:
            raise RuntimeError(
                f"Embeddings require {EMBEDDING_DIMENSION}-dimensional vectors"
            )
        # The caller excludes runtime writes. All encoding finishes before any
        # mutation; failure leaves both the old corpus and fingerprint intact.
        async with self.client.transaction() as cur:
            await cur.execute("SET TRANSACTION READ ONLY")
            scope = "WHERE p.project_id = %s AND p.user_name = %s" if project_id else ""
            params = (project_id, user_name) if project_id else ()
            await cur.execute(
                "SELECT e.episode_id, e.summary, e.new_developments, e.updates, e.unresolved "
                "FROM public.episodes e JOIN public.projects p ON p.project_id = e.project_id "
                + scope,
                params,
            )
            episodes = list(await cur.fetchall())
            await cur.execute(
                "SELECT c.chunk_id, c.content, c.relative_path, c.language, c.symbol_name "
                "FROM public.document_chunks c "
                "JOIN public.project_documents d ON d.document_id = c.document_id "
                "JOIN public.projects p ON p.project_id = d.project_id " + scope,
                params,
            )
            chunks = list(await cur.fetchall())
            await cur.execute(
                "SELECT x.project_id, x.user_message_id, u.content AS user_content, "
                "a.content AS assistant_content "
                "FROM public.exchange_embeddings x "
                "JOIN public.messages u ON u.project_id = x.project_id "
                "AND u.message_id = x.user_message_id "
                "LEFT JOIN public.messages a ON a.project_id = x.project_id "
                "AND a.role = 'assistant' AND a.user_msg_id = x.user_message_id "
                "JOIN public.projects p ON p.project_id = x.project_id " + scope,
                params,
            )
            exchanges = list(await cur.fetchall())
        episode_inputs = [
            build_episode_embedding_text_from_fields(
                row["summary"],
                self._json_list(row["new_developments"]),
                self._json_list(row["updates"]),
                self._json_list(row["unresolved"]),
            )
            for row in episodes
        ]
        chunk_inputs = [
            embedding_text(
                DocumentChunk(
                    content=row["content"],
                    language=row["language"],
                    symbol_name=row["symbol_name"],
                ),
                row["relative_path"],
            )
            for row in chunks
        ]
        episode_vectors = await encode_episode_embedding_texts(
            self.embedding_service, episode_inputs
        )
        chunk_vectors = (
            await self.embedding_service.encode(chunk_inputs) if chunk_inputs else []
        )
        exchange_vectors = await encode_episode_embedding_texts(
            self.embedding_service,
            [exchange_embedding_text(row) for row in exchanges],
        )
        episode_vectors = self._validate_embeddings(
            episode_vectors, len(episode_inputs), "episode"
        )
        exchange_vectors = self._validate_embeddings(
            exchange_vectors, len(exchanges), "exchange"
        )
        chunk_vectors = self._validate_embeddings(
            chunk_vectors, len(chunk_inputs), "document"
        )
        async with self.client.transaction() as cur:
            for row, vector in zip(episodes, episode_vectors):
                await cur.execute(
                    "UPDATE public.episodes SET embedding = %s::vector WHERE episode_id = %s",
                    (json.dumps(vector), row["episode_id"]),
                )
            for row, vector in zip(chunks, chunk_vectors):
                await cur.execute(
                    "UPDATE public.document_chunks SET embedding = %s::vector WHERE chunk_id = %s",
                    (json.dumps(vector), row["chunk_id"]),
                )
            for row, vector in zip(exchanges, exchange_vectors):
                await cur.execute(
                    "UPDATE public.exchange_embeddings SET embedding = %s::vector "
                    "WHERE project_id = %s AND user_message_id = %s",
                    (json.dumps(vector), row["project_id"], row["user_message_id"]),
                )
            if project_id is None:
                await self._record_configuration(
                    cur, self.embedding_service.configuration_fingerprint
                )
        return {
            "episodes": len(episodes),
            "document_chunks": len(chunks),
            "exchanges": len(exchanges),
        }

    @staticmethod
    async def _record_configuration(cur, fingerprint: str) -> None:
        await cur.execute(
            "INSERT INTO public.embedding_configuration (singleton, fingerprint) VALUES (TRUE, %s) "
            "ON CONFLICT (singleton) DO UPDATE SET fingerprint = EXCLUDED.fingerprint",
            (fingerprint,),
        )

    @staticmethod
    def _json_list(value) -> list[str]:
        if isinstance(value, str):
            value = json.loads(value)
        return [str(item) for item in value or []]
