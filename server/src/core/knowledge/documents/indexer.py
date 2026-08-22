"""Project-scoped durable document indexing execution."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional

from common.utils.time_utils import get_now_iso
from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.documents.constants import (
    EXPECTED_EMBEDDING_DIMENSION,
    MAX_ERROR_MESSAGE_LENGTH,
)
from core.knowledge.documents.policy import DocumentIndexPolicy
from core.knowledge.documents.storage import (
    DocumentChunk,
    embedding_text,
    extract_and_split_document,
)
from core.knowledge.services.embedding_service import EmbeddingService

BlockingRunner = Callable[..., Awaitable[Any]]


class DocumentIndexer:
    """Owns extraction and atomic derived-data publication for one project."""

    def __init__(
        self,
        *,
        project_id: str,
        reader: DocumentReader,
        writer: DocumentWriter,
        embedding_service: EmbeddingService,
        policy: DocumentIndexPolicy,
        blocking_runner: BlockingRunner,
    ) -> None:
        self.project_id = project_id
        self._reader = reader
        self._writer = writer
        self._embedding = embedding_service
        self._policy = policy
        self._run_blocking = blocking_runner

    @property
    def policy(self) -> DocumentIndexPolicy:
        return self._policy

    def update_policy(self, policy: DocumentIndexPolicy) -> None:
        self._policy = policy

    async def index_document(
        self,
        *,
        document_id: str,
        session_id: Optional[str] = None,
        policy: Optional[DocumentIndexPolicy] = None,
    ) -> Dict:
        """Claim, derive, and atomically publish one document's index."""

        policy = policy or self._policy
        rows = await self._reader.fetch_documents_by_reference(
            document_id=document_id,
            relative_path=None,
            session_id=session_id,
        )
        metadata = rows[0] if rows else None
        if metadata is None:
            raise FileNotFoundError("Document not found")
        if metadata["status"] == "indexed":
            return metadata

        claimed = await self._writer.transition_index_status(
            document_id=document_id,
            session_id=session_id,
            status="indexing",
            allowed_statuses=("queued", "failed"),
            updated_at=get_now_iso(),
        )
        if claimed is None:
            refreshed = await self._reader.fetch_documents_by_reference(
                document_id=document_id,
                relative_path=None,
                session_id=session_id,
            )
            if not refreshed:
                raise FileNotFoundError("Document not found")
            return refreshed[0]

        try:
            async with self._index_claim(
                document_id=document_id,
                session_id=session_id,
            ):
                raw_bytes = await self._reader.fetch_document_content(
                    document_id=str(claimed["document_id"]),
                    session_id=session_id,
                )
                if raw_bytes is None:
                    raise FileNotFoundError("Document content is missing")
                extraction = await self._run_blocking(
                    extract_and_split_document,
                    raw_bytes,
                    claimed["extension"],
                )
                chunks = extraction.chunks
                embeddings = await self._encode_embeddings(
                    [
                        embedding_text(chunk, claimed["relative_path"])
                        for chunk in chunks
                    ],
                    policy=policy,
                )
                self._validate_embeddings(embeddings, chunks)
                row = await self._writer.persist_indexed_chunks(
                    document_id=document_id,
                    session_id=session_id,
                    chunks=chunks,
                    embeddings=embeddings,
                    extracted_text=extraction.text,
                    indexed_at=get_now_iso(),
                    expected_content_hash=claimed["content_hash"],
                )
                if row is None:
                    refreshed = await self._reader.fetch_documents_by_reference(
                        document_id=document_id,
                        relative_path=None,
                        session_id=session_id,
                    )
                    if not refreshed:
                        raise FileNotFoundError("Document not found")
                    return refreshed[0]
                return row
        except asyncio.CancelledError:
            raise
        except FileNotFoundError:
            raise
        except Exception as exc:
            detail = str(exc).strip() or type(exc).__name__
            raise RuntimeError(
                f"Failed to index document: {detail[:MAX_ERROR_MESSAGE_LENGTH]}"
            ) from exc

    @staticmethod
    def _validate_embeddings(
        embeddings: List[List[float]],
        chunks: List[DocumentChunk],
    ) -> None:
        if len(embeddings) != len(chunks):
            raise ValueError("Embedding count does not match chunk count")
        if any(
            len(embedding) != EXPECTED_EMBEDDING_DIMENSION
            for embedding in embeddings
        ):
            raise ValueError(
                "Document chunk embeddings must have exactly "
                f"{EXPECTED_EMBEDDING_DIMENSION} dimensions"
            )

    async def _encode_embeddings(
        self,
        values: List[str],
        *,
        policy: DocumentIndexPolicy,
    ) -> List[List[float]]:
        embeddings: List[List[float]] = []
        for start in range(0, len(values), policy.embedding_chunk_batch_size):
            embeddings.extend(
                await self._embedding.encode(
                    values[start : start + policy.embedding_chunk_batch_size]
                )
            )
        return embeddings

    @asynccontextmanager
    async def _index_claim(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
    ) -> AsyncIterator[None]:
        try:
            yield
        except asyncio.CancelledError:
            await self._writer.requeue_index_claims(
                document_ids=[document_id],
                updated_at=get_now_iso(),
            )
            raise
        except Exception as exc:
            detail = str(exc).strip() or type(exc).__name__
            await self._writer.record_index_failure(
                document_id=document_id,
                session_id=session_id,
                error_message=detail[:MAX_ERROR_MESSAGE_LENGTH],
                updated_at=get_now_iso(),
            )
            raise
