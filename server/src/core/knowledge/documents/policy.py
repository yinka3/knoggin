"""Immutable rules captured when a document-index operation is admitted."""

from __future__ import annotations

from dataclasses import dataclass

from .constants import (
    INDEX_EMBEDDING_CHUNK_BATCH_SIZE,
    INLINE_INDEX_MAX_BYTES,
    WORKSPACE_INDEX_DOCUMENT_BATCH_SIZE,
    WORKSPACE_PREPARE_CONCURRENCY,
)


@dataclass(frozen=True, slots=True)
class DocumentIndexPolicy:
    """Execution settings that must not change while an index operation runs."""

    inline_index_max_bytes: int
    embedding_chunk_batch_size: int
    workspace_document_batch_size: int
    workspace_prepare_concurrency: int

    @classmethod
    def capture(
        cls,
        *,
        inline_index_max_bytes: int = INLINE_INDEX_MAX_BYTES,
        embedding_chunk_batch_size: int = INDEX_EMBEDDING_CHUNK_BATCH_SIZE,
        workspace_document_batch_size: int = WORKSPACE_INDEX_DOCUMENT_BATCH_SIZE,
        workspace_prepare_concurrency: int = WORKSPACE_PREPARE_CONCURRENCY,
    ) -> "DocumentIndexPolicy":
        values = {
            "inline_index_max_bytes": inline_index_max_bytes,
            "embedding_chunk_batch_size": embedding_chunk_batch_size,
            "workspace_document_batch_size": workspace_document_batch_size,
            "workspace_prepare_concurrency": workspace_prepare_concurrency,
        }
        cls._validate(values)
        return cls(**values)

    @staticmethod
    def _validate(values: dict[str, int]) -> None:
        for name, value in values.items():
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(f"{name} must be an integer")
            if name == "inline_index_max_bytes":
                if value < 0:
                    raise ValueError("inline_index_max_bytes must be non-negative")
            elif value < 1:
                raise ValueError(f"{name} must be positive")
