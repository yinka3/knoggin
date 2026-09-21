"""Immutable rules captured when a document-index operation is admitted."""

from __future__ import annotations

from dataclasses import dataclass

from .constants import (
    INDEX_EMBEDDING_CHUNK_BATCH_SIZE,
    INDEX_MAX_ATTEMPTS,
    INDEX_RETRY_BACKOFF_SECONDS,
    INLINE_INDEX_MAX_BYTES,
)


@dataclass(frozen=True, slots=True)
class DocumentIndexPolicy:
    """Execution settings that must not change while an index operation runs."""

    inline_index_max_bytes: int
    embedding_chunk_batch_size: int
    max_attempts: int
    retry_backoff_seconds: int

    @classmethod
    def capture(
        cls,
        *,
        inline_index_max_bytes: int = INLINE_INDEX_MAX_BYTES,
        embedding_chunk_batch_size: int = INDEX_EMBEDDING_CHUNK_BATCH_SIZE,
        max_attempts: int = INDEX_MAX_ATTEMPTS,
        retry_backoff_seconds: int = INDEX_RETRY_BACKOFF_SECONDS,
    ) -> "DocumentIndexPolicy":
        values = {
            "inline_index_max_bytes": inline_index_max_bytes,
            "embedding_chunk_batch_size": embedding_chunk_batch_size,
            "max_attempts": max_attempts,
            "retry_backoff_seconds": retry_backoff_seconds,
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
