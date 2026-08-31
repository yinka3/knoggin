"""Immutable rules captured when a document-index operation is admitted."""

from __future__ import annotations

from dataclasses import dataclass

from .constants import (
    INDEX_EMBEDDING_CHUNK_BATCH_SIZE,
    INLINE_INDEX_MAX_BYTES,
)


@dataclass(frozen=True, slots=True)
class DocumentIndexPolicy:
    """Execution settings that must not change while an index operation runs."""

    inline_index_max_bytes: int
    embedding_chunk_batch_size: int

    @classmethod
    def capture(
        cls,
        *,
        inline_index_max_bytes: int = INLINE_INDEX_MAX_BYTES,
        embedding_chunk_batch_size: int = INDEX_EMBEDDING_CHUNK_BATCH_SIZE,
    ) -> "DocumentIndexPolicy":
        values = {
            "inline_index_max_bytes": inline_index_max_bytes,
            "embedding_chunk_batch_size": embedding_chunk_batch_size,
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
