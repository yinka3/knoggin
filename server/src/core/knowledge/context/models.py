"""Internal Context revision materialization contracts.

These objects are deliberately kept at the Context boundary.  The public
schema package owns durable records and updater commands; this module captures
the already-validated next snapshot that a renderer, importer, or later LLM
stage hands to the storage writer.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from common.schema.context import ContextBlockRecord, ContextSupportKind


class ContextRevisionConflictError(RuntimeError):
    """Raised when a Context write is based on a no-longer-current revision."""


class ContextProjectionConflictError(RuntimeError):
    """Raised when a local Context file cannot safely be imported or replaced."""


@dataclass(frozen=True, slots=True)
class ContextBlockSupport:
    """One message/source reference attached to a newly-created block version."""

    block_id: UUID
    message_id: int
    session_id: str
    support_kind: ContextSupportKind
    source_ref_id: UUID | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.message_id, int) or isinstance(self.message_id, bool) or self.message_id <= 0:
            raise ValueError("Context support message_id must be a positive integer")
        if not isinstance(self.session_id, str) or not self.session_id.strip():
            raise ValueError("Context support session_id must be non-blank")
        if self.support_kind is ContextSupportKind.ASSISTANT_SOURCE:
            if self.source_ref_id is None:
                raise ValueError("assistant_source support requires source_ref_id")
        elif self.source_ref_id is not None:
            raise ValueError("only assistant_source support may include source_ref_id")


@dataclass(frozen=True, slots=True)
class ContextMaterialization:
    """The deterministic next Context snapshot before a revision is committed."""

    blocks: tuple[ContextBlockRecord, ...]
    content_hash: str
    new_block_ids: frozenset[UUID]
    supports: tuple[ContextBlockSupport, ...] = ()
    impacted_block_ids: frozenset[UUID] = frozenset()
    # Request-local metadata only: it lets the updater attach validated support
    # to the exact new block created by each ADD or REPLACE.
    operation_new_block_ids: tuple[UUID | None, ...] = ()

    def __post_init__(self) -> None:
        block_ids = [block.block_id for block in self.blocks]
        if len(block_ids) != len(set(block_ids)):
            raise ValueError("Context materialization cannot repeat a block")
        if not self.new_block_ids.issubset(set(block_ids)):
            raise ValueError("new Context blocks must appear in the materialized snapshot")
        if len(self.content_hash) != 64 or any(char not in "0123456789abcdef" for char in self.content_hash):
            raise ValueError("Context materialization content_hash must be a SHA-256 hex digest")
        for support in self.supports:
            if support.block_id not in self.new_block_ids:
                raise ValueError("Context support may only be attached to a new block version")
