"""Strict contracts for durable project Context state.

These models deliberately describe durable identifiers and edit commands, not
renderer-specific Markdown.  The renderer/importer and LLM updater are added in
later batches.
"""

from __future__ import annotations

import re
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from loguru import logger
from pydantic import (
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from common.schema.config import ConfigModel
from common.schema.llm import StructuredLLMOutput, normalize_optional_text

_LOCAL_BLOCK_REF = re.compile(r"C[1-9][0-9]*$")
_LOCAL_EVIDENCE_REF = re.compile(r"[MS][1-9][0-9]*$")
_SECTION_KEY = re.compile(r"[a-z][a-z0-9_]{0,39}$")


class AssertionKind(StrEnum):
    """The trust classification attached to one immutable Context block."""

    USER_ASSERTED = "user_asserted"
    SOURCE_GROUNDED = "source_grounded"
    AGENT_DERIVED = "agent_derived"
    HUMAN_ASSERTED = "human_asserted"


class ContextRevisionOrigin(StrEnum):
    """The durable source that created a Context revision."""

    CONVERSATION = "conversation"
    HUMAN_EDIT = "human_edit"


class ContextEditOperationKind(StrEnum):
    ADD = "add"
    REPLACE = "replace"
    DELETE = "delete"


class ContextSupportKind(StrEnum):
    """The canonical evidence shape used to support a Context block."""

    USER_MESSAGE = "user_message"
    ASSISTANT_MESSAGE = "assistant_message"
    ASSISTANT_SOURCE = "assistant_source"


class ContextBlockReference(ConfigModel):
    """A server-assigned immutable Context block-version identifier."""

    block_id: UUID


class LocalContextBlockReference(ConfigModel):
    """A revision-local model/debug handle; never a durable identifier."""

    handle: str

    @field_validator("handle")
    @classmethod
    def require_local_handle(cls, value: str) -> str:
        value = value.strip()
        if not _LOCAL_BLOCK_REF.fullmatch(value):
            raise ValueError("Context block handle must have the form C1, C2, ...")
        return value


class LocalContextEvidenceReference(ConfigModel):
    """A request-local message or assistant-source handle.

    ``M1`` and ``S1`` are deliberately distinct from durable IDs. The updater
    resolves them only against the frozen catalog for one window.
    """

    handle: str

    @field_validator("handle")
    @classmethod
    def require_local_handle(cls, value: str) -> str:
        value = value.strip()
        if not _LOCAL_EVIDENCE_REF.fullmatch(value):
            raise ValueError("Context evidence handle must have the form M1 or S1")
        return value


class ContextBlockRecord(ContextBlockReference):
    """One immutable Context block version loaded from durable storage."""

    project_id: str = Field(min_length=1)
    section_key: str
    markdown: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    assertion_kind: AssertionKind
    supersedes_block_id: UUID | None = None
    source_time_ms: int | None = Field(default=None, ge=0)

    @field_validator("section_key")
    @classmethod
    def require_section_key(cls, value: str) -> str:
        value = value.strip()
        if not _SECTION_KEY.fullmatch(value):
            raise ValueError("section_key must be a normalized Context section key")
        return value


class ContextBlockSupportRecord(ConfigModel):
    """One durable evidence link attached to an immutable Context block."""

    block_id: UUID
    project_id: str = Field(min_length=1)
    message_id: int = Field(gt=0)
    session_id: str = Field(min_length=1)
    support_kind: ContextSupportKind
    source_ref_id: UUID | None = None

    @model_validator(mode="after")
    def validate_source_shape(self) -> "ContextBlockSupportRecord":
        if self.support_kind is ContextSupportKind.ASSISTANT_SOURCE:
            if self.source_ref_id is None:
                raise ValueError("assistant_source support requires source_ref_id")
        elif self.source_ref_id is not None:
            raise ValueError("only assistant_source support may include source_ref_id")
        return self


class ContextRevisionRecord(ConfigModel):
    """A complete Context snapshot revision without its block materialization."""

    revision_id: UUID
    project_id: str = Field(min_length=1)
    revision_number: int = Field(ge=1)
    parent_revision_id: UUID | None = None
    window_id: UUID | None = None
    origin: ContextRevisionOrigin
    domain_version: int = Field(ge=0)
    edit_summary: str = Field(default="", max_length=2_000)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ContextSnapshot(ContextRevisionRecord):
    """A materialized Context revision in deterministic persisted order."""

    blocks: list[ContextBlockRecord] = Field(default_factory=list)


class ContextProjectionState(ConfigModel):
    """The projection checkpoint paired with the current Context revision."""

    project_id: str = Field(min_length=1)
    current_revision_id: UUID | None = None
    projection_revision_id: UUID | None = None
    projection_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    projection_pending_revision_id: UUID | None = None
    projection_pending_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    projection_failure_code: str | None = Field(default=None, max_length=120)
    projection_failure_summary: str | None = Field(default=None, max_length=2_000)
    projection_failure_at: datetime | None = None


_MAX_EDIT_EVIDENCE = 8
_MAX_USER_QUOTE = 1_000


def _keep_quote_start(value: object) -> object:
    # Models quote whole passages; the start is still the user's own words,
    # so it passes the updater's quote check where rejection would fail the
    # whole window.
    if isinstance(value, str):
        return value[:_MAX_USER_QUOTE]
    return value


class ContextEditBase(ConfigModel):
    """Shared bounded shape for updater edit commands."""

    operation: ContextEditOperationKind
    section_key: str
    dependencies: list[LocalContextBlockReference] = Field(default_factory=list)
    evidence: list[LocalContextEvidenceReference] = Field(
        default_factory=list,
        max_length=_MAX_EDIT_EVIDENCE,
    )

    @field_validator("evidence", mode="before")
    @classmethod
    def keep_first_evidence(cls, values: object) -> object:
        # Models cite every message behind a block from a long chat and ignore
        # the schema's limit; rejecting the edit would fail the whole window.
        if isinstance(values, list):
            return values[:_MAX_EDIT_EVIDENCE]
        return values

    @field_validator("section_key")
    @classmethod
    def require_section_key(cls, value: str) -> str:
        # Models sometimes return the heading ("Current State") for its key.
        value = re.sub(r"[\s-]+", "_", value.strip().lower())
        if not _SECTION_KEY.fullmatch(value):
            raise ValueError("section_key must be a normalized Context section key")
        return value

    @field_validator("evidence")
    @classmethod
    def require_distinct_evidence(
        cls, values: list[LocalContextEvidenceReference]
    ) -> list[LocalContextEvidenceReference]:
        handles = [item.handle for item in values]
        if len(handles) != len(set(handles)):
            raise ValueError("Context evidence handles must not repeat")
        return values


class ContextAdd(ContextEditBase):
    operation: Literal[ContextEditOperationKind.ADD] = ContextEditOperationKind.ADD
    markdown: str = Field(min_length=1, max_length=50_000)
    assertion_kind: AssertionKind = AssertionKind.AGENT_DERIVED
    # Exact words from a cited user message; required for user_asserted.
    user_quote: str | None = Field(default=None, max_length=_MAX_USER_QUOTE)
    source_time_ms: int | None = Field(default=None, ge=0)

    _quote = field_validator("user_quote", mode="before")(_keep_quote_start)


class ContextReplace(ContextEditBase):
    operation: Literal[ContextEditOperationKind.REPLACE] = (
        ContextEditOperationKind.REPLACE
    )
    target: LocalContextBlockReference
    markdown: str = Field(min_length=1, max_length=50_000)
    assertion_kind: AssertionKind = AssertionKind.AGENT_DERIVED
    user_quote: str | None = Field(default=None, max_length=_MAX_USER_QUOTE)
    source_time_ms: int | None = Field(default=None, ge=0)

    _quote = field_validator("user_quote", mode="before")(_keep_quote_start)


class ContextDelete(ContextEditBase):
    operation: Literal[ContextEditOperationKind.DELETE] = ContextEditOperationKind.DELETE
    target: LocalContextBlockReference


# The per-window update may add and replace but never delete, so one window
# cannot empty Context. A finished plan is replaced with its outcome.
ContextUpdateOperation = Annotated[
    ContextAdd | ContextReplace,
    Field(discriminator="operation"),
]


_OPERATION: TypeAdapter[ContextAdd | ContextReplace] = TypeAdapter(
    ContextUpdateOperation
)


def _operation_rejection(operation: ContextEditBase) -> str | None:
    """Rules only the model can break; the server owns these values."""

    if not operation.evidence:
        return "Context operations require at least one evidence handle"
    if isinstance(operation, (ContextAdd, ContextReplace)):
        if operation.assertion_kind is AssertionKind.HUMAN_ASSERTED:
            return "the Context model cannot create human_asserted blocks"
    return None


class LLMContextUpdate(StructuredLLMOutput):
    """The bounded local-reference operation set returned by the Context LLM."""

    operations: list[ContextUpdateOperation] = Field(default_factory=list, max_length=64)
    edit_summary: str | None = Field(default=None, max_length=2_000)

    @field_validator("operations", mode="before")
    @classmethod
    def keep_valid_operations(cls, values: object) -> object:
        # Check each edit alone: one bad edit must not reject the others and
        # fail the whole window. ContextUpdateBuild.apply drops edits that
        # break later rules the same way.
        if not isinstance(values, list):
            return values
        kept = []
        for value in values:
            if isinstance(value, dict):
                # The server computes source time; ignore a model-set value.
                value = {k: v for k, v in value.items() if k != "source_time_ms"}
            try:
                operation = _OPERATION.validate_python(value)
            except ValidationError as exc:
                logger.warning(
                    "Dropped malformed Context operation: {}", exc.errors()[0]["msg"]
                )
                continue
            if reason := _operation_rejection(operation):
                logger.warning("Dropped invalid Context operation: {}", reason)
                continue
            kept.append(operation)
        return kept

    @field_validator("edit_summary", mode="before")
    @classmethod
    def normalize_edit_summary(cls, value: object) -> str | None:
        if isinstance(value, str):
            value = value[:2_000]
        return normalize_optional_text(value, field_name="edit_summary")

    @model_validator(mode="after")
    def validate_operations(self) -> "LLMContextUpdate":
        markdown_chars = sum(
            len(operation.markdown)
            for operation in self.operations
            if isinstance(operation, (ContextAdd, ContextReplace))
        )
        if markdown_chars > 100_000:
            raise ValueError("Context model output exceeds the 100000-character limit")
        return self


class ContextMaintenanceEdit(ConfigModel):
    """One maintainer change: merge near-duplicates, compress one block, or
    drop one block. The server checks budget, size, and removal limits."""

    operation: Literal["merge", "compress", "drop"]
    targets: list[LocalContextBlockReference] = Field(min_length=1, max_length=16)
    markdown: str | None = Field(default=None, max_length=50_000)

    @model_validator(mode="after")
    def validate_shape(self) -> "ContextMaintenanceEdit":
        handles = [target.handle for target in self.targets]
        if len(handles) != len(set(handles)):
            raise ValueError("maintenance targets must not repeat")
        if self.operation == "merge" and len(handles) < 2:
            raise ValueError("merge needs two or more targets")
        if self.operation != "merge" and len(handles) != 1:
            raise ValueError(f"{self.operation} takes exactly one target")
        if self.operation == "drop":
            if self.markdown is not None:
                raise ValueError("drop takes no markdown")
        elif not (self.markdown or "").strip():
            raise ValueError(f"{self.operation} needs markdown")
        return self


_MAINTENANCE_EDIT = TypeAdapter(ContextMaintenanceEdit)


class LLMContextMaintenance(StructuredLLMOutput):
    """The maintainer's proposed edits for one Context."""

    edits: list[ContextMaintenanceEdit] = Field(default_factory=list, max_length=64)
    summary: str | None = Field(default=None, max_length=2_000)

    @field_validator("edits", mode="before")
    @classmethod
    def keep_valid_edits(cls, values: object) -> object:
        if not isinstance(values, list):
            return values
        kept = []
        for value in values:
            try:
                kept.append(_MAINTENANCE_EDIT.validate_python(value))
            except ValidationError as exc:
                logger.warning(
                    "Dropped malformed Context maintenance edit: {}", exc.errors()[0]["msg"]
                )
        return kept

    @field_validator("summary", mode="before")
    @classmethod
    def normalize_summary(cls, value: object) -> object:
        return value[:2_000] if isinstance(value, str) else value
