"""Strict contracts for durable project Context state.

These models deliberately describe durable identifiers and edit commands, not
renderer-specific Markdown.  The renderer/importer and LLM updater are added in
later batches.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from common.schema.config import ConfigModel
from common.schema.llm import StructuredLLMOutput, normalize_optional_text

_LOCAL_BLOCK_REF = re.compile(r"C[1-9][0-9]*$")
_LOCAL_EVIDENCE_REF = re.compile(r"[MSE][1-9][0-9]*$")
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
    """A request-local message, assistant-source, or Episode handle.

    ``M1``, ``S1``, and ``E1`` are deliberately distinct from durable IDs. The
    updater resolves them only against the frozen catalog for one window.
    """

    handle: str

    @field_validator("handle")
    @classmethod
    def require_local_handle(cls, value: str) -> str:
        value = value.strip()
        if not _LOCAL_EVIDENCE_REF.fullmatch(value):
            raise ValueError("Context evidence handle must have the form M1, S1, or E1")
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
    projection_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )


class ContextEditBase(ConfigModel):
    """Shared bounded shape for updater edit commands."""

    operation: ContextEditOperationKind
    section_key: str
    dependencies: list[LocalContextBlockReference] = Field(default_factory=list)
    evidence: list[LocalContextEvidenceReference] = Field(
        default_factory=list,
        max_length=8,
    )

    @field_validator("section_key")
    @classmethod
    def require_section_key(cls, value: str) -> str:
        value = value.strip()
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
    source_time_ms: int | None = Field(default=None, ge=0)


class ContextReplace(ContextEditBase):
    operation: Literal[ContextEditOperationKind.REPLACE] = (
        ContextEditOperationKind.REPLACE
    )
    target: LocalContextBlockReference
    markdown: str = Field(min_length=1, max_length=50_000)
    assertion_kind: AssertionKind = AssertionKind.AGENT_DERIVED
    source_time_ms: int | None = Field(default=None, ge=0)


class ContextDelete(ContextEditBase):
    operation: Literal[ContextEditOperationKind.DELETE] = ContextEditOperationKind.DELETE
    target: LocalContextBlockReference


ContextUpdateOperation = Annotated[
    ContextAdd | ContextReplace | ContextDelete,
    Field(discriminator="operation"),
]


class LLMContextUpdate(StructuredLLMOutput):
    """The bounded local-reference operation set returned by the Context LLM."""

    operations: list[ContextUpdateOperation] = Field(default_factory=list, max_length=64)
    edit_summary: str | None = Field(default=None, max_length=2_000)

    @field_validator("edit_summary")
    @classmethod
    def normalize_edit_summary(cls, value: str | None) -> str | None:
        return normalize_optional_text(value, field_name="edit_summary")

    @model_validator(mode="after")
    def validate_operations(self) -> "LLMContextUpdate":
        markdown_chars = 0
        for operation in self.operations:
            if not operation.evidence:
                raise ValueError("Context operations require at least one evidence handle")
            if isinstance(operation, (ContextAdd, ContextReplace)):
                if operation.assertion_kind is AssertionKind.HUMAN_ASSERTED:
                    raise ValueError("the Context model cannot create human_asserted blocks")
                if operation.source_time_ms is not None:
                    raise ValueError("the server computes Context block source_time_ms")
                markdown_chars += len(operation.markdown)
        if markdown_chars > 100_000:
            raise ValueError("Context model output exceeds the 100000-character limit")
        return self
