"""Validated contracts for source context consulted by an agent response.

These models deliberately describe the small, answer-level provenance surface.
They are not a general source ledger and do not assert that a source proves an
answer's claims.
"""

import re
from datetime import datetime
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from common.schema.immutable import FrozenDict
from common.schema.source.locators import (
    CodeLineLocator,
    CsvRowLocator,
    DocxParagraphLocator,
    PastedTextLocator,
    PdfPageLocator,
    SearchResultLocator,
    SourceLocator,
    TextLineLocator,
)

SourceKind = Literal[
    "pdf_document",
    "text_document",
    "user_pasted_text",
    "web_search_result",
    "news_search_result",
]
EncounterKind = Literal[
    "document_search",
    "document_read",
    "user_pasted_text",
    "web_search",
    "news_search",
]


class SourceReferenceCandidate(BaseModel):
    """A validated source result collected before its assistant message exists."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    project_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    source_kind: SourceKind
    document_id: str | None = None
    canonical_url: str | None = None
    source_message_id: int | None = Field(default=None, gt=0)
    content_hash: str = Field(min_length=64, max_length=64)
    locator: SourceLocator
    excerpt: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=FrozenDict)
    encounter_kind: EncounterKind
    agent_run_id: str = Field(min_length=1)
    tool_call_id: str | None = None
    result_position: int = Field(ge=0)

    @field_validator(
        "project_id",
        "session_id",
        "document_id",
        "agent_run_id",
        "tool_call_id",
    )
    @classmethod
    def _reject_blank_identifiers(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("source reference identifiers must not be blank")
        return value

    @field_validator("content_hash")
    @classmethod
    def _validate_content_hash(cls, value: str) -> str:
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("content_hash must be a lowercase SHA-256 hex digest")
        return value

    @field_validator("excerpt")
    @classmethod
    def _reject_blank_excerpt(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("excerpt must not be blank")
        return value

    @field_validator("metadata")
    @classmethod
    def _freeze_metadata(cls, value: dict[str, Any]) -> FrozenDict:
        return FrozenDict(value)

    @field_validator("canonical_url")
    @classmethod
    def _validate_canonical_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("canonical_url must be an absolute HTTP(S) URL")
        if parsed.fragment:
            raise ValueError("canonical_url must not contain a fragment")
        return value

    @model_validator(mode="after")
    def _validate_source_shape(self):
        document_kinds = {"pdf_document", "text_document"}
        search_kinds = {"web_search_result", "news_search_result"}

        if self.source_kind in document_kinds:
            if not self.document_id:
                raise ValueError("document sources require document_id")
            if self.canonical_url is not None or self.source_message_id is not None:
                raise ValueError(
                    "document sources cannot include URL or source message"
                )
            if self.tool_call_id is None:
                raise ValueError("tool-derived sources require tool_call_id")
            if self.encounter_kind not in {"document_search", "document_read"}:
                raise ValueError("document sources require a document encounter kind")
            if self.source_kind == "pdf_document" and not isinstance(
                self.locator, PdfPageLocator
            ):
                raise ValueError("pdf_document sources require a PDF page locator")
            if self.source_kind == "text_document" and not isinstance(
                self.locator,
                (TextLineLocator, CsvRowLocator, CodeLineLocator, DocxParagraphLocator),
            ):
                raise ValueError("text_document sources require a text locator")
            self._require_text_metadata("document_name")

        elif self.source_kind == "user_pasted_text":
            if self.document_id is not None or self.canonical_url is not None:
                raise ValueError("pasted text cannot include document or URL identity")
            if self.source_message_id is None:
                raise ValueError("pasted text requires source_message_id")
            if self.tool_call_id is not None:
                raise ValueError("pasted text must not include tool_call_id")
            if self.encounter_kind != "user_pasted_text":
                raise ValueError("pasted text requires the pasted-text encounter kind")
            if not isinstance(self.locator, PastedTextLocator):
                raise ValueError("pasted text requires a character-span locator")

        elif self.source_kind in search_kinds:
            expected_encounter = (
                "web_search"
                if self.source_kind == "web_search_result"
                else "news_search"
            )
            if self.document_id is not None or self.source_message_id is not None:
                raise ValueError(
                    "search results cannot include document or source message"
                )
            if self.canonical_url is None:
                raise ValueError("search results require canonical_url")
            if self.tool_call_id is None:
                raise ValueError("tool-derived sources require tool_call_id")
            if self.encounter_kind != expected_encounter:
                raise ValueError("search result has an incompatible encounter kind")
            if not isinstance(self.locator, SearchResultLocator):
                raise ValueError("search results require a provider-result locator")
            self._require_text_metadata("title")
            self._require_metadata("discovery_snippet")
            if self.metadata["discovery_snippet"] is not True:
                raise ValueError("search result metadata must mark discovery_snippet")

        return self

    def _require_metadata(self, *keys: str) -> None:
        missing = [key for key in keys if not self.metadata.get(key)]
        if missing:
            rendered = ", ".join(missing)
            raise ValueError(f"source metadata is missing required fields: {rendered}")

    def _require_text_metadata(self, *keys: str) -> None:
        invalid = [
            key
            for key in keys
            if not isinstance(self.metadata.get(key), str)
            or not self.metadata[key].strip()
        ]
        if invalid:
            rendered = ", ".join(invalid)
            raise ValueError(
                f"source metadata must contain non-blank text fields: {rendered}"
            )


class SourceReference(SourceReferenceCandidate):
    """A source candidate durably attached to one assistant message."""

    source_ref_id: str = Field(min_length=1)
    message_id: int = Field(gt=0)
    idempotency_key: str = Field(min_length=1)
    created_at: datetime

    @field_validator("source_ref_id", "idempotency_key")
    @classmethod
    def _reject_blank_persistence_identifiers(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("persisted source-reference identifiers must not be blank")
        return value


SourceStatus = Literal["available", "unavailable", "search_result_snippet"]


class SourceConsulted(BaseModel):
    """Stable engine provenance for one source consulted by an answer."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_kind: SourceKind
    locator: SourceLocator
    excerpt: str = Field(min_length=1)
    document_id: str | None = None
    canonical_url: str | None = None
    source_message_id: int | None = Field(default=None, gt=0)
    source_status: SourceStatus
    contributing_message_id: int = Field(gt=0)


class AssistantMessageWithSources(BaseModel):
    """One owned assistant response with its ordered source context."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    message_id: int = Field(gt=0)
    content: str
    sources_consulted: tuple[SourceConsulted, ...] = Field(default_factory=tuple)
