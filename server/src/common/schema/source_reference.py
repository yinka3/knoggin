"""Validated contracts for source context consulted by an agent response.

These models deliberately describe the small, answer-level provenance surface.
They are not a general source ledger and do not assert that a source proves an
answer's claims.
"""

import re
from datetime import datetime
from typing import Annotated, Any, Literal, Union
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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


class _StrictLocator(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PdfPageLocator(_StrictLocator):
    """A one-based page locator for a PDF retrieval chunk."""

    kind: Literal["pdf_page"] = "pdf_page"
    page: int = Field(ge=1)


class TextLineLocator(_StrictLocator):
    """A one-based inclusive line range, optionally under a Markdown section."""

    kind: Literal["text_lines"] = "text_lines"
    start_line: int = Field(ge=1)
    end_line: int = Field(ge=1)
    section_path: tuple[str, ...] | None = None

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_line < self.start_line:
            raise ValueError("end_line must be greater than or equal to start_line")
        return self

    @field_validator("section_path")
    @classmethod
    def _validate_section_path(
        cls, value: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        if value is not None and any(not part.strip() for part in value):
            raise ValueError("section_path must not contain blank headings")
        return value


class CsvRowLocator(_StrictLocator):
    """A one-based inclusive data-row range; the header is not a data row."""

    kind: Literal["csv_rows"] = "csv_rows"
    start_row: int = Field(ge=1)
    end_row: int = Field(ge=1)

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_row < self.start_row:
            raise ValueError("end_row must be greater than or equal to start_row")
        return self


class CodeLineLocator(_StrictLocator):
    """A one-based inclusive source-code line range with an optional symbol."""

    kind: Literal["code_lines"] = "code_lines"
    start_line: int = Field(ge=1)
    end_line: int = Field(ge=1)
    symbol_name: str | None = None

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_line < self.start_line:
            raise ValueError("end_line must be greater than or equal to start_line")
        return self

    @field_validator("symbol_name")
    @classmethod
    def _validate_symbol_name(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("symbol_name must not be blank")
        return value


class DocxParagraphLocator(_StrictLocator):
    """A one-based inclusive DOCX body-paragraph range under Word headings."""

    kind: Literal["docx_paragraphs"] = "docx_paragraphs"
    start_paragraph: int = Field(ge=1)
    end_paragraph: int = Field(ge=1)
    heading_path: tuple[str, ...] | None = None

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_paragraph < self.start_paragraph:
            raise ValueError(
                "end_paragraph must be greater than or equal to start_paragraph"
            )
        return self

    @field_validator("heading_path")
    @classmethod
    def _validate_heading_path(
        cls, value: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        if value is not None and any(not part.strip() for part in value):
            raise ValueError("heading_path must not contain blank headings")
        return value


class PastedTextLocator(_StrictLocator):
    """A zero-based, end-exclusive span in the canonical user message."""

    kind: Literal["character_span"] = "character_span"
    start_char: int = Field(ge=0)
    end_char: int = Field(ge=1)

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_char <= self.start_char:
            raise ValueError("end_char must be greater than start_char")
        return self


class SearchResultLocator(_StrictLocator):
    """The provider result returned to the agent, not the linked web page."""

    kind: Literal["search_result"] = "search_result"
    provider: str = Field(min_length=1)
    query: str = Field(min_length=1)
    rank: int = Field(ge=1)

    @field_validator("provider", "query")
    @classmethod
    def _reject_blank_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("search locator fields must not be blank")
        return value


DocumentLocator = Annotated[
    Union[
        PdfPageLocator,
        TextLineLocator,
        CsvRowLocator,
        CodeLineLocator,
        DocxParagraphLocator,
    ],
    Field(discriminator="kind"),
]
SourceLocator = Annotated[
    Union[
        PdfPageLocator,
        TextLineLocator,
        CsvRowLocator,
        CodeLineLocator,
        DocxParagraphLocator,
        PastedTextLocator,
        SearchResultLocator,
    ],
    Field(discriminator="kind"),
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
    metadata: dict[str, Any] = Field(default_factory=dict)
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


SourceStatus = Literal["available", "search_result_snippet"]


class SourceConsulted(BaseModel):
    """Stable response projection for one source consulted by an answer."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_kind: SourceKind
    display_label: str = Field(min_length=1)
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
    sources_consulted: list[SourceConsulted] = Field(default_factory=list)
