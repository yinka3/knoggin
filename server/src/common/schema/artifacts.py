"""Backend contracts for durable, structured assistant artifacts.

Artifacts are intentionally a small, server-owned block vocabulary.  The
canonical representation is structured JSON; Markdown is a deterministic
fallback for export and clients that do not render blocks yet.  There is no
arbitrary HTML or executable content block.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Annotated, Any, Literal, Union
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)

ArtifactStatus = Literal["complete", "incomplete"]
ArtifactKind = Literal["general", "research_brief", "research_report"]

_DANGEROUS_MARKUP_RE = re.compile(
    r"<\s*(?:script|iframe|object|embed|style)\b|javascript\s*:|on[a-z]+\s*=",
    re.IGNORECASE,
)


def _safe_text(value: str, field_name: str) -> str:
    if _DANGEROUS_MARKUP_RE.search(value):
        raise ValueError(f"{field_name} contains executable or embedded markup")
    return value


class MarkdownArtifactBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["markdown"] = "markdown"
    content: str = Field(min_length=1, max_length=20_000)

    @field_validator("content")
    @classmethod
    def _validate_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("markdown block content must not be blank")
        return _safe_text(value, "markdown block content")


class CalloutArtifactBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["callout"] = "callout"
    tone: Literal["info", "success", "warning", "critical"] = "info"
    title: str | None = Field(default=None, max_length=200)
    content: str = Field(min_length=1, max_length=20_000)

    @field_validator("title", "content")
    @classmethod
    def _validate_text(cls, value: str | None, info) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError(f"{info.field_name} must not be blank")
        return _safe_text(value, f"callout {info.field_name}")


class ChecklistItem(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    label: str = Field(min_length=1, max_length=2_000)
    completed: StrictBool = False

    @field_validator("label")
    @classmethod
    def _validate_label(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("checklist item label must not be blank")
        return _safe_text(value, "checklist item label")


class ChecklistArtifactBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["checklist"] = "checklist"
    title: str | None = Field(default=None, max_length=200)
    items: tuple[ChecklistItem, ...] = Field(min_length=1, max_length=100)

    @field_validator("title")
    @classmethod
    def _validate_title(cls, value: str | None) -> str | None:
        if value is not None:
            if not value.strip():
                raise ValueError("checklist title must not be blank")
            return _safe_text(value, "checklist title")
        return value


class TableArtifactBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["table"] = "table"
    title: str | None = Field(default=None, max_length=200)
    columns: tuple[str, ...] = Field(min_length=1, max_length=20)
    rows: tuple[tuple[str, ...], ...] = Field(default_factory=tuple, max_length=100)

    @field_validator("title")
    @classmethod
    def _validate_title(cls, value: str | None) -> str | None:
        if value is not None:
            if not value.strip():
                raise ValueError("table title must not be blank")
            return _safe_text(value, "table title")
        return value

    @field_validator("columns")
    @classmethod
    def _validate_columns(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not column.strip() for column in value):
            raise ValueError("table columns must not be blank")
        return tuple(_safe_text(column, "table column") for column in value)

    @field_validator("rows")
    @classmethod
    def _validate_rows(cls, value: tuple[tuple[str, ...], ...], info):
        columns = info.data.get("columns")
        if columns is not None and any(len(row) != len(columns) for row in value):
            raise ValueError("table rows must have the same width as columns")
        return tuple(
            tuple(_safe_text(cell, "table cell") for cell in row) for row in value
        )


ArtifactBlock = Annotated[
    Union[
        MarkdownArtifactBlock,
        CalloutArtifactBlock,
        ChecklistArtifactBlock,
        TableArtifactBlock,
    ],
    Field(discriminator="kind"),
]


class ArtifactDraft(BaseModel):
    """A validated artifact emitted alongside one assistant completion."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: StrictInt = Field(default=1, ge=1)
    kind: ArtifactKind = "general"
    title: str = Field(min_length=1, max_length=200)
    blocks: tuple[ArtifactBlock, ...] = Field(min_length=1, max_length=50)
    status: ArtifactStatus = "complete"

    @field_validator("title")
    @classmethod
    def _validate_title(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("artifact title must not be blank")
        return _safe_text(value, "artifact title")

    @model_validator(mode="after")
    def _validate_rendered_size(self) -> "ArtifactDraft":
        if len(render_artifact_markdown(self)) > 100_000:
            raise ValueError("artifact Markdown fallback exceeds 100000 characters")
        return self


class ArtifactReference(BaseModel):
    """Stable identity and current revision metadata for one project artifact."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    artifact_id: UUID
    project_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    originating_message_id: StrictInt = Field(gt=0)
    kind: ArtifactKind
    title: str = Field(min_length=1, max_length=200)
    status: ArtifactStatus
    current_revision: StrictInt = Field(ge=1)
    created_at: datetime
    updated_at: datetime


class ArtifactRevision(BaseModel):
    """One immutable artifact revision, including its export representation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    artifact_id: UUID
    revision: StrictInt = Field(ge=1)
    schema_version: StrictInt = Field(ge=1)
    kind: ArtifactKind
    title: str = Field(min_length=1, max_length=200)
    blocks: tuple[ArtifactBlock, ...] = Field(min_length=1, max_length=50)
    status: ArtifactStatus
    markdown: str = Field(min_length=1, max_length=100_000)
    content_hash: str = Field(min_length=64, max_length=64)
    created_at: datetime

    @field_validator("content_hash")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("content_hash must be a lowercase SHA-256 hex digest")
        return value

    @model_validator(mode="after")
    def _verify_content_hash(self) -> "ArtifactRevision":
        expected = hashlib.sha256(self.markdown.encode("utf-8")).hexdigest()
        if self.content_hash != expected:
            raise ValueError("content_hash does not match revision Markdown")
        return self


def render_artifact_markdown(artifact: ArtifactDraft) -> str:
    """Render an artifact deterministically without introducing raw HTML."""

    lines = [f"# {artifact.title.strip()}"]
    for block in artifact.blocks:
        if isinstance(block, MarkdownArtifactBlock):
            lines.extend(("", block.content.strip()))
        elif isinstance(block, CalloutArtifactBlock):
            heading = f"**{block.title.strip()}**" if block.title else f"**{block.tone.title()}**"
            lines.extend(("", f"> {heading}", ">", *[f"> {line}" for line in block.content.strip().splitlines()]))
        elif isinstance(block, ChecklistArtifactBlock):
            if block.title:
                lines.extend(("", f"## {block.title.strip()}"))
            lines.extend(
                f"- [{'x' if item.completed else ' '}] {item.label.strip()}"
                for item in block.items
            )
        elif isinstance(block, TableArtifactBlock):
            if block.title:
                lines.extend(("", f"## {block.title.strip()}"))
            lines.extend(
                (
                    "| " + " | ".join(_escape_table_cell(column) for column in block.columns) + " |",
                    "| " + " | ".join("---" for _ in block.columns) + " |",
                )
            )
            lines.extend(
                "| " + " | ".join(_escape_table_cell(cell) for cell in row) + " |"
                for row in block.rows
            )
    return "\n".join(lines).strip() + "\n"


def _escape_table_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def artifact_content_hash(artifact: ArtifactDraft, markdown: str | None = None) -> str:
    """Return the stable hash used to identify an artifact revision's content."""

    rendered = markdown if markdown is not None else render_artifact_markdown(artifact)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def artifact_json(artifact: ArtifactDraft) -> dict[str, Any]:
    """Return JSON-safe canonical block data for JSONB persistence."""

    return artifact.model_dump(mode="json")


def default_artifact_from_answer(
    content: str,
    *,
    kind: ArtifactKind,
) -> ArtifactDraft:
    """Create the bounded default artifact required by research profiles."""

    # The answer is already Markdown, but auto-created artifacts must not turn
    # dangerous HTML or URL schemes into executable UI content later.
    safe_content = re.sub(r"<", "&lt;", content)
    safe_content = re.sub(r">", "&gt;", safe_content)
    safe_content = re.sub(
        r"javascript\s*:", "javascript&#58;", safe_content, flags=re.IGNORECASE
    )
    safe_content = re.sub(
        r"on[a-z]+\s*=",
        lambda match: match.group(0).replace("=", "&#61;"),
        safe_content,
        flags=re.IGNORECASE,
    )
    title = {
        "general": "Assistant artifact",
        "research_brief": "Research brief",
        "research_report": "Research report",
    }[kind]
    return ArtifactDraft(
        kind=kind,
        title=title,
        blocks=(MarkdownArtifactBlock(content=safe_content.strip()),),
    )
