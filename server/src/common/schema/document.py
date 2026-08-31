import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Annotated, Dict, List, Literal, Optional, Set, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from common.schema.source.locators import DocumentLocator


class FolderUploadEntry(BaseModel):
    """One browser-uploaded file in a virtual folder manifest."""

    model_config = ConfigDict(frozen=True)

    relative_path: str = Field(min_length=1)
    content: bytes = Field(repr=False)


class WorkspaceSyncChanges(BaseModel):
    """Incremental changes to one previously synchronized workspace source."""

    model_config = ConfigDict(frozen=True)

    upserts: tuple[FolderUploadEntry, ...] = Field(default_factory=tuple)
    deleted_paths: tuple[str, ...] = Field(default_factory=tuple)


class FolderScanSettings(BaseModel):
    """Validated, caller-supplied rules for a folder preview scan."""

    model_config = ConfigDict(frozen=True)

    respect_gitignore: bool = True
    include_hidden: bool = False
    ignored_patterns: tuple[str, ...] = Field(default_factory=tuple)
    allowed_extensions: Optional[frozenset[str]] = None
    blocked_extensions: frozenset[str] = Field(default_factory=frozenset)
    blocked_file_names: frozenset[str] = Field(default_factory=frozenset)
    blocked_directory_names: frozenset[str] = Field(default_factory=frozenset)
    max_document_size_bytes: int = Field(
        25 * 1024 * 1024,
        ge=1,
    )
    max_total_size_bytes: int = Field(
        500 * 1024 * 1024,
        ge=1,
    )
    max_file_count: int = Field(1000, ge=1)
    max_folder_depth: int = Field(20, ge=1)

    @field_validator("ignored_patterns")
    @classmethod
    def _normalize_patterns(cls, values: List[str]) -> tuple[str, ...]:
        return tuple(value.strip() for value in values if value.strip())

    @field_validator(
        "allowed_extensions",
        "blocked_extensions",
        mode="before",
    )
    @classmethod
    def _normalize_extensions(cls, values):
        if values is None:
            return None
        normalized = set()
        for value in values:
            extension = str(value).strip().lower()
            if not extension:
                continue
            normalized.add(extension if extension.startswith(".") else f".{extension}")
        return normalized

    @field_validator(
        "blocked_file_names",
        "blocked_directory_names",
        mode="before",
    )
    @classmethod
    def _normalize_names(cls, values):
        return {
            str(value).strip().lower() for value in (values or []) if str(value).strip()
        }

    @model_validator(mode="after")
    def _validate_consistent_limits_and_extensions(self) -> "FolderScanSettings":
        if self.max_document_size_bytes > self.max_total_size_bytes:
            raise ValueError(
                "max_document_size_bytes must not exceed max_total_size_bytes"
            )
        if self.allowed_extensions is not None:
            overlap = self.allowed_extensions & self.blocked_extensions
            if overlap:
                raise ValueError(
                    "allowed_extensions and blocked_extensions must not overlap: "
                    f"{sorted(overlap)}"
                )
        return self


@dataclass(slots=True)
class FolderPreviewEntry:
    """Lightweight metadata for one included or excluded manifest entry."""

    relative_path: str
    original_name: str
    extension: str
    size_bytes: int
    content_hash: Optional[str] = None
    reason: Optional[str] = None
    rule_source: Optional[str] = None
    overridable: bool = False


@dataclass(slots=True)
class FolderPreviewSummary:
    included_count: int
    excluded_count: int
    included_bytes: int
    excluded_bytes: int
    excluded_directory_count: int
    reason_counts: Dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_entries(
        cls,
        included: List[FolderPreviewEntry],
        excluded: List[FolderPreviewEntry],
        excluded_directories: Set[str],
    ) -> "FolderPreviewSummary":
        reasons = Counter(
            entry.reason for entry in excluded if entry.reason is not None
        )
        return cls(
            included_count=len(included),
            excluded_count=len(excluded),
            included_bytes=sum(entry.size_bytes for entry in included),
            excluded_bytes=sum(entry.size_bytes for entry in excluded),
            excluded_directory_count=len(excluded_directories),
            reason_counts=dict(sorted(reasons.items())),
        )


@dataclass(slots=True)
class FolderPreview:
    folder_name: str
    settings: FolderScanSettings
    summary: FolderPreviewSummary
    force_include_paths: List[str] = field(default_factory=list)
    included: List[FolderPreviewEntry] = field(default_factory=list)
    excluded: List[FolderPreviewEntry] = field(default_factory=list)


class _DocumentFocusBase(BaseModel):
    """Common immutable metadata for a session- or request-scoped focus."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: Literal["pinned", "request"] = "pinned"
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def _require_timezone_aware_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("document focus created_at must include a timezone")
        return value.astimezone(timezone.utc)


class DocumentSelection(BaseModel):
    """One version-bound passage selected from a durable document."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    content_hash: str = Field(min_length=64, max_length=64)
    locator: DocumentLocator

    @field_validator("content_hash")
    @classmethod
    def _require_sha256_content_hash(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError(
                "document selection content_hash must be a SHA-256 hex digest"
            )
        return value


class DocumentFocusDocument(_DocumentFocusBase):
    """A focus targeting one exact document."""

    target_type: Literal["document"]
    document_id: str = Field(min_length=1)
    relative_path: str = Field(min_length=1)
    selection: DocumentSelection | None = None

    @field_validator("document_id", "relative_path")
    @classmethod
    def _normalize_selectors(cls, value: str) -> str:
        if not (normalized := value.strip()):
            raise ValueError("document focus selectors must not be blank")
        return normalized

    @model_validator(mode="after")
    def _selection_is_request_scoped(self):
        if self.selection is not None and self.mode != "request":
            raise ValueError("document selections are only valid for request focus")
        return self


class DocumentFocusSubtree(_DocumentFocusBase):
    """A focus targeting one project-relative path subtree."""

    target_type: Literal["subtree"]
    path_prefix: str = Field(min_length=1)

    @field_validator("path_prefix")
    @classmethod
    def _normalize_selectors(cls, value: str) -> str:
        if not (normalized := value.strip()):
            raise ValueError("document focus selectors must not be blank")
        return normalized


DocumentFocus = Annotated[
    Union[DocumentFocusDocument, DocumentFocusSubtree],
    Field(discriminator="target_type"),
]

_DOCUMENT_FOCUS_ADAPTER = TypeAdapter(DocumentFocus)
_LEGACY_OPTIONAL_SELECTORS = {
    "document_id",
    "relative_path",
    "path_prefix",
}


def parse_document_focus(value: object) -> DocumentFocus:
    """Validate a focus, accepting legacy persisted null selector fields.

    Newly written focus values contain only the selectors owned by their
    discriminated variant. Removing null legacy fields makes old persisted
    records readable without allowing conflicting non-null selectors.
    """

    if isinstance(
        value,
        (DocumentFocusDocument, DocumentFocusSubtree),
    ):
        return value
    if not isinstance(value, dict):
        raise ValueError("document focus must be an object")
    normalized = dict(value)
    for selector in _LEGACY_OPTIONAL_SELECTORS:
        if normalized.get(selector) is None:
            normalized.pop(selector, None)
    return _DOCUMENT_FOCUS_ADAPTER.validate_python(normalized)


def create_document_focus(
    *,
    mode: Literal["pinned", "request"] = "pinned",
    created_at: datetime | str,
    **target: object,
) -> DocumentFocus:
    """Create one focus variant from a resolved document-service target."""

    return parse_document_focus({"mode": mode, "created_at": created_at, **target})


def dump_document_focus(value: DocumentFocus) -> dict:
    """Serialize a validated focus using only its variant's selector fields."""

    focus = parse_document_focus(value)
    payload = focus.model_dump(mode="json", exclude_none=True)
    # Keep the application's stable ISO-8601 UTC form (+00:00) rather than
    # inheriting Pydantic's version-dependent `Z` JSON rendering.
    payload["created_at"] = focus.created_at.isoformat()
    return payload
