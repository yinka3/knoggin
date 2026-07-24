from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Set

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class FolderUploadEntry(BaseModel):
    """One browser-uploaded file in a virtual folder manifest."""

    model_config = ConfigDict(frozen=True)

    relative_path: str = Field(min_length=1)
    content: bytes = Field(repr=False)


class WorkspaceSyncChanges(BaseModel):
    """Incremental changes to one previously synchronized workspace source."""

    model_config = ConfigDict(frozen=True)

    upserts: List[FolderUploadEntry] = Field(default_factory=list)
    deleted_paths: List[str] = Field(default_factory=list)


class FolderScanSettings(BaseModel):
    """Validated, caller-supplied rules for a folder preview scan."""

    model_config = ConfigDict(frozen=True)

    respect_gitignore: bool = True
    include_hidden: bool = False
    ignored_patterns: List[str] = Field(default_factory=list)
    allowed_extensions: Optional[Set[str]] = None
    blocked_extensions: Set[str] = Field(default_factory=set)
    blocked_file_names: Set[str] = Field(default_factory=set)
    blocked_directory_names: Set[str] = Field(default_factory=set)
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
    def _normalize_patterns(cls, values: List[str]) -> List[str]:
        return [value.strip() for value in values if value.strip()]

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
            normalized.add(
                extension if extension.startswith(".") else f".{extension}"
            )
        return normalized

    @field_validator(
        "blocked_file_names",
        "blocked_directory_names",
        mode="before",
    )
    @classmethod
    def _normalize_names(cls, values):
        return {
            str(value).strip().lower()
            for value in (values or [])
            if str(value).strip()
        }


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


class DocumentFocus(BaseModel):
    """Validated session-scoped document focus."""

    model_config = ConfigDict(frozen=True)

    mode: Literal["pinned"] = "pinned"
    target_type: Literal["document", "subtree", "folder_upload"]
    document_id: Optional[str] = None
    relative_path: Optional[str] = None
    folder_root_id: Optional[str] = None
    path_prefix: Optional[str] = None
    created_at: str

    @field_validator(
        "document_id",
        "relative_path",
        "folder_root_id",
        "path_prefix",
    )
    @classmethod
    def _reject_blank_values(cls, value):
        if value is not None and not str(value).strip():
            raise ValueError("document focus values must not be blank")
        return value

    @model_validator(mode="after")
    def _validate_target_shape(self):
        if self.target_type == "document":
            if self.document_id is None or self.relative_path is None:
                raise ValueError(
                    "document focus requires document_id and relative_path"
                )
            if self.path_prefix is not None:
                raise ValueError(
                    "document focus cannot include path_prefix"
                )
        elif self.target_type == "subtree":
            if self.folder_root_id is None or self.path_prefix is None:
                raise ValueError(
                    "subtree focus requires folder_root_id and path_prefix"
                )
            if self.document_id is not None or self.relative_path is not None:
                raise ValueError(
                    "subtree focus cannot include document selectors"
                )
        elif self.target_type == "folder_upload":
            if self.folder_root_id is None:
                raise ValueError(
                    "folder upload focus requires folder_root_id"
                )
            if (
                self.document_id is not None
                or self.relative_path is not None
                or self.path_prefix is not None
            ):
                raise ValueError(
                    "folder upload focus cannot include other selectors"
                )
        return self
