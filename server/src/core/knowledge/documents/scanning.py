import fnmatch
import hashlib
from pathlib import PurePosixPath, PureWindowsPath
from typing import Dict, List, Optional

from pathspec import GitIgnoreSpec

from common.schema.document import (
    FolderPreview,
    FolderPreviewEntry,
    FolderPreviewSummary,
    FolderScanSettings,
    FolderUploadEntry,
)
from core.knowledge.documents.constants import (
    ARCHIVE_EXTENSIONS,
    AUDIO_EXTENSIONS,
    BINARY_TEXT_EXEMPT_EXTENSIONS,
    DEFAULT_IGNORED_DIRECTORIES,
    DEFAULT_IGNORED_PATTERNS,
    EXECUTABLE_EXTENSIONS,
    IMAGE_EXTENSIONS,
    SENSITIVE_FILE_PATTERNS,
    VIDEO_EXTENSIONS,
)
from core.knowledge.documents.storage import looks_binary


def normalize_relative_path(
    relative_path: Optional[str],
    original_name: str,
) -> str:
    raw_path = relative_path if relative_path is not None else original_name
    if not raw_path or not raw_path.strip():
        raise ValueError("relative_path must not be empty")
    if "\x00" in raw_path:
        raise ValueError("relative_path contains an invalid null byte")

    slash_path = raw_path.strip().replace("\\", "/")
    windows_path = PureWindowsPath(raw_path)
    posix_path = PurePosixPath(slash_path)
    if windows_path.is_absolute() or windows_path.drive or posix_path.is_absolute():
        raise ValueError("relative_path must be relative")
    if any(part == ".." for part in posix_path.parts):
        raise ValueError("relative_path must not escape the project root")

    normalized = posix_path.as_posix()
    if normalized in {"", "."}:
        raise ValueError("relative_path must identify a document")
    return normalized


def matches_pattern(path: str, patterns) -> bool:
    name = PurePosixPath(path).name
    return any(
        fnmatch.fnmatchcase(path, pattern)
        or fnmatch.fnmatchcase(name, pattern)
        for pattern in patterns
    )


def has_hidden_part(path: str) -> bool:
    return any(
        part.startswith(".") and part not in {".", ".."}
        for part in PurePosixPath(path).parts
    )


def has_archive_signature(content: bytes) -> bool:
    return content.startswith(
        (
            b"PK\x03\x04",
            b"\x1f\x8b",
            b"7z\xbc\xaf\x27\x1c",
            b"Rar!\x1a\x07",
        )
    )


def has_executable_signature(content: bytes) -> bool:
    return content.startswith(
        (
            b"MZ",
            b"\x7fELF",
            b"\xfe\xed\xfa\xce",
            b"\xfe\xed\xfa\xcf",
            b"\xce\xfa\xed\xfe",
            b"\xcf\xfa\xed\xfe",
        )
    )


def gitignore_decision(
    path: str,
    *,
    is_directory: bool,
    specs,
) -> bool:
    ignored = False
    path_parts = PurePosixPath(path).parts
    for base_parts, spec in specs:
        if base_parts and path_parts[: len(base_parts)] != base_parts:
            continue
        relative_parts = path_parts[len(base_parts) :]
        if not relative_parts:
            continue
        candidate = PurePosixPath(*relative_parts).as_posix()
        if is_directory:
            candidate += "/"
        decision = spec.check_file(candidate).include
        if decision is not None:
            ignored = bool(decision)
    return ignored


def ancestor_match(path: str, excluded_directories):
    parts = PurePosixPath(path).parts[:-1]
    for index in range(1, len(parts) + 1):
        ancestor = PurePosixPath(*parts[:index]).as_posix()
        if ancestor in excluded_directories:
            return ancestor
    return None


def build_folder_preview(
    folder_name: str,
    entries: List[FolderUploadEntry],
    settings: FolderScanSettings,
    force_include_paths: List[str],
) -> FolderPreview:
    normalized_entries = {}
    for entry in entries:
        normalized = normalize_relative_path(
            entry.relative_path,
            entry.relative_path,
        )
        if normalized in normalized_entries:
            raise ValueError(
                f"duplicate normalized relative_path: {normalized}"
            )
        normalized_entries[normalized] = entry.content

    normalized_overrides = {
        normalize_relative_path(path, path)
        for path in force_include_paths
    }
    unknown_overrides = normalized_overrides - normalized_entries.keys()
    if unknown_overrides:
        raise ValueError(
            "force_include_paths contain unknown entries: "
            + ", ".join(sorted(unknown_overrides))
        )

    gitignore_specs = []
    if settings.respect_gitignore:
        for path, content in normalized_entries.items():
            path_obj = PurePosixPath(path)
            if path_obj.name != ".gitignore":
                continue
            base_parts = (
                ()
                if path_obj.parent.as_posix() == "."
                else path_obj.parent.parts
            )
            lines = content.decode("utf-8-sig", errors="replace").splitlines()
            gitignore_specs.append(
                (base_parts, GitIgnoreSpec.from_lines(lines))
            )
        gitignore_specs.sort(key=lambda item: (len(item[0]), item[0]))

    directories = set()
    for path in normalized_entries:
        parts = PurePosixPath(path).parts[:-1]
        for index in range(1, len(parts) + 1):
            directories.add(PurePosixPath(*parts[:index]).as_posix())

    hidden_directories = {
        path
        for path in directories
        if not settings.include_hidden and has_hidden_part(path)
    }
    default_directories = {
        path
        for path in directories
        if PurePosixPath(path).name.lower() in DEFAULT_IGNORED_DIRECTORIES
    }
    gitignored_directories = {
        path
        for path in directories
        if settings.respect_gitignore
        and gitignore_decision(
            path,
            is_directory=True,
            specs=gitignore_specs,
        )
    }
    custom_directories = {
        path
        for path in directories
        if (
            PurePosixPath(path).name.lower()
            in settings.blocked_directory_names
            or matches_pattern(path, settings.ignored_patterns)
        )
    }

    included = []
    excluded = []
    excluded_directories = set()
    accepted_bytes = 0

    def exclude(
        path: str,
        content: bytes,
        reason: str,
        source: str,
        overridable: bool,
        directory: Optional[str] = None,
    ):
        if directory is not None:
            excluded_directories.add(directory)
        path_obj = PurePosixPath(path)
        excluded.append(
            FolderPreviewEntry(
                relative_path=path,
                original_name=path_obj.name,
                extension=PurePosixPath(path_obj.name).suffix.lower(),
                size_bytes=len(content),
                reason=reason,
                rule_source=source,
                overridable=overridable,
            )
        )

    for path in sorted(normalized_entries):
        content = normalized_entries[path]
        path_obj = PurePosixPath(path)
        name = path_obj.name
        lower_name = name.lower()
        extension = PurePosixPath(name).suffix.lower()
        depth = len(path_obj.parts) - 1
        forced = path in normalized_overrides

        if depth > settings.max_folder_depth:
            exclude(
                path,
                content,
                "folder_too_deep",
                "safety_limit",
                False,
            )
            continue
        if len(content) > settings.max_document_size_bytes:
            exclude(
                path,
                content,
                "document_too_large",
                "safety_limit",
                False,
            )
            continue
        if matches_pattern(lower_name, SENSITIVE_FILE_PATTERNS):
            exclude(
                path,
                content,
                "blocked_sensitive_file",
                "sensitive_default",
                False,
            )
            continue
        if (
            extension in ARCHIVE_EXTENSIONS
            or (
                extension not in {".docx", ".pdf"}
                and has_archive_signature(content)
            )
        ):
            exclude(path, content, "archive_file", "content_type", False)
            continue
        if (
            extension in EXECUTABLE_EXTENSIONS
            or has_executable_signature(content)
        ):
            exclude(
                path,
                content,
                "executable_file",
                "content_type",
                False,
            )
            continue
        if extension in VIDEO_EXTENSIONS:
            exclude(path, content, "video_file", "content_type", False)
            continue
        if extension in AUDIO_EXTENSIONS:
            exclude(path, content, "audio_file", "content_type", False)
            continue
        if extension in IMAGE_EXTENSIONS:
            exclude(path, content, "image_file", "content_type", False)
            continue
        if (
            extension not in BINARY_TEXT_EXEMPT_EXTENSIONS
            and looks_binary(content)
        ):
            exclude(path, content, "binary_file", "content_type", False)
            continue

        hidden_directory = ancestor_match(path, hidden_directories)
        if not settings.include_hidden and (
            lower_name.startswith(".") or hidden_directory is not None
        ):
            if not forced:
                exclude(
                    path,
                    content,
                    "hidden_path",
                    "hidden_default",
                    True,
                    hidden_directory,
                )
                continue

        default_directory = ancestor_match(
            path, default_directories
        )
        if default_directory is not None and not forced:
            exclude(
                path,
                content,
                "default_directory_ignore",
                "default_ignore",
                True,
                default_directory,
            )
            continue
        if (
            matches_pattern(path, DEFAULT_IGNORED_PATTERNS)
            and not forced
        ):
            exclude(
                path,
                content,
                "default_file_ignore",
                "default_ignore",
                True,
            )
            continue

        gitignored_directory = ancestor_match(
            path, gitignored_directories
        )
        gitignored = (
            settings.respect_gitignore
            and gitignore_decision(
                path,
                is_directory=False,
                specs=gitignore_specs,
            )
        )
        if (
            (gitignored_directory is not None or gitignored)
            and not forced
        ):
            exclude(
                path,
                content,
                "gitignore",
                "gitignore",
                True,
                gitignored_directory,
            )
            continue

        custom_directory = ancestor_match(path, custom_directories)
        if custom_directory is not None and not forced:
            exclude(
                path,
                content,
                "custom_directory_ignore",
                "project_filter",
                True,
                custom_directory,
            )
            continue
        if (
            lower_name in settings.blocked_file_names
            and not forced
        ):
            exclude(
                path,
                content,
                "custom_file_name",
                "project_filter",
                True,
            )
            continue
        if (
            extension in settings.blocked_extensions
            and not forced
        ):
            exclude(
                path,
                content,
                "blocked_extension",
                "project_filter",
                True,
            )
            continue
        if (
            settings.allowed_extensions is not None
            and extension not in settings.allowed_extensions
            and not forced
        ):
            exclude(
                path,
                content,
                "unsupported_extension",
                "project_filter",
                True,
            )
            continue
        if (
            matches_pattern(path, settings.ignored_patterns)
            and not forced
        ):
            exclude(
                path,
                content,
                "custom_pattern",
                "project_filter",
                True,
            )
            continue

        if len(included) >= settings.max_file_count:
            exclude(
                path,
                content,
                "file_count_limit",
                "safety_limit",
                False,
            )
            continue
        if accepted_bytes + len(content) > settings.max_total_size_bytes:
            exclude(
                path,
                content,
                "total_size_limit",
                "safety_limit",
                False,
            )
            continue

        included.append(
            FolderPreviewEntry(
                relative_path=path,
                original_name=name,
                extension=extension,
                size_bytes=len(content),
                content_hash=hashlib.sha256(content).hexdigest(),
            )
        )
        accepted_bytes += len(content)

    summary = FolderPreviewSummary.from_entries(
        included,
        excluded,
        excluded_directories,
    )
    return FolderPreview(
        folder_name=folder_name,
        settings=settings,
        force_include_paths=sorted(normalized_overrides),
        included=included,
        excluded=excluded,
        summary=summary,
    )
