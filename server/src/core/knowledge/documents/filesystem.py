"""Confined native filesystem access for one Knoggin project."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterator

from core.knowledge.documents.constants import MAX_DOCUMENT_SIZE
from core.knowledge.documents.scanning import normalize_relative_path


class ProjectFilesystemConflictError(RuntimeError):
    """Raised when a caller attempts to replace stale project-file content."""


@dataclass(frozen=True, slots=True)
class ProjectFile:
    """A filesystem snapshot suitable for document-catalog reconciliation."""

    relative_path: str
    size_bytes: int
    content_hash: str


@dataclass(frozen=True, slots=True)
class ProjectFilePath:
    """A regular file location discovered without reading its bytes."""

    relative_path: str
    size_bytes: int


class ProjectFilesystem:
    """Read and write one project's local directory without escaping its root.

    The caller supplies a trusted absolute project root. Every public path is a
    normalized project-relative path; symlinks inside that root are rejected so
    document and agent operations cannot silently reach elsewhere on the host.
    """

    def __init__(self, project_root: Path | str) -> None:
        root = Path(project_root)
        if not root.is_absolute():
            raise ValueError("project_root must be absolute")
        if root.is_symlink():
            raise ValueError("project_root must not be a symlink")
        self._root = root

    @property
    def root(self) -> Path:
        return self._root

    @staticmethod
    def normalize_path(relative_path: str) -> str:
        return normalize_relative_path(relative_path, relative_path)

    def ensure_root(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        if self._root.is_symlink() or not self._root.is_dir():
            raise ValueError("project_root must be a real directory")

    def read_bytes(self, relative_path: str, *, max_bytes: int = MAX_DOCUMENT_SIZE) -> bytes:
        """Return one regular project file, enforcing the requested read bound."""
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        path = self._path_for_read(relative_path)
        size = path.stat().st_size
        if size > max_bytes:
            raise ValueError(f"file exceeds the {max_bytes}-byte read limit")
        content = path.read_bytes()
        if len(content) > max_bytes:
            raise ValueError(f"file exceeds the {max_bytes}-byte read limit")
        return content

    def write_bytes(
        self,
        relative_path: str,
        content: bytes,
        *,
        overwrite: bool = False,
        expected_content_hash: str | None = None,
    ) -> ProjectFile:
        """Atomically create or replace one project file.

        ``expected_content_hash`` provides the local optimistic-concurrency
        check used by agent editing operations.  A temporary sibling file plus
        ``os.replace`` prevents readers from observing partially written bytes.
        """
        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")
        if not content:
            raise ValueError("project file content must not be empty")
        if len(content) > MAX_DOCUMENT_SIZE:
            raise ValueError("document exceeds the 50 MB size limit")
        if expected_content_hash is not None and (
            not isinstance(expected_content_hash, str) or not expected_content_hash
        ):
            raise ValueError("expected_content_hash must be a non-empty string")

        normalized = self.normalize_path(relative_path)
        path = self._path_for_write(normalized)
        exists = path.exists() or path.is_symlink()
        if path.is_symlink():
            raise ValueError("project file path must not be a symlink")
        if exists and not path.is_file():
            raise IsADirectoryError(f"project path is not a regular file: {normalized}")
        if exists and not overwrite:
            raise FileExistsError(f"project file already exists: {normalized}")
        if expected_content_hash is not None:
            if not exists:
                raise FileNotFoundError(f"project file not found: {normalized}")
            current_hash = self._content_hash(path.read_bytes())
            if current_hash != expected_content_hash:
                raise ProjectFilesystemConflictError("project file changed")

        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=".knoggin-",
            suffix=".tmp",
            dir=path.parent,
        )
        try:
            with os.fdopen(file_descriptor, "wb") as temporary_file:
                temporary_file.write(content)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_name, path)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise

        return ProjectFile(
            relative_path=normalized,
            size_bytes=len(content),
            content_hash=self._content_hash(content),
        )

    def delete_file(
        self,
        relative_path: str,
        *,
        expected_content_hash: str | None = None,
    ) -> ProjectFile:
        """Remove one regular file, optionally only when its hash is current."""
        path = self._path_for_read(relative_path)
        content = path.read_bytes()
        content_hash = self._content_hash(content)
        if expected_content_hash is not None and content_hash != expected_content_hash:
            raise ProjectFilesystemConflictError("project file changed")
        path.unlink()
        return ProjectFile(
            relative_path=self.normalize_path(relative_path),
            size_bytes=len(content),
            content_hash=content_hash,
        )

    def move_file(
        self,
        source_path: str,
        destination_path: str,
        *,
        expected_content_hash: str | None = None,
    ) -> ProjectFile:
        """Atomically move a regular project file to an unused relative path."""
        source = self._path_for_read(source_path)
        content = source.read_bytes()
        content_hash = self._content_hash(content)
        if expected_content_hash is not None and content_hash != expected_content_hash:
            raise ProjectFilesystemConflictError("project file changed")
        destination_normalized = self.normalize_path(destination_path)
        destination = self._path_for_write(destination_normalized)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(
                f"project file already exists: {destination_normalized}"
            )
        os.replace(source, destination)
        return ProjectFile(
            relative_path=destination_normalized,
            size_bytes=len(content),
            content_hash=content_hash,
        )

    def create_folder(self, relative_path: str) -> str:
        """Create an empty project-relative directory without accepting links."""
        normalized = self.normalize_path(relative_path)
        self.ensure_root()
        folder = self._root.joinpath(*PurePosixPath(normalized).parts)
        self._ensure_real_parent_directories(folder.parent)
        if folder.exists() or folder.is_symlink():
            if folder.is_symlink() or not folder.is_dir():
                raise FileExistsError(f"project path already exists: {normalized}")
            return normalized
        folder.mkdir()
        return normalized

    def iter_files(self, *, limit: int | None = None) -> Iterator[ProjectFile]:
        """Yield regular project files in stable path order without following links."""
        for path in self.iter_paths(limit=limit):
            content = self.read_bytes(path.relative_path)
            yield ProjectFile(
                relative_path=path.relative_path,
                size_bytes=len(content),
                content_hash=self._content_hash(content),
            )

    def iter_paths(self, *, limit: int | None = None) -> Iterator[ProjectFilePath]:
        """Yield regular project paths in stable order without following links."""
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit < 1
        ):
            raise ValueError("limit must be a positive integer")
        if not self._root.exists():
            return
        self.ensure_root()

        files: list[ProjectFilePath] = []
        pending = [self._root]
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as scan:
                entries = sorted(scan, key=lambda entry: entry.name)
            directories: list[Path] = []
            for entry in entries:
                if entry.is_symlink():
                    continue
                entry_path = Path(entry.path)
                if entry.is_dir(follow_symlinks=False):
                    directories.append(entry_path)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
                relative_path = entry_path.relative_to(self._root).as_posix()
                files.append(
                    ProjectFilePath(
                        relative_path=relative_path,
                        size_bytes=entry.stat(follow_symlinks=False).st_size,
                    )
                )
            pending.extend(reversed(directories))
        for entry in sorted(files, key=lambda item: item.relative_path)[:limit]:
            yield entry

    def _path_for_read(self, relative_path: str) -> Path:
        normalized = self.normalize_path(relative_path)
        self.ensure_root()
        path = self._root.joinpath(*PurePosixPath(normalized).parts)
        self._assert_no_symlink_components(path)
        if not path.is_file():
            raise FileNotFoundError(f"project file not found: {normalized}")
        return path

    def _path_for_write(self, normalized_path: str) -> Path:
        self.ensure_root()
        path = self._root.joinpath(*PurePosixPath(normalized_path).parts)
        self._ensure_real_parent_directories(path.parent)
        return path

    def _ensure_real_parent_directories(self, parent: Path) -> None:
        relative_parent = parent.relative_to(self._root)
        current = self._root
        for part in relative_parent.parts:
            current = current / part
            if current.exists() or current.is_symlink():
                if current.is_symlink() or not current.is_dir():
                    raise ValueError("project path contains a non-directory or symlink")
                continue
            current.mkdir()

    def _assert_no_symlink_components(self, path: Path) -> None:
        relative_path = path.relative_to(self._root)
        current = self._root
        for part in relative_path.parts:
            current = current / part
            if current.is_symlink():
                raise ValueError("project path must not traverse a symlink")

    @staticmethod
    def _content_hash(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()


class ProjectFilesystemFactory:
    """Create one confined filesystem boundary for each local project root."""

    def __init__(self, library_root: Path | str) -> None:
        self._library_root = Path(library_root).expanduser().resolve()

    @property
    def library_root(self) -> Path:
        return self._library_root

    def for_project(self, project_id: str) -> ProjectFilesystem:
        if (
            not isinstance(project_id, str)
            or not project_id.strip()
            or any(separator in project_id for separator in ("/", "\\", "\x00"))
        ):
            raise ValueError("project_id must be a single path component")
        if self._library_root.is_symlink():
            raise ValueError("project library root must not be a symlink")
        return ProjectFilesystem(self._library_root / project_id)
