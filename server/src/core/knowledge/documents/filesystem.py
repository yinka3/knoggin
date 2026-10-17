"""Confined native filesystem access for one Knoggin project."""

from __future__ import annotations

import errno
import hashlib
import os
import shutil
import stat
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterator

from common.exceptions import WorkspaceConflictError
from core.knowledge.documents.constants import MAX_DOCUMENT_SIZE
from core.knowledge.documents.scanning import normalize_relative_path


def _is_link_or_reparse_point(metadata: os.stat_result) -> bool:
    """Treat Windows junctions and other reparse points like symlinks."""
    return stat.S_ISLNK(metadata.st_mode) or bool(
        getattr(metadata, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


def _is_link_or_reparse_path(path: Path) -> bool:
    try:
        return _is_link_or_reparse_point(path.lstat())
    except FileNotFoundError:
        return False


def _assert_real_ancestors(path: Path, *, label: str) -> None:
    """Reject links above a trusted directory, including Windows junctions."""
    for ancestor in path.parents:
        if _is_link_or_reparse_path(ancestor):
            raise ValueError(f"{label} ancestor must be a real directory")


class ProjectFilesystemConflictError(WorkspaceConflictError):
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
    modified_ns: int


@dataclass(slots=True)
class _MutationLease:
    root_key: str
    descriptor: int
    active: bool = True


class ProjectFilesystem:
    """Read and write one project's local directory without escaping its root.

    The caller supplies a trusted absolute project root. Every public path is a
    normalized project-relative path; links inside that root are rejected so
    document and agent operations cannot silently reach elsewhere on the host.
    """

    def __init__(self, project_root: Path | str) -> None:
        root = Path(project_root)
        if not root.is_absolute():
            raise ValueError("project_root must be absolute")
        if _is_link_or_reparse_path(root.parent):
            raise ValueError("project_root parent must be a real directory")
        if _is_link_or_reparse_path(root):
            raise ValueError("project_root must not be a symlink or reparse point")
        _assert_real_ancestors(root.parent, label="project_root")
        self._root = root

    @property
    def root(self) -> Path:
        return self._root

    @staticmethod
    def normalize_path(relative_path: str) -> str:
        return normalize_relative_path(relative_path, relative_path)

    def ensure_root(self) -> None:
        if _is_link_or_reparse_path(self._root.parent):
            raise ValueError("project_root parent must be a real directory")
        if _is_link_or_reparse_path(self._root):
            raise ValueError("project_root must be a real directory")
        _assert_real_ancestors(self._root.parent, label="project_root")
        self._root.mkdir(parents=True, exist_ok=True)
        if _is_link_or_reparse_path(self._root) or not self._root.is_dir():
            raise ValueError("project_root must be a real directory")

    def _lock_key(self) -> str:
        return os.path.normcase(os.path.realpath(self._root))

    def acquire_mutation_lease(self) -> _MutationLease:
        """Hold the project mutation lock across filesystem and catalog work."""
        if _is_link_or_reparse_path(self._root.parent):
            raise ValueError("project_root parent must be a real directory")
        _assert_real_ancestors(self._root.parent, label="project_root")
        # Keep coordination files outside the project tree scanned for documents.
        lock_directory = self._root.parent / ".knoggin-file-locks"
        if _is_link_or_reparse_path(lock_directory):
            raise ValueError("project file lock directory must be a real directory")
        lock_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if _is_link_or_reparse_path(lock_directory) or not lock_directory.is_dir():
            raise ValueError("project file lock directory must be a real directory")
        root_key = self._lock_key()
        lock_name = hashlib.sha256(os.fsencode(root_key)).hexdigest() + ".lock"
        lock_path = lock_directory / lock_name
        if _is_link_or_reparse_path(lock_path):
            raise ValueError("project file lock must not be a symlink or reparse point")
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(lock_path, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ValueError("project file lock must be a regular file")
            self._acquire_file_lock(descriptor)
        except BaseException:
            os.close(descriptor)
            raise
        return _MutationLease(root_key=root_key, descriptor=descriptor)

    def release_mutation_lease(self, lease: _MutationLease) -> None:
        if not isinstance(lease, _MutationLease) or not lease.active:
            raise ValueError("project mutation lease is not active")
        lease.active = False
        try:
            self._release_file_lock(lease.descriptor)
        finally:
            os.close(lease.descriptor)

    def _require_mutation_lease(self, lease: _MutationLease) -> None:
        if not isinstance(lease, _MutationLease) or not lease.active:
            raise ValueError("project mutation lease is not active")
        if lease.root_key != self._lock_key():
            raise ValueError("project mutation lease belongs to another root")

    @contextmanager
    def _mutation_lock(
        self, lease: _MutationLease | None = None
    ) -> Iterator[_MutationLease]:
        """Serialize project mutations unless a caller already owns the lease."""
        if lease is not None:
            self._require_mutation_lease(lease)
            yield lease
            return
        owned = self.acquire_mutation_lease()
        try:
            yield owned
        finally:
            self.release_mutation_lease(owned)

    @staticmethod
    def _read_mutation_generation(descriptor: int) -> int:
        os.lseek(descriptor, 1, os.SEEK_SET)
        return int.from_bytes(os.read(descriptor, 8).ljust(8, b"\0"), "big")

    @classmethod
    def _bump_mutation_generation(cls, lease: _MutationLease) -> None:
        next_generation = (cls._read_mutation_generation(lease.descriptor) + 1) % (1 << 64)
        os.lseek(lease.descriptor, 1, os.SEEK_SET)
        if os.write(lease.descriptor, next_generation.to_bytes(8, "big")) != 8:
            raise OSError("could not record project file mutation")

    def get_mutation_generation(self, *, _lease: _MutationLease | None = None) -> int:
        """Return the persistent generation of Knoggin-managed file changes."""
        with self._mutation_lock(_lease) as lease:
            return self._read_mutation_generation(lease.descriptor)

    @staticmethod
    def _acquire_file_lock(descriptor: int) -> None:
        deadline = time.monotonic() + 30
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    os.lseek(descriptor, 0, os.SEEK_SET)
                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError("timed out waiting for project file mutation") from exc
                time.sleep(0.01)

    @staticmethod
    def _release_file_lock(descriptor: int) -> None:
        if os.name == "nt":
            import msvcrt

            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_UN)

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

    def read_file(
        self,
        relative_path: str,
        *,
        max_bytes: int = MAX_DOCUMENT_SIZE,
    ) -> ProjectFile:
        """Read and fingerprint one bounded regular project file."""
        content = self.read_bytes(relative_path, max_bytes=max_bytes)
        return ProjectFile(
            relative_path=self.normalize_path(relative_path),
            size_bytes=len(content),
            content_hash=self._content_hash(content),
        )

    def write_bytes(
        self,
        relative_path: str,
        content: bytes,
        *,
        overwrite: bool = False,
        expected_content_hash: str | None = None,
        _allow_empty: bool = False,
        _lease: _MutationLease | None = None,
    ) -> ProjectFile:
        """Atomically create or replace one project file.

        ``expected_content_hash`` provides the local optimistic-concurrency
        check used by agent editing operations.  A temporary sibling file plus
        ``os.replace`` prevents readers from observing partially written bytes.
        """
        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")
        if not content and not _allow_empty:
            raise ValueError("project file content must not be empty")
        if len(content) > MAX_DOCUMENT_SIZE:
            raise ValueError("document exceeds the 50 MB size limit")
        if expected_content_hash is not None and (
            not isinstance(expected_content_hash, str) or not expected_content_hash
        ):
            raise ValueError("expected_content_hash must be a non-empty string")

        normalized = self.normalize_path(relative_path)
        path = self._path_for_write(normalized)
        self._check_write_precondition(
            path,
            normalized,
            overwrite=overwrite,
            expected_content_hash=expected_content_hash,
        )

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
            with self._mutation_lock(_lease) as lease:
                path = self._path_for_write(normalized)
                self._check_write_precondition(
                    path,
                    normalized,
                    overwrite=overwrite,
                    expected_content_hash=expected_content_hash,
                )
                if overwrite:
                    os.replace(temporary_name, path)
                    self._bump_mutation_generation(lease)
                else:
                    # Publish complete bytes only if the destination is absent.
                    os.link(temporary_name, path)
                    self._bump_mutation_generation(lease)
                    os.unlink(temporary_name)
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

    def _check_write_precondition(
        self,
        path: Path,
        normalized: str,
        *,
        overwrite: bool,
        expected_content_hash: str | None,
    ) -> None:
        is_link = _is_link_or_reparse_path(path)
        exists = path.exists() or is_link
        if is_link:
            raise ValueError("project file path must not be a symlink or reparse point")
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

    def delete_file(
        self,
        relative_path: str,
        *,
        expected_content_hash: str | None = None,
        _lease: _MutationLease | None = None,
    ) -> ProjectFile:
        """Remove one regular file, optionally only when its hash is current."""
        with self._mutation_lock(_lease) as lease:
            path = self._path_for_read(relative_path)
            content = path.read_bytes()
            content_hash = self._content_hash(content)
            if expected_content_hash is not None and content_hash != expected_content_hash:
                raise ProjectFilesystemConflictError("project file changed")
            path.unlink()
            self._bump_mutation_generation(lease)
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
        _lease: _MutationLease | None = None,
    ) -> ProjectFile:
        """Atomically move a regular project file to an unused relative path."""
        destination_normalized = self.normalize_path(destination_path)
        with self._mutation_lock(_lease) as lease:
            source = self._path_for_read(source_path)
            content = source.read_bytes()
            content_hash = self._content_hash(content)
            if expected_content_hash is not None and content_hash != expected_content_hash:
                raise ProjectFilesystemConflictError("project file changed")
            destination = self._path_for_write(destination_normalized)
            if destination.exists() or _is_link_or_reparse_path(destination):
                raise FileExistsError(
                    f"project file already exists: {destination_normalized}"
                )
            os.replace(source, destination)
            self._bump_mutation_generation(lease)
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
        is_link = _is_link_or_reparse_path(folder)
        if folder.exists() or is_link:
            if is_link or not folder.is_dir():
                raise FileExistsError(f"project path already exists: {normalized}")
            return normalized
        folder.mkdir()
        return normalized

    def iter_files(self, *, limit: int | None = None) -> Iterator[ProjectFile]:
        """Yield regular project files in stable path order without following links."""
        for path in self.iter_paths(limit=limit):
            yield self.read_file(path.relative_path)

    def iter_paths(self, *, limit: int | None = None) -> Iterator[ProjectFilePath]:
        """Yield regular project paths in stable order without following links."""
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit < 1
        ):
            raise ValueError("limit must be a positive integer")
        if _is_link_or_reparse_path(self._root):
            raise ValueError("project_root must be a real directory")
        if not self._root.exists():
            return
        self.ensure_root()

        # A directory sorts as "name/" so its descendants keep the same order
        # as sorting complete relative paths (including names like a.txt, a/).
        pending: list[Path | ProjectFilePath] = [self._root]
        yielded = 0
        while pending:
            item = pending.pop()
            if isinstance(item, ProjectFilePath):
                yield item
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
                continue
            with os.scandir(item) as scan:
                entries = list(scan)
            children: list[tuple[str, Path | ProjectFilePath]] = []
            for entry in entries:
                metadata = entry.stat(follow_symlinks=False)
                if _is_link_or_reparse_point(metadata):
                    continue
                entry_path = Path(entry.path)
                if stat.S_ISDIR(metadata.st_mode):
                    children.append((entry.name + "/", entry_path))
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    continue
                relative_path = entry_path.relative_to(self._root).as_posix()
                children.append(
                    (
                        entry.name,
                        ProjectFilePath(
                            relative_path=relative_path,
                            size_bytes=metadata.st_size,
                            modified_ns=metadata.st_mtime_ns,
                        ),
                    )
                )
            pending.extend(
                child for _, child in sorted(children, key=lambda pair: pair[0], reverse=True)
            )

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
            is_link = _is_link_or_reparse_path(current)
            if current.exists() or is_link:
                if is_link or not current.is_dir():
                    raise ValueError(
                        "project path contains a non-directory, symlink, or reparse point"
                    )
                continue
            current.mkdir()

    def _assert_no_symlink_components(self, path: Path) -> None:
        relative_path = path.relative_to(self._root)
        current = self._root
        for part in relative_path.parts:
            current = current / part
            if _is_link_or_reparse_path(current):
                raise ValueError("project path must not traverse a symlink or reparse point")

    @staticmethod
    def _content_hash(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()


class ProjectFilesystemFactory:
    """Create one confined filesystem boundary for each local project root."""

    def __init__(self, library_root: Path | str) -> None:
        configured_root = Path(library_root).expanduser()
        if not configured_root.is_absolute():
            configured_root = Path.cwd() / configured_root
        self._library_root = Path(os.path.abspath(configured_root))

    @property
    def library_root(self) -> Path:
        return self._library_root

    def _project_root(self, project_id: str) -> Path:
        if (
            not isinstance(project_id, str)
            or not project_id.strip()
            or project_id in {".", ".."}
            or any(separator in project_id for separator in ("/", "\\", "\x00"))
        ):
            raise ValueError("project_id must be a single path component")
        if _is_link_or_reparse_path(self._library_root):
            raise ValueError("project library root must not be a symlink or reparse point")
        _assert_real_ancestors(self._library_root, label="project library root")
        return self._library_root / project_id

    def for_project(self, project_id: str) -> ProjectFilesystem:
        return ProjectFilesystem(self._project_root(project_id))

    @staticmethod
    def _is_windows_reparse_point(metadata: os.stat_result) -> bool:
        return bool(
            getattr(metadata, "st_file_attributes", 0)
            & stat.FILE_ATTRIBUTE_REPARSE_POINT
        )

    @classmethod
    def _remove_windows_directory(cls, directory: Path) -> None:
        """Remove a tree without descending into Windows links or junctions."""
        metadata = directory.lstat()
        if cls._is_windows_reparse_point(metadata) or not stat.S_ISDIR(metadata.st_mode):
            raise RuntimeError("project directory changed during file cleanup")
        with os.scandir(directory) as scan:
            entries = list(scan)
        for entry in entries:
            child = Path(entry.path)
            metadata = entry.stat(follow_symlinks=False)
            if cls._is_windows_reparse_point(metadata):
                if child.is_junction():
                    child.rmdir()
                else:
                    child.unlink()
            elif stat.S_ISDIR(metadata.st_mode):
                cls._remove_windows_directory(child)
            else:
                child.unlink()
        directory.rmdir()

    def remove_project_directory(self, project_id: str) -> bool:
        """Remove one direct project child without traversing symlinks.

        A project directory is the complete native-file ownership boundary. A
        missing directory is already clean. A symlink at that direct child is
        unlinked rather than traversed so cleanup cannot reach outside the
        configured project library.
        """

        root = self._project_root(project_id)
        try:
            metadata = root.lstat()
        except FileNotFoundError:
            return False

        if os.name == "nt" and self._is_windows_reparse_point(metadata):
            if root.is_junction():
                root.rmdir()
            else:
                root.unlink()
            return True

        if root.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
            try:
                root.unlink()
            except FileNotFoundError:
                return False
            return True

        if os.name == "nt":
            self._remove_windows_directory(root)
            return True
        if not shutil.rmtree.avoids_symlink_attacks:
            raise RuntimeError("safe project-directory removal is unavailable")
        shutil.rmtree(root)
        return True
