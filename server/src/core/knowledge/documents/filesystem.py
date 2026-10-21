"""Confined native filesystem access for one Knoggin project."""

from __future__ import annotations

import errno
import hashlib
import os
import stat
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


def _opened_windows_path(descriptor: int) -> str:
    """Resolve the opened handle, rather than checking the path a second time."""
    import ctypes
    import msvcrt

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_path = kernel32.GetFinalPathNameByHandleW
    get_path.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32]
    get_path.restype = ctypes.c_uint32
    handle = msvcrt.get_osfhandle(descriptor)
    buffer = ctypes.create_unicode_buffer(32768)
    length = get_path(handle, buffer, len(buffer), 0)
    if length == 0 or length >= len(buffer):
        raise OSError(ctypes.get_last_error(), "Could not resolve opened project file")
    result = buffer.value
    return result[4:] if result.startswith("\\\\?\\") else result


def _open_posix_file_without_links(path: Path) -> int:
    """Walk from the filesystem root using pinned directory descriptors."""
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory = os.open(path.anchor, directory_flags)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, directory_flags, dir_fd=directory)
            os.close(directory)
            directory = child
        return os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    finally:
        os.close(directory)


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
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                open_directory_chain,
            )
        else:
            from core.knowledge.documents.io.posix_handle_io import open_directory_chain

        with open_directory_chain(self._root, create=True):
            pass

    def _lock_key(self) -> str:
        return os.path.normcase(os.path.abspath(self._root))

    def acquire_mutation_lease(self) -> _MutationLease:
        """Hold the project mutation lock across filesystem and catalog work."""
        root_key = self._lock_key()
        lock_name = hashlib.sha256(os.fsencode(root_key)).hexdigest() + ".lock"
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                open_lock_descriptor,
            )
        else:
            from core.knowledge.documents.io.posix_handle_io import open_lock_descriptor

        descriptor = open_lock_descriptor(self._root.parent, lock_name)
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

    def _pending_admission_marker_name(self, relative_path: str) -> str:
        normalized = self.normalize_path(relative_path)
        root_key = hashlib.sha256(os.fsencode(self._lock_key())).hexdigest()
        path_key = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        return f"{root_key}.upload-{path_key}.pending"

    def mark_pending_admission(
        self, relative_path: str, content_hash: str, *, _lease: _MutationLease
    ) -> None:
        """Journal an upload before its source enters the scanned tree."""
        if len(content_hash) != 64 or any(char not in "0123456789abcdef" for char in content_hash):
            raise ValueError("content_hash must be a SHA-256 hex digest")
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                delete_pending_marker,
                project_source_exists,
                read_pending_marker,
                write_pending_marker,
            )
        else:
            from core.knowledge.documents.io.posix_handle_io import (
                delete_pending_marker,
                project_source_exists,
                read_pending_marker,
                write_pending_marker,
            )

        with self._mutation_lock(_lease) as lease:
            marker_name = self._pending_admission_marker_name(relative_path)
            if read_pending_marker(self._root.parent, marker_name) is not None:
                if project_source_exists(self._root, self.normalize_path(relative_path)):
                    raise FileExistsError(f"project file already exists: {relative_path}")
                delete_pending_marker(self._root.parent, marker_name)
            write_pending_marker(self._root.parent, marker_name, content_hash)
            self._bump_mutation_generation(lease)

    def pending_admission_hash(self, relative_path: str) -> str | None:
        """Return the failed/pending upload hash without reading source bytes."""
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                read_pending_marker,
            )
        else:
            from core.knowledge.documents.io.posix_handle_io import read_pending_marker

        return read_pending_marker(
            self._root.parent, self._pending_admission_marker_name(relative_path)
        )

    def clear_pending_admission(
        self, relative_path: str, *, _lease: _MutationLease
    ) -> None:
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                delete_pending_marker,
            )
        else:
            from core.knowledge.documents.io.posix_handle_io import (
                delete_pending_marker,
            )

        with self._mutation_lock(_lease) as lease:
            if delete_pending_marker(
                self._root.parent, self._pending_admission_marker_name(relative_path)
            ):
                self._bump_mutation_generation(lease)

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
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = (
            os.open(path, flags)
            if os.name == "nt"
            else _open_posix_file_without_links(path)
        )
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or _is_link_or_reparse_point(metadata):
                raise ValueError("project file must be a regular file")
            if os.name == "nt":
                actual = os.path.normcase(os.path.abspath(_opened_windows_path(descriptor)))
                expected_root = os.path.normcase(os.path.abspath(self._root))
                if os.path.commonpath((actual, expected_root)) != expected_root:
                    raise ValueError("project file resolved outside its root")
            if metadata.st_size > max_bytes:
                raise ValueError(f"file exceeds the {max_bytes}-byte read limit")
            with os.fdopen(descriptor, "rb", closefd=False) as opened:
                content = opened.read(max_bytes + 1)
            if len(content) > max_bytes:
                raise ValueError(f"file exceeds the {max_bytes}-byte read limit")
            return content
        finally:
            os.close(descriptor)

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
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                WindowsFileConflictError,
                write_project_file,
            )

            self.ensure_root()
            with self._mutation_lock(_lease) as lease:
                try:
                    write_project_file(
                        self._root, normalized, content,
                        overwrite=overwrite, expected_hash=expected_content_hash,
                    )
                except WindowsFileConflictError as exc:
                    raise ProjectFilesystemConflictError(str(exc)) from exc
                self._bump_mutation_generation(lease)
            return ProjectFile(
                relative_path=normalized,
                size_bytes=len(content),
                content_hash=self._content_hash(content),
            )
        from core.knowledge.documents.io.posix_handle_io import (
            PosixFileConflictError,
            write_project_file,
        )

        self.ensure_root()
        with self._mutation_lock(_lease) as lease:
            try:
                write_project_file(
                    self._root, normalized, content,
                    overwrite=overwrite, expected_hash=expected_content_hash,
                )
            except PosixFileConflictError as exc:
                raise ProjectFilesystemConflictError(str(exc)) from exc
            self._bump_mutation_generation(lease)
        return ProjectFile(
            relative_path=normalized,
            size_bytes=len(content), content_hash=self._content_hash(content),
        )

    def delete_file(
        self,
        relative_path: str,
        *,
        expected_content_hash: str | None = None,
        _lease: _MutationLease | None = None,
    ) -> ProjectFile:
        """Remove one regular file, optionally only when its hash is current."""
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                WindowsFileConflictError,
                delete_project_file,
            )

            with self._mutation_lock(_lease) as lease:
                try:
                    content = delete_project_file(
                        self._root, self.normalize_path(relative_path),
                        expected_hash=expected_content_hash,
                    )
                except WindowsFileConflictError as exc:
                    raise ProjectFilesystemConflictError(str(exc)) from exc
                self._bump_mutation_generation(lease)
            return ProjectFile(
                relative_path=self.normalize_path(relative_path),
                size_bytes=len(content), content_hash=self._content_hash(content),
            )
        from core.knowledge.documents.io.posix_handle_io import (
            PosixFileConflictError,
            delete_project_file,
        )

        normalized = self.normalize_path(relative_path)
        with self._mutation_lock(_lease) as lease:
            try:
                content = delete_project_file(
                    self._root, normalized, expected_hash=expected_content_hash,
                )
            except PosixFileConflictError as exc:
                raise ProjectFilesystemConflictError(str(exc)) from exc
            self._bump_mutation_generation(lease)
        return ProjectFile(
            relative_path=normalized,
            size_bytes=len(content), content_hash=self._content_hash(content),
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
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                WindowsFileConflictError,
                move_project_file,
            )

            with self._mutation_lock(_lease) as lease:
                try:
                    content = move_project_file(
                        self._root, self.normalize_path(source_path),
                        destination_normalized, expected_hash=expected_content_hash,
                    )
                except WindowsFileConflictError as exc:
                    raise ProjectFilesystemConflictError(str(exc)) from exc
                self._bump_mutation_generation(lease)
            return ProjectFile(
                relative_path=destination_normalized,
                size_bytes=len(content), content_hash=self._content_hash(content),
            )
        from core.knowledge.documents.io.posix_handle_io import (
            PosixFileConflictError,
            move_project_file,
        )

        with self._mutation_lock(_lease) as lease:
            try:
                content = move_project_file(
                    self._root, self.normalize_path(source_path), destination_normalized,
                    expected_hash=expected_content_hash,
                )
            except PosixFileConflictError as exc:
                raise ProjectFilesystemConflictError(str(exc)) from exc
            self._bump_mutation_generation(lease)
        return ProjectFile(
            relative_path=destination_normalized,
            size_bytes=len(content), content_hash=self._content_hash(content),
        )

    def create_folder(self, relative_path: str) -> str:
        """Create an empty project-relative directory without accepting links."""
        normalized = self.normalize_path(relative_path)
        self.ensure_root()
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                create_project_folder,
            )

            create_project_folder(self._root, normalized)
            return normalized
        from core.knowledge.documents.io.posix_handle_io import create_project_folder

        create_project_folder(self._root, normalized)
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
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import iter_project_paths

            if not self._root.exists():
                return
            for index, (relative_path, size_bytes, modified_ns) in enumerate(
                iter_project_paths(self._root), start=1
            ):
                yield ProjectFilePath(relative_path, size_bytes, modified_ns)
                if limit is not None and index >= limit:
                    return
            return
        from core.knowledge.documents.io.posix_handle_io import iter_project_paths

        if not self._root.exists():
            return
        for index, (relative_path, size_bytes, modified_ns) in enumerate(
            iter_project_paths(self._root), start=1
        ):
            yield ProjectFilePath(relative_path, size_bytes, modified_ns)
            if limit is not None and index >= limit:
                return

    def _path_for_read(self, relative_path: str) -> Path:
        normalized = self.normalize_path(relative_path)
        self.ensure_root()
        path = self._root.joinpath(*PurePosixPath(normalized).parts)
        self._assert_no_symlink_components(path)
        if not path.is_file():
            raise FileNotFoundError(f"project file not found: {normalized}")
        return path

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

    def remove_project_directory(
        self, project_id: str, *, _lease: _MutationLease | None = None
    ) -> bool:
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
            metadata = None
        # A linked root cannot be opened as a ProjectFilesystem, and normal
        # file mutations reject it. Unlink that direct child without traversal.
        if metadata is not None and (
            self._is_windows_reparse_point(metadata)
            or root.is_symlink()
            or not stat.S_ISDIR(metadata.st_mode)
        ):
            removed = self._remove_project_directory_locked(
                project_id, expected_file_id=metadata.st_ino
            )
            self._remove_pending_admission_markers(root)
            return removed
        filesystem = self.for_project(project_id)
        with filesystem._mutation_lock(_lease):
            removed = self._remove_project_directory_locked(
                project_id, expected_file_id=(metadata.st_ino if metadata else None)
            )
            self._remove_pending_admission_markers(root)
            return removed

    @staticmethod
    def _remove_pending_admission_markers(root: Path) -> None:
        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                delete_pending_markers_for_project,
            )
        else:
            from core.knowledge.documents.io.posix_handle_io import (
                delete_pending_markers_for_project,
            )

        root_key = os.path.normcase(os.path.abspath(root))
        prefix = hashlib.sha256(os.fsencode(root_key)).hexdigest()
        delete_pending_markers_for_project(root.parent, f"{prefix}.upload-")

    def _remove_project_directory_locked(
        self, project_id: str, *, expected_file_id: int | None = None
    ) -> bool:
        """Remove a project tree while its shared mutation lease is held."""

        if os.name == "nt":
            from core.knowledge.documents.io.windows_handle_io import (
                remove_project_tree,
            )
        else:
            from core.knowledge.documents.io.posix_handle_io import remove_project_tree

        return remove_project_tree(
            self._library_root, project_id, expected_file_id=expected_file_id
        )
