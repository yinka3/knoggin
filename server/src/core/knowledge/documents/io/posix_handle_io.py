"""POSIX directory-descriptor operations for confined project files."""

from __future__ import annotations

import errno
import hashlib
import os
import secrets
import stat
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Iterator

_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_FILE_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW
_MAX_FILE_SIZE = 50 * 1024 * 1024


class PosixFileConflictError(Exception):
    """The expected file bytes changed before the mutation."""


def _open_directory(parent: int, name: str, *, create: bool = False) -> int:
    try:
        try:
            return os.open(name, _DIR_FLAGS, dir_fd=parent)
        except FileNotFoundError:
            if not create:
                raise
            try:
                os.mkdir(name, mode=0o700, dir_fd=parent)
            except FileExistsError:
                pass
            return os.open(name, _DIR_FLAGS, dir_fd=parent)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise ValueError("project path contains a symlink or non-directory") from exc
        raise


@contextmanager
def open_directory_chain(path: Path, *, create: bool = False) -> Iterator[int]:
    if not path.is_absolute():
        raise ValueError("project root must be absolute")
    descriptors = [os.open(path.anchor, _DIR_FLAGS)]
    try:
        for part in path.parts[1:]:
            descriptors.append(_open_directory(descriptors[-1], part, create=create))
        yield descriptors[-1]
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


@contextmanager
def open_relative_parent(root: int, relative_path: str, *, create: bool = False) -> Iterator[tuple[int, str]]:
    parts = PurePosixPath(relative_path).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("relative project path is invalid")
    descriptors: list[int] = []
    try:
        for part in parts[:-1]:
            descriptors.append(_open_directory(
                descriptors[-1] if descriptors else root, part, create=create
            ))
        yield (descriptors[-1] if descriptors else root, parts[-1])
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _assert_parent_at_path(descriptor: int, expected: Path) -> None:
    try:
        actual = os.stat(expected, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ValueError("project directory moved during file mutation") from exc
    opened = os.fstat(descriptor)
    if not stat.S_ISDIR(actual.st_mode) or (actual.st_dev, actual.st_ino) != (
        opened.st_dev, opened.st_ino
    ):
        raise ValueError("project directory moved during file mutation")


def _open_file(parent: int, name: str, *, access: int = os.O_RDONLY) -> int:
    try:
        descriptor = os.open(name, access | os.O_NOFOLLOW, dir_fd=parent)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise ValueError("project file path must not be a symlink") from exc
        raise
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise ValueError("project file must be a regular file")
    return descriptor


def _read(descriptor: int, max_bytes: int = _MAX_FILE_SIZE) -> bytes:
    result = bytearray()
    while len(result) <= max_bytes:
        chunk = os.read(descriptor, min(1024 * 1024, max_bytes + 1 - len(result)))
        if not chunk:
            break
        result.extend(chunk)
    if len(result) > max_bytes:
        raise ValueError(f"file exceeds the {max_bytes}-byte read limit")
    return bytes(result)


def _write(descriptor: int, content: bytes) -> None:
    view = memoryview(content)
    while view:
        view = view[os.write(descriptor, view):]
    os.fsync(descriptor)


def write_project_file(root: Path, relative_path: str, content: bytes, *, overwrite: bool, expected_hash: str | None) -> None:
    with open_directory_chain(root) as project:
        with open_relative_parent(project, relative_path, create=True) as (parent, name):
            try:
                existing = _open_file(parent, name)
            except FileNotFoundError:
                existing = None
            if existing is not None:
                try:
                    if not overwrite:
                        raise FileExistsError(f"project file already exists: {relative_path}")
                    if expected_hash is not None and hashlib.sha256(_read(existing)).hexdigest() != expected_hash:
                        raise PosixFileConflictError("project file changed")
                finally:
                    os.close(existing)
            elif expected_hash is not None:
                raise FileNotFoundError(relative_path)
            temporary_name = ".knoggin-" + secrets.token_hex(16) + ".tmp"
            temporary = os.open(
                temporary_name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW,
                0o600, dir_fd=parent,
            )
            try:
                _assert_parent_at_path(parent, root.joinpath(*PurePosixPath(relative_path).parts[:-1]))
                _write(temporary, content)
                if overwrite:
                    os.replace(temporary_name, name, src_dir_fd=parent, dst_dir_fd=parent)
                else:
                    try:
                        os.link(temporary_name, name, src_dir_fd=parent, dst_dir_fd=parent,
                                follow_symlinks=False)
                    except FileExistsError as exc:
                        raise FileExistsError(
                            f"project file already exists: {relative_path}"
                        ) from exc
                    os.unlink(temporary_name, dir_fd=parent)
            except BaseException:
                try:
                    os.unlink(temporary_name, dir_fd=parent)
                except FileNotFoundError:
                    pass
                raise
            finally:
                os.close(temporary)


def delete_project_file(root: Path, relative_path: str, *, expected_hash: str | None) -> bytes:
    with open_directory_chain(root) as project:
        with open_relative_parent(project, relative_path) as (parent, name):
            target = _open_file(parent, name)
            try:
                _assert_parent_at_path(parent, root.joinpath(*PurePosixPath(relative_path).parts[:-1]))
                content = _read(target)
                if expected_hash is not None and hashlib.sha256(content).hexdigest() != expected_hash:
                    raise PosixFileConflictError("project file changed")
                _assert_parent_at_path(parent, root.joinpath(*PurePosixPath(relative_path).parts[:-1]))
                os.unlink(name, dir_fd=parent)
                return content
            finally:
                os.close(target)


def move_project_file(root: Path, source_path: str, destination_path: str, *, expected_hash: str | None) -> bytes:
    with open_directory_chain(root) as project:
        with open_relative_parent(project, source_path) as (source_parent, source_name):
            source = _open_file(source_parent, source_name)
            try:
                content = _read(source)
                if expected_hash is not None and hashlib.sha256(content).hexdigest() != expected_hash:
                    raise PosixFileConflictError("project file changed")
                with open_relative_parent(project, destination_path, create=True) as (target_parent, target_name):
                    _assert_parent_at_path(source_parent, root.joinpath(*PurePosixPath(source_path).parts[:-1]))
                    _assert_parent_at_path(target_parent, root.joinpath(*PurePosixPath(destination_path).parts[:-1]))
                    try:
                        os.link(source_name, target_name, src_dir_fd=source_parent,
                                dst_dir_fd=target_parent, follow_symlinks=False)
                    except FileExistsError as exc:
                        raise FileExistsError(
                            f"project file already exists: {destination_path}"
                        ) from exc
                    os.unlink(source_name, dir_fd=source_parent)
                return content
            finally:
                os.close(source)


def create_project_folder(root: Path, relative_path: str) -> None:
    with open_directory_chain(root) as project:
        with open_relative_parent(project, relative_path, create=True) as (parent, name):
            try:
                os.mkdir(name, dir_fd=parent)
                created = True
            except FileExistsError:
                created = False
            try:
                child = _open_directory(parent, name)
            except OSError:
                if created:
                    os.rmdir(name, dir_fd=parent)
                raise
            try:
                _assert_parent_at_path(parent, root.joinpath(*PurePosixPath(relative_path).parts[:-1]))
            except BaseException:
                if created:
                    os.rmdir(name, dir_fd=parent)
                raise
            finally:
                os.close(child)


def _remove_tree_handle(directory: int) -> None:
    for name in os.listdir(directory):
        try:
            expected = os.stat(name, dir_fd=directory, follow_symlinks=False)
            child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
        except FileNotFoundError:
            continue
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                if not stat.S_ISLNK(expected.st_mode):
                    raise RuntimeError("project directory changed during file cleanup") from exc
                os.unlink(name, dir_fd=directory)
                continue
            raise
        try:
            current = os.fstat(child)
            if (expected.st_dev, expected.st_ino) != (current.st_dev, current.st_ino):
                raise RuntimeError("project directory changed during file cleanup")
            if stat.S_ISDIR(current.st_mode):
                _remove_tree_handle(child)
                os.rmdir(name, dir_fd=directory)
            else:
                os.unlink(name, dir_fd=directory)
        finally:
            os.close(child)


def remove_project_tree(
    library_root: Path, project_id: str, *, expected_file_id: int | None = None
) -> bool:
    with open_directory_chain(library_root) as library:
        try:
            metadata = os.stat(project_id, dir_fd=library, follow_symlinks=False)
        except FileNotFoundError:
            return False
        if expected_file_id is not None and metadata.st_ino != expected_file_id:
            raise RuntimeError("project directory changed during file cleanup")
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            os.unlink(project_id, dir_fd=library)
            return True
        try:
            root = os.open(project_id, _DIR_FLAGS, dir_fd=library)
        except FileNotFoundError:
            return False
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise RuntimeError("project directory changed during file cleanup") from exc
            raise
        try:
            if expected_file_id is not None and os.fstat(root).st_ino != expected_file_id:
                raise RuntimeError("project directory changed during file cleanup")
            _remove_tree_handle(root)
            os.rmdir(project_id, dir_fd=library)
        finally:
            os.close(root)
        return True


def iter_project_paths(root: Path) -> Iterator[tuple[str, int, int]]:
    def walk(directory: int, prefix: str) -> Iterator[tuple[str, int, int]]:
        entries = []
        for name in os.listdir(directory):
            metadata = os.stat(name, dir_fd=directory, follow_symlinks=False)
            entries.append((name, metadata))
        for name, metadata in sorted(
            entries, key=lambda item: item[0] + ("/" if stat.S_ISDIR(item[1].st_mode) else "")
        ):
            if stat.S_ISLNK(metadata.st_mode):
                continue
            try:
                child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
            except FileNotFoundError:
                continue
            try:
                current = os.fstat(child)
                if (metadata.st_dev, metadata.st_ino) != (current.st_dev, current.st_ino):
                    raise RuntimeError("project directory changed during file scan")
                if stat.S_ISDIR(current.st_mode):
                    yield from walk(child, prefix + name + "/")
                elif stat.S_ISREG(current.st_mode):
                    yield prefix + name, current.st_size, current.st_mtime_ns
            finally:
                os.close(child)

    with open_directory_chain(root) as project:
        yield from walk(project, "")


@contextmanager
def open_lock_directory(library_root: Path, *, create: bool = False) -> Iterator[int]:
    with open_directory_chain(library_root, create=create) as library:
        lock_directory = _open_directory(library, ".knoggin-file-locks", create=create)
        try:
            _assert_parent_at_path(lock_directory, library_root / ".knoggin-file-locks")
            yield lock_directory
        finally:
            os.close(lock_directory)


def open_lock_descriptor(library_root: Path, lock_name: str) -> int:
    with open_lock_directory(library_root, create=True) as directory:
        descriptor = os.open(
            lock_name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
            0o600, dir_fd=directory,
        )
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ValueError("project file lock must be a regular file")
            _assert_parent_at_path(directory, library_root / ".knoggin-file-locks")
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise


def project_source_exists(root: Path, relative_path: str) -> bool:
    with open_directory_chain(root) as project:
        try:
            with open_relative_parent(project, relative_path) as (parent, name):
                os.stat(name, dir_fd=parent, follow_symlinks=False)
                return True
        except FileNotFoundError:
            return False


def read_pending_marker(library_root: Path, marker_name: str) -> str | None:
    try:
        with open_lock_directory(library_root) as directory:
            marker = _open_file(directory, marker_name)
            try:
                return _read(marker, 64).decode("ascii")
            finally:
                os.close(marker)
    except FileNotFoundError:
        return None


def write_pending_marker(library_root: Path, marker_name: str, content_hash: str) -> None:
    with open_lock_directory(library_root, create=True) as directory:
        temporary_name = ".knoggin-admission-" + secrets.token_hex(16) + ".tmp"
        temporary = os.open(
            temporary_name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW,
            0o600, dir_fd=directory,
        )
        try:
            _assert_parent_at_path(directory, library_root / ".knoggin-file-locks")
            _write(temporary, content_hash.encode("ascii"))
            os.link(temporary_name, marker_name, src_dir_fd=directory,
                    dst_dir_fd=directory, follow_symlinks=False)
        finally:
            os.close(temporary)
            os.unlink(temporary_name, dir_fd=directory)


def delete_pending_marker(library_root: Path, marker_name: str) -> bool:
    try:
        with open_lock_directory(library_root) as directory:
            marker = _open_file(directory, marker_name)
            try:
                os.unlink(marker_name, dir_fd=directory)
                return True
            finally:
                os.close(marker)
    except FileNotFoundError:
        return False


def delete_pending_markers_for_project(library_root: Path, prefix: str) -> None:
    try:
        with open_lock_directory(library_root) as directory:
            for name in os.listdir(directory):
                if not name.startswith(prefix):
                    continue
                try:
                    marker = _open_file(directory, name)
                except FileNotFoundError:
                    continue
                try:
                    os.unlink(name, dir_fd=directory)
                finally:
                    os.close(marker)
    except FileNotFoundError:
        return
