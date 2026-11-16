"""Windows handle-relative filesystem primitives for project file confinement.

This module is imported only on Windows. Each path component is opened relative
to an already verified directory handle, so a concurrent path replacement does
not redirect later operations through a junction.
"""

from __future__ import annotations

import ctypes
import os
import secrets
import stat
import time
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Callable, Iterator

from common.document_limits import MAX_DOCUMENT_SIZE


class _UnicodeString(ctypes.Structure):
    _fields_ = [
        ("Length", ctypes.c_ushort),
        ("MaximumLength", ctypes.c_ushort),
        ("Buffer", ctypes.c_wchar_p),
    ]


class _ObjectAttributes(ctypes.Structure):
    _fields_ = [
        ("Length", ctypes.c_ulong),
        ("RootDirectory", ctypes.c_void_p),
        ("ObjectName", ctypes.POINTER(_UnicodeString)),
        ("Attributes", ctypes.c_ulong),
        ("SecurityDescriptor", ctypes.c_void_p),
        ("SecurityQualityOfService", ctypes.c_void_p),
    ]


class _IoStatusBlock(ctypes.Structure):
    _fields_ = [("Status", ctypes.c_void_p), ("Information", ctypes.c_void_p)]


class _FileAttributeTagInfo(ctypes.Structure):
    _fields_ = [("attributes", ctypes.c_uint32), ("reparse_tag", ctypes.c_uint32)]


class _FileIdInfo(ctypes.Structure):
    _fields_ = [
        ("volume_serial", ctypes.c_uint64),
        ("file_id", ctypes.c_ubyte * 16),
    ]


class _FileBasicInfo(ctypes.Structure):
    _fields_ = [
        ("creation_time", ctypes.c_int64),
        ("last_access_time", ctypes.c_int64),
        ("last_write_time", ctypes.c_int64),
        ("change_time", ctypes.c_int64),
        ("attributes", ctypes.c_uint32),
    ]


class _FileStandardInfo(ctypes.Structure):
    _fields_ = [
        ("allocation_size", ctypes.c_int64),
        ("end_of_file", ctypes.c_int64),
        ("number_of_links", ctypes.c_uint32),
        ("delete_pending", ctypes.c_int32),
        ("directory", ctypes.c_int32),
    ]


_ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
_kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
_nt_create_file = _ntdll.NtCreateFile
_nt_create_file.argtypes = [
    ctypes.POINTER(ctypes.c_void_p), ctypes.c_uint32,
    ctypes.POINTER(_ObjectAttributes), ctypes.POINTER(_IoStatusBlock),
    ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_uint32,
    ctypes.c_uint32, ctypes.c_void_p, ctypes.c_uint32,
]
_nt_create_file.restype = ctypes.c_long
_rtl_error = _ntdll.RtlNtStatusToDosError
_rtl_error.argtypes = [ctypes.c_long]
_rtl_error.restype = ctypes.c_ulong
_close = _kernel32.CloseHandle
_close.argtypes = [ctypes.c_void_p]
_close.restype = ctypes.c_int
_get_info = _kernel32.GetFileInformationByHandleEx
_get_info.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
_get_info.restype = ctypes.c_int
_get_final_path = _kernel32.GetFinalPathNameByHandleW
_get_final_path.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32]
_get_final_path.restype = ctypes.c_uint32
_read_file = _kernel32.ReadFile
_read_file.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32,
                       ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p]
_read_file.restype = ctypes.c_int
_write_file = _kernel32.WriteFile
_write_file.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32,
                        ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p]
_write_file.restype = ctypes.c_int
_flush_file = _kernel32.FlushFileBuffers
_flush_file.argtypes = [ctypes.c_void_p]
_flush_file.restype = ctypes.c_int
_set_information = _kernel32.SetFileInformationByHandle
_set_information.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
_set_information.restype = ctypes.c_int
_nt_set_information = _ntdll.NtSetInformationFile
_nt_set_information.argtypes = [ctypes.c_void_p, ctypes.POINTER(_IoStatusBlock),
                                 ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int]
_nt_set_information.restype = ctypes.c_long

_SYNCHRONIZE = 0x00100000
_READ_ATTRIBUTES = 0x80
_READ_DATA = 0x1
_WRITE_DATA = 0x2
_DELETE = 0x10000
_FILE_TRAVERSE = 0x20
_SHARE_ALL = 0x7
_OPEN = 1
_CREATE = 2
_DIRECTORY = 0x1
_NON_DIRECTORY = 0x40
_SYNCHRONOUS = 0x20
_OPEN_REPARSE = 0x00200000
_REPARSE_ATTRIBUTE = 0x400
_DIRECTORY_ATTRIBUTE = 0x10


class WindowsFileConflictError(Exception):
    """The expected file bytes changed before the mutation."""


def _open_child(
    parent: int | None, name: str, *, directory: bool | None, create: bool = False,
    open_if: bool = False,
    access: int = _READ_ATTRIBUTES,
    created_out: list[bool] | None = None,
    attributes_out: list[int] | None = None,
    allow_reparse: bool = False,
    delete_on_close: bool = False,
) -> int:
    if parent is not None and (not name or "\\" in name or "/" in name):
        raise ValueError("child name must be one path component")
    encoded = name.encode("utf-16-le")
    name_buffer = ctypes.create_unicode_buffer(name)
    unicode_name = _UnicodeString(
        len(encoded), len(encoded) + 2,
        ctypes.cast(name_buffer, ctypes.c_wchar_p),
    )
    attributes = _ObjectAttributes(
        ctypes.sizeof(_ObjectAttributes), parent,
        ctypes.pointer(unicode_name), 0x40, None, None,
    )
    handle = ctypes.c_void_p()
    status_block = _IoStatusBlock()
    deadline = time.monotonic() + 1
    while True:
        status = _nt_create_file(
            ctypes.byref(handle), access | _SYNCHRONIZE,
            ctypes.byref(attributes), ctypes.byref(status_block), None,
            0x80, _SHARE_ALL,
            3 if open_if or (create and directory) else _CREATE if create else _OPEN,
            _SYNCHRONOUS | _OPEN_REPARSE | (
                _DIRECTORY if directory is True else _NON_DIRECTORY if directory is False else 0
            ) | (0x1000 if delete_on_close else 0),
            None, 0,
        )
        if status >= 0 or _rtl_error(status) != 32 or time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    if status < 0:
        error = _rtl_error(status)
        if error in {2, 3}:
            raise FileNotFoundError(error, name)
        if error in {80, 183}:
            raise FileExistsError(error, name)
        raise ctypes.WinError(error)
    info = _FileAttributeTagInfo()
    if not _get_info(handle, 9, ctypes.byref(info), ctypes.sizeof(info)):
        error = ctypes.get_last_error()
        _close(handle)
        raise OSError(error, "Could not inspect opened project component")
    if (
        (info.attributes & _REPARSE_ATTRIBUTE and not allow_reparse)
        or (directory is not None and bool(info.attributes & _DIRECTORY_ATTRIBUTE) != directory)
    ):
        _close(handle)
        raise ValueError(
            "project path contains a symlink or reparse point, or is not a real directory or file"
        )
    if created_out is not None:
        created_out.append(status_block.Information == 2)
    if attributes_out is not None:
        attributes_out.append(info.attributes)
    return int(handle.value)


@contextmanager
def open_directory_chain(path: Path, *, create: bool = False) -> Iterator[int]:
    """Open a local absolute directory one component at a time."""
    if not path.is_absolute() or not path.drive.endswith(":"):
        raise ValueError("Windows project path must be on a local drive")
    handles: list[int] = []
    try:
        handles.append(_open_child(None, "\\??\\" + path.anchor, directory=True))
        for part in path.parts[1:]:
            handles.append(_open_child(handles[-1], part, directory=True, create=create))
        yield handles[-1]
    finally:
        for handle in reversed(handles):
            _close(handle)


@contextmanager
def open_relative_parent(root_handle: int, relative_path: str, *, create: bool = False) -> Iterator[tuple[int, str]]:
    parts = PurePosixPath(relative_path).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("relative project path is invalid")
    handles: list[int] = []
    try:
        for part in parts[:-1]:
            handles.append(_open_child(
                handles[-1] if handles else root_handle,
                part, directory=True, create=create,
                access=_READ_ATTRIBUTES | _FILE_TRAVERSE,
            ))
        yield (handles[-1] if handles else root_handle, parts[-1])
    finally:
        for handle in reversed(handles):
            _close(handle)


def _read(handle: int, max_bytes: int) -> bytes:
    chunks = []
    total = 0
    while total <= max_bytes:
        buffer = ctypes.create_string_buffer(min(1024 * 1024, max_bytes + 1 - total))
        count = ctypes.c_uint32()
        if not _read_file(handle, buffer, len(buffer), ctypes.byref(count), None):
            raise OSError(ctypes.get_last_error(), "Could not read project file")
        if not count.value:
            break
        chunks.append(buffer.raw[:count.value])
        total += count.value
    if total > max_bytes:
        raise ValueError(f"file exceeds the {max_bytes}-byte read limit")
    return b"".join(chunks)


def _write(handle: int, content: bytes) -> None:
    for offset in range(0, len(content), 1024 * 1024):
        part = content[offset:offset + 1024 * 1024]
        buffer = ctypes.create_string_buffer(part)
        count = ctypes.c_uint32()
        if not _write_file(handle, buffer, len(part), ctypes.byref(count), None):
            raise OSError(ctypes.get_last_error(), "Could not write project file")
        if count.value != len(part):
            raise OSError("Incomplete project file write")
    if not _flush_file(handle):
        raise OSError(ctypes.get_last_error(), "Could not flush project file")


class _RenameInformation(ctypes.Structure):
    _fields_ = [
        ("ReplaceIfExists", ctypes.c_ubyte),
        ("RootDirectory", ctypes.c_void_p),
        ("FileNameLength", ctypes.c_uint32),
        ("FileName", ctypes.c_wchar * 1),
    ]


def _rename(handle: int, destination_parent: int, destination_name: str, *, replace: bool) -> None:
    encoded = destination_name.encode("utf-16-le")
    buffer = ctypes.create_string_buffer(ctypes.sizeof(_RenameInformation) + len(encoded))
    info = _RenameInformation.from_buffer(buffer)
    info.ReplaceIfExists = int(replace)
    info.RootDirectory = destination_parent
    info.FileNameLength = len(encoded)
    ctypes.memmove(ctypes.addressof(buffer) + _RenameInformation.FileName.offset, encoded, len(encoded))
    status_block = _IoStatusBlock()
    status = _nt_set_information(handle, ctypes.byref(status_block), buffer, len(buffer), 10)
    if status < 0:
        error = _rtl_error(status)
        if error in {80, 183}:
            raise FileExistsError(error, destination_name)
        raise OSError(error, "Could not rename project file")


def _delete(handle: int) -> None:
    delete = ctypes.c_ubyte(1)
    if not _set_information(handle, 4, ctypes.byref(delete), ctypes.sizeof(delete)):
        raise OSError(ctypes.get_last_error(), "Could not delete project file")


def _handle_path(handle: int) -> Path:
    buffer = ctypes.create_unicode_buffer(32768)
    length = _get_final_path(handle, buffer, len(buffer), 0)
    if length == 0 or length >= len(buffer):
        raise OSError(ctypes.get_last_error(), "Could not resolve opened project directory")
    actual = buffer.value
    if actual.startswith("\\\\?\\"):
        actual = actual[4:]
    return Path(actual)


def _handle_file_id(handle: int) -> int:
    info = _FileIdInfo()
    if not _get_info(handle, 18, ctypes.byref(info), ctypes.sizeof(info)):
        raise OSError(ctypes.get_last_error(), "Could not inspect project file identity")
    return int.from_bytes(bytes(info.file_id), "little")


def _handle_size_and_mtime_ns(handle: int) -> tuple[int, int]:
    basic = _FileBasicInfo()
    standard = _FileStandardInfo()
    if not _get_info(handle, 0, ctypes.byref(basic), ctypes.sizeof(basic)):
        raise OSError(ctypes.get_last_error(), "Could not inspect project file time")
    if not _get_info(handle, 1, ctypes.byref(standard), ctypes.sizeof(standard)):
        raise OSError(ctypes.get_last_error(), "Could not inspect project file size")
    return standard.end_of_file, (basic.last_write_time - 116444736000000000) * 100


def _assert_handle_at_path(handle: int, expected: Path) -> None:
    actual = _handle_path(handle)
    if os.path.normcase(os.path.abspath(actual)) != os.path.normcase(os.path.abspath(expected)):
        raise ValueError("project directory moved during file mutation")


def write_project_file(root: Path, relative_path: str, content: bytes, *, overwrite: bool, expected_hash: str | None) -> None:
    import hashlib

    with open_directory_chain(root) as project:
        with open_relative_parent(project, relative_path, create=True) as (parent, name):
            try:
                existing = _open_child(parent, name, directory=False, access=_READ_ATTRIBUTES | _READ_DATA)
            except FileNotFoundError:
                existing = None
            if existing is not None:
                try:
                    if not overwrite:
                        raise FileExistsError(f"project file already exists: {relative_path}")
                    if expected_hash is not None and hashlib.sha256(_read(existing, MAX_DOCUMENT_SIZE)).hexdigest() != expected_hash:
                        raise WindowsFileConflictError("project file changed")
                finally:
                    _close(existing)
            elif expected_hash is not None:
                raise FileNotFoundError(relative_path)
            temporary_name = ".knoggin-" + secrets.token_hex(16) + ".tmp"
            temporary = _open_child(parent, temporary_name, directory=False, create=True,
                                    access=_READ_ATTRIBUTES | _WRITE_DATA | _DELETE)
            try:
                _assert_handle_at_path(
                    parent, root.joinpath(*PurePosixPath(relative_path).parts[:-1])
                )
                _write(temporary, content)
                _rename(temporary, parent, name, replace=overwrite)
            except BaseException:
                try:
                    _delete(temporary)
                finally:
                    raise
            finally:
                _close(temporary)


def delete_project_file(root: Path, relative_path: str, *, expected_hash: str | None) -> bytes:
    import hashlib

    with open_directory_chain(root) as project:
        with open_relative_parent(project, relative_path) as (parent, name):
            target = _open_child(
                parent, name, directory=False,
                access=_READ_ATTRIBUTES | _READ_DATA | _DELETE,
            )
            try:
                _assert_handle_at_path(
                    parent, root.joinpath(*PurePosixPath(relative_path).parts[:-1])
                )
                content = _read(target, MAX_DOCUMENT_SIZE)
                if expected_hash is not None and hashlib.sha256(content).hexdigest() != expected_hash:
                    raise WindowsFileConflictError("project file changed")
                _assert_handle_at_path(
                    parent, root.joinpath(*PurePosixPath(relative_path).parts[:-1])
                )
                _delete(target)
                return content
            finally:
                _close(target)


def move_project_file(
    root: Path, source_path: str, destination_path: str, *, expected_hash: str | None
) -> bytes:
    import hashlib

    with open_directory_chain(root) as project:
        with open_relative_parent(project, source_path) as (source_parent, source_name):
            source = _open_child(
                source_parent, source_name, directory=False,
                access=_READ_ATTRIBUTES | _READ_DATA | _DELETE,
            )
            try:
                _assert_handle_at_path(
                    source_parent, root.joinpath(*PurePosixPath(source_path).parts[:-1])
                )
                content = _read(source, MAX_DOCUMENT_SIZE)
                if expected_hash is not None and hashlib.sha256(content).hexdigest() != expected_hash:
                    raise WindowsFileConflictError("project file changed")
                with open_relative_parent(project, destination_path, create=True) as (target_parent, target_name):
                    try:
                        target = _open_child(
                            target_parent, target_name, directory=False,
                            access=_READ_ATTRIBUTES,
                        )
                    except FileNotFoundError:
                        pass
                    else:
                        _close(target)
                        raise FileExistsError(destination_path)
                    temporary_name = ".knoggin-move-" + secrets.token_hex(16) + ".tmp"
                    temporary = _open_child(
                        target_parent, temporary_name, directory=False, create=True,
                        access=_READ_ATTRIBUTES | _DELETE, delete_on_close=True,
                    )
                    try:
                        _assert_handle_at_path(
                            source_parent,
                            root.joinpath(*PurePosixPath(source_path).parts[:-1]),
                        )
                        _assert_handle_at_path(
                            target_parent,
                            root.joinpath(*PurePosixPath(destination_path).parts[:-1]),
                        )
                        _rename(source, target_parent, target_name, replace=False)
                    finally:
                        _close(temporary)
                return content
            finally:
                _close(source)


def create_project_folder(root: Path, relative_path: str) -> None:
    with open_directory_chain(root) as project:
        with open_relative_parent(project, relative_path, create=True) as (parent, name):
            was_created: list[bool] = []
            child = _open_child(
                parent, name, directory=True, create=True,
                access=_READ_ATTRIBUTES | _DELETE, created_out=was_created,
            )
            try:
                _assert_handle_at_path(
                    parent, root.joinpath(*PurePosixPath(relative_path).parts[:-1])
                )
            except BaseException:
                if was_created[0]:
                    _delete(child)
                raise
            finally:
                _close(child)


def _remove_tree_handle(directory: int) -> None:
    # Enumeration supplies names only. Every child is opened relative to this
    # directory handle, so a concurrent path swap cannot redirect deletion.
    with os.scandir(_handle_path(directory)) as scan:
        entries = [
            (entry.name, os.lstat(entry.path).st_ino)
            for entry in scan
        ]
    for name, expected_file_id in entries:
        attributes: list[int] = []
        try:
            child = _open_child(
                directory, name, directory=None,
                access=_READ_ATTRIBUTES | _DELETE,
                attributes_out=attributes, allow_reparse=True,
            )
        except FileNotFoundError:
            continue
        try:
            if _handle_file_id(child) != expected_file_id:
                raise RuntimeError("project directory changed during file cleanup")
            if attributes[0] & _DIRECTORY_ATTRIBUTE and not attributes[0] & _REPARSE_ATTRIBUTE:
                _remove_tree_handle(child)
            _delete(child)
        finally:
            _close(child)


def remove_project_tree(
    library_root: Path, project_id: str, *, expected_file_id: int | None = None
) -> bool:
    with open_directory_chain(library_root) as library:
        attributes: list[int] = []
        try:
            root = _open_child(
                library, project_id, directory=None,
                access=_READ_ATTRIBUTES | _DELETE,
                attributes_out=attributes, allow_reparse=True,
            )
        except FileNotFoundError:
            return False
        try:
            if expected_file_id is not None and _handle_file_id(root) != expected_file_id:
                raise RuntimeError("project directory changed during file cleanup")
            if attributes[0] & _DIRECTORY_ATTRIBUTE and not attributes[0] & _REPARSE_ATTRIBUTE:
                _remove_tree_handle(root)
            _delete(root)
        finally:
            _close(root)
        return True


@contextmanager
def open_lock_directory(library_root: Path, *, create: bool = False) -> Iterator[int]:
    with open_directory_chain(library_root, create=create) as library:
        lock_directory = _open_child(
            library, ".knoggin-file-locks", directory=True, create=create,
            access=_READ_ATTRIBUTES | _FILE_TRAVERSE,
        )
        try:
            _assert_handle_at_path(lock_directory, library_root / ".knoggin-file-locks")
            yield lock_directory
        finally:
            _close(lock_directory)


def open_lock_descriptor(library_root: Path, lock_name: str) -> int:
    import msvcrt

    with open_lock_directory(library_root, create=True) as directory:
        handle = _open_child(
            directory, lock_name, directory=False, open_if=True,
            access=_READ_ATTRIBUTES | _READ_DATA | _WRITE_DATA,
        )
        try:
            _assert_handle_at_path(
                directory, library_root / ".knoggin-file-locks"
            )
            descriptor = msvcrt.open_osfhandle(handle, os.O_BINARY | os.O_RDWR)
        except BaseException:
            _close(handle)
            raise
        return descriptor


def project_source_exists(root: Path, relative_path: str) -> bool:
    with open_directory_chain(root) as project:
        try:
            with open_relative_parent(project, relative_path) as (parent, name):
                child = _open_child(
                    parent, name, directory=None, allow_reparse=True
                )
                _close(child)
                return True
        except FileNotFoundError:
            return False


def read_pending_marker(library_root: Path, marker_name: str) -> str | None:
    try:
        with open_lock_directory(library_root) as directory:
            marker = _open_child(
                directory, marker_name, directory=False,
                access=_READ_ATTRIBUTES | _READ_DATA,
            )
            try:
                return _read(marker, 64).decode("ascii")
            finally:
                _close(marker)
    except FileNotFoundError:
        return None


def write_pending_marker(library_root: Path, marker_name: str, content_hash: str) -> None:
    with open_lock_directory(library_root, create=True) as directory:
        temporary_name = ".knoggin-admission-" + secrets.token_hex(16) + ".tmp"
        temporary = _open_child(
            directory, temporary_name, directory=False, create=True,
            access=_READ_ATTRIBUTES | _WRITE_DATA | _DELETE,
        )
        try:
            _assert_handle_at_path(directory, library_root / ".knoggin-file-locks")
            _write(temporary, content_hash.encode("ascii"))
            _rename(temporary, directory, marker_name, replace=False)
        except BaseException:
            try:
                _delete(temporary)
            finally:
                raise
        finally:
            _close(temporary)


def delete_pending_marker(library_root: Path, marker_name: str) -> bool:
    try:
        with open_lock_directory(library_root) as directory:
            marker = _open_child(
                directory, marker_name, directory=False,
                access=_READ_ATTRIBUTES | _DELETE,
            )
            try:
                _delete(marker)
            finally:
                _close(marker)
            return True
    except FileNotFoundError:
        return False


def delete_pending_markers_for_project(library_root: Path, prefix: str) -> None:
    try:
        with open_lock_directory(library_root) as directory:
            with os.scandir(_handle_path(directory)) as scan:
                names = [entry.name for entry in scan if entry.name.startswith(prefix)]
            for name in names:
                try:
                    marker = _open_child(
                        directory, name, directory=None,
                        access=_READ_ATTRIBUTES | _DELETE,
                        allow_reparse=True,
                    )
                except FileNotFoundError:
                    continue
                try:
                    _delete(marker)
                finally:
                    _close(marker)
    except FileNotFoundError:
        return


def iter_project_paths(
    root: Path, skip_directory: Callable[[str], bool] | None = None
) -> Iterator[tuple[str, int, int]]:
    """List regular files by opened directory identity, skipping links."""

    def walk(directory: int, prefix: str) -> Iterator[tuple[str, int, int]]:
        with os.scandir(_handle_path(directory)) as scan:
            entries = [
                (entry.name, (metadata := os.lstat(entry.path)).st_ino,
                 stat.S_ISDIR(metadata.st_mode))
                for entry in scan
            ]
        for name, expected_file_id, _ in sorted(
            entries, key=lambda item: item[0] + ("/" if item[2] else "")
        ):
            attributes: list[int] = []
            try:
                child = _open_child(
                    directory, name, directory=None,
                    attributes_out=attributes, allow_reparse=True,
                )
            except FileNotFoundError:
                continue
            try:
                if _handle_file_id(child) != expected_file_id:
                    raise RuntimeError("project directory changed during file scan")
                if attributes[0] & _REPARSE_ATTRIBUTE:
                    continue
                relative_path = f"{prefix}{name}"
                if attributes[0] & _DIRECTORY_ATTRIBUTE:
                    if skip_directory is not None and skip_directory(relative_path):
                        continue
                    yield from walk(child, relative_path + "/")
                else:
                    size, mtime_ns = _handle_size_and_mtime_ns(child)
                    yield relative_path, size, mtime_ns
            finally:
                _close(child)

    with open_directory_chain(root) as project:
        yield from walk(project, "")
