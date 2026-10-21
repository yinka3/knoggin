"""Windows handle error translation and cleanup contracts."""

import os
from pathlib import Path

import pytest

if os.name == "nt":
    from core.knowledge.documents.io import windows_handle_io as handles


pytestmark = [pytest.mark.unit, pytest.mark.no_network, pytest.mark.skipif(os.name != "nt", reason="Windows native handles")]


def test_component_paths_must_be_local_and_single_component():
    with pytest.raises(ValueError, match="one path component"):
        handles._open_child(1, "nested/file.md", directory=False)
    with pytest.raises(ValueError, match="local drive"):
        with handles.open_directory_chain(Path("relative")):
            pass
    with pytest.raises(ValueError, match="relative project path"):
        with handles.open_relative_parent(1, "../escape.md"):
            pass


@pytest.mark.parametrize(
    ("windows_error", "error_type"),
    [(2, FileNotFoundError), (183, FileExistsError), (5, OSError)],
)
def test_open_child_translates_native_failures(monkeypatch, windows_error, error_type):
    monkeypatch.setattr(handles, "_nt_create_file", lambda *_args: -1)
    monkeypatch.setattr(handles, "_rtl_error", lambda _status: windows_error)

    with pytest.raises(error_type):
        handles._open_child(1, "notes.md", directory=False)


def test_native_read_write_and_flush_failures_are_reported(monkeypatch):
    monkeypatch.setattr(handles, "_read_file", lambda *_args: 0)
    with pytest.raises(OSError, match="Could not read"):
        handles._read(1, 5)

    monkeypatch.setattr(handles, "_write_file", lambda *_args: 0)
    with pytest.raises(OSError, match="Could not write"):
        handles._write(1, b"x")
    monkeypatch.setattr(handles, "_write_file", lambda *_args: 1)
    with pytest.raises(OSError, match="Incomplete"):
        handles._write(1, b"x")
    monkeypatch.setattr(handles, "_flush_file", lambda *_args: 0)
    with pytest.raises(OSError, match="Could not flush"):
        handles._write(1, b"")


def test_native_rename_delete_and_metadata_failures_are_reported(monkeypatch):
    monkeypatch.setattr(handles, "_nt_set_information", lambda *_args: -1)
    monkeypatch.setattr(handles, "_rtl_error", lambda _status: 183)
    with pytest.raises(FileExistsError):
        handles._rename(1, 2, "taken.md", replace=False)
    monkeypatch.setattr(handles, "_rtl_error", lambda _status: 5)
    with pytest.raises(OSError, match="Could not rename"):
        handles._rename(1, 2, "blocked.md", replace=False)

    monkeypatch.setattr(handles, "_set_information", lambda *_args: 0)
    with pytest.raises(OSError, match="Could not delete"):
        handles._delete(1)
    monkeypatch.setattr(handles, "_get_final_path", lambda *_args: 0)
    with pytest.raises(OSError, match="Could not resolve"):
        handles._handle_path(1)
    monkeypatch.setattr(handles, "_get_info", lambda *_args: 0)
    with pytest.raises(OSError, match="identity"):
        handles._handle_file_id(1)
    with pytest.raises(OSError, match="time"):
        handles._handle_size_and_mtime_ns(1)


def test_open_child_closes_handle_when_metadata_cannot_be_read(tmp_path, monkeypatch):
    root = tmp_path / "project"
    root.mkdir()
    monkeypatch.setattr(handles, "_get_info", lambda *_args: 0)

    with pytest.raises(OSError, match="inspect opened project component"):
        with handles.open_directory_chain(root):
            pass


def test_read_bound_and_second_metadata_failure(tmp_path, monkeypatch):
    root = tmp_path / "project"
    root.mkdir()
    (root / "notes.md").write_bytes(b"owned")
    with handles.open_directory_chain(root) as directory:
        handle = handles._open_child(
            directory, "notes.md", directory=False,
            access=handles._READ_ATTRIBUTES | handles._READ_DATA,
        )
        try:
            with pytest.raises(ValueError, match="read limit"):
                handles._read(handle, 2)
        finally:
            handles._close(handle)

    calls = 0

    def fail_second_metadata_query(*_args):
        nonlocal calls
        calls += 1
        return int(calls == 1)

    monkeypatch.setattr(handles, "_get_info", fail_second_metadata_query)
    with pytest.raises(OSError, match="inspect project file size"):
        handles._handle_size_and_mtime_ns(1)


def test_write_requires_expected_source_and_move_preserves_occupied_target(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    with pytest.raises(FileNotFoundError):
        handles.write_project_file(
            root, "missing.md", b"new", overwrite=True, expected_hash="a" * 64
        )
    handles.write_project_file(root, "source.md", b"source", overwrite=False, expected_hash=None)
    handles.write_project_file(root, "target.md", b"target", overwrite=False, expected_hash=None)
    with pytest.raises(FileExistsError):
        handles.move_project_file(root, "source.md", "target.md", expected_hash=None)
    assert (root / "source.md").read_bytes() == b"source"
    assert (root / "target.md").read_bytes() == b"target"


def test_failed_folder_creation_removes_new_directory(tmp_path, monkeypatch):
    root = tmp_path / "project"
    root.mkdir()
    real_assert = handles._assert_handle_at_path

    def reject_new_folder_parent(handle, expected):
        if expected == root:
            raise ValueError("parent moved")
        return real_assert(handle, expected)

    monkeypatch.setattr(handles, "_assert_handle_at_path", reject_new_folder_parent)
    with pytest.raises(ValueError, match="parent moved"):
        handles.create_project_folder(root, "new-folder")
    assert not (root / "new-folder").exists()


def test_failed_marker_publish_removes_temporary_file(tmp_path):
    library = tmp_path / "projects"
    library.mkdir()
    handles.write_pending_marker(library, "project.pending", "a" * 64)

    with pytest.raises(FileExistsError):
        handles.write_pending_marker(library, "project.pending", "b" * 64)

    lock_dir = library / ".knoggin-file-locks"
    assert not list(lock_dir.glob(".knoggin-admission-*.tmp"))
    assert handles.read_pending_marker(library, "project.pending") == "a" * 64


def test_remove_project_rejects_stale_identity(tmp_path):
    library = tmp_path / "projects"
    (library / "project").mkdir(parents=True)
    with pytest.raises(RuntimeError, match="changed during file cleanup"):
        handles.remove_project_tree(library, "project", expected_file_id=-1)
    assert (library / "project").is_dir()


def test_project_cleanup_ignores_a_child_removed_after_listing(tmp_path, monkeypatch):
    library = tmp_path / "projects"
    root = library / "project"
    root.mkdir(parents=True)
    (root / "vanish.md").write_bytes(b"gone")
    real_open = handles._open_child

    def remove_before_open(parent, name, **kwargs):
        if name == "vanish.md":
            (root / name).unlink()
            raise FileNotFoundError(name)
        return real_open(parent, name, **kwargs)

    monkeypatch.setattr(handles, "_open_child", remove_before_open)
    assert handles.remove_project_tree(library, "project")
    assert not root.exists()


def test_marker_cleanup_handles_missing_directory_and_vanished_file(
    tmp_path, monkeypatch
):
    library = tmp_path / "projects"
    library.mkdir()
    handles.delete_pending_markers_for_project(library, "project.")
    handles.write_pending_marker(library, "project.pending", "a" * 64)
    lock_dir = library / ".knoggin-file-locks"
    real_open = handles._open_child

    def remove_before_open(parent, name, **kwargs):
        if name == "project.pending":
            (lock_dir / name).unlink()
            raise FileNotFoundError(name)
        return real_open(parent, name, **kwargs)

    monkeypatch.setattr(handles, "_open_child", remove_before_open)
    handles.delete_pending_markers_for_project(library, "project.")
    assert not (lock_dir / "project.pending").exists()


def test_lock_descriptor_closes_handle_when_parent_changes(tmp_path, monkeypatch):
    library = tmp_path / "projects"
    library.mkdir()
    real_assert = handles._assert_handle_at_path

    def reject_lock_parent(handle, expected):
        if expected == library / ".knoggin-file-locks":
            raise ValueError("lock parent moved")
        return real_assert(handle, expected)

    monkeypatch.setattr(handles, "_assert_handle_at_path", reject_lock_parent)
    with pytest.raises(ValueError, match="lock parent moved"):
        handles.open_lock_descriptor(library, "project.lock")
