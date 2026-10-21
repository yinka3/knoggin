import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from core.knowledge.documents import filesystem as filesystem_module
from core.knowledge.documents.filesystem import (
    ProjectFilesystem,
    ProjectFilesystemConflictError,
    ProjectFilesystemFactory,
)

if os.name == "nt":
    from core.knowledge.documents.io import (
        windows_handle_io as windows_handle_io_module,
    )


def _pause_project_replace(monkeypatch, root, entered, release, *, once):
    if os.name == "nt":
        real_rename = windows_handle_io_module._rename

        def pause(handle, parent, name, *, replace):
            if name == "notes.md" and (not once or not entered.is_set()):
                entered.set()
                assert release.wait(10)
            return real_rename(handle, parent, name, replace=replace)

        monkeypatch.setattr(windows_handle_io_module, "_rename", pause)
        return

    real_replace = os.replace

    def pause(source, destination, *args, **kwargs):
        parent_fd = kwargs.get("dst_dir_fd")
        relative_project_file = (
            destination == "notes.md"
            and parent_fd is not None
            and (os.fstat(parent_fd).st_dev, os.fstat(parent_fd).st_ino)
            == (root.stat().st_dev, root.stat().st_ino)
        )
        if (
            (Path(destination) == root / "notes.md" or relative_project_file)
            and (not once or not entered.is_set())
        ):
            entered.set()
            assert release.wait(10)
        return real_replace(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, "replace", pause)


def test_project_filesystem_requires_an_absolute_root(tmp_path):
    with pytest.raises(ValueError, match="absolute"):
        ProjectFilesystem("relative-project-root")

    filesystem = ProjectFilesystem(tmp_path / "project")
    created = filesystem.write_bytes("notes/plan.md", b"first")

    assert created.relative_path == "notes/plan.md"
    assert filesystem.read_bytes("notes/plan.md") == b"first"


@pytest.mark.skipif(os.name != "nt", reason="Windows opened-handle path check")
def test_project_read_rejects_an_opened_file_outside_the_root(tmp_path, monkeypatch):
    filesystem = ProjectFilesystem(tmp_path / "project")
    filesystem.write_bytes("notes.md", b"private")
    monkeypatch.setattr(
        filesystem_module,
        "_opened_windows_path",
        lambda _descriptor: str(tmp_path / "outside.md"),
    )
    with pytest.raises(ValueError, match="outside its root"):
        filesystem.read_bytes("notes.md")


@pytest.mark.skipif(os.name != "nt", reason="Windows handle-relative rename")
def test_project_write_does_not_follow_a_swapped_parent_junction(tmp_path, monkeypatch):
    filesystem = ProjectFilesystem(tmp_path / "project")
    filesystem.write_bytes("notes/seed.md", b"seed")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "new.md").write_bytes(b"private")
    moved = tmp_path / "moved-original"
    real_open = windows_handle_io_module._open_child
    swapped = False

    def swap_before_temporary(parent, name, **kwargs):
        nonlocal swapped
        if name.startswith(".knoggin-") and name.endswith(".tmp") and not swapped:
            swapped = True
            os.rename(filesystem.root / "notes", moved)
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(filesystem.root / "notes"), str(outside)],
                check=True, capture_output=True,
            )
        return real_open(parent, name, **kwargs)

    monkeypatch.setattr(windows_handle_io_module, "_open_child", swap_before_temporary)
    with pytest.raises(ValueError, match="directory moved"):
        filesystem.write_bytes("notes/new.md", b"owned")
    assert (outside / "new.md").read_bytes() == b"private"
    assert not (moved / "new.md").exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows handle-relative cleanup")
def test_project_cleanup_does_not_follow_a_swapped_child_junction(tmp_path, monkeypatch):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    filesystem = factory.for_project("project-1")
    filesystem.write_bytes("nested/owned.md", b"owned")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.md").write_bytes(b"private")
    moved = tmp_path / "moved-original"
    real_open = windows_handle_io_module._open_child
    swapped = False

    def swap_before_child_open(parent, name, **kwargs):
        nonlocal swapped
        if name == "nested" and kwargs.get("directory") is None and not swapped:
            swapped = True
            os.rename(filesystem.root / "nested", moved)
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(filesystem.root / "nested"), str(outside)],
                check=True, capture_output=True,
            )
        return real_open(parent, name, **kwargs)

    monkeypatch.setattr(windows_handle_io_module, "_open_child", swap_before_child_open)
    with pytest.raises(RuntimeError, match="changed during file cleanup"):
        factory.remove_project_directory("project-1")
    assert (outside / "keep.md").read_bytes() == b"private"
    assert (moved / "owned.md").read_bytes() == b"owned"


@pytest.mark.skipif(os.name != "nt", reason="Windows handle-relative scan")
def test_project_scan_rejects_a_swapped_child_junction(tmp_path, monkeypatch):
    filesystem = ProjectFilesystem(tmp_path / "project")
    filesystem.write_bytes("nested/owned.md", b"owned")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.md").write_bytes(b"private")
    moved = tmp_path / "moved-original"
    real_open = windows_handle_io_module._open_child
    swapped = False

    def swap_before_child_open(parent, name, **kwargs):
        nonlocal swapped
        if name == "nested" and kwargs.get("directory") is None and not swapped:
            swapped = True
            os.rename(filesystem.root / "nested", moved)
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(filesystem.root / "nested"), str(outside)],
                check=True, capture_output=True,
            )
        return real_open(parent, name, **kwargs)

    monkeypatch.setattr(windows_handle_io_module, "_open_child", swap_before_child_open)
    with pytest.raises(RuntimeError, match="changed during file scan"):
        list(filesystem.iter_paths())
    assert (outside / "secret.md").read_bytes() == b"private"


@pytest.mark.skipif(os.name != "nt", reason="Windows handle-relative move")
def test_project_move_rejects_a_swapped_destination_parent(tmp_path, monkeypatch):
    filesystem = ProjectFilesystem(tmp_path / "project")
    filesystem.write_bytes("source.md", b"owned")
    filesystem.create_folder("target")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "moved.md").write_bytes(b"private")
    moved = tmp_path / "moved-target"
    real_open = windows_handle_io_module._open_child
    swapped = False

    def swap_before_placeholder(parent, name, **kwargs):
        nonlocal swapped
        if name.startswith(".knoggin-move-") and not swapped:
            swapped = True
            os.rename(filesystem.root / "target", moved)
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(filesystem.root / "target"), str(outside)],
                check=True, capture_output=True,
            )
        return real_open(parent, name, **kwargs)

    monkeypatch.setattr(windows_handle_io_module, "_open_child", swap_before_placeholder)
    with pytest.raises(ValueError, match="directory moved"):
        filesystem.move_file("source.md", "target/moved.md")
    assert (outside / "moved.md").read_bytes() == b"private"
    assert filesystem.read_bytes("source.md") == b"owned"


@pytest.mark.skipif(os.name != "nt", reason="Windows handle-relative deletion")
def test_project_delete_rejects_a_swapped_source_parent(tmp_path, monkeypatch):
    filesystem = ProjectFilesystem(tmp_path / "project")
    filesystem.write_bytes("nested/owned.md", b"owned")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "owned.md").write_bytes(b"private")
    moved = tmp_path / "moved-original"
    real_open = windows_handle_io_module._open_child
    swapped = False

    def swap_before_target(parent, name, **kwargs):
        nonlocal swapped
        if name == "owned.md" and not swapped:
            swapped = True
            os.rename(filesystem.root / "nested", moved)
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(filesystem.root / "nested"), str(outside)],
                check=True, capture_output=True,
            )
        return real_open(parent, name, **kwargs)

    monkeypatch.setattr(windows_handle_io_module, "_open_child", swap_before_target)
    with pytest.raises(ValueError, match="directory moved"):
        filesystem.delete_file("nested/owned.md")
    assert (outside / "owned.md").read_bytes() == b"private"
    assert (moved / "owned.md").read_bytes() == b"owned"


@pytest.mark.skipif(os.name != "nt", reason="Windows handle-relative folder creation")
def test_project_folder_creation_rejects_a_swapped_parent(tmp_path, monkeypatch):
    filesystem = ProjectFilesystem(tmp_path / "project")
    filesystem.create_folder("nested")
    outside = tmp_path / "outside"
    outside.mkdir()
    moved = tmp_path / "moved-original"
    real_open = windows_handle_io_module._open_child
    swapped = False

    def swap_before_folder(parent, name, **kwargs):
        nonlocal swapped
        if name == "created" and kwargs.get("directory") is True and not swapped:
            swapped = True
            os.rename(filesystem.root / "nested", moved)
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(filesystem.root / "nested"), str(outside)],
                check=True, capture_output=True,
            )
        return real_open(parent, name, **kwargs)

    monkeypatch.setattr(windows_handle_io_module, "_open_child", swap_before_folder)
    with pytest.raises(ValueError, match="directory moved"):
        filesystem.create_folder("nested/created")
    assert not (outside / "created").exists()
    assert not (moved / "created").exists()


@pytest.mark.parametrize("path", ["../outside.txt", "/outside.txt", "C:\\outside.txt"])
def test_project_filesystem_rejects_paths_that_escape_the_project_root(tmp_path, path):
    filesystem = ProjectFilesystem(tmp_path / "project")

    with pytest.raises(ValueError, match="relative|escape"):
        filesystem.write_bytes(path, b"nope")


def test_project_filesystem_uses_atomic_replace_and_hash_preconditions(tmp_path):
    filesystem = ProjectFilesystem(tmp_path / "project")
    first = filesystem.write_bytes("notes.md", b"first")

    with pytest.raises(FileExistsError):
        filesystem.write_bytes("notes.md", b"second")
    with pytest.raises(ProjectFilesystemConflictError):
        filesystem.write_bytes(
            "notes.md",
            b"second",
            overwrite=True,
            expected_content_hash="stale",
        )

    replaced = filesystem.write_bytes(
        "notes.md",
        b"second",
        overwrite=True,
        expected_content_hash=first.content_hash,
    )

    assert filesystem.read_bytes("notes.md") == b"second"
    assert replaced.content_hash != first.content_hash
    assert not list((tmp_path / "project").rglob(".knoggin-*.tmp"))


def test_mutation_lease_rejects_reuse_and_another_project(tmp_path):
    first = ProjectFilesystem(tmp_path / "first")
    second = ProjectFilesystem(tmp_path / "second")
    lease = first.acquire_mutation_lease()
    try:
        with pytest.raises(ValueError, match="another root"):
            second.write_bytes("notes.md", b"wrong project", _lease=lease)
        assert not second.root.joinpath("notes.md").exists()
    finally:
        first.release_mutation_lease(lease)

    with pytest.raises(ValueError, match="not active"):
        first.release_mutation_lease(lease)
    with pytest.raises(ValueError, match="not active"):
        first.write_bytes("notes.md", b"after release", _lease=lease)


def test_pending_marker_rejects_invalid_digest_and_only_counts_real_changes(tmp_path):
    filesystem = ProjectFilesystem(tmp_path / "project")
    before = filesystem.get_mutation_generation()
    with filesystem._mutation_lock() as lease:
        with pytest.raises(ValueError, match="SHA-256"):
            filesystem.mark_pending_admission("notes.md", "not-a-hash", _lease=lease)
        filesystem.clear_pending_admission("notes.md", _lease=lease)
        assert filesystem.get_mutation_generation(_lease=lease) == before
        filesystem.mark_pending_admission("notes.md", "a" * 64, _lease=lease)
        assert filesystem.get_mutation_generation(_lease=lease) == before + 1
        filesystem.clear_pending_admission("notes.md", _lease=lease)
        assert filesystem.get_mutation_generation(_lease=lease) == before + 2
    assert filesystem.pending_admission_hash("notes.md") is None


def test_project_file_rejects_empty_bytes_and_directory_reads(tmp_path):
    filesystem = ProjectFilesystem(tmp_path / "project")
    with pytest.raises(ValueError, match="must not be empty"):
        filesystem.write_bytes("empty.md", b"")
    filesystem.create_folder("folder")
    with pytest.raises((ValueError, PermissionError, OSError)):
        filesystem.read_bytes("folder")


def test_project_removal_waits_for_the_shared_mutation_lease(tmp_path):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    filesystem = factory.for_project("project-1")
    filesystem.write_bytes("notes.md", b"owned")
    lease = filesystem.acquire_mutation_lease()
    with ThreadPoolExecutor(max_workers=1) as pool:
        removal = pool.submit(factory.remove_project_directory, "project-1")
        try:
            with pytest.raises(TimeoutError):
                removal.result(timeout=0.2)
            assert filesystem.read_bytes("notes.md") == b"owned"
        finally:
            filesystem.release_mutation_lease(lease)
        assert removal.result(timeout=10) is True
    assert not filesystem.root.exists()


def test_project_removal_clears_pending_upload_markers(tmp_path):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    filesystem = factory.for_project("project-1")
    with filesystem._mutation_lock() as lease:
        filesystem.mark_pending_admission(
            "orphan.md", "a" * 64, _lease=lease
        )
        filesystem.write_bytes("orphan.md", b"orphan", _lease=lease)
    assert filesystem.pending_admission_hash("orphan.md") == "a" * 64

    assert factory.remove_project_directory("project-1") is True
    assert filesystem.pending_admission_hash("orphan.md") is None


def test_pending_upload_marker_survives_duplicate_and_reclaims_absent_source(tmp_path):
    filesystem = ProjectFilesystem(tmp_path / "project")
    with filesystem._mutation_lock() as lease:
        filesystem.mark_pending_admission("orphan.md", "a" * 64, _lease=lease)
        filesystem.write_bytes("orphan.md", b"failed upload", _lease=lease)
        with pytest.raises(FileExistsError):
            filesystem.mark_pending_admission("orphan.md", "b" * 64, _lease=lease)
        assert filesystem.pending_admission_hash("orphan.md") == "a" * 64

        filesystem.delete_file("orphan.md", _lease=lease)
        filesystem.mark_pending_admission("orphan.md", "b" * 64, _lease=lease)
        assert filesystem.pending_admission_hash("orphan.md") == "b" * 64


@pytest.mark.parametrize("competing_operation", ["write", "delete", "move"])
def test_expected_hash_mutations_recheck_after_a_concurrent_write(
    tmp_path, monkeypatch, competing_operation
):
    first = ProjectFilesystem(tmp_path / "project")
    second = ProjectFilesystem(tmp_path / "project")
    original = first.write_bytes("notes.md", b"original")
    entered_replace = threading.Event()
    release_replace = threading.Event()
    _pause_project_replace(
        monkeypatch, first.root, entered_replace, release_replace, once=True
    )

    def compete():
        if competing_operation == "write":
            return second.write_bytes(
                "notes.md", b"other", overwrite=True,
                expected_content_hash=original.content_hash,
            )
        if competing_operation == "delete":
            return second.delete_file(
                "notes.md", expected_content_hash=original.content_hash
            )
        return second.move_file(
            "notes.md", "moved.md", expected_content_hash=original.content_hash
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        winner = pool.submit(
            first.write_bytes, "notes.md", b"winner", overwrite=True,
            expected_content_hash=original.content_hash,
        )
        try:
            assert entered_replace.wait(10)
            loser = pool.submit(compete)
            with pytest.raises(TimeoutError):
                loser.result(timeout=0.2)
        finally:
            release_replace.set()
        assert winner.result(timeout=10).content_hash == first.read_file("notes.md").content_hash
        with pytest.raises(ProjectFilesystemConflictError):
            loser.result(timeout=10)

    assert first.read_bytes("notes.md") == b"winner"
    assert not (first.root / "moved.md").exists()
    assert not list(first.root.rglob(".knoggin-*.tmp"))


def test_expected_hash_write_conflicts_across_processes(tmp_path, monkeypatch):
    filesystem = ProjectFilesystem(tmp_path / "project")
    original = filesystem.write_bytes("notes.md", b"original")
    entered_replace = threading.Event()
    release_replace = threading.Event()
    _pause_project_replace(
        monkeypatch, filesystem.root, entered_replace, release_replace, once=False
    )
    script = """
import sys
sys.path.insert(0, sys.argv[3])
from core.knowledge.documents.filesystem import (
    ProjectFilesystem, ProjectFilesystemConflictError,
)
filesystem = ProjectFilesystem(sys.argv[1])
print('ready', flush=True)
sys.stdin.readline()
try:
    filesystem.write_bytes(
        'notes.md', b'other', overwrite=True, expected_content_hash=sys.argv[2]
    )
except ProjectFilesystemConflictError:
    print('conflict', flush=True)
else:
    print('unexpected success', flush=True)
"""

    process = subprocess.Popen(
        [
            sys.executable, "-c", script, str(filesystem.root),
            original.content_hash,
            str(Path(__file__).resolve().parents[4] / "src"),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "ready"

    with ThreadPoolExecutor(max_workers=1) as pool:
        winner = pool.submit(
            filesystem.write_bytes, "notes.md", b"winner", overwrite=True,
            expected_content_hash=original.content_hash,
        )
        try:
            assert entered_replace.wait(10)
            assert process.stdin is not None
            process.stdin.write("go\n")
            process.stdin.flush()
            with pytest.raises(subprocess.TimeoutExpired):
                process.wait(timeout=0.2)
        finally:
            release_replace.set()
        winner.result(timeout=10)
        output, errors = process.communicate(timeout=10)
        assert process.returncode == 0, errors
        assert output.strip() == "conflict"

    assert filesystem.read_bytes("notes.md") == b"winner"


def test_project_filesystem_bounds_reads_and_lists_regular_files_in_path_order(tmp_path):
    filesystem = ProjectFilesystem(tmp_path / "project")
    filesystem.write_bytes("z.md", b"z")
    filesystem.write_bytes("docs/a.md", b"alpha")

    with pytest.raises(ValueError, match="read limit"):
        filesystem.read_bytes("docs/a.md", max_bytes=4)

    assert [entry.relative_path for entry in filesystem.iter_files()] == [
        "docs/a.md",
        "z.md",
    ]


def test_project_filesystem_limited_walk_stops_before_later_directories(
    tmp_path, monkeypatch
):
    filesystem = ProjectFilesystem(tmp_path / "project")
    filesystem.write_bytes("a.txt", b"one")
    filesystem.write_bytes("a/b.md", b"two")
    filesystem.write_bytes("z/late.md", b"three")

    assert [item.relative_path for item in filesystem.iter_paths()] == [
        "a.txt", "a/b.md", "z/late.md",
    ]

    real_scandir = os.scandir

    def reject_late_directory(directory):
        if Path(directory) == filesystem.root / "z":
            raise AssertionError("limited walk entered a directory after its bound")
        return real_scandir(directory)

    monkeypatch.setattr(os, "scandir", reject_late_directory)
    assert [item.relative_path for item in filesystem.iter_paths(limit=2)] == [
        "a.txt", "a/b.md",
    ]


def test_project_filesystem_does_not_follow_symlinks_inside_the_project_root(tmp_path):
    project_root = tmp_path / "project"
    outside = tmp_path / "outside.txt"
    outside.write_text("private")
    filesystem = ProjectFilesystem(project_root)
    filesystem.ensure_root()
    os.symlink(outside, project_root / "linked.txt")
    os.symlink(tmp_path, project_root / "linked-dir")

    with pytest.raises(ValueError, match="symlink"):
        filesystem.read_bytes("linked.txt")
    with pytest.raises(ValueError, match="symlink|reparse point"):
        filesystem.write_bytes("linked.txt", b"overwrite", overwrite=True)
    with pytest.raises(ValueError, match="symlink|reparse point"):
        filesystem.write_bytes("linked-dir/new.txt", b"nope")

    assert outside.read_text() == "private"
    assert list(filesystem.iter_files()) == []


@pytest.mark.skipif(os.name != "nt", reason="Windows junction behavior")
def test_project_filesystem_rejects_windows_junctions_during_access_and_scan(tmp_path):
    filesystem = ProjectFilesystem(tmp_path / "project")
    filesystem.write_bytes("owned.md", b"owned")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.md").write_text("private")
    junction = filesystem.root / "linked"
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(outside)],
        check=True,
        capture_output=True,
    )
    assert junction.is_junction() and not junction.is_symlink()

    with pytest.raises(ValueError, match="reparse point"):
        filesystem.read_bytes("linked/keep.md")
    with pytest.raises(ValueError, match="reparse point"):
        filesystem.write_bytes("linked/new.md", b"external")
    with pytest.raises(ValueError, match="reparse point"):
        filesystem.delete_file("linked/keep.md")
    with pytest.raises(ValueError, match="reparse point"):
        filesystem.move_file("owned.md", "linked/moved.md")
    with pytest.raises(ValueError, match="reparse point"):
        filesystem.create_folder("linked/folder")

    assert [item.relative_path for item in filesystem.iter_paths()] == ["owned.md"]
    assert (outside / "keep.md").read_text() == "private"
    assert not (outside / "new.md").exists()
    assert filesystem.read_bytes("owned.md") == b"owned"


@pytest.mark.skipif(os.name != "nt", reason="Windows junction behavior")
def test_project_filesystem_rejects_windows_junction_roots_and_lock_directory(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    project_root = tmp_path / "project"
    filesystem = ProjectFilesystem(project_root)
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(project_root), str(outside)],
        check=True,
        capture_output=True,
    )
    with pytest.raises(ValueError, match="reparse point"):
        ProjectFilesystem(project_root)
    with pytest.raises(ValueError, match="real directory"):
        filesystem.ensure_root()
    project_root.rmdir()

    library_root = tmp_path / "projects"
    existing_filesystem = ProjectFilesystem(library_root / "project-1")
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(library_root), str(outside)],
        check=True,
        capture_output=True,
    )
    with pytest.raises(ValueError, match="reparse point"):
        ProjectFilesystemFactory(library_root).for_project("project-1")
    with pytest.raises(ValueError, match="real directory"):
        existing_filesystem.ensure_root()
    library_root.rmdir()

    filesystem.ensure_root()
    lock_directory = tmp_path / ".knoggin-file-locks"
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(lock_directory), str(outside)],
        check=True,
        capture_output=True,
    )
    with pytest.raises(ValueError, match="real directory"):
        filesystem.acquire_mutation_lease()
    assert not list(outside.glob("*.lock"))


def test_project_filesystem_rejects_a_link_above_the_library_root(tmp_path):
    outside = tmp_path / "outside"
    library_root = outside / "projects"
    library_root.mkdir(parents=True)
    linked_ancestor = tmp_path / "linked-ancestor"
    os.symlink(outside, linked_ancestor, target_is_directory=True)
    redirected_library = linked_ancestor / "projects"

    with pytest.raises(ValueError, match="ancestor must be a real directory"):
        ProjectFilesystemFactory(redirected_library).for_project("project-1")
    with pytest.raises(ValueError, match="ancestor must be a real directory"):
        ProjectFilesystem(redirected_library / "project-1")
    assert not (library_root / "project-1").exists()


def test_existing_project_filesystem_rechecks_ancestor_before_access(tmp_path):
    ancestor = tmp_path / "ancestor"
    project_root = ancestor / "projects" / "project-1"
    filesystem = ProjectFilesystem(project_root)
    filesystem.ensure_root()
    ancestor.rename(tmp_path / "original-ancestor")
    outside = tmp_path / "outside"
    outside.mkdir()
    os.symlink(outside, ancestor, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink|directory"):
        filesystem.ensure_root()
    with pytest.raises(ValueError, match="symlink|directory|reparse point"):
        filesystem.acquire_mutation_lease()
    assert not list(outside.rglob("*.lock"))


@pytest.mark.skipif(os.name != "nt", reason="Windows junction behavior")
def test_project_filesystem_rejects_a_junction_above_the_library_root(tmp_path):
    outside = tmp_path / "outside"
    library_root = outside / "projects"
    library_root.mkdir(parents=True)
    junction = tmp_path / "junction-ancestor"
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(outside)],
        check=True,
        capture_output=True,
    )
    redirected_library = junction / "projects"

    with pytest.raises(ValueError, match="ancestor must be a real directory"):
        ProjectFilesystemFactory(redirected_library).for_project("project-1")
    with pytest.raises(ValueError, match="ancestor must be a real directory"):
        ProjectFilesystem(redirected_library / "project-1")
    assert not (library_root / "project-1").exists()


def test_project_filesystem_factory_creates_isolated_project_roots(tmp_path):
    factory = ProjectFilesystemFactory(tmp_path / "projects")

    first = factory.for_project("project-1")
    second = factory.for_project("project-2")
    first.write_bytes("notes.md", b"first")

    assert first.root != second.root
    assert not second.root.exists()
    with pytest.raises(ValueError, match="path component"):
        factory.for_project("../outside")


def test_project_filesystem_factory_removes_only_the_owned_project_tree(tmp_path):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    filesystem = factory.for_project("project-1")
    filesystem.write_bytes("workspace/notes.md", b"owned workspace file")
    filesystem.write_bytes("documents/source.md", b"owned document")

    outside_file = tmp_path / "outside.txt"
    outside_file.write_text("keep me")
    outside_directory = tmp_path / "outside-directory"
    outside_directory.mkdir()
    outside_child = outside_directory / "keep.md"
    outside_child.write_text("keep me too")
    os.symlink(outside_file, filesystem.root / "outside-link.txt")
    os.symlink(outside_directory, filesystem.root / "outside-link-dir")

    assert factory.remove_project_directory("project-1") is True
    assert not os.path.lexists(filesystem.root)
    assert outside_file.read_text() == "keep me"
    assert outside_child.read_text() == "keep me too"
    assert factory.remove_project_directory("project-1") is False

    linked_project = factory.library_root / "linked-project"
    os.symlink(outside_directory, linked_project)

    assert factory.remove_project_directory("linked-project") is True
    assert not os.path.lexists(linked_project)
    assert outside_child.read_text() == "keep me too"


@pytest.mark.skipif(os.name != "nt", reason="Windows junction behavior")
def test_project_removal_does_not_follow_windows_junctions(tmp_path):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    filesystem = factory.for_project("project-1")
    filesystem.write_bytes("notes.md", b"owned")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.md").write_text("keep")

    nested_junction = filesystem.root / "linked-directory"
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(nested_junction), str(outside)],
        check=True,
        capture_output=True,
    )
    assert nested_junction.is_junction()

    assert factory.remove_project_directory("project-1") is True
    assert not filesystem.root.exists()
    assert (outside / "keep.md").read_text() == "keep"

    project_junction = factory.library_root / "linked-project"
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(project_junction), str(outside)],
        check=True,
        capture_output=True,
    )
    assert project_junction.is_junction()

    assert factory.remove_project_directory("linked-project") is True
    assert not project_junction.exists()
    assert (outside / "keep.md").read_text() == "keep"


@pytest.mark.parametrize("project_id", [".", ".."])
def test_project_removal_rejects_dot_paths_and_preserves_siblings(tmp_path, project_id):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    sibling = factory.for_project("other-project")
    sibling.write_bytes("keep.txt", b"preserve")

    with pytest.raises(ValueError, match="path component"):
        factory.remove_project_directory(project_id)

    assert sibling.read_bytes("keep.txt") == b"preserve"


def test_project_filesystem_factory_rejects_a_symlinked_library_root(tmp_path):
    outside_directory = tmp_path / "outside-directory"
    outside_directory.mkdir()
    linked_root = tmp_path / "projects"
    os.symlink(outside_directory, linked_root)
    factory = ProjectFilesystemFactory(linked_root)

    with pytest.raises(ValueError, match="library root"):
        factory.remove_project_directory("project-1")
