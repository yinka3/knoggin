import os

import pytest

from core.knowledge.documents.filesystem import (
    ProjectFilesystem,
    ProjectFilesystemConflictError,
    ProjectFilesystemFactory,
)


def test_project_filesystem_requires_an_absolute_root(tmp_path):
    with pytest.raises(ValueError, match="absolute"):
        ProjectFilesystem("relative-project-root")

    filesystem = ProjectFilesystem(tmp_path / "project")
    created = filesystem.write_bytes("notes/plan.md", b"first")

    assert created.relative_path == "notes/plan.md"
    assert filesystem.read_bytes("notes/plan.md") == b"first"


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
    with pytest.raises(ValueError, match="symlink"):
        filesystem.write_bytes("linked.txt", b"overwrite", overwrite=True)
    with pytest.raises(ValueError, match="symlink"):
        filesystem.write_bytes("linked-dir/new.txt", b"nope")

    assert outside.read_text() == "private"
    assert list(filesystem.iter_files()) == []


def test_project_filesystem_factory_creates_isolated_project_roots(tmp_path):
    factory = ProjectFilesystemFactory(tmp_path / "projects")

    first = factory.for_project("project-1")
    second = factory.for_project("project-2")
    first.write_bytes("notes.md", b"first")

    assert first.root != second.root
    assert not second.root.exists()
    with pytest.raises(ValueError, match="path component"):
        factory.for_project("../outside")
