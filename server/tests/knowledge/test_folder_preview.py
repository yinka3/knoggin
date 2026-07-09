import hashlib

import pytest

from common.schema.document import FolderScanSettings, FolderUploadEntry
from core.knowledge.documents import DocumentService


class ForbiddenDependency:
    def __getattr__(self, name):
        raise AssertionError(f"preview touched forbidden dependency: {name}")


class DefaultSettingsPostgres:
    async def fetch_all(self, query, params=None):
        assert "project_document_scan_settings" in query
        return []


@pytest.fixture
def preview_service(tmp_path):
    return DocumentService(
        project_id="project-1",
        postgres_client=DefaultSettingsPostgres(),
        storage_root=tmp_path,
        embedding_service=ForbiddenDependency(),
    )


def entry(path: str, content: bytes = b"content") -> FolderUploadEntry:
    return FolderUploadEntry(relative_path=path, content=content)


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_normalizes_paths_orders_entries_and_does_not_write(
    preview_service,
    tmp_path,
):
    preview = await preview_service.preview_folder(
        folder_name="repo",
        entries=[
            entry(r"src\z.py", b"z"),
            entry("README.md", b"readme"),
            entry("src/a.py", b"a"),
        ],
    )

    assert [item.relative_path for item in preview.included] == [
        "README.md",
        "src/a.py",
        "src/z.py",
    ]
    assert preview.included[0].content_hash == hashlib.sha256(
        b"readme"
    ).hexdigest()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    "unsafe_path",
    [
        "",
        "../secret.txt",
        "/absolute.txt",
        r"C:\secret.txt",
        "bad\x00name.txt",
    ],
)
async def test_preview_rejects_unsafe_paths(preview_service, unsafe_path):
    with pytest.raises(ValueError):
        await preview_service.preview_folder(
            folder_name="repo",
            entries=[entry(unsafe_path)],
        )


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_rejects_duplicate_normalized_paths(preview_service):
    with pytest.raises(ValueError, match="duplicate normalized"):
        await preview_service.preview_folder(
            folder_name="repo",
            entries=[entry("src/a.py"), entry(r"src\a.py")],
        )


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_excludes_hidden_and_noisy_directories(preview_service):
    preview = await preview_service.preview_folder(
        folder_name="repo",
        entries=[
            entry(".notes/todo.md"),
            entry("node_modules/pkg/index.js"),
            entry("src/main.py"),
        ],
    )

    assert [item.relative_path for item in preview.included] == ["src/main.py"]
    assert {
        item.relative_path: item.reason for item in preview.excluded
    } == {
        ".notes/todo.md": "hidden_path",
        "node_modules/pkg/index.js": "default_directory_ignore",
    }
    assert preview.summary.excluded_directory_count == 2


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_accepts_large_source_by_lines_not_line_count(
    preview_service,
):
    content = ("\n".join(f"print({index})" for index in range(2000))).encode()

    preview = await preview_service.preview_folder(
        folder_name="repo",
        entries=[entry("src/large.py", content)],
    )

    assert [item.relative_path for item in preview.included] == [
        "src/large.py"
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_reports_hard_content_exclusions(preview_service):
    preview = await preview_service.preview_folder(
        folder_name="repo",
        entries=[
            entry(".env", b"TOKEN=secret"),
            entry("archive.zip", b"PK\x03\x04payload"),
            entry("program", b"\x7fELFpayload"),
            entry("image.png", b"\x89PNG"),
            entry("movie.mp4", b"video"),
            entry("binary.txt", b"alpha\x00beta"),
            entry("notes.txt", b"alpha"),
        ],
        force_include_paths=[
            ".env",
            "archive.zip",
            "program",
            "image.png",
            "movie.mp4",
            "binary.txt",
        ],
    )

    assert [item.relative_path for item in preview.included] == ["notes.txt"]
    assert {
        item.relative_path: (item.reason, item.overridable)
        for item in preview.excluded
    } == {
        ".env": ("blocked_sensitive_file", False),
        "archive.zip": ("archive_file", False),
        "binary.txt": ("binary_file", False),
        "image.png": ("image_file", False),
        "movie.mp4": ("video_file", False),
        "program": ("executable_file", False),
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_does_not_treat_docx_zip_container_as_archive(
    preview_service,
):
    preview = await preview_service.preview_folder(
        folder_name="docs",
        entries=[entry("report.docx", b"PK\x03\x04docx container")],
    )

    assert [item.relative_path for item in preview.included] == [
        "report.docx"
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_applies_root_and_nested_gitignore_negation(
    preview_service,
):
    preview = await preview_service.preview_folder(
        folder_name="repo",
        entries=[
            entry(".gitignore", b"*.dropme\n!keep.dropme\nnested/*.skipme\n"),
            entry("drop.dropme"),
            entry("keep.dropme"),
            entry("nested/.gitignore", b"!keep.skipme\n"),
            entry("nested/drop.skipme"),
            entry("nested/keep.skipme"),
        ],
    )

    assert [item.relative_path for item in preview.included] == [
        "keep.dropme",
        "nested/keep.skipme",
    ]
    reasons = {
        item.relative_path: item.reason for item in preview.excluded
    }
    assert reasons["drop.dropme"] == "gitignore"
    assert reasons["nested/drop.skipme"] == "gitignore"
    assert reasons[".gitignore"] == "hidden_path"
    assert reasons["nested/.gitignore"] == "hidden_path"


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_applies_project_filters_and_safe_overrides(
    preview_service,
):
    settings = FolderScanSettings(
        ignored_patterns=["generated/*"],
        allowed_extensions={".py"},
        blocked_file_names={"blocked.py"},
        blocked_directory_names={"vendor"},
    )
    preview = await preview_service.preview_folder(
        folder_name="repo",
        entries=[
            entry(".hidden.py"),
            entry("generated/output.py"),
            entry("notes.md"),
            entry("blocked.py"),
            entry("vendor/library.py"),
        ],
        settings=settings,
        force_include_paths=[
            ".hidden.py",
            "generated/output.py",
            "notes.md",
            "blocked.py",
            "vendor/library.py",
        ],
    )

    assert [item.relative_path for item in preview.included] == [
        ".hidden.py",
        "blocked.py",
        "generated/output.py",
        "notes.md",
        "vendor/library.py",
    ]
    assert preview.excluded == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_applies_depth_size_count_and_total_limits(
    preview_service,
):
    settings = FolderScanSettings(
        max_document_size_bytes=3,
        max_total_size_bytes=4,
        max_file_count=2,
        max_folder_depth=1,
    )
    preview = await preview_service.preview_folder(
        folder_name="repo",
        entries=[
            entry("a.txt", b"aa"),
            entry("b.txt", b"bb"),
            entry("c.txt", b"cc"),
            entry("deep/nested/d.txt", b"d"),
            entry("large.txt", b"four"),
        ],
        settings=settings,
    )

    assert [item.relative_path for item in preview.included] == [
        "a.txt",
        "b.txt",
    ]
    reasons = {
        item.relative_path: item.reason for item in preview.excluded
    }
    assert reasons == {
        "c.txt": "file_count_limit",
        "deep/nested/d.txt": "folder_too_deep",
        "large.txt": "document_too_large",
    }
    assert preview.summary.included_bytes == 4
    assert preview.summary.reason_counts == {
        "document_too_large": 1,
        "file_count_limit": 1,
        "folder_too_deep": 1,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_total_size_limit_is_not_overridable(preview_service):
    settings = FolderScanSettings(
        max_document_size_bytes=10,
        max_total_size_bytes=3,
        max_file_count=10,
    )
    preview = await preview_service.preview_folder(
        folder_name="repo",
        entries=[entry("a.txt", b"aa"), entry("b.txt", b"bb")],
        settings=settings,
        force_include_paths=["b.txt"],
    )

    assert [item.relative_path for item in preview.included] == ["a.txt"]
    assert preview.excluded[0].reason == "total_size_limit"
    assert preview.excluded[0].overridable is False


def test_folder_scan_default_limits():
    settings = FolderScanSettings()

    assert settings.max_file_count == 1000
    assert settings.max_total_size_bytes == 500 * 1024 * 1024
    assert settings.max_folder_depth == 20
    assert settings.max_document_size_bytes == 25 * 1024 * 1024
