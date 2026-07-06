import pytest

from common.schema.tool_schema import TOOL_SCHEMAS
from core.agent.tools.search import SearchTools


class EmptyDocumentService:
    def __init__(self):
        self.session_ids = []

    async def list_documents(
        self,
        *,
        session_id=None,
        folder_root_id=None,
        path_prefix=None,
        visibility_scope=None,
        limit=50,
    ):
        self.session_ids.append(session_id)
        return []


class SearchableDocumentService:
    def __init__(self, files):
        self.files = files
        self.search_calls = []

    async def list_documents(
        self,
        *,
        session_id=None,
        folder_root_id=None,
        path_prefix=None,
        visibility_scope=None,
        limit=50,
    ):
        return self.files

    async def get_document_info(
        self,
        *,
        session_id=None,
        document_id=None,
        relative_path=None,
    ):
        return next(
            item
            for item in self.files
            if item["document_id"] == document_id
        )

    async def search(
        self,
        query,
        *,
        session_id=None,
        n_results=5,
        document_filter=None,
        folder_root_id=None,
        relative_path=None,
        path_prefix=None,
    ):
        self.search_calls.append(
            {
                "query": query,
                "session_id": session_id,
                "n_results": n_results,
                "document_filter": document_filter,
                "folder_root_id": folder_root_id,
                "relative_path": relative_path,
                "path_prefix": path_prefix,
            }
        )
        return [
            {
                "document_id": document_filter or "file-1",
                "document_name": "notes.md",
                "relative_path": "docs/notes.md",
                "chunk_index": 0,
                "content": "alpha",
                "score": 0.9,
            }
        ]


class ReadOnlyDocumentService:
    def __init__(self):
        self.calls = []

    async def list_documents(
        self,
        *,
        session_id=None,
        folder_root_id=None,
        path_prefix=None,
        visibility_scope=None,
        limit=50,
    ):
        self.calls.append(
            (
                "list_documents",
                session_id,
                folder_root_id,
                path_prefix,
                visibility_scope,
                limit,
            )
        )
        return [
            {
                "document_id": "file-1",
                "relative_path": "docs/notes.md",
                "status": "indexed",
            }
        ]

    async def get_document_info(
        self,
        *,
        session_id=None,
        document_id=None,
        relative_path=None,
    ):
        self.calls.append(
            ("get_document_info", session_id, document_id, relative_path)
        )
        return {
            "document_id": document_id or "file-1",
            "relative_path": relative_path or "docs/notes.md",
        }

    async def read_document(
        self,
        *,
        session_id=None,
        document_id=None,
        relative_path=None,
        start_line=1,
        end_line=None,
    ):
        self.calls.append(
            (
                "read_document",
                session_id,
                document_id,
                relative_path,
                start_line,
                end_line,
            )
        )
        return {
            "document_id": document_id or "file-1",
            "document_name": "notes.md",
            "relative_path": relative_path or "docs/notes.md",
            "chunk_index": f"lines:{start_line}-{end_line or 3}",
            "content": "2: alpha\n3: beta",
        }

    async def list_folder_uploads(
        self,
        *,
        session_id=None,
        visibility_scope=None,
        limit=25,
    ):
        self.calls.append(
            (
                "list_folder_uploads",
                session_id,
                visibility_scope,
                limit,
            )
        )
        return [{"folder_root_id": "folder-1", "folder_name": "repo"}]

    async def get_folder_upload_summary(
        self,
        *,
        folder_root_id,
        session_id=None,
        path_prefix=None,
    ):
        self.calls.append(
            (
                "get_folder_upload_summary",
                session_id,
                folder_root_id,
                path_prefix,
            )
        )
        return {
            "folder_root_id": folder_root_id,
            "folder_name": "repo",
            "document_count": 1,
        }

    async def list_folder_tree(
        self,
        *,
        folder_root_id,
        session_id=None,
        path_prefix=None,
        max_depth=3,
    ):
        self.calls.append(
            (
                "list_folder_tree",
                session_id,
                folder_root_id,
                path_prefix,
                max_depth,
            )
        )
        return [
            {
                "name": "docs",
                "relative_path": "docs",
                "type": "directory",
                "children": [],
            }
        ]


@pytest.mark.no_network
def test_folder_document_tool_schemas_expose_expected_filters():
    schemas = {
        schema["function"]["name"]: schema["function"]
        for schema in TOOL_SCHEMAS
    }

    assert {
        "list_documents",
        "list_folder_uploads",
        "get_folder_upload_summary",
        "list_folder_tree",
        "search_documents",
    }.issubset(schemas)
    assert set(
        schemas["list_documents"]["parameters"]["properties"]
    ) == {
        "folder_root_id",
        "path_prefix",
        "visibility_scope",
        "limit",
        "use_focus",
    }
    assert set(
        schemas["search_documents"]["parameters"]["properties"]
    ) == {
        "query",
        "document_name",
        "relative_path",
        "path_prefix",
        "folder_root_id",
        "limit",
        "use_focus",
    }
    assert schemas["list_folder_tree"]["parameters"]["required"] == []


@pytest.mark.no_network
async def test_search_documents_reports_project_empty_state():
    tools = SearchTools()
    tools.document_service = EmptyDocumentService()
    tools.session_id = "session-1"

    assert await tools.search_documents("alpha") == [
        {"error": "No indexed documents available in this project"}
    ]
    assert tools.document_service.session_ids == ["session-1"]


@pytest.mark.no_network
async def test_search_documents_passes_session_and_exact_path_filter():
    document_service = SearchableDocumentService(
        [
            {
                "document_id": "file-1",
                "original_name": "notes.md",
                "relative_path": "docs/notes.md",
                "status": "indexed",
            }
        ]
    )
    tools = SearchTools()
    tools.document_service = document_service
    tools.session_id = "session-1"

    results = await tools.search_documents(
        "alpha",
        document_name="docs/notes.md",
        limit=4,
    )

    assert results[0]["content"] == "alpha"
    assert document_service.search_calls == [
        {
            "query": "alpha",
            "session_id": "session-1",
            "n_results": 4,
            "document_filter": "file-1",
            "folder_root_id": None,
            "relative_path": None,
            "path_prefix": None,
        }
    ]


@pytest.mark.no_network
async def test_search_documents_rejects_ambiguous_document_names():
    document_service = SearchableDocumentService(
        [
            {
                "document_id": "file-1",
                "original_name": "notes.md",
                "relative_path": "docs/notes.md",
                "status": "indexed",
            },
            {
                "document_id": "file-2",
                "original_name": "notes.md",
                "relative_path": "archive/notes.md",
                "status": "indexed",
            },
        ]
    )
    tools = SearchTools()
    tools.document_service = document_service
    tools.session_id = "session-1"

    result = await tools.search_documents("alpha", document_name="notes.md")

    assert "ambiguous" in result[0]["error"]
    assert document_service.search_calls == []


@pytest.mark.no_network
async def test_read_only_document_tools_pass_session_scope_and_bounds():
    document_service = ReadOnlyDocumentService()
    tools = SearchTools()
    tools.document_service = document_service
    tools.session_id = "session-1"

    documents = await tools.list_documents(
        folder_root_id="folder-1",
        path_prefix="docs",
        visibility_scope="project",
        limit=10,
    )
    info = await tools.get_document_info(document_id="file-1")
    content = await tools.read_document(
        relative_path="docs/notes.md",
        start_line=2,
        end_line=3,
    )

    assert documents[0]["document_id"] == "file-1"
    assert info["document_id"] == "file-1"
    assert content[0]["content"] == "2: alpha\n3: beta"
    assert document_service.calls == [
        (
            "list_documents",
            "session-1",
            "folder-1",
            "docs",
            "project",
            10,
        ),
        ("get_document_info", "session-1", "file-1", None),
        (
            "read_document",
            "session-1",
            None,
            "docs/notes.md",
            2,
            3,
        ),
    ]


@pytest.mark.no_network
async def test_list_documents_validates_limit():
    tools = SearchTools()
    tools.document_service = ReadOnlyDocumentService()
    tools.session_id = "session-1"

    with pytest.raises(ValueError, match="between 1 and 100"):
        await tools.list_documents(limit=0)


@pytest.mark.no_network
async def test_folder_read_tools_propagate_session_and_filters():
    document_service = ReadOnlyDocumentService()
    tools = SearchTools()
    tools.document_service = document_service
    tools.session_id = "session-1"

    uploads = await tools.list_folder_uploads(
        visibility_scope="session",
        limit=10,
    )
    summary = await tools.get_folder_upload_summary("folder-1")
    tree = await tools.list_folder_tree(
        "folder-1",
        path_prefix="docs",
        max_depth=4,
    )

    assert uploads[0]["folder_root_id"] == "folder-1"
    assert summary["document_count"] == 1
    assert tree[0]["relative_path"] == "docs"
    assert document_service.calls == [
        ("list_folder_uploads", "session-1", "session", 10),
        ("get_folder_upload_summary", "session-1", "folder-1", None),
        ("list_folder_tree", "session-1", "folder-1", "docs", 4),
    ]


@pytest.mark.no_network
async def test_folder_read_tools_validate_bounds():
    tools = SearchTools()
    tools.document_service = ReadOnlyDocumentService()
    tools.session_id = "session-1"

    with pytest.raises(ValueError, match="between 1 and 100"):
        await tools.list_folder_uploads(limit=101)

    with pytest.raises(ValueError, match="between 1 and 10"):
        await tools.list_folder_tree("folder-1", max_depth=11)


@pytest.mark.no_network
async def test_search_documents_passes_folder_and_path_prefix_filters():
    document_service = SearchableDocumentService(
        [
            {
                "document_id": "file-1",
                "original_name": "notes.md",
                "relative_path": "docs/notes.md",
                "status": "indexed",
            }
        ]
    )
    tools = SearchTools()
    tools.document_service = document_service
    tools.session_id = "session-1"

    await tools.search_documents(
        "alpha",
        folder_root_id="folder-1",
        path_prefix="docs",
    )

    assert document_service.search_calls == [
        {
            "query": "alpha",
            "session_id": "session-1",
            "n_results": 5,
            "document_filter": None,
            "folder_root_id": "folder-1",
            "relative_path": None,
            "path_prefix": "docs",
        }
    ]


@pytest.mark.no_network
async def test_search_documents_rejects_conflicting_exact_and_prefix_filters():
    tools = SearchTools()
    tools.document_service = SearchableDocumentService([])
    tools.session_id = "session-1"

    with pytest.raises(ValueError, match="mutually exclusive"):
        await tools.search_documents(
            "alpha",
            document_name="notes.md",
            relative_path="docs/notes.md",
        )

    with pytest.raises(ValueError, match="path_prefix"):
        await tools.search_documents(
            "alpha",
            relative_path="docs/notes.md",
            path_prefix="docs",
        )

    with pytest.raises(ValueError, match="between 1 and 50"):
        await tools.search_documents("alpha", limit=0)


@pytest.mark.no_network
async def test_exact_document_focus_defaults_reads_and_search():
    document_service = SearchableDocumentService(
        [
            {
                "document_id": "file-1",
                "original_name": "notes.md",
                "relative_path": "docs/notes.md",
                "visibility_scope": "project",
                "status": "indexed",
            }
        ]
    )
    tools = SearchTools()
    tools.document_service = document_service
    tools.document_focus = {
        "target_type": "document",
        "document_id": "file-1",
        "relative_path": "docs/notes.md",
    }
    tools.session_id = "session-1"

    documents = await tools.list_documents()
    await tools.search_documents("alpha")
    await tools.search_documents("alpha", use_focus=False)

    assert [item["document_id"] for item in documents] == ["file-1"]
    assert document_service.search_calls[0]["document_filter"] == "file-1"
    assert document_service.search_calls[1]["document_filter"] is None


@pytest.mark.no_network
async def test_exact_document_focus_defaults_info_and_content_reads():
    document_service = ReadOnlyDocumentService()
    tools = SearchTools()
    tools.document_service = document_service
    tools.document_focus = {
        "target_type": "document",
        "document_id": "file-1",
        "relative_path": "docs/notes.md",
    }
    tools.session_id = "session-1"

    await tools.get_document_info()
    await tools.read_document()

    assert document_service.calls == [
        ("get_document_info", "session-1", "file-1", None),
        ("read_document", "session-1", "file-1", None, 1, None),
    ]


@pytest.mark.no_network
async def test_subtree_focus_defaults_filters_and_explicit_values_override():
    document_service = ReadOnlyDocumentService()
    tools = SearchTools()
    tools.document_service = document_service
    tools.document_focus = {
        "target_type": "subtree",
        "folder_root_id": "folder-1",
        "path_prefix": "src",
    }
    tools.session_id = "session-1"

    await tools.list_documents()
    await tools.get_folder_upload_summary()
    await tools.list_folder_tree(path_prefix="tests")
    await tools.list_documents(
        folder_root_id="folder-2",
        path_prefix="docs",
    )
    await tools.list_documents(use_focus=False)

    assert document_service.calls == [
        ("list_documents", "session-1", "folder-1", "src", None, 50),
        (
            "get_folder_upload_summary",
            "session-1",
            "folder-1",
            "src",
        ),
        ("list_folder_tree", "session-1", "folder-1", "tests", 3),
        ("list_documents", "session-1", "folder-2", "docs", None, 50),
        ("list_documents", "session-1", None, None, None, 50),
    ]
