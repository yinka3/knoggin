import pytest

from common.schema.agent.tool_contracts import TOOL_SCHEMAS
from common.schema.source.references import SourceReferenceCandidate
from core.agent.tools.search import SearchTools


class EmptyDocumentService:
    def __init__(self):
        self.session_ids = []

    async def list_documents(
        self,
        *,
        session_id=None,
        path_prefix=None,
        visibility_scope=None,
        limit=50,
    ):
        self.session_ids.append(session_id)
        return []


class SearchableDocumentService:
    def __init__(self, files, search_results=None):
        self.files = files
        self.search_results = search_results
        self.search_calls = []

    async def list_documents(
        self,
        *,
        session_id=None,
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
        return next(item for item in self.files if item["document_id"] == document_id)

    async def search(
        self,
        query,
        *,
        session_id=None,
        n_results=5,
        document_filter=None,
        relative_path=None,
        path_prefix=None,
    ):
        self.search_calls.append(
            {
                "query": query,
                "session_id": session_id,
                "n_results": n_results,
                "document_filter": document_filter,
                "relative_path": relative_path,
                "path_prefix": path_prefix,
            }
        )
        if self.search_results is not None:
            return self.search_results
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
    def __init__(self, read_result=None):
        self.calls = []
        self.read_result = read_result

    async def list_documents(
        self,
        *,
        session_id=None,
        path_prefix=None,
        visibility_scope=None,
        limit=50,
    ):
        self.calls.append(
            (
                "list_documents",
                session_id,
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
        self.calls.append(("get_document_info", session_id, document_id, relative_path))
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
        return self.read_result or {
            "document_id": document_id or "file-1",
            "document_name": "notes.md",
            "relative_path": relative_path or "docs/notes.md",
            "chunk_index": f"lines:{start_line}-{end_line or 3}",
            "content": "2: alpha\n3: beta",
        }



@pytest.mark.no_network
def test_document_tool_schemas_expose_path_filters_without_folder_handles():
    schemas = {
        schema["function"]["name"]: schema["function"] for schema in TOOL_SCHEMAS
    }

    assert {
        "list_documents",
        "search_documents",
    }.issubset(schemas)
    assert set(schemas["list_documents"]["parameters"]["properties"]) == {
        "path_prefix",
        "limit",
        "use_focus",
    }
    assert set(schemas["search_documents"]["parameters"]["properties"]) == {
        "query",
        "document_name",
        "relative_path",
        "path_prefix",
        "limit",
        "use_focus",
    }


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
        path_prefix="docs",
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
            "docs",
            None,
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
async def test_folder_read_tools_are_not_exposed():
    document_service = ReadOnlyDocumentService()
    tools = SearchTools()
    tools.document_service = document_service
    tools.session_id = "session-1"

    assert not hasattr(tools, "list_folder_uploads")
    assert not hasattr(tools, "get_folder_upload_summary")
    assert not hasattr(tools, "list_folder_tree")


@pytest.mark.no_network
async def test_folder_read_tools_have_no_legacy_boundaries():
    tools = SearchTools()
    tools.document_service = ReadOnlyDocumentService()
    tools.session_id = "session-1"

    assert not hasattr(tools, "list_folder_uploads")


@pytest.mark.no_network
async def test_search_documents_passes_path_prefix_filters():
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
        path_prefix="docs",
    )

    assert document_service.search_calls == [
        {
            "query": "alpha",
            "session_id": "session-1",
            "n_results": 5,
            "document_filter": None,
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
async def test_request_document_focus_cannot_be_bypassed_by_tool_arguments():
    document_service = ReadOnlyDocumentService()
    tools = SearchTools()
    tools.document_service = document_service
    tools.document_focus = {
        "mode": "request",
        "target_type": "document",
        "document_id": "file-1",
        "relative_path": "docs/notes.md",
    }
    tools.session_id = "session-1"

    await tools.read_document(document_id="file-1", use_focus=False)
    with pytest.raises(ValueError, match="restricted to the selected document"):
        await tools.read_document(document_id="file-2", use_focus=False)
    with pytest.raises(ValueError, match="restricted to the selected document"):
        await tools.read_document(relative_path="docs/other.md")

    assert document_service.calls == [
        ("read_document", "session-1", "file-1", None, 1, None),
    ]


@pytest.mark.no_network
async def test_request_document_focus_forces_search_to_the_selected_document():
    document_service = SearchableDocumentService(
        [
            {
                "document_id": "file-1",
                "original_name": "notes.md",
                "relative_path": "docs/notes.md",
                "visibility_scope": "project",
                "status": "indexed",
            },
            {
                "document_id": "file-2",
                "original_name": "other.md",
                "relative_path": "docs/other.md",
                "visibility_scope": "project",
                "status": "indexed",
            },
        ]
    )
    tools = SearchTools()
    tools.document_service = document_service
    tools.document_focus = {
        "mode": "request",
        "target_type": "document",
        "document_id": "file-1",
        "relative_path": "docs/notes.md",
    }
    tools.session_id = "session-1"

    with pytest.raises(ValueError, match="restricted to the selected document"):
        await tools.search_documents(
            "alpha",
            relative_path="docs/other.md",
            use_focus=False,
        )

    await tools.search_documents("alpha", use_focus=False)

    assert document_service.search_calls[0]["document_filter"] == "file-1"


@pytest.mark.no_network
async def test_search_documents_adds_source_context_from_the_stored_chunk():
    content_hash = "a" * 64
    stored_chunk = {
        "document_id": "file-1",
        "project_id": "project-1",
        "original_name": "report.pdf",
        "relative_path": "reports/q2.pdf",
        "extension": ".pdf",
        "content_hash": content_hash,
        "chunk_index": 3,
        "content": "Revenue grew 18% year over year.",
        "page_number": 7,
        "status": "indexed",
    }
    document_service = SearchableDocumentService(
        [
            {
                "document_id": "file-1",
                "original_name": "report.pdf",
                "relative_path": "reports/q2.pdf",
                "status": "indexed",
            }
        ],
        search_results=[stored_chunk],
    )
    tools = SearchTools()
    tools.document_service = document_service
    tools.session_id = "session-1"

    results = await tools.search_documents("revenue")

    assert results[0]["content"] == stored_chunk["content"]
    assert results[0]["source_context"] == {
        "source_kind": "pdf_document",
        "document_id": "file-1",
        "source_project_id": "project-1",
        "content_hash": content_hash,
        "locator": {"kind": "pdf_page", "page": 7},
        "excerpt": stored_chunk["content"],
        "metadata": {
            "document_name": "report.pdf",
            "relative_path": "reports/q2.pdf",
            "extension": ".pdf",
            "chunk_index": 3,
        },
    }
    candidate = SourceReferenceCandidate.model_validate(
        {
            **results[0]["source_context"],
            "project_id": "project-1",
            "session_id": "session-1",
            "encounter_kind": "document_search",
            "agent_run_id": "run-1",
            "tool_call_id": "call-1",
            "result_position": 0,
        }
    )
    assert candidate.excerpt == stored_chunk["content"]


@pytest.mark.no_network
async def test_read_document_adds_source_context_from_the_returned_read_range():
    content_hash = "b" * 64
    read_result = {
        "document_id": "file-1",
        "project_id": "project-1",
        "document_name": "notes.md",
        "relative_path": "docs/notes.md",
        "extension": ".md",
        "content_hash": content_hash,
        "chunk_index": "lines:3-4",
        "content": "3: alpha\n4: beta",
        "locator": {
            "kind": "text_lines",
            "start_line": 3,
            "end_line": 4,
            "section_path": ["Results"],
        },
    }

    tools = SearchTools()
    tools.document_service = ReadOnlyDocumentService(read_result=read_result)
    tools.session_id = "session-1"

    results = await tools.read_document(document_id="file-1", start_line=3, end_line=4)

    assert results[0]["source_context"] == {
        "source_kind": "text_document",
        "document_id": "file-1",
        "source_project_id": "project-1",
        "content_hash": content_hash,
        "locator": {
            "kind": "text_lines",
            "start_line": 3,
            "end_line": 4,
            "section_path": ["Results"],
        },
        "excerpt": "3: alpha\n4: beta",
        "metadata": {
            "document_name": "notes.md",
            "relative_path": "docs/notes.md",
            "extension": ".md",
            "chunk_index": "lines:3-4",
        },
    }


@pytest.mark.no_network
async def test_request_document_selection_defaults_reads_to_the_selected_range():
    tools = SearchTools()
    tools.document_service = ReadOnlyDocumentService()
    tools.session_id = "session-1"
    tools.document_focus = {
        "mode": "request",
        "target_type": "document",
        "document_id": "file-1",
        "relative_path": "docs/notes.md",
        "selection": {
            "content_hash": "a" * 64,
            "locator": {
                "kind": "text_lines",
                "start_line": 3,
                "end_line": 4,
            },
        },
    }

    await tools.read_document()

    assert tools.document_service.calls == [
        ("read_document", "session-1", "file-1", None, 3, 4)
    ]

    await tools.read_document(start_line=8, end_line=9)
    assert tools.document_service.calls[-1] == (
        "read_document",
        "session-1",
        "file-1",
        None,
        8,
        9,
    )


@pytest.mark.no_network
async def test_search_documents_adds_docx_paragraph_source_context():
    content_hash = "d" * 64
    stored_chunk = {
        "document_id": "file-1",
        "project_id": "project-1",
        "original_name": "outline.docx",
        "relative_path": "docs/outline.docx",
        "extension": ".docx",
        "content_hash": content_hash,
        "chunk_index": 3,
        "content": "Architecture\nThe worker stores each passage.",
        "locator": {
            "kind": "docx_paragraphs",
            "start_paragraph": 7,
            "end_paragraph": 8,
            "heading_path": ["Architecture"],
        },
    }

    result = SearchTools._with_document_source_context(stored_chunk)

    assert result["source_context"]["source_kind"] == "text_document"
    assert result["source_context"]["locator"] == stored_chunk["locator"]


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("extension", "locator", "expected_source_kind"),
    [
        (
            ".csv",
            {"kind": "csv_rows", "start_row": 1, "end_row": 2},
            "text_document",
        ),
        (
            ".py",
            {
                "kind": "code_lines",
                "start_line": 4,
                "end_line": 5,
                "symbol_name": "Beta",
            },
            "text_document",
        ),
        (
            ".md",
            {
                "kind": "text_lines",
                "start_line": 4,
                "end_line": 5,
                "section_path": ["Overview", "Risks"],
            },
            "text_document",
        ),
        (
            ".ipynb",
            {"kind": "text_lines", "start_line": 1, "end_line": 2},
            "text_document",
        ),
    ],
)
def test_search_documents_adds_exact_source_context_for_each_text_strategy(
    extension,
    locator,
    expected_source_kind,
):
    result = SearchTools._with_document_source_context(
        {
            "document_id": "file-1",
            "project_id": "project-1",
            "original_name": f"source{extension}",
            "relative_path": f"docs/source{extension}",
            "extension": extension,
            "content_hash": "e" * 64,
            "chunk_index": 2,
            "content": "The exact searchable passage.",
            "locator": locator,
        }
    )

    assert result["source_context"] == {
        "source_kind": expected_source_kind,
        "document_id": "file-1",
        "source_project_id": "project-1",
        "content_hash": "e" * 64,
        "locator": locator,
        "excerpt": "The exact searchable passage.",
        "metadata": {
            "document_name": f"source{extension}",
            "relative_path": f"docs/source{extension}",
            "extension": extension,
            "chunk_index": 2,
        },
    }


@pytest.mark.no_network
def test_ocr_image_results_remain_searchable_without_unreliable_source_context():
    result = {
        "document_id": "image-1",
        "project_id": "project-1",
        "original_name": "scan.png",
        "relative_path": "scans/scan.png",
        "extension": ".png",
        "content_hash": "f" * 64,
        "content": "OCR passage",
        "start_line": 1,
        "end_line": 1,
    }

    assert SearchTools._with_document_source_context(result) == result


@pytest.mark.no_network
@pytest.mark.parametrize(
    "result",
    [
        {
            "document_id": "file-1",
            "document_name": "legacy.txt",
            "relative_path": "docs/legacy.txt",
            "extension": ".txt",
            "content_hash": "c" * 64,
            "content": "A locator-less legacy chunk.",
        },
        {
            "document_id": "file-1",
            "document_name": "outline.docx",
            "relative_path": "docs/outline.docx",
            "extension": ".docx",
            "content_hash": "c" * 64,
            "content": "A DOCX paragraph.",
            "start_line": 1,
            "end_line": 1,
        },
        {
            "document_id": "file-1",
            "document_name": "empty.txt",
            "relative_path": "docs/empty.txt",
            "extension": ".txt",
            "content_hash": "c" * 64,
            "content": "   ",
            "start_line": 1,
            "end_line": 1,
        },
    ],
)
def test_document_source_context_excludes_unsupported_or_unlocatable_results(result):
    assert SearchTools._with_document_source_context(result) == result


@pytest.mark.no_network
async def test_subtree_focus_defaults_filters_and_explicit_values_override():
    document_service = ReadOnlyDocumentService()
    tools = SearchTools()
    tools.document_service = document_service
    tools.document_focus = {
        "target_type": "subtree",
        "path_prefix": "src",
    }
    tools.session_id = "session-1"

    await tools.list_documents()
    await tools.list_documents(path_prefix="docs")
    await tools.list_documents(use_focus=False)

    assert document_service.calls == [
        ("list_documents", "session-1", "src", None, 50),
        ("list_documents", "session-1", "docs", None, 50),
        ("list_documents", "session-1", None, None, 50),
    ]
