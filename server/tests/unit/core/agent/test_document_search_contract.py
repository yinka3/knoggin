import pytest

from common.schema.agent.tool_contracts import TOOL_SCHEMAS
from common.schema.source.references import SourceReferenceCandidate
from core.agent.tools.search import SearchTools, _layout_region_locator


class EmptyDocumentService:
    async def list_documents(
        self,
        *,
        path_prefix=None,
        limit=50,
    ):
        return []


class SearchableDocumentService:
    def __init__(self, files, search_results=None):
        self.files = files
        self.search_results = search_results
        self.search_calls = []

    async def list_documents(
        self,
        *,
        path_prefix=None,
        limit=50,
    ):
        return self.files

    async def get_document_info(
        self,
        *,
        document_id=None,
        relative_path=None,
    ):
        return next(item for item in self.files if item["document_id"] == document_id)

    async def search(
        self,
        query,
        *,
        n_results=5,
        document_filter=None,
        relative_path=None,
        path_prefix=None,
    ):
        self.search_calls.append(
            {
                "query": query,
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
        path_prefix=None,
        limit=50,
    ):
        self.calls.append(("list_documents", path_prefix, limit))
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
        document_id=None,
        relative_path=None,
    ):
        self.calls.append(("get_document_info", document_id, relative_path))
        return {
            "document_id": document_id or "file-1",
            "relative_path": relative_path or "docs/notes.md",
        }

    async def read_document(
        self,
        *,
        document_id=None,
        relative_path=None,
        start_line=1,
        end_line=None,
    ):
        self.calls.append(
            ("read_document", document_id, relative_path, start_line, end_line)
        )
        return self.read_result or {
            "document_id": document_id or "file-1",
            "document_name": "notes.md",
            "relative_path": relative_path or "docs/notes.md",
            "chunk_index": f"lines:{start_line}-{end_line or 3}",
            "content": "2: alpha\n3: beta",
        }


def _source_result(**overrides):
    result = {
        "content": "Exact stored passage.",
        "document_id": "doc_abc123",
        "project_id": "project-1",
        "parse_snapshot_id": "snapshot-1",
        "content_hash": "a" * 64,
        "document_name": "report.pdf",
        "relative_path": "reports/report.pdf",
        "extension": ".pdf",
        "locator": {
            "kind": "layout_region",
            "page": 2,
            "element_type": "paragraph",
            "extraction_method": "native_text",
            "bbox": {"left": 10, "bottom": 20, "right": 110, "top": 60},
            "text_start": 0,
            "text_end": 21,
        },
    }
    result.update(overrides)
    return result


def test_document_source_context_preserves_valid_pdf_layout_provenance():
    source = SearchTools._document_source_context(_source_result())

    assert source is not None
    assert source["source_kind"] == "pdf_document"
    assert source["locator"] == {
        "kind": "layout_region",
        "page": 2,
        "element_type": "paragraph",
        "extraction_method": "native_text",
        "coordinate_unit": "pdf_points",
        "coordinate_origin": "bottom_left",
        "bbox": {"left": 10, "bottom": 20, "right": 110, "top": 60},
        "text_start": 0,
        "text_end": 21,
    }


@pytest.mark.parametrize(
    "locator",
    [
        None,
        {"kind": "layout_region"},
        {
            "kind": "layout_region",
            "page": 1,
            "element_type": "paragraph",
            "extraction_method": "native_text",
            "bbox": "not-coordinates",
        },
        {
            "kind": "layout_region",
            "page": 1,
            "element_type": "paragraph",
            "extraction_method": "native_text",
            "bbox": {"left": 10, "bottom": 0, "right": 5, "top": 20},
        },
        {
            "kind": "layout_region",
            "page": 1,
            "element_type": "paragraph",
            "extraction_method": "native_text",
            "text_start": 9,
            "text_end": 4,
        },
        {"kind": "text_lines", "start_line": 1, "end_line": 2},
    ],
)
def test_document_source_context_rejects_unreliable_pdf_locators(locator):
    assert SearchTools._document_source_context(_source_result(locator=locator)) is None


@pytest.mark.parametrize(
    "locator",
    [
        "not-a-locator",
        {
            "kind": "layout_region",
            "page": 1,
            "element_type": "paragraph",
            "extraction_method": "native_text",
            "bbox": {"left": "10", "bottom": 0, "right": 20, "top": 30},
        },
    ],
)
def test_layout_region_normalization_rejects_malformed_boundaries(locator):
    assert _layout_region_locator(locator) is None


@pytest.mark.parametrize(
    ("locator", "expected"),
    [
        ({"kind": "layout_region", "page": 4}, (4, 1, None)),
        (
            {"kind": "code_lines", "start_line": 5, "end_line": 8},
            (None, 5, 8),
        ),
        ({"kind": "csv_rows", "start_row": 2, "end_row": 6}, (None, 2, 6)),
        ({"kind": "unknown"}, (None, 1, None)),
    ],
)
def test_request_selection_defaults_follow_the_selected_locator(locator, expected):
    tools = SearchTools()
    tools.document_focus = {
        "mode": "request",
        "target_type": "document",
        "selection": {"locator": locator},
    }

    assert tools._request_selection_defaults(
        page_number=None,
        start_line=1,
        end_line=None,
    ) == expected



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
            "docs",
            10,
        ),
        ("get_document_info", "file-1", None),
        (
            "read_document",
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
        ("get_document_info", "file-1", None),
        ("read_document", "file-1", None, 1, None),
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
        ("read_document", "file-1", None, 1, None),
    ]


@pytest.mark.no_network
async def test_request_document_focus_forces_search_to_the_selected_document():
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
                "original_name": "other.md",
                "relative_path": "docs/other.md",
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
        "parse_snapshot_id": "snapshot-1",
        "chunk_index": 3,
        "content": "Revenue grew 18% year over year.",
        "layout_region": {
            "kind": "layout_region",
            "page": 7,
            "element_type": "text",
            "extraction_method": "native_text",
            "coordinate_unit": "pdf_points",
            "coordinate_origin": "bottom_left",
        },
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
        "parse_snapshot_id": "snapshot-1",
        "source_project_id": "project-1",
        "content_hash": content_hash,
        "locator": stored_chunk["layout_region"],
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
        "parse_snapshot_id": "snapshot-1",
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
        "parse_snapshot_id": "snapshot-1",
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
            "parse_snapshot_id": "snapshot-1",
            "locator": {
                "kind": "text_lines",
                "start_line": 3,
                "end_line": 4,
            },
        },
    }

    await tools.read_document()

    assert tools.document_service.calls == [
        ("read_document", "file-1", None, 3, 4)
    ]

    await tools.read_document(start_line=8, end_line=9)
    assert tools.document_service.calls[-1] == (
        "read_document",
        "file-1",
        None,
        8,
        9,
    )


@pytest.mark.no_network
async def test_search_documents_adds_docling_markdown_source_context():
    content_hash = "d" * 64
    stored_chunk = {
        "document_id": "file-1",
        "project_id": "project-1",
        "original_name": "outline.docx",
        "relative_path": "docs/outline.docx",
        "extension": ".docx",
        "content_hash": content_hash,
        "parse_snapshot_id": "snapshot-1",
        "chunk_index": 3,
        "content": "Architecture\nThe worker stores each passage.",
        "locator": {
            "kind": "text_lines",
            "start_line": 1,
            "end_line": 2,
            "section_path": ["Architecture"],
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
            "parse_snapshot_id": "snapshot-1",
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
        "parse_snapshot_id": "snapshot-1",
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
        "parse_snapshot_id": "snapshot-1",
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
        ("list_documents", "src", 50),
        ("list_documents", "docs", 50),
        ("list_documents", None, 50),
    ]
