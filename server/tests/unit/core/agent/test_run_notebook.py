from copy import deepcopy

import pytest
from jinja2 import StrictUndefined, UndefinedError

from core.agent.notebook import RunNotebook
from core.agent.notebook_renderer import (
    _passage_text,
    _result_locator,
    _source_continuation,
    notebook_environment,
    render_notebook,
)


def test_empty_notebook_renders_a_stable_minimal_view():
    assert render_notebook(RunNotebook()) == "RUN NOTEBOOK"


def test_notebook_renderer_is_strict_localized_and_read_only():
    notebook = RunNotebook()
    notebook.apply(
        "search_entity",
        {
            "data": [
                {"id": 24, "canonical_name": "Sarah Johnson", "project_id": "project-a"}
            ]
        },
    )
    notebook.apply(
        "episode_check",
        {
            "data": {
                "resolution": "semantic",
                "results": [
                    {"episodes": [{"episode_id": "ep-secret", "summary": "Changed"}]}
                ],
            }
        },
    )
    before = deepcopy(notebook.as_dict())

    rendered = render_notebook(notebook)

    assert "E1 Sarah Johnson" in rendered
    assert "ep_epsecr: Changed" in rendered
    assert '"episode_id": "ep_epsecr"' in rendered
    assert "ep-secret" not in rendered
    assert "project-a" not in rendered
    assert notebook.as_dict() == before

    environment = notebook_environment()
    assert environment.undefined is StrictUndefined
    with pytest.raises(UndefinedError):
        environment.from_string("{{ missing_value }}").render()


def test_notebook_renderer_preserves_cross_project_records_without_duplicate_ids():
    notebook = RunNotebook()
    notebook.apply(
        "search_messages",
        {
            "data": [
                {"id": "msg-1", "project_id": "project-a", "message": "A"},
                {"id": "msg-1", "project_id": "project-b", "message": "B"},
            ]
        },
    )

    rendered = render_notebook(notebook)

    assert len(notebook.section_items("messages")) == 2
    assert "M1: A" in rendered
    assert "M2: B" in rendered
    assert "project-a" not in rendered
    assert "project-b" not in rendered


def test_path_observation_handles_are_retained_and_expand_only_on_demand():
    notebook = RunNotebook()
    bundle = {
        "subject": {"kind": "relationship_observation", "identifier": "17"},
        "nodes": [
            {
                "pointer": {
                    "kind": "relationship_observation",
                    "identifier": "17",
                },
                "label": "works at",
                "status": "active",
            },
            {
                "pointer": {
                    "kind": "context_block",
                    "identifier": "00000000-0000-0000-0000-000000000017",
                },
                "excerpt": "Ada joined Acme.",
            },
            {
                "pointer": {
                    "kind": "source_reference",
                    "identifier": "00000000-0000-0000-0000-000000000018",
                },
                "source_kind": "text_document",
                "locator": {"kind": "text_lines", "start_line": 4, "end_line": 6},
                "excerpt": "Ada joined Acme.",
            },
        ],
        "edges": [],
        "total_nodes": 3,
        "total_edges": 0,
        "nodes_truncated": False,
        "edges_truncated": False,
        "state_token": "a" * 64,
    }

    notebook.apply(
        "find_path",
        {
            "data": [
                {
                    "entity_a": "Ada",
                    "entity_b": "Acme",
                    "evidence": [bundle],
                }
            ]
        },
    )

    initial = render_notebook(notebook)

    assert "Paths:" in initial
    assert "(support: O1)" in initial
    assert "read_observation_evidence" in initial
    assert "Observation support (expanded on demand):" not in initial
    assert "Ada joined Acme." not in initial

    notebook.apply("read_observation_evidence", {"data": bundle})
    expanded = render_notebook(notebook)

    assert "Observation support (expanded on demand):" in expanded
    assert "O1 observation 17 (active)" in expanded
    assert "context blocks: Ada joined Acme." in expanded
    assert "text document [lines 4-6]: Ada joined Acme." in expanded


def test_notebook_renderer_keeps_long_text_evidence_and_source_continuations():
    """Useful evidence after the old 320-character display cap stays visible."""

    notebook = RunNotebook()
    leading_context = "introductory context " * 24
    notebook.apply(
        "search_messages",
        {
            "data": [
                {
                    "id": "message-1",
                    "message": f"{leading_context}MESSAGE_DECISION_IS_CANCELLED",
                }
            ]
        },
    )
    notebook.apply(
        "read_document",
        {
            "data": [
                {
                    "document_id": "doc_abc123",
                    "document_name": "decision.md",
                    "content": f"{leading_context}DOCUMENT_DECISION_IS_CANCELLED",
                    "locator": {
                        "kind": "text_lines",
                        "start_line": 20,
                        "end_line": 40,
                    },
                    "end_line": 40,
                    "total_lines": 80,
                    "truncated": True,
                }
            ]
        },
    )
    notebook.apply(
        "read_web_page",
        {
            "data": [
                {
                    "title": "Decision record",
                    "url": "https://example.test/decision",
                    "content": f"{leading_context}WEB_DECISION_IS_CANCELLED",
                    "start_line": 50,
                    "end_line": 90,
                    "total_lines": 140,
                    "has_more": True,
                    "next_start_line": 91,
                }
            ]
        },
    )

    rendered = render_notebook(notebook)

    assert "MESSAGE_DECISION_IS_CANCELLED" in rendered
    assert "DOCUMENT_DECISION_IS_CANCELLED" in rendered
    assert "WEB_DECISION_IS_CANCELLED" in rendered
    assert "decision.md:" in rendered
    assert "[document: doc_abc123]" in rendered
    assert "[lines 20-40]" in rendered
    assert (
        "continuation: more source text is available from line 41; reread a "
        "narrower range or use a targeted query."
    ) in rendered
    assert (
        "continuation: more source text is available from line 91; reread a "
        "narrower range or use a targeted query."
    ) in rendered


def test_notebook_renderer_marks_a_clipped_read_passage_for_follow_up():
    notebook = RunNotebook()
    notebook.apply(
        "read_document",
        {
            "data": [
                {
                    "document_id": "doc_abc123",
                    "document_name": "long-decision.md",
                    "content": ("leading evidence " * 90) + "TAIL_IS_NOT_RENDERED",
                    "locator": {
                        "kind": "text_lines",
                        "start_line": 1,
                        "end_line": 1,
                    },
                    "end_line": 1,
                    "total_lines": 1,
                    "truncated": False,
                }
            ]
        },
    )

    rendered = render_notebook(notebook)

    assert "long-decision.md:" in rendered
    assert "[lines 1-1]" in rendered
    assert "TAIL_IS_NOT_RENDERED" not in rendered
    assert (
        "continuation: the displayed passage is clipped; reread a narrower "
        "range or use a targeted query."
    ) in rendered


def test_notebook_renderer_handles_missing_passages_and_available_continuations():
    notebook = RunNotebook()
    notebook.apply(
        "read_document",
        {
            "data": [
                {
                    "document_id": "doc_empty",
                    "document_name": "empty.md",
                    "content": None,
                    "locator": {"kind": "csv_rows", "start_row": 2, "end_row": 4},
                    "has_more": True,
                },
                {
                    "document_id": "doc_pdf",
                    "document_name": "report.pdf",
                    "content": "A bounded PDF passage.",
                    "locator": {"kind": "layout_region", "page": 3},
                },
            ]
        },
    )

    rendered = render_notebook(notebook)

    assert "None" not in rendered
    assert "[rows 2-4]" in rendered
    assert "[page 3]" in rendered
    assert (
        "continuation: more source text is available; reread a narrower range "
        "or use a targeted query."
    ) in rendered


def test_notebook_renderer_defensively_formats_optional_source_metadata():
    assert _passage_text(None) == ("", False)
    assert _result_locator({"page_number": 7}) == "page 7"
    assert _result_locator({"locator": {"kind": "search_result", "rank": 2}}) == (
        "search result 2"
    )
    assert _source_continuation({}, display_clipped=False) == ""
