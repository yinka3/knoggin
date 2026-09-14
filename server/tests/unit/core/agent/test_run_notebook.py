from copy import deepcopy

import pytest
from jinja2 import StrictUndefined, UndefinedError

from core.agent.notebook import RunNotebook
from core.agent.notebook_renderer import (
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
    notebook.record_agent_hint(
        "get_connections", {"entity_id": 24}, "inspect the relationship neighborhood"
    )
    before = deepcopy(notebook.as_dict())

    rendered = render_notebook(notebook)

    assert "E1 Sarah Johnson" in rendered
    assert "ep_epsecr: Changed" in rendered
    assert '"entity_id": 24' in rendered
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
