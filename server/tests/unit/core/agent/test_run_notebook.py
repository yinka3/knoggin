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
        {"data": [{"id": 24, "canonical_name": "Sarah Johnson", "project_id": "project-a"}]},
    )
    notebook.apply(
        "episode_check",
        {
            "data": {
                "resolution": "semantic",
                "results": [{"episodes": [{"episode_id": "ep-secret", "summary": "Changed"}]}],
            }
        },
    )
    notebook.record_agent_hint(
        "get_connections", {"entity_id": 24}, "inspect the relationship neighborhood"
    )
    before = deepcopy(notebook.as_dict())

    rendered = render_notebook(notebook)

    assert "E1 Sarah Johnson" in rendered
    assert "EP1: Changed" in rendered
    assert '"entity_id": "E1"' in rendered
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
