import pytest

from common.scoping import (
    IDENTITY_SCOPE,
    build_readable_project_ids,
    require_scope_value,
    require_visible_project_ids,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_build_readable_project_ids_includes_global():
    result = build_readable_project_ids("proj_1")
    assert result == [IDENTITY_SCOPE, "proj_1"]


@pytest.mark.unit
@pytest.mark.no_network
def test_build_readable_project_ids_with_allowed_projects():
    result = build_readable_project_ids("proj_1", ["proj_2", "proj_3"])
    assert result == [IDENTITY_SCOPE, "proj_1", "proj_2", "proj_3"]


@pytest.mark.unit
@pytest.mark.no_network
def test_build_readable_project_ids_deduplicates_scopes():
    result = build_readable_project_ids(
        "proj_1",
        ["proj_1", "proj_2", IDENTITY_SCOPE],
    )
    # Deduplication should preserve order of first appearance
    assert result == [IDENTITY_SCOPE, "proj_1", "proj_2"]


@pytest.mark.unit
@pytest.mark.no_network
def test_scope_values_are_trimmed_before_deduplication():
    assert build_readable_project_ids(
        " project-1 ",
        [" project-2 ", "project-1", " project-2", "   "],
    ) == [IDENTITY_SCOPE, "project-1", "project-2"]
    assert require_scope_value(" ada ", "user_name", "read") == "ada"
    assert require_visible_project_ids(
        [" project-1 ", "project-1", " project-2 ", ""],
        "read",
    ) == ["project-1", "project-2"]


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("project_id", [None, "", "   "])
def test_build_readable_project_ids_requires_project_id(project_id):
    with pytest.raises(ValueError, match="project_id is required"):
        build_readable_project_ids(project_id)
