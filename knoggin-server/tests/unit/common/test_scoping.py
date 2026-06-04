import pytest

from common.scoping import GLOBAL_PROJECT_SCOPE, build_readable_project_ids


@pytest.mark.unit
@pytest.mark.no_network
def test_build_readable_project_ids_includes_global():
    result = build_readable_project_ids("proj_1")
    assert result == [GLOBAL_PROJECT_SCOPE, "proj_1"]


@pytest.mark.unit
@pytest.mark.no_network
def test_build_readable_project_ids_with_allowed_projects():
    result = build_readable_project_ids("proj_1", ["proj_2", "proj_3"])
    assert result == [GLOBAL_PROJECT_SCOPE, "proj_1", "proj_2", "proj_3"]


@pytest.mark.unit
@pytest.mark.no_network
def test_build_readable_project_ids_deduplicates_scopes():
    result = build_readable_project_ids("proj_1", ["proj_1", "proj_2", GLOBAL_PROJECT_SCOPE])
    # Deduplication should preserve order of first appearance
    assert result == [GLOBAL_PROJECT_SCOPE, "proj_1", "proj_2"]


@pytest.mark.unit
@pytest.mark.no_network
def test_build_readable_project_ids_none_project_id():
    result = build_readable_project_ids(None, ["proj_2"])
    assert result == [GLOBAL_PROJECT_SCOPE, "proj_2"]


@pytest.mark.unit
@pytest.mark.no_network
def test_build_readable_project_ids_none_iterables():
    result = build_readable_project_ids(None, None)
    assert result == [GLOBAL_PROJECT_SCOPE]
