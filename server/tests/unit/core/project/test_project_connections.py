import pytest

from core.project.project_manager import _project_connections


@pytest.mark.unit
@pytest.mark.no_network
def test_projects_may_only_allow_known_sandbox_logins():
    assert _project_connections(["kaggle", "github", "kaggle"]) == ["github", "kaggle"]
    with pytest.raises(ValueError, match="Unknown connections: openrouter"):
        _project_connections(["openrouter"])  # model keys are app-wide, not per project
    with pytest.raises(ValueError, match="list of service names"):
        _project_connections("github")
