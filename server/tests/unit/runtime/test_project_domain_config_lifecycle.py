from types import SimpleNamespace

import pytest

from common.conf.domain_config import DomainConfig
from core.project.domain_config_store import DomainActivation
from core.project.project_manager import ProjectManager, ProjectStatus
from tests.fixtures.fakes import RecordingPostgresClient


def make_domain(version=4):
    return DomainConfig.from_mapping(
        {
            "version": version,
            "topics": {
                "Software Development": {"active": True},
                "Operations": {"active": False},
            },
            "entity_types": {
                "Project": {
                    "topic": "Software Development",
                    "labels": ["project"],
                },
                "Platform": {
                    "topic": "Operations",
                    "labels": ["platform"],
                },
            },
        }
    )


def project_row(status=ProjectStatus.ACTIVE.value):
    return {
        "project_id": "project-1",
        "user_name": "ada",
        "name": "Research",
        "description": None,
        "status": status,
        "allowed_projects": [],
        "session_count": 0,
        "created_at": None,
        "updated_at": None,
        "archived_at": None,
        "deleted_at": None,
        "last_activity_at": None,
    }


def manager(postgres):
    return ProjectManager(SimpleNamespace(postgres=postgres), user_name="ada")


@pytest.mark.runtime
@pytest.mark.no_network
def test_validate_domain_config_is_pure_and_public():
    postgres = RecordingPostgresClient()
    result = manager(postgres).validate_domain_config(make_domain(version=0).to_dict())

    assert result.valid
    assert result.config.version == 0
    assert postgres.calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_read_and_preview_domain_config_use_the_durable_active_revision():
    current = make_domain()
    candidate = make_domain(version=0).to_dict()
    candidate["topics"]["Operations"]["active"] = True
    postgres = RecordingPostgresClient(
        fetch_one_results=[
            {"domain_config": current.to_dict()},
            {"domain_config": current.to_dict()},
        ],
        fetch_all_results=[[project_row()], [project_row()],],
    )

    project_manager = manager(postgres)
    loaded = await project_manager.get_domain_config("project-1")
    preview = await project_manager.preview_domain_config("project-1", candidate)

    assert loaded == current
    assert preview.current_version == current.version
    assert preview.topics_activated == ("Operations",)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_activation_without_loaded_runtime_uses_optimistic_store():
    current = make_domain()
    candidate = make_domain(version=0)
    postgres = RecordingPostgresClient(
        fetch_one_results=[
            {"domain_config": current.to_dict()},
            {"domain_config": current.to_dict()},
        ],
        fetch_all_results=[[project_row()]],
    )

    activation = await manager(postgres).activate_domain_config(
        "project-1",
        candidate,
        expected_version=current.version,
    )

    assert isinstance(activation, DomainActivation)
    assert activation.previous_version == current.version
    assert activation.config.version == current.version + 1
    assert postgres.transaction_enters == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_activation_with_loaded_runtime_delegates_to_state_snapshot():
    candidate = make_domain(version=0)
    postgres = RecordingPostgresClient(fetch_all_results=[[project_row()]])
    project_manager = manager(postgres)
    calls = []

    class ActiveState:
        async def activate_domain_config(self, config, *, expected_version):
            calls.append((config, expected_version))
            return "activated"

    project_manager.active_projects["project-1"] = ActiveState()

    result = await project_manager.activate_domain_config(
        "project-1",
        candidate,
        expected_version=4,
    )

    assert result == "activated"
    assert calls == [(candidate, 4)]
