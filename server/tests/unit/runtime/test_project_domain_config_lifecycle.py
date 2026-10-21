import asyncio
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
    async def get_vp01(_language):
        return SimpleNamespace(extract_entities=lambda *_args, **_kwargs: ())

    return ProjectManager(
        SimpleNamespace(postgres=postgres, get_vp01=get_vp01), user_name="ada"
    )


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
        fetch_all_results=[[project_row()], [project_row()]],
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
    postgres = RecordingPostgresClient(
        fetch_all_results=[[project_row()], [project_row()]]
    )
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


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize("invalid_adapter", [False, True])
async def test_unloaded_activation_rejects_unavailable_vp01_before_storage(
    invalid_adapter,
):
    postgres = RecordingPostgresClient(fetch_all_results=[[project_row()]])

    async def get_vp01(language):
        assert language == "multilingual"
        if invalid_adapter:
            return object()
        raise RuntimeError("model unavailable")

    project_manager = ProjectManager(
        SimpleNamespace(postgres=postgres, get_vp01=get_vp01), user_name="ada"
    )
    candidate = make_domain(version=0).to_dict()
    candidate["vp01_language"] = "multilingual"

    with pytest.raises(
        TypeError if invalid_adapter else RuntimeError,
        match="VP-01 adapter" if invalid_adapter else "model unavailable",
    ):
        await project_manager.activate_domain_config(
            "project-1", candidate, expected_version=4
        )

    assert [call[0] for call in postgres.calls] == ["fetch_all"]
    assert postgres.transaction_enters == 0


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize("expected_version", [-1, True])
async def test_activation_rejects_invalid_version_before_model_load(expected_version):
    postgres = RecordingPostgresClient()

    async def get_vp01(_language):
        pytest.fail("invalid version should not load VP-01")

    project_manager = ProjectManager(
        SimpleNamespace(postgres=postgres, get_vp01=get_vp01), user_name="ada"
    )

    with pytest.raises(ValueError, match="expected_version"):
        await project_manager.activate_domain_config(
            "project-1", make_domain(version=0), expected_version=expected_version
        )

    assert postgres.calls == []


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("rows", "error"),
    [([], "does not exist"), ([project_row(ProjectStatus.ARCHIVED.value)], "archived")],
)
async def test_activation_rejects_unavailable_project_before_model_load(rows, error):
    postgres = RecordingPostgresClient(fetch_all_results=[rows])

    async def get_vp01(_language):
        pytest.fail("unavailable project should not load VP-01")

    project_manager = ProjectManager(
        SimpleNamespace(postgres=postgres, get_vp01=get_vp01), user_name="ada"
    )

    with pytest.raises(ValueError, match=error):
        await project_manager.activate_domain_config(
            "project-1", make_domain(version=0), expected_version=4
        )

    assert [call[0] for call in postgres.calls] == ["fetch_all"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_activation_model_load_does_not_hold_manager_lock_or_outlive_shutdown():
    postgres = RecordingPostgresClient(fetch_all_results=[[project_row()]])
    started = asyncio.Event()
    finish = asyncio.Event()

    async def get_vp01(_language):
        started.set()
        await finish.wait()
        return SimpleNamespace(extract_entities=lambda *_args, **_kwargs: ())

    project_manager = ProjectManager(
        SimpleNamespace(postgres=postgres, get_vp01=get_vp01), user_name="ada"
    )
    activation = asyncio.create_task(
        project_manager.activate_domain_config(
            "project-1", make_domain(version=0), expected_version=4
        )
    )
    await started.wait()

    await asyncio.wait_for(project_manager.shutdown(), timeout=1)

    finish.set()
    with pytest.raises(RuntimeError, match="shutting down"):
        await activation
    assert [call[0] for call in postgres.calls] == ["fetch_all"]
