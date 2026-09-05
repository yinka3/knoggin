import json

import pytest

from common.conf.domain_config import DomainConfig
from core.project.domain_config_store import (
    DomainActivation,
    DomainConfigConflict,
    DomainConfigStore,
)
from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import RecordingPostgresClient


def make_domain(version=0):
    return DomainConfig.from_mapping(
        {
            "version": version,
            "topics": {"Software Development": {"active": True}},
            "entity_types": {
                "Project": {
                    "topic": "Software Development",
                    "labels": ["project"],
                }
            },
        }
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_loads_and_decodes_persisted_config():
    persisted = make_domain(version=4)
    postgres = RecordingPostgresClient(
        fetch_one_results=[{"domain_config": json.dumps(persisted.to_dict())}]
    )

    loaded = await DomainConfigStore(postgres).load("ada", "project-1")

    assert loaded == persisted
    assert postgres.calls[0][0] == "fetch_one"
    assert "SELECT domain_config" in postgres.calls[0][1]
    assert postgres.transaction_enters == 0


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_activation_assigns_next_revision_transactionally():
    current = make_domain(version=4)
    candidate = make_domain(version=0)
    postgres = RecordingPostgresClient(
        fetch_one_results=[{"domain_config": current.to_dict()}]
    )

    activation = await DomainConfigStore(postgres).activate(
        user_name="ada",
        project_id="project-1",
        candidate=candidate,
        expected_version=4,
    )

    assert activation.previous_version == 4
    assert activation.config.version == 5
    assert activation.compiled.version == 5
    update_call = next(
        call
        for call in postgres.calls
        if call[0] == "execute" and "domain_config =" in call[1]
    )
    assert update_call[0] == "execute"
    assert json.loads(update_call[2]["config"])["version"] == 5
    assert postgres.transaction_enters == 1
    assert postgres.transaction_exits == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_rejects_stale_activation_without_writing():
    current = make_domain(version=4)
    postgres = RecordingPostgresClient(
        fetch_one_results=[{"domain_config": current.to_dict()}]
    )

    with pytest.raises(DomainConfigConflict) as error:
        await DomainConfigStore(postgres).activate(
            user_name="ada",
            project_id="project-1",
            candidate=make_domain(),
            expected_version=3,
        )

    assert error.value.expected_version == 3
    assert error.value.actual_version == 4
    assert len(postgres.calls) == 1
    assert "FOR UPDATE" in postgres.calls[0][1]


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_rejects_empty_active_domain():
    postgres = RecordingPostgresClient(fetch_one_results=[{"domain_config": {}}])

    with pytest.raises(ValueError, match="domain configuration is required"):
        await DomainConfigStore(postgres).load("ada", "project-1")


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_state_load_and_activation_replace_runtime_snapshot():
    initial = make_domain(version=2)
    activated = make_domain(version=3)

    class Store:
        async def load(self, user_name, project_id):
            assert (user_name, project_id) == ("ada", "project-1")
            return initial

        async def activate(
            self,
            *,
            user_name,
            project_id,
            candidate,
            expected_version,
        ):
            assert (user_name, project_id) == ("ada", "project-1")
            assert candidate.version == 0
            assert expected_version == 2
            return DomainActivation(
                config=activated,
                compiled=activated.compile(),
                previous_version=2,
            )

    state = make_project_state()
    state.domain_config_store = Store()

    await state.load_domain_config()
    captured_before = await state.capture_domain()
    assert captured_before.version == 2

    result = await state.activate_domain_config(
        make_domain(),
        expected_version=2,
    )

    assert result.config.version == 3
    assert state.domain_config.version == 3
    assert (await state.capture_domain()).version == 3
