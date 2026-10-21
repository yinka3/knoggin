import json

import pytest

from common.conf.domain_config import (
    DEFAULT_CONTEXT_SECTIONS,
    DomainConfig,
    DomainConfigError,
)
from core.project.domain_config_store import (
    DomainActivation,
    DomainConfigConflict,
    DomainConfigSectionRemovalConflict,
    DomainConfigStore,
)
from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import RecordingPostgresClient


def make_domain(version=0, *, context_sections=None):
    payload = {
        "version": version,
        "topics": {"Software Development": {"active": True}},
        "entity_types": {
            "Project": {
                "topic": "Software Development",
                "labels": ["project"],
            }
        },
    }
    if context_sections is not None:
        payload["context_sections"] = [
            {"key": section.key, "title": section.title}
            for section in context_sections
        ]
    return DomainConfig.from_mapping(payload)


def legacy_ambiguous_domain(version=4):
    payload = make_domain(version=version).to_dict()
    payload["relationships"] = {
        "USES": {
            "source_types": ["Project"],
            "target_types": ["Project"],
        },
        "DEPLOYS": {
            "source_types": ["Project"],
            "target_types": ["Project"],
            "labels": ["uses"],
        },
    }
    return payload


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
async def test_legacy_ambiguous_relationships_load_with_canonical_precedence():
    persisted = legacy_ambiguous_domain()
    postgres = RecordingPostgresClient(
        fetch_one_results=[{"domain_config": json.dumps(persisted)}]
    )

    loaded = await DomainConfigStore(postgres).load("ada", "project-1")

    assert loaded.version == 4
    assert loaded.compile().relationship_by_key("uses").name == "USES"
    with pytest.raises(DomainConfigError, match="claimed by both"):
        DomainConfig.from_mapping(loaded.to_dict())


@pytest.mark.unit
@pytest.mark.no_network
async def test_legacy_relationship_collision_can_be_repaired_but_not_reactivated():
    persisted = legacy_ambiguous_domain()
    postgres = RecordingPostgresClient(
        fetch_one_results=[
            {"domain_config": persisted},
            {"domain_config": persisted},
            {"current_revision_id": None},
        ]
    )
    store = DomainConfigStore(postgres)
    loaded = await store.load("ada", "project-1")

    with pytest.raises(DomainConfigError, match="claimed by both"):
        await store.activate(
            user_name="ada",
            project_id="project-1",
            candidate=loaded,
            expected_version=4,
        )
    assert postgres.transaction_enters == 0

    corrected = loaded.to_dict()
    corrected["relationships"]["DEPLOYS"]["labels"] = ["deploys"]
    activation = await store.activate(
        user_name="ada",
        project_id="project-1",
        candidate=DomainConfig.from_mapping(corrected),
        expected_version=4,
    )

    assert activation.config.version == 5
    assert activation.compiled.relationship_by_key("uses").name == "USES"
    assert activation.compiled.relationship_by_key("deploys").name == "DEPLOYS"
    assert postgres.transaction_enters == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_legacy_read_still_rejects_other_invalid_domain_fields():
    persisted = legacy_ambiguous_domain()
    persisted["relationships"]["DEPLOYS"]["source_types"] = ["Unknown"]
    postgres = RecordingPostgresClient(
        fetch_one_results=[{"domain_config": persisted}]
    )

    with pytest.raises(DomainConfigError, match="unknown entity types"):
        await DomainConfigStore(postgres).load("ada", "project-1")


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_activation_assigns_next_revision_transactionally():
    current = make_domain(version=4)
    candidate = make_domain(version=0)
    postgres = RecordingPostgresClient(
        fetch_one_results=[
            {"domain_config": current.to_dict()},
            {"current_revision_id": None},
        ]
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
    lock_queries = [
        call[1]
        for call in postgres.calls
        if call[0] == "execute" and "FOR UPDATE" in call[1]
    ]
    assert "public.projects" in lock_queries[0]
    assert "project_contexts" in lock_queries[1]
    assert postgres.transaction_enters == 1
    assert postgres.transaction_exits == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_rejects_stale_activation_without_writing():
    current = make_domain(version=4)
    postgres = RecordingPostgresClient(
        fetch_one_results=[
            {"domain_config": current.to_dict()},
            {"current_revision_id": None},
        ]
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
    lock_queries = [
        call[1]
        for call in postgres.calls
        if call[0] == "execute" and "FOR UPDATE" in call[1]
    ]
    assert len(lock_queries) == 1
    assert "public.projects" in lock_queries[0]
    assert "project_contexts" not in lock_queries[0]
    assert not any(
        call[0] == "execute" and "SET domain_config" in call[1]
        for call in postgres.calls
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_rejects_removing_a_section_with_live_blocks():
    current = make_domain(version=4)
    candidate = make_domain(
        context_sections=tuple(
            section
            for section in DEFAULT_CONTEXT_SECTIONS
            if section.key != "active_work"
        )
    )
    postgres = RecordingPostgresClient(
        fetch_one_results=[
            {"domain_config": current.to_dict()},
            {"current_revision_id": "revision-1"},
            {"section_key": "active_work"},
        ]
    )

    with pytest.raises(
        DomainConfigSectionRemovalConflict,
        match="live blocks.*active_work",
    ):
        await DomainConfigStore(postgres).activate(
            user_name="ada",
            project_id="project-1",
            candidate=candidate,
            expected_version=4,
        )

    assert not any(
        call[0] == "execute" and "SET domain_config" in call[1]
        for call in postgres.calls
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_rejects_section_removal_while_old_window_can_write_context():
    current = make_domain(version=4)
    candidate = make_domain(
        context_sections=tuple(
            section
            for section in DEFAULT_CONTEXT_SECTIONS
            if section.key != "active_work"
        )
    )
    postgres = RecordingPostgresClient(
        fetch_one_results=[
            {"domain_config": current.to_dict()},
            {"current_revision_id": None},
            {"window_id": "window-1"},
        ]
    )

    with pytest.raises(
        DomainConfigSectionRemovalConflict,
        match="semantic window is active.*active_work",
    ):
        await DomainConfigStore(postgres).activate(
            user_name="ada",
            project_id="project-1",
            candidate=candidate,
            expected_version=4,
        )

    assert any(
        call[0] == "execute" and "stage <> 'completed'" in call[1]
        for call in postgres.calls
    )
    assert not any(
        call[0] == "execute" and "SET domain_config" in call[1]
        for call in postgres.calls
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_allows_removing_an_empty_section_after_window_drains():
    current = make_domain(version=4)
    candidate = make_domain(
        context_sections=tuple(
            section
            for section in DEFAULT_CONTEXT_SECTIONS
            if section.key != "active_work"
        )
    )
    postgres = RecordingPostgresClient(
        fetch_one_results=[
            {"domain_config": current.to_dict()},
            {"current_revision_id": "revision-1"},
            None,
            None,
        ]
    )

    activation = await DomainConfigStore(postgres).activate(
        user_name="ada",
        project_id="project-1",
        candidate=candidate,
        expected_version=4,
    )

    assert activation.config.version == 5
    assert activation.config.context_sections == candidate.context_sections
    assert any(
        call[0] == "execute" and "SET domain_config" in call[1]
        for call in postgres.calls
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_domain_config_store_rejects_empty_active_domain():
    postgres = RecordingPostgresClient(fetch_one_results=[{"domain_config": {}}])

    with pytest.raises(ValueError, match="domain configuration is required"):
        await DomainConfigStore(postgres).load("ada", "project-1")


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_state_activation_replaces_runtime_snapshot():
    initial = make_domain(version=2)
    activated = make_domain(version=3)

    class Store:
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

    state = make_project_state(domain_config=initial)
    state.domain_config_store = Store()

    assert state.compiled_domain.version == 2

    result = await state.activate_domain_config(
        make_domain(),
        expected_version=2,
    )

    assert result.config.version == 3
    assert state.domain_config.version == 3
    assert state.compiled_domain.version == 3
