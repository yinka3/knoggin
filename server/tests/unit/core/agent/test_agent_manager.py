import asyncio
import math

import pytest

from common.schema.agent.identity import AgentConfig
from core.agent.services.agent_manager import (
    AgentBrainRevisionConflictError,
    AgentManager,
)
from tests.fixtures.fakes import FakePostgresClient, FakeResources


class _RejectedBrainWritePostgres(FakePostgresClient):
    """Preserve the newer row when a revision-CAS write loses its race."""

    async def execute(self, query, params=None):
        if "brain_revision = %(expected_brain_revision)s" in query:
            self.calls.append(("execute", query, params))
            return 0
        return await super().execute(query, params)


@pytest.fixture
def manager():
    resources = FakeResources()
    return AgentManager(resources, user_name="ada"), resources


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_seeds_default_agent_only_when_explicitly_ensured(manager):
    agent_manager, resources = manager

    assert await agent_manager.list_agents() == []
    assert resources.postgres.agents == {}

    default_id = await agent_manager.ensure_default_agent()
    agents = await agent_manager.list_agents()

    assert default_id == agents[0].id
    assert len(agents) == 1
    assert agents[0].name == "STELLA"
    assert agents[0].is_default is True
    assert resources.postgres.agents[agents[0].id]["is_default"] is True


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_create_update_and_lookup_preserves_config(manager):
    agent_manager, resources = manager

    created = await agent_manager.create_agent(
        name="Researcher",
        persona="Careful",
        brain="Use sources",
        model="test-model",
        temperature=0.2,
        enabled_tools=["search_entity"],
    )
    updated = await agent_manager.update_agent(
        created.id,
        model="new-model",
        temperature=0.4,
        enabled_tools=["episode_check"],
    )
    fetched = await agent_manager.get_agent_by_name("researcher")

    assert updated.model == "new-model"
    assert updated.temperature == 0.4
    assert updated.enabled_tools == ["episode_check"]
    assert fetched.id == created.id
    create_snapshot_write = next(
        call
        for call in resources.postgres.calls
        if call[0] == "execute"
        and "INSERT INTO public.agent_brain_snapshots" in call[1]
    )
    assert create_snapshot_write[2]["brain"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_keeps_aac_participation_user_owned_and_opt_in(manager):
    agent_manager, resources = manager

    created = await agent_manager.create_agent("Researcher", "Careful")
    enabled = await agent_manager.set_aac_enabled(created.id, True)

    assert created.aac_enabled is False
    assert enabled.aac_enabled is True
    assert enabled.is_spawned is False
    stored = resources.postgres.agents[created.id]
    assert stored["aac_enabled"] is True
    assert "project_id" not in stored


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_records_completed_turn_timestamp(manager):
    agent_manager, resources = manager
    created = await agent_manager.create_agent("Researcher", "Careful")

    assert await agent_manager.mark_turn_completed(created.id) is True
    assert resources.postgres.agents[created.id]["last_turn_at"] is not None


@pytest.mark.runtime
@pytest.mark.no_network
def test_agent_config_derives_specialist_status_from_parent():
    specialist = AgentConfig(
        id="specialist-1",
        name="Evidence steward",
        persona="Careful",
        spawned_by="parent-1",
    )

    assert specialist.is_spawned is True
    assert "is_spawned" not in specialist.to_dict()


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_preserves_explicit_empty_tool_allowlist(manager):
    agent_manager, resources = manager

    created = await agent_manager.create_agent(
        name="No Tools",
        persona="Careful",
        enabled_tools=[],
    )

    assert created.enabled_tools == []
    assert resources.postgres.agents[created.id]["enabled_tools"] == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_distinguishes_omitted_and_explicit_nullable_updates(manager):
    agent_manager, resources = manager
    created = await agent_manager.create_agent(
        name="Researcher",
        persona="Careful",
        model="test-model",
        enabled_tools=["search_entity"],
    )

    unchanged = await agent_manager.update_agent(created.id)
    inherited = await agent_manager.update_agent(
        created.id,
        model=None,
        enabled_tools=None,
    )
    disabled = await agent_manager.update_agent(created.id, enabled_tools=[])

    assert unchanged.model == "test-model"
    assert unchanged.enabled_tools == ["search_entity"]
    assert inherited.model is None
    assert inherited.enabled_tools is None
    assert disabled.enabled_tools == []
    assert resources.postgres.agents[created.id]["enabled_tools"] == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_default_agent_cannot_be_deleted(manager):
    agent_manager, _ = manager
    default_id = await agent_manager.ensure_default_agent()

    assert await agent_manager.delete_agent(default_id) is False


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_set_default_unsets_previous_default(manager):
    agent_manager, resources = manager
    old_default_id = await agent_manager.ensure_default_agent()
    created = await agent_manager.create_agent("Alt", "Alternative")
    resources.postgres.read_results = [{"agent_id": created.id}]

    assert await agent_manager.set_default_agent(created.id) is True

    old_default = await agent_manager.get_agent(old_default_id)
    new_default = await agent_manager.get_agent(created.id)
    assert old_default.is_default is False
    assert new_default.is_default is True
    assert await agent_manager.get_default_agent_id() == created.id
    default_updates = [
        call
        for call in resources.postgres.calls
        if call[0] == "execute" and "UPDATE public.agents" in call[1]
    ]
    assert len(default_updates) == 2


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_serializes_overlapping_default_and_delete(manager):
    agent_manager, resources = manager
    old_default_id = await agent_manager.ensure_default_agent()
    created = await agent_manager.create_agent("Alt", "Alternative")

    async with agent_manager._lifecycle_lock:
        resources.postgres.read_results = [{"agent_id": created.id}]
        promote_task = asyncio.create_task(agent_manager.set_default_agent(created.id))
        await asyncio.sleep(0)
        delete_task = asyncio.create_task(agent_manager.delete_agent(created.id))
        await asyncio.sleep(0)
        assert not promote_task.done()
        assert not delete_task.done()

    promoted, deleted = await asyncio.gather(promote_task, delete_task)

    assert promoted is True
    assert deleted is False
    assert await agent_manager.get_default_agent_id() == created.id
    assert (await agent_manager.get_agent(old_default_id)).is_default is False
    assert (await agent_manager.get_agent(created.id)).is_default is True


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_ensure_default_shares_lifecycle_lock(manager):
    agent_manager, _ = manager
    established_id = await agent_manager.ensure_default_agent()

    async with agent_manager._lifecycle_lock:
        pending = asyncio.create_task(agent_manager.ensure_default_agent())
        await asyncio.sleep(0)
        assert not pending.done()

    assert await pending == established_id


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_validates_default_target_in_mutation_transaction(manager):
    agent_manager, resources = manager
    default_id = await agent_manager.ensure_default_agent()

    assert await agent_manager.set_default_agent("missing-agent") is False
    assert await agent_manager.get_default_agent_id() == default_id
    target_lock = next(
        call
        for call in resources.postgres.calls
        if call[0] == "execute"
        and "SELECT agent_id" in call[1]
        and "FOR UPDATE" in call[1]
    )
    assert target_lock[2]["agent_id"] == "missing-agent"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_requires_revision_for_full_brain_replacement(manager):
    agent_manager, resources = manager
    created = await agent_manager.create_agent("Researcher", "Careful")
    writes_before = len(
        [call for call in resources.postgres.calls if call[0] == "execute"]
    )

    with pytest.raises(ValueError, match="expected_brain_revision"):
        await agent_manager.update_agent(created.id, brain="Use sources")

    assert resources.postgres.agents[created.id]["brain_revision"] == 1
    assert len([call for call in resources.postgres.calls if call[0] == "execute"]) == (
        writes_before
    )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_brain_replace_uses_cas_and_snapshots_committed_revision(
    manager,
):
    agent_manager, resources = manager
    created = await agent_manager.create_agent("Researcher", "Careful")

    for expected_revision in range(1, 5):
        updated = await agent_manager.update_agent(
            created.id,
            brain=f"Directive {expected_revision}",
            expected_brain_revision=expected_revision,
        )
        assert updated.brain_revision == expected_revision + 1

    committed_snapshot_writes = [
        call
        for call in resources.postgres.calls
        if call[0] == "execute" and "'full_user_update'" in call[1]
    ]
    assert len(committed_snapshot_writes) == 1
    query, params = committed_snapshot_writes[0][1:]
    assert "AND brain_revision = %(expected_brain_revision)s" in query
    assert params["expected_brain_revision"] == 4
    assert resources.postgres.agents[created.id]["brain_revision"] == 5


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_rejects_stale_full_brain_replacement():
    resources = FakeResources()
    resources.postgres = _RejectedBrainWritePostgres()
    agent_manager = AgentManager(resources, user_name="ada")
    created = await agent_manager.create_agent("Researcher", "Careful")
    resources.postgres.agents[created.id]["brain"] = "## Role\nNewer Brain"
    resources.postgres.agents[created.id]["brain_revision"] = 2

    with pytest.raises(AgentBrainRevisionConflictError) as caught:
        await agent_manager.update_agent(
            created.id,
            brain="Older Brain",
            expected_brain_revision=1,
        )

    assert caught.value.expected_revision == 1
    assert caught.value.current_revision == 2
    stored = await agent_manager.get_agent(created.id)
    assert stored.brain_revision == 2
    assert "Newer Brain" in stored.brain


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("name", "temperature", "brain_revision", "enabled_tools"),
    [
        ("  ", 0.7, 1, None),
        ("Analyst", math.nan, 1, None),
        ("Analyst", 0.7, 0, None),
        ("Analyst", 0.7, 1, ["search_entity", " SEARCH_ENTITY "]),
    ],
)
def test_agent_config_rejects_invalid_domain_values(
    name,
    temperature,
    brain_revision,
    enabled_tools,
):
    with pytest.raises(ValueError):
        AgentConfig(
            id="agent-1",
            name=name,
            persona="Careful analyst",
            temperature=temperature,
            brain_revision=brain_revision,
            enabled_tools=enabled_tools,
        )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_rejects_unknown_tools_before_persistence(manager):
    agent_manager, resources = manager

    with pytest.raises(ValueError, match="Unknown agent tools"):
        await agent_manager.create_agent(
            name="Researcher",
            persona="Careful analyst",
            enabled_tools=["not_a_tool"],
        )

    assert resources.postgres.agents == {}
