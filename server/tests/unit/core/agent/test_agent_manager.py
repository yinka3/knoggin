import math

import pytest

from common.schema.agent.identity import AgentConfig
from core.agent.services.agent_manager import AgentManager
from tests.fixtures.fakes import FakeResources


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
