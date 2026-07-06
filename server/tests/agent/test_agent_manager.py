import pytest

from core.agent.services.agent_manager import AgentManager
from tests.fixtures.fakes import FakeResources


@pytest.fixture
def manager():
    resources = FakeResources()
    return AgentManager(resources, user_name="ada", active_sessions={}), resources


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_seeds_default_agent_when_listing_empty(manager):
    agent_manager, resources = manager

    agents = await agent_manager.list_agents()

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
        enabled_tools=["fact_check"],
    )
    fetched = await agent_manager.get_agent_by_name("researcher")

    assert updated.model == "new-model"
    assert updated.temperature == 0.4
    assert updated.enabled_tools == ["fact_check"]
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
async def test_agent_manager_default_agent_cannot_be_deleted(manager):
    agent_manager, _ = manager
    default_id = await agent_manager.get_default_agent_id()

    assert await agent_manager.delete_agent(default_id) is False


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_set_default_unsets_previous_default(manager):
    agent_manager, _ = manager
    old_default_id = await agent_manager.get_default_agent_id()
    created = await agent_manager.create_agent("Alt", "Alternative")

    assert await agent_manager.set_default_agent(created.id) is True

    old_default = await agent_manager.get_agent(old_default_id)
    new_default = await agent_manager.get_agent(created.id)
    assert old_default.is_default is False
    assert new_default.is_default is True
    assert await agent_manager.get_default_agent_id() == created.id
