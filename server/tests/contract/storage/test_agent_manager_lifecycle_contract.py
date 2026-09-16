import asyncio
from types import SimpleNamespace

import pytest

from core.agent.services.agent_manager import (
    AgentBrainRevisionConflictError,
    AgentManager,
)


def _manager(postgres) -> AgentManager:
    return AgentManager(SimpleNamespace(postgres=postgres), user_name="ada")


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_full_brain_replacement_uses_cas_and_snapshots_the_committed_revision(
    real_postgres_client,
):
    manager = _manager(real_postgres_client)
    created = await manager.create_agent("Researcher", "Careful")

    for expected_revision in range(1, 5):
        updated = await manager.update_agent(
            created.id,
            brain=f"Directive {expected_revision}",
            expected_brain_revision=expected_revision,
        )
        assert updated is not None
        assert updated.brain_revision == expected_revision + 1

    with pytest.raises(AgentBrainRevisionConflictError) as caught:
        await manager.update_agent(
            created.id,
            brain="Stale directive",
            expected_brain_revision=4,
        )

    assert caught.value.expected_revision == 4
    assert caught.value.current_revision == 5
    stored = await real_postgres_client.fetch_one(
        """
        SELECT brain, brain_revision
        FROM public.agents
        WHERE user_name = 'ada' AND agent_id = %s
        """,
        (created.id,),
    )
    snapshots = await real_postgres_client.fetch_all(
        """
        SELECT revision, content, change_type
        FROM public.agent_brain_snapshots
        WHERE user_name = 'ada' AND agent_id = %s
        ORDER BY revision
        """,
        (created.id,),
    )

    assert stored["brain_revision"] == 5
    assert "Directive 4" in stored["brain"]
    assert [(row["revision"], row["change_type"]) for row in snapshots] == [
        (1, "initial_seed"),
        (5, "full_user_update"),
    ]
    assert snapshots[-1]["content"] == stored["brain"]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_lifecycle_operations_preserve_one_default_and_conditional_delete(
    real_postgres_client,
    monkeypatch,
):
    manager = _manager(real_postgres_client)
    old_default_id = await manager.ensure_default_agent()
    candidate = await manager.create_agent("Alternate", "Careful")

    promote = asyncio.create_task(manager.set_default_agent(candidate.id))
    delete = asyncio.create_task(manager.delete_agent(candidate.id))
    promoted, deleted = await asyncio.gather(promote, delete)

    assert (promoted, deleted) in {(True, False), (False, True)}
    defaults = await real_postgres_client.fetch_all(
        """
        SELECT agent_id
        FROM public.agents
        WHERE user_name = 'ada' AND is_default = true
        """
    )
    assert len(defaults) == 1
    assert defaults[0]["agent_id"] in {old_default_id, candidate.id}

    stale_target = await manager.create_agent("Stale target", "Careful")
    stale_candidate = await manager.get_agent(stale_target.id)
    assert stale_candidate is not None
    assert stale_candidate.is_default is False
    assert await manager.set_default_agent(stale_target.id) is True

    async def stale_read(_agent_id):
        return stale_candidate

    monkeypatch.setattr(manager, "get_agent", stale_read)
    assert await manager.delete_agent(stale_target.id) is False
    still_default = await real_postgres_client.fetch_one(
        """
        SELECT is_default
        FROM public.agents
        WHERE user_name = 'ada' AND agent_id = %s
        """,
        (stale_target.id,),
    )
    assert still_default == {"is_default": True}
