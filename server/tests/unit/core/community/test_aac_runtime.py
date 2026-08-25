from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.agent.services.agent_manager import AgentManager
from core.community.aac_store import AACStore
from core.community.read_context import AACReadContext
from core.community.runtime import AACAdmissionOutcome, AACRuntime
from core.community.seeding import SeedDecision
from tests.fixtures.fakes import FakeResources


def _provider():
    return SimpleNamespace(
        get=staticmethod(
            lambda: SimpleNamespace(
                config=SimpleNamespace(
                    search=SimpleNamespace(model_dump=lambda: {}),
                    developer_settings=SimpleNamespace(
                        search=SimpleNamespace(model_dump=lambda: {}),
                        community=SimpleNamespace(
                            enabled=False,
                            interval_minutes=30,
                            token_budget=100,
                        ),
                    ),
                )
            )
        )
    )


class FakeSeeder:
    def __init__(self, decision):
        self.decision = decision
        self.budgets = []

    async def decide(self, *, budget):
        self.budgets.append(budget)
        return self.decision


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_runtime_owns_local_discussion_admission_and_stop():
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    agent = await manager.create_agent("Researcher", "Careful")
    await manager.set_aac_enabled(agent.id, True)
    context = await AACReadContext.create(
        user_name="ada",
        postgres=resources.postgres,
        knowledge_store=resources.knowledge_store,
        embedding_service=resources.embedding,
    )
    seeder = FakeSeeder(SeedDecision("START", "Compare evidence"))
    runtime = AACRuntime(
        user_name="ada",
        resources=resources,
        agent_manager=manager,
        read_context=context,
        store=AACStore(resources.postgres),
        config_provider=_provider(),
        seeder=seeder,
    )

    async def finish_discussion(**_kwargs):
        await asyncio.sleep(0)

    import asyncio

    runtime._run_discussion = finish_discussion
    admission = await runtime.trigger_discussion()

    assert admission.outcome is AACAdmissionOutcome.STARTED
    assert admission.discussion_id == runtime.active_discussion_id
    assert seeder.budgets[0].token_budget == 100
    assert await runtime.request_stop() is True
    await runtime.shutdown()


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_runtime_skips_without_enabled_participants():
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    context = await AACReadContext.create(
        user_name="ada",
        postgres=resources.postgres,
        knowledge_store=resources.knowledge_store,
        embedding_service=resources.embedding,
    )
    runtime = AACRuntime(
        user_name="ada",
        resources=resources,
        agent_manager=manager,
        read_context=context,
        store=AACStore(resources.postgres),
        config_provider=_provider(),
        seeder=FakeSeeder(SeedDecision("START", "Unused")),
    )

    admission = await runtime.trigger_discussion()

    assert admission.outcome is AACAdmissionOutcome.SKIPPED
    assert admission.reason == "no_enabled_agents"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_runtime_reconciles_durable_participation_and_records_events():
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    first = await manager.create_agent("Researcher", "Careful")
    second = await manager.create_agent("Critic", "Skeptical")
    await manager.set_aac_enabled(first.id, True)
    context = await AACReadContext.create(
        user_name="ada",
        postgres=resources.postgres,
        knowledge_store=resources.knowledge_store,
        embedding_service=resources.embedding,
    )
    runtime = AACRuntime(
        user_name="ada",
        resources=resources,
        agent_manager=manager,
        read_context=context,
        store=AACStore(resources.postgres),
        config_provider=_provider(),
        seeder=FakeSeeder(SeedDecision("SKIP")),
    )
    runtime._discussion_id = "discussion-1"
    runtime._participants = [first.id]

    promoted = await runtime.set_participation(second.id, True)
    assert promoted is not None
    assert runtime._participants == sorted([first.id, second.id])

    removed = await runtime.set_participation(first.id, False)
    assert removed is not None
    assert runtime._participants == [second.id]

    timeline_writes = [
        params
        for kind, query, params in resources.postgres.calls
        if kind == "execute" and "INSERT INTO public.aac_timeline" in query
    ]
    assert [write["content"] for write in timeline_writes] == [
        f"Agent {second.id} joined the discussion.",
        f"Agent {first.id} left the discussion.",
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_runtime_ignores_stop_when_no_discussion_is_active():
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    context = await AACReadContext.create(
        user_name="ada",
        postgres=resources.postgres,
        knowledge_store=resources.knowledge_store,
        embedding_service=resources.embedding,
    )
    runtime = AACRuntime(
        user_name="ada",
        resources=resources,
        agent_manager=manager,
        read_context=context,
        store=AACStore(resources.postgres),
        config_provider=_provider(),
        seeder=FakeSeeder(SeedDecision("SKIP")),
    )

    assert await runtime.request_stop() is False
    assert runtime._discussion_stop_event.is_set() is False
