from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from core.agent.services.agent_manager import AgentManager
from core.community.aac_store import AACStore
from core.community.read_context import AACReadContext
from core.community.runtime import AACAdmission, AACAdmissionOutcome, AACRuntime
from core.community.seeding import SeedDecision
from core.community.token_budget import AACTokenBudget
from tests.fixtures.fakes import FakeResources


def _provider(*, enabled: bool = True):
    community = SimpleNamespace(
        enabled=enabled,
        interval_minutes=30,
        token_budget=100,
    )
    config = SimpleNamespace(
        search=SimpleNamespace(model_dump=lambda: {}),
        developer_settings=SimpleNamespace(
            search=SimpleNamespace(model_dump=lambda: {}),
            community=community,
        ),
    )
    return SimpleNamespace(get=staticmethod(lambda: SimpleNamespace(config=config)))


class ConfigBus:
    def __init__(self, *, enabled: bool) -> None:
        self.community = SimpleNamespace(
            enabled=enabled,
            interval_minutes=30,
            token_budget=100,
        )
        self.config = SimpleNamespace(
            search=SimpleNamespace(model_dump=lambda: {}),
            developer_settings=SimpleNamespace(
                search=SimpleNamespace(model_dump=lambda: {}),
                community=self.community,
            ),
        )
        self.callback = None

    def get(self):
        return self

    def subscribe(self, callback, _path):
        self.callback = callback
        callback(self.community)

        def unsubscribe():
            self.callback = None

        return unsubscribe

    def set_enabled(self, enabled: bool) -> None:
        self.community.enabled = enabled
        if self.callback is not None:
            self.callback(self.community)


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


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_runtime_persists_terminal_end_reason_and_user_stop_event():
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
    runtime._discussion_id = "discussion-1"
    task = asyncio.create_task(
        runtime._run_discussion(
            discussion_id="discussion-1",
            topic="Review evidence",
            participants=[],
            budget=AACTokenBudget(0),
        )
    )
    runtime._discussion_task = task

    assert await runtime.request_stop() is True
    await task

    timeline_writes = [
        params
        for kind, query, params in resources.postgres.calls
        if kind == "execute" and "INSERT INTO public.aac_timeline" in query
    ]
    finish_params = next(
        params
        for kind, query, params in resources.postgres.calls
        if kind == "execute" and "UPDATE public.aac_discussions" in query
    )
    assert "Discussion stopped by user." in [item["content"] for item in timeline_writes]
    assert finish_params["status"] == "stopped"
    assert finish_params["end_reason"] == "user_stopped"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_runtime_wakes_when_community_is_enabled_after_startup():
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    context = await AACReadContext.create(
        user_name="ada",
        postgres=resources.postgres,
        knowledge_store=resources.knowledge_store,
        embedding_service=resources.embedding,
    )
    config = ConfigBus(enabled=False)
    runtime = AACRuntime(
        user_name="ada",
        resources=resources,
        agent_manager=manager,
        read_context=context,
        store=AACStore(resources.postgres),
        config_provider=config,
        seeder=FakeSeeder(SeedDecision("SKIP")),
    )
    admitted = asyncio.Event()

    async def trigger() -> AACAdmission:
        admitted.set()
        return AACAdmission(AACAdmissionOutcome.SKIPPED, "no_seed")

    runtime.trigger_discussion = trigger
    await runtime.start()
    await asyncio.sleep(0)
    assert admitted.is_set() is False

    config.set_enabled(True)
    await asyncio.wait_for(admitted.wait(), timeout=0.5)
    await runtime.shutdown()


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_runtime_refreshes_project_read_scope_before_admission():
    resources = FakeResources()
    resources.postgres.upsert_project("existing")
    manager = AgentManager(resources, user_name="ada")
    agent = await manager.create_agent("Researcher", "Careful")
    await manager.set_aac_enabled(agent.id, True)
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
    resources.postgres.upsert_project("created-after-runtime")

    admission = await runtime.trigger_discussion()

    assert admission.reason == "no_seed"
    assert runtime.read_context.readable_project_ids == (
        "__identity__",
        "created-after-runtime",
        "existing",
    )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_private_specialists_cannot_publish_or_spawn_without_promotion(monkeypatch):
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    parent = await manager.create_agent("Parent", "Careful")
    specialist = await manager.create_specialist(
        parent_id=parent.id,
        name="Evidence checker",
        persona={
            "attention_bias": "evidence",
            "reasoning_style": "careful",
            "social_temperament": "curious",
            "communication_signature": "concise",
            "productive_flaw": "overchecks",
        },
    )
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
    captured = {}

    class CapturingExecutor:
        def __init__(self, run, *_args, **_kwargs):
            captured["run"] = run

        async def execute(self):
            yield {"event": "response", "data": {"content": "Checked dates."}}

    monkeypatch.setattr("core.community.runtime.AgentExecutor", CapturingExecutor)

    result = await runtime._specialist_runner(
        discussion_id="discussion-1",
        topic="Check conflicting dates",
        budget=AACTokenBudget(100),
    )(specialist, "Which source is newer?")

    visible_tools = {
        schema["function"]["name"] for schema in captured["run"].tool_runtime.schemas
    }
    assert result == "Checked dates."
    assert captured["run"].is_community is False
    assert {"save_insight", "vote_insight", "remove_insight_vote", "spawn_specialist", "consult_specialist"}.isdisjoint(visible_tools)
    assert {"search_documents", "search_insights", "edit_brain"}.issubset(visible_tools)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_participants_cannot_reexpose_project_tools_from_agent_settings(
    monkeypatch,
):
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    agent = await manager.create_agent(
        "Researcher",
        "Careful",
        enabled_tools=[
            "search_documents",
            "create_file",
            "update_file",
            "append_file",
        ],
    )
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
    captured = {}

    class CapturingExecutor:
        def __init__(self, run, *_args, **_kwargs):
            captured["run"] = run

        async def execute(self):
            yield {"event": "response", "data": {"content": "Reviewed."}}

    monkeypatch.setattr("core.community.runtime.AgentExecutor", CapturingExecutor)

    await runtime._agent_turn(
        discussion_id="discussion-1",
        topic="Review evidence",
        agent=agent,
        history=[],
        participants=[agent.id],
        budget=AACTokenBudget(100),
    )

    visible_tools = {
        schema["function"]["name"] for schema in captured["run"].tool_runtime.schemas
    }
    assert "search_documents" in visible_tools
    assert {
        "create_file",
        "update_file",
        "append_file",
    }.isdisjoint(visible_tools)
