import asyncio
from types import SimpleNamespace

import pytest

from common.schema.agent.community_tools import AAC_SPECIFIC_SCHEMAS
from common.schema.agent.identity import AgentConfig
from common.schema.settings import CommunitySettings, DeveloperSettings, RootConfig
from core.community.community_manager import (
    ACTIVE_DISCUSSION_TTL_SECONDS,
    COMMUNITY_ENABLED_TOOLS,
    CommunityManager,
)
from tests.fixtures.fakes import FakePostgresClient, FakeRedis


class RecordingCommunityGraph:
    def __init__(self):
        self.created = []
        self.closed = []
        self.messages = []

    async def create_discussion(
        self, discussion_id, topic, agent_ids, *, user_name, project_id
    ):
        self.created.append(
            {
                "discussion_id": discussion_id,
                "topic": topic,
                "agent_ids": list(agent_ids),
                "user_name": user_name,
                "project_id": project_id,
            }
        )

    async def close_discussion(self, discussion_id, *, user_name, project_id):
        self.closed.append((discussion_id, user_name, project_id))

    async def add_message(
        self, discussion_id, agent_id, message, role, *, user_name, project_id
    ):
        self.messages.append(
            {
                "discussion_id": discussion_id,
                "agent_id": agent_id,
                "message": message,
                "role": role,
                "user_name": user_name,
                "project_id": project_id,
            }
        )


class RecordingKnowledgeStore:
    def __init__(self):
        self.community = RecordingCommunityGraph()


def root_config(max_turns=3):
    root = RootConfig(developer_settings=DeveloperSettings())
    root.llm.agent_model = "fallback-model"
    root.developer_settings.community = CommunitySettings(
        enabled=True,
        max_turns=max_turns,
    )
    return root


def patch_manager_config(monkeypatch, *, max_turns=3):
    root = root_config(max_turns=max_turns)
    monkeypatch.setattr(
        "core.community.community_manager.ConfigManager.get",
        staticmethod(lambda: SimpleNamespace(config=root)),
    )


def patch_events(monkeypatch):
    events = []

    async def fake_emit_community(*args):
        events.append(args)

    monkeypatch.setattr(
        "core.community.community_manager.emit_community",
        fake_emit_community,
    )
    return events


def make_resources(*, redis=None, llm_service=None):
    return SimpleNamespace(
        redis=redis or FakeRedis(),
        postgres=FakePostgresClient(),
        knowledge_store=RecordingKnowledgeStore(),
        llm_service=llm_service or object(),
    )


def make_project_state():
    return SimpleNamespace(
        project_id="project-1",
        topic_config=SimpleNamespace(active_topics=["General"]),
        entities=SimpleNamespace(
            embedding_service=object(),
            project_id="project-1",
            readable_project_ids=["project-1"],
        ),
    )


async def save_agent(postgres, agent_id, *, name=None, persona=None, **overrides):
    agent = AgentConfig(
        id=agent_id,
        name=name or agent_id.title(),
        persona=persona or f"{agent_id} persona",
        model=overrides.pop("model", "agent-model"),
        **overrides,
    )
    postgres.upsert_agent(agent)
    return agent


class RecordingSeedingLLM:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def generate_text(self, system, user, **kwargs):
        self.calls.append({"system": system, "user": user, **kwargs})
        return self.response


@pytest.mark.no_network
async def test_trigger_discussion_skips_when_active_discussion_exists(monkeypatch):
    patch_manager_config(monkeypatch)
    patch_events(monkeypatch)
    resources = make_resources()
    manager = CommunityManager(make_project_state(), "ada", resources)
    await resources.redis.set(manager._active_discussion_key(), "existing-discussion")

    async def fail_seed():
        raise AssertionError("_seed_discussion should not run")

    manager._seed_discussion = fail_seed

    await manager.trigger_discussion()

    assert resources.knowledge_store.community.created == []
    assert manager._discussion_task is None


@pytest.mark.no_network
async def test_seed_discussion_uses_local_agent_references(monkeypatch):
    config = root_config()
    config.developer_settings.community.seeding_agent_id = "agent-omega"
    monkeypatch.setattr(
        "core.community.community_manager.ConfigManager.get",
        staticmethod(lambda: SimpleNamespace(config=config)),
    )
    events = patch_events(monkeypatch)
    llm = RecordingSeedingLLM(
        '{"topic":"Episode quality","agent_ids":["a1","a2","a99"]}'
    )
    resources = make_resources(llm_service=llm)
    await save_agent(
        resources.postgres,
        "agent-alpha",
        name="Analyst",
        persona="Careful evidence analyst",
    )
    await save_agent(
        resources.postgres,
        "agent-omega",
        name="Explorer",
        persona="Curious systems explorer",
    )
    manager = CommunityManager(make_project_state(), "ada", resources)

    seeded = await manager._seed_discussion()

    assert seeded is not None
    assert seeded["agent_ids"] == ["agent-alpha", "agent-omega"]
    prompt = llm.calls[0]["user"]
    assert "- a1: Analyst:" in prompt
    assert "- a2: Explorer:" in prompt
    assert "agent-alpha" not in prompt
    assert "agent-omega" not in prompt
    assert '"agent_ids": ["a1", "a2"]' in prompt
    assert events[-1][2] == "discussion_seeded"
    assert events[-1][3]["agent_ids"] == ["agent-alpha", "agent-omega"]


@pytest.mark.no_network
async def test_seed_discussion_falls_back_to_the_seeding_agent_for_invalid_refs(
    monkeypatch,
):
    config = root_config()
    config.developer_settings.community.seeding_agent_id = "agent-omega"
    monkeypatch.setattr(
        "core.community.community_manager.ConfigManager.get",
        staticmethod(lambda: SimpleNamespace(config=config)),
    )
    patch_events(monkeypatch)
    resources = make_resources(
        llm_service=RecordingSeedingLLM(
            '{"topic":"Episode quality","agent_ids":["a99"]}'
        )
    )
    await save_agent(
        resources.postgres,
        "agent-alpha",
        name="Analyst",
        persona="Careful evidence analyst",
    )
    await save_agent(
        resources.postgres,
        "agent-omega",
        name="Explorer",
        persona="Curious systems explorer",
    )
    manager = CommunityManager(make_project_state(), "ada", resources)

    seeded = await manager._seed_discussion()

    assert seeded is not None
    assert seeded["agent_ids"] == ["agent-omega"]


@pytest.mark.no_network
async def test_trigger_discussion_creates_discussion_and_cleans_up_after_error(
    monkeypatch,
):
    patch_manager_config(monkeypatch)
    events = patch_events(monkeypatch)
    redis = FakeRedis()
    resources = make_resources(redis=redis)
    await save_agent(resources.postgres, "agent-1", name="Analyst")
    manager = CommunityManager(make_project_state(), "ada", resources)

    async def seed_discussion():
        return {
            "topic": "Profile stability",
            "agent_ids": ["agent-1", "missing-agent"],
        }

    async def run_loop(discussion_id, topic, agent_ids):
        assert topic == "Profile stability"
        assert agent_ids == ["agent-1"]
        assert await redis.get(manager._active_discussion_key()) == discussion_id
        raise RuntimeError("loop failed")

    manager._seed_discussion = seed_discussion
    manager._run_loop = run_loop

    await manager.trigger_discussion()
    await manager._discussion_task

    created = resources.knowledge_store.community.created
    assert len(created) == 1
    discussion_id = created[0]["discussion_id"]
    assert created[0]["topic"] == "Profile stability"
    assert created[0]["agent_ids"] == ["agent-1"]
    assert resources.knowledge_store.community.closed == [
        (discussion_id, "ada", "project-1")
    ]
    assert await redis.get(manager._active_discussion_key()) is None
    assert manager._active_discussion_id is None
    assert [event[2] for event in events] == [
        "discussion_started",
        "discussion_ended",
    ]


@pytest.mark.no_network
async def test_trigger_discussion_claims_before_running_the_seed(monkeypatch):
    patch_manager_config(monkeypatch)
    patch_events(monkeypatch)
    redis = FakeRedis()
    resources = make_resources(redis=redis)
    manager = CommunityManager(make_project_state(), "ada", resources)

    async def seed_discussion():
        assert await redis.get(manager._active_discussion_key()) == (
            manager._active_discussion_id
        )
        return None

    manager._seed_discussion = seed_discussion

    await manager.trigger_discussion()

    assert await redis.get(manager._active_discussion_key()) is None
    assert manager._active_discussion_id is None


@pytest.mark.no_network
async def test_trigger_discussion_releases_claim_when_seeding_times_out(monkeypatch):
    patch_manager_config(monkeypatch)
    patch_events(monkeypatch)
    redis = FakeRedis()
    resources = make_resources(redis=redis)
    manager = CommunityManager(make_project_state(), "ada", resources)

    async def timed_out_wait_for(coroutine, *, timeout):
        coroutine.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(
        "core.community.community_manager.asyncio.wait_for", timed_out_wait_for
    )

    await manager.trigger_discussion()

    assert resources.knowledge_store.community.created == []
    assert await redis.get(manager._active_discussion_key()) is None
    assert manager._active_discussion_id is None


@pytest.mark.no_network
async def test_concurrent_aac_triggers_only_seed_one_discussion(monkeypatch):
    patch_manager_config(monkeypatch)
    patch_events(monkeypatch)
    redis = FakeRedis()
    resources = make_resources(redis=redis)
    first = CommunityManager(make_project_state(), "ada", resources)
    second = CommunityManager(make_project_state(), "ada", resources)
    first_seed_started = asyncio.Event()
    release_first_seed = asyncio.Event()
    second_seed_calls = 0

    async def first_seed():
        first_seed_started.set()
        await release_first_seed.wait()
        return None

    async def second_seed():
        nonlocal second_seed_calls
        second_seed_calls += 1
        return None

    first._seed_discussion = first_seed
    second._seed_discussion = second_seed

    first_trigger = asyncio.create_task(first.trigger_discussion())
    await first_seed_started.wait()
    await second.trigger_discussion()

    assert second_seed_calls == 0
    release_first_seed.set()
    await first_trigger
    assert await redis.get(first._active_discussion_key()) is None


@pytest.mark.no_network
async def test_run_loop_rotates_participants_persists_messages_and_stops_on_end(
    monkeypatch,
):
    patch_manager_config(monkeypatch, max_turns=5)
    events = patch_events(monkeypatch)
    resources = make_resources()
    manager = CommunityManager(make_project_state(), "ada", resources)
    await resources.redis.set(manager._active_discussion_key(), "disc-1")
    seen_turns = []

    async def get_agent_config(agent_id):
        return AgentConfig(
            id=agent_id,
            name=agent_id.title(),
            persona=f"{agent_id} persona",
            model="agent-model",
        )

    async def agent_turn(discussion_id, agent, topic, history, participants, ctx):
        seen_turns.append(
            {
                "discussion_id": discussion_id,
                "agent_id": agent.id,
                "topic": topic,
                "history": list(history),
                "participants": list(participants),
                "session_id": ctx.session_id,
            }
        )
        if agent.id == "agent-1":
            return "First contribution"
        return "Second contribution [[END_DISCUSSION]]"

    manager._get_agent_config = get_agent_config
    manager._agent_turn = agent_turn

    await manager._run_loop("disc-1", "Profile stability", ["agent-1", "agent-2"])

    assert [turn["agent_id"] for turn in seen_turns] == ["agent-1", "agent-2"]
    assert seen_turns[1]["history"][0]["content"] == "First contribution"
    assert seen_turns[1]["participants"] == ["agent-1", "agent-2"]
    assert resources.knowledge_store.community.messages == [
        {
            "discussion_id": "disc-1",
            "agent_id": "agent-1",
            "message": "First contribution",
            "role": "assistant",
            "user_name": "ada",
            "project_id": "project-1",
        },
        {
            "discussion_id": "disc-1",
            "agent_id": "agent-2",
            "message": "Second contribution [[END_DISCUSSION]]",
            "role": "assistant",
            "user_name": "ada",
            "project_id": "project-1",
        },
    ]
    assert [event[2] for event in events] == ["message_added", "message_added"]
    assert [args for _, args in resources.redis.evals] == [
        (
            manager._active_discussion_key(),
            "disc-1",
            ACTIVE_DISCUSSION_TTL_SECONDS,
        ),
        (
            manager._active_discussion_key(),
            "disc-1",
            ACTIVE_DISCUSSION_TTL_SECONDS,
        ),
    ]


@pytest.mark.no_network
async def test_run_loop_stops_when_active_discussion_key_changes(monkeypatch):
    patch_manager_config(monkeypatch, max_turns=5)
    patch_events(monkeypatch)
    resources = make_resources()
    manager = CommunityManager(make_project_state(), "ada", resources)
    await resources.redis.set(manager._active_discussion_key(), "disc-1")
    turns = 0

    async def get_agent_config(agent_id):
        return AgentConfig(
            id=agent_id,
            name=agent_id.title(),
            persona=f"{agent_id} persona",
            model="agent-model",
        )

    async def agent_turn(*_args):
        nonlocal turns
        turns += 1
        await resources.redis.set(manager._active_discussion_key(), "other-discussion")
        return "Only first turn"

    manager._get_agent_config = get_agent_config
    manager._agent_turn = agent_turn

    await manager._run_loop("disc-1", "Profile stability", ["agent-1"])

    assert turns == 1
    assert len(resources.knowledge_store.community.messages) == 1


@pytest.mark.no_network
async def test_agent_turn_wires_community_context_tools_memory_and_reasoning(
    monkeypatch,
):
    patch_manager_config(monkeypatch)
    events = patch_events(monkeypatch)
    resources = make_resources()
    manager = CommunityManager(make_project_state(), "ada", resources)
    captured = {}

    async def directives(agent_id):
        assert agent_id == "agent-1"
        return (
            "Required:\n"
            "- Stay grounded\n\n"
            "Preferred:\n"
            "- Prefer evidence\n\n"
            "Avoid:\n"
            "- Vague claims"
        )

    class FakeExecutor:
        def __init__(self, ctx, llm, tools):
            captured["ctx"] = ctx
            captured["llm"] = llm
            captured["tools"] = tools

        async def execute(self, **kwargs):
            captured["execute_kwargs"] = kwargs
            yield {
                "event": "thinking",
                "data": {"content": "Checking prior evidence"},
            }
            yield {"event": "token", "data": {"content": "partial "}}
            yield {"event": "response", "data": {"content": "final answer"}}

    monkeypatch.setattr(
        "core.community.community_manager.AgentExecutor",
        FakeExecutor,
    )
    manager._get_agent_directives = directives
    ctx = SimpleNamespace(
        session_id="aac-disc-1",
        project=make_project_state(),
        document_service=None,
    )
    agent = AgentConfig(
        id="agent-1",
        name="Analyst",
        persona="Careful analyst",
        model="agent-model",
        temperature=0.2,
        brain="Use evidence.",
        enabled_tools=["search_entity", "save_insight", "not_allowed"],
    )

    response = await manager._agent_turn(
        "disc-1",
        agent,
        "Profile stability",
        history=[{"role": "assistant", "content": "previous"}],
        participants=["agent-1", "agent-2"],
        ctx=ctx,
    )

    assert response == "final answer"
    agent_ctx = captured["ctx"]
    assert agent_ctx.is_community is True
    assert agent_ctx.user_query == "Community Discussion Topic: Profile stability"
    assert agent_ctx.scope.session_id == "aac-disc-1"
    assert agent_ctx.agent.config.id == "agent-1"
    assert agent_ctx.agent.name == "Analyst"
    assert "Careful analyst" in agent_ctx.agent.persona
    assert agent_ctx.current_participants == ["agent-1", "agent-2"]
    assert agent_ctx.history == [{"role": "assistant", "content": "previous"}]

    execute_kwargs = captured["execute_kwargs"]
    assert execute_kwargs["model"] == "agent-model"
    assert execute_kwargs["agent_temperature"] == 0.2
    assert execute_kwargs["agent_brain"] == "Use evidence."
    assert execute_kwargs["agent_directives"] == (
        "Required:\n"
        "- Stay grounded\n\n"
        "Preferred:\n"
        "- Prefer evidence\n\n"
        "Avoid:\n"
        "- Vague claims"
    )
    assert execute_kwargs["enabled_tools"] == ["search_entity"]
    assert execute_kwargs["client_tools"] == [
        schema
        for schema in AAC_SPECIFIC_SCHEMAS
        if schema["function"]["name"] == "save_insight"
    ]

    assert captured["tools"].discussion_id == "disc-1"
    assert captured["tools"].agent_id == "agent-1"
    assert captured["tools"].current_participants == ["agent-1", "agent-2"]
    assert events == [
        (
            "ada",
            "community",
            "agent_reasoning",
            {
                "discussion_id": "disc-1",
                "agent_id": "agent-1",
                "reasoning": "Checking prior evidence",
            },
        )
    ]


@pytest.mark.no_network
def test_community_tool_resolution_preserves_empty_allowlists_and_brain_restore():
    manager = CommunityManager(
        make_project_state(),
        "ada",
        make_resources(),
    )

    default_agent = AgentConfig(
        id="agent-default",
        name="Default",
        persona="Default persona",
        enabled_tools=None,
    )
    empty_agent = AgentConfig(
        id="agent-empty",
        name="Empty",
        persona="Empty persona",
        enabled_tools=[],
    )
    restore_agent = AgentConfig(
        id="agent-restore",
        name="Restore",
        persona="Restore persona",
        enabled_tools=["restore_brain_section"],
    )

    enabled, client_tools = manager._resolve_agent_tools(default_agent)
    assert enabled == COMMUNITY_ENABLED_TOOLS
    assert "restore_brain_section" in enabled
    assert client_tools == AAC_SPECIFIC_SCHEMAS

    assert manager._resolve_agent_tools(empty_agent) == ([], [])
    assert manager._resolve_agent_tools(restore_agent) == (
        ["restore_brain_section"],
        [],
    )
