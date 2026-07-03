from types import SimpleNamespace

import pytest

from common.schema.aac_schema import AAC_SPECIFIC_SCHEMAS
from common.schema.agent_contracts import AgentConfig
from common.schema.settings import CommunitySettings, DeveloperSettings, RootConfig
from knoggin_server.community.community_manager import CommunityManager
from tests.fixtures.fakes import FakePostgresClient, FakeRedis


class RecordingCommunityGraph:
    def __init__(self):
        self.created = []
        self.closed = []
        self.messages = []

    async def create_discussion(self, discussion_id, topic, agent_ids):
        self.created.append(
            {
                "discussion_id": discussion_id,
                "topic": topic,
                "agent_ids": list(agent_ids),
            }
        )

    async def close_discussion(self, discussion_id):
        self.closed.append(discussion_id)

    async def add_message(self, discussion_id, agent_id, message, role):
        self.messages.append(
            {
                "discussion_id": discussion_id,
                "agent_id": agent_id,
                "message": message,
                "role": role,
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
        "knoggin_server.community.community_manager.ConfigManager.get",
        staticmethod(lambda: SimpleNamespace(config=root)),
    )


def patch_events(monkeypatch):
    events = []

    async def fake_emit_community(*args):
        events.append(args)

    monkeypatch.setattr(
        "knoggin_server.community.community_manager.emit_community",
        fake_emit_community,
    )
    return events


def make_resources(*, redis=None):
    return SimpleNamespace(
        redis=redis or FakeRedis(),
        postgres=FakePostgresClient(),
        knowledge_store=RecordingKnowledgeStore(),
        llm_service=object(),
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
    assert resources.knowledge_store.community.closed == [discussion_id]
    assert await redis.get(manager._active_discussion_key()) is None
    assert manager._active_discussion_id is None
    assert [event[2] for event in events] == [
        "discussion_started",
        "discussion_ended",
    ]


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

    class FakeAssembler:
        def __init__(self, user_name, resources_arg):
            assert user_name == "ada"
            assert resources_arg is resources

        async def assemble(self, project_state, session_id):
            assert session_id == "aac_disc-1"
            return SimpleNamespace(
                session_id=session_id,
                project=project_state,
                document_service=None,
            )

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

    monkeypatch.setattr(
        "knoggin_server.community.community_manager.SessionFactory",
        FakeAssembler,
    )
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
        },
        {
            "discussion_id": "disc-1",
            "agent_id": "agent-2",
            "message": "Second contribution [[END_DISCUSSION]]",
            "role": "assistant",
        },
    ]
    assert [event[2] for event in events] == ["message_added", "message_added"]


@pytest.mark.no_network
async def test_run_loop_stops_when_active_discussion_key_changes(monkeypatch):
    patch_manager_config(monkeypatch, max_turns=5)
    patch_events(monkeypatch)
    resources = make_resources()
    manager = CommunityManager(make_project_state(), "ada", resources)
    await resources.redis.set(manager._active_discussion_key(), "disc-1")
    turns = 0

    class FakeAssembler:
        def __init__(self, *_args):
            pass

        async def assemble(self, project_state, session_id):
            return SimpleNamespace(
                session_id=session_id,
                project=project_state,
                document_service=None,
            )

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

    monkeypatch.setattr(
        "knoggin_server.community.community_manager.SessionFactory",
        FakeAssembler,
    )
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
        "knoggin_server.community.community_manager.AgentExecutor",
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
    assert agent_ctx.session_id == "aac-disc-1"
    assert agent_ctx.agent_id == "agent-1"
    assert agent_ctx.agent_name == "Analyst"
    assert "Careful analyst" in agent_ctx.agent_persona
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
