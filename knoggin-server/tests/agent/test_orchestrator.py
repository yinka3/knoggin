import json

import pytest

from common.schema.agent_contracts import AgentConfig
from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.orchestrator import Orchestrator
from tests.fixtures.fakes import FakeResources


class FakeLimits:
    max_tool_calls = 9
    max_attempts = 8
    agent_history_turns = 3
    max_accumulated_messages = 12
    max_consecutive_errors = 2
    tool_limits = {"search_entity": 4}


class FakeConfig:
    developer_settings = type(
        "DeveloperSettings",
        (),
        {
            "limits": FakeLimits(),
            "search": type("Search", (), {"model_dump": lambda self: {}})(),
        },
    )()
    search = type("SearchKeys", (), {"model_dump": lambda self: {}})()


class FakeConfigManager:
    config = FakeConfig()


class FakeTools:
    def __init__(self):
        self.closed = False
        self.hot_topic_calls = []

    async def close(self):
        self.closed = True

    async def get_hot_topic_context(self, hot_topics, slim=False):
        self.hot_topic_calls.append((hot_topics, slim))
        return {
            topic: {"entities": [{"name": f"{topic} entity"}], "messages": []}
            for topic in hot_topics
        }


class FakeTopicConfig:
    hot_topics = ["Research", "unknown"]
    active_topics = ["Research", "Identity"]

    def validate_hot_topics(self, hot_topics):
        valid = []
        for topic in hot_topics:
            if topic in self.active_topics and topic not in valid:
                valid.append(topic)
        return valid


class FakeExecutor:
    instances = []

    def __init__(self, ctx, llm, tools, memory_mgr):
        self.ctx = ctx
        self.llm = llm
        self.tools = tools
        self.memory_mgr = memory_mgr
        self.execute_kwargs = None
        self.__class__.instances.append(self)

    async def execute(self, **kwargs):
        self.execute_kwargs = kwargs
        yield {"event": "final", "data": {"content": "done"}}


class FakeContext:
    def __init__(self):
        self.resources = FakeResources()
        self.redis_client = self.resources.redis
        self.graph_client = self.resources.graph
        self.llm = self.resources.llm_service
        self.user_name = "ada"
        self.session_id = "session-1"


@pytest.fixture(autouse=True)
def reset_fake_executor():
    FakeExecutor.instances = []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_resolves_agent_identity_from_redis():
    redis = FakeResources().redis
    agent = AgentConfig(
        id="agent-1",
        name="Researcher",
        persona="Careful",
        model="model-a",
        temperature=0.3,
        enabled_tools=["search_entity"],
    )
    await redis.hset(RedisKeys.agents("ada"), "agent-1", json.dumps(agent.to_dict()))

    identity = await Orchestrator()._resolve_agent_identity(
        "ada",
        redis,
        agent_id="agent-1",
        name_override=None,
        persona_override=None,
    )

    assert identity["config"].id == "agent-1"
    assert identity["name"] == "Researcher"
    assert identity["persona"] == "Careful"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_identity_overrides_take_precedence():
    redis = FakeResources().redis

    identity = await Orchestrator()._resolve_agent_identity(
        "ada",
        redis,
        agent_id=None,
        name_override="Custom",
        persona_override="Direct",
    )

    assert identity["config"] is None
    assert identity["name"] == "Custom"
    assert identity["persona"] == "Direct"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_stream_builds_context_and_forwards_effective_agent_config(
    monkeypatch,
):
    context = FakeContext()
    tools = FakeTools()
    agent = AgentConfig(
        id="agent-1",
        name="Researcher",
        persona="Careful",
        model="agent-model",
        temperature=0.25,
        instructions="Use memory",
        enabled_tools=["fact_check"],
    )
    await context.redis_client.hset(
        RedisKeys.agents("ada"), "agent-1", json.dumps(agent.to_dict())
    )

    monkeypatch.setattr(
        "knoggin_server.agent.orchestrator.ConfigManager.get",
        staticmethod(lambda: FakeConfigManager()),
    )
    monkeypatch.setattr(
        "knoggin_server.agent.orchestrator.AgentExecutor", FakeExecutor
    )

    async def fake_bootstrap_services(self, context_arg, agent_id):
        assert context_arg is context
        assert agent_id == "agent-1"
        return {
            "topic_config": FakeTopicConfig(),
            "memory": object(),
            "entities": object(),
            "file_rag": None,
            "tools": tools,
        }

    monkeypatch.setattr(
        Orchestrator, "_bootstrap_services", fake_bootstrap_services
    )

    events = [
        event
        async for event in Orchestrator().run_stream(
            user_query="hello",
            user_name="ada",
            session_id="session-1",
            context=context,
            agent_id="agent-1",
            conversation_history=[{"role": "user", "content": "prior"}],
        )
    ]

    assert events == [{"event": "final", "data": {"content": "done"}}]
    executor = FakeExecutor.instances[0]
    assert executor.ctx.user_query == "hello"
    assert executor.ctx.history == [{"role": "user", "content": "prior"}]
    assert executor.ctx.config.max_calls == 9
    assert executor.ctx.config.get_tool_limit("search_entity") == 4
    assert executor.ctx.hot_topics == ["Research"]
    assert executor.ctx.active_topics == ["Research", "Identity"]
    assert executor.ctx.hot_topic_context == {
        "Research": {
            "entities": [{"name": "Research entity"}],
            "messages": [],
        }
    }
    assert tools.hot_topic_calls == [(["Research"], True)]
    assert executor.execute_kwargs["model"] == "agent-model"
    assert executor.execute_kwargs["agent_temperature"] == 0.25
    assert executor.execute_kwargs["agent_instructions"] == "Use memory"
    assert executor.execute_kwargs["enabled_tools"] == ["fact_check"]
    assert tools.closed is True


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_explicit_hot_topics_override_config_and_are_validated(
    monkeypatch,
):
    context = FakeContext()
    tools = FakeTools()

    monkeypatch.setattr(
        "knoggin_server.agent.orchestrator.ConfigManager.get",
        staticmethod(lambda: FakeConfigManager()),
    )
    monkeypatch.setattr(
        "knoggin_server.agent.orchestrator.AgentExecutor", FakeExecutor
    )

    async def fake_bootstrap_services(self, context_arg, agent_id):
        return {
            "topic_config": FakeTopicConfig(),
            "memory": object(),
            "entities": object(),
            "file_rag": None,
            "tools": tools,
        }

    monkeypatch.setattr(Orchestrator, "_bootstrap_services", fake_bootstrap_services)

    events = [
        event
        async for event in Orchestrator().run_stream(
            user_query="hello",
            user_name="ada",
            session_id="session-1",
            context=context,
            hot_topics=["Identity", "General", "Identity"],
        )
    ]

    assert events == [{"event": "final", "data": {"content": "done"}}]
    executor = FakeExecutor.instances[0]
    assert executor.ctx.hot_topics == ["Identity"]
    assert executor.ctx.hot_topic_context == {
        "Identity": {
            "entities": [{"name": "Identity entity"}],
            "messages": [],
        }
    }
    assert tools.hot_topic_calls == [(["Identity"], True)]
