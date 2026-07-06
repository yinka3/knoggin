import pytest

from common.schema.agent_contracts import AgentConfig
from infrastructure.redis_client import RedisKeys
from core.agent.orchestrator import Orchestrator
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
            "topic_evaluation": type(
                "TopicEvaluation",
                (),
                {"enabled": True, "interval_msgs": 10},
            )(),
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

    def __init__(self, ctx, llm, tools):
        self.ctx = ctx
        self.llm = llm
        self.tools = tools
        self.execute_kwargs = None
        self.__class__.instances.append(self)

    async def execute(self, **kwargs):
        self.execute_kwargs = kwargs
        yield {"event": "final", "data": {"content": "done"}}


class FakeSession:
    def __init__(self):
        self.resources = FakeResources()
        self.redis_client = self.resources.redis
        self.knowledge_store = self.resources.knowledge_store
        self.llm = self.resources.llm_service
        self.user_name = "ada"
        self.session_id = "session-1"
        self.project_id = "project-1"


@pytest.fixture(autouse=True)
def reset_fake_executor():
    FakeExecutor.instances = []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_resolves_agent_identity_from_redis():
    context = FakeSession()
    agent = AgentConfig(
        id="agent-1",
        name="Researcher",
        persona="Careful",
        model="model-a",
        temperature=0.3,
        enabled_tools=["search_entity"],
    )
    context.resources.postgres.upsert_agent(agent)

    identity = await Orchestrator()._resolve_agent_identity(
        context,
        agent_id="agent-1",
        name_override=None,
        persona_override=None,
    )

    assert identity["config"].id == "agent-1"
    assert identity["name"] == "Researcher"
    assert "Careful" in identity["persona"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_identity_overrides_take_precedence():
    context = FakeSession()
    context.resources.postgres.upsert_agent(
        AgentConfig(
            id="agent-1",
            name="Researcher",
            persona="Careful",
            is_default=True,
        )
    )

    identity = await Orchestrator()._resolve_agent_identity(
        context,
        agent_id=None,
        name_override="Custom",
        persona_override="Direct",
    )

    assert identity["config"].id == "agent-1"
    assert identity["name"] == "Custom"
    assert identity["persona"] == "Direct"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_stream_builds_context_and_forwards_effective_agent_config(
    monkeypatch,
):
    context = FakeSession()
    tools = FakeTools()
    agent = AgentConfig(
        id="agent-1",
        name="Researcher",
        persona="Careful",
        model="agent-model",
        temperature=0.25,
        brain="Use memory",
        enabled_tools=["fact_check"],
    )
    context.resources.postgres.upsert_agent(agent)

    monkeypatch.setattr(
        "core.agent.orchestrator.ConfigManager.get",
        staticmethod(lambda: FakeConfigManager()),
    )
    monkeypatch.setattr(
        "core.agent.orchestrator.AgentExecutor", FakeExecutor
    )

    async def fake_bootstrap_services(self, context_arg, agent_id):
        assert context_arg is context
        assert agent_id == "agent-1"
        return {
            "topic_config": FakeTopicConfig(),
            "memory": object(),
            "entities": object(),
            "document_service": None,
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
    assert "Use memory" in executor.execute_kwargs["agent_brain"]
    assert executor.execute_kwargs["enabled_tools"] == ["fact_check"]
    assert tools.closed is True


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_forwards_python_selected_maintenance_candidates(
    monkeypatch,
):
    context = FakeSession()
    tools = FakeTools()
    await context.redis_client.set(
        RedisKeys.project_heartbeat_counter("ada", "project-1"),
        "10",
    )
    agent = AgentConfig(
        id="agent-1",
        name="Researcher",
        persona="Careful",
        enabled_tools=["update_topics"],
    )
    context.resources.postgres.upsert_agent(agent)

    monkeypatch.setattr(
        "core.agent.orchestrator.ConfigManager.get",
        staticmethod(lambda: FakeConfigManager()),
    )
    monkeypatch.setattr(
        "core.agent.orchestrator.AgentExecutor", FakeExecutor
    )

    async def fake_bootstrap_services(self, context_arg, agent_id):
        return {
            "topic_config": FakeTopicConfig(),
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
        )
    ]

    assert events == [{"event": "final", "data": {"content": "done"}}]
    candidate = FakeExecutor.instances[0].ctx.maintenance_candidates[0]
    assert candidate.kind == "topic_evaluation"
    assert candidate.suggested_tool == "update_topics"
    assert "heartbeat reached 10" in candidate.reason


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_explicit_hot_topics_override_config_and_are_validated(
    monkeypatch,
):
    context = FakeSession()
    tools = FakeTools()
    context.resources.postgres.upsert_agent(
        AgentConfig(
            id="agent-1",
            name="Researcher",
            persona="Careful",
            is_default=True,
        )
    )

    monkeypatch.setattr(
        "core.agent.orchestrator.ConfigManager.get",
        staticmethod(lambda: FakeConfigManager()),
    )
    monkeypatch.setattr(
        "core.agent.orchestrator.AgentExecutor", FakeExecutor
    )

    async def fake_bootstrap_services(self, context_arg, agent_id):
        return {
            "topic_config": FakeTopicConfig(),
            "memory": object(),
            "entities": object(),
            "document_service": None,
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


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_loads_validated_document_focus_once():
    context = FakeSession()
    focus = {
        "mode": "pinned",
        "target_type": "subtree",
        "document_id": None,
        "relative_path": None,
        "folder_root_id": "folder-1",
        "path_prefix": "src",
        "created_at": "2026-06-22T12:00:00+00:00",
    }
    context.resources.postgres.read_results.append([{"document_focus": focus}])

    class FocusDocumentService:
        async def resolve_focus_target(self, **kwargs):
            assert kwargs == {
                "session_id": "session-1",
                "document_id": None,
                "folder_root_id": "folder-1",
                "path_prefix": "src",
            }
            return {
                "target_type": "subtree",
                "document_id": None,
                "relative_path": None,
                "folder_root_id": "folder-1",
                "path_prefix": "src",
            }

    context.document_service = FocusDocumentService()

    loaded = await Orchestrator()._load_document_focus(context)

    assert loaded == focus


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_ignores_stale_document_focus():
    context = FakeSession()

    class MissingFocusService:
        async def resolve_focus_target(self, **kwargs):
            raise FileNotFoundError("Document focus target not found")

    context.document_service = MissingFocusService()
    context.resources.postgres.read_results.append(
        [
            {
                "document_focus": {
                    "mode": "pinned",
                    "target_type": "folder_upload",
                    "document_id": None,
                    "relative_path": None,
                    "folder_root_id": "missing-folder",
                    "path_prefix": None,
                    "created_at": "2026-06-22T12:00:00+00:00",
                }
            }
        ]
    )

    assert await Orchestrator()._load_document_focus(context) is None
