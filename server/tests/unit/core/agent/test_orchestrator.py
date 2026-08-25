from types import SimpleNamespace

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.agent.identity import AgentConfig
from core.agent.orchestrator import AgentOrchestrator
from core.agent.services.agent_manager import AgentManager
from tests.fixtures.fakes import FakeResources

FAKE_RESPONSE_EVENT = {
    "event": "response",
    "data": {
        "content": "done",
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "approximate": False,
        },
    },
}


class FakeLimits:
    max_tool_calls = 9
    tool_timeout = 1.5
    max_attempts = 8
    agent_history_turns = 3
    max_accumulated_messages = 12
    max_consecutive_errors = 2
    tool_limit_overrides = {"search_entity": 4}


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

    @staticmethod
    def get():
        return FakeConfigManager()


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


class FakeExecutor:
    instances = []

    def __init__(self, ctx, llm, tools, *, on_successful_completion=None):
        self.ctx = ctx
        self.llm = llm
        self.tools = tools
        self.on_successful_completion = on_successful_completion
        self.execute_kwargs = None
        self.__class__.instances.append(self)

    async def execute(self, **kwargs):
        self.execute_kwargs = kwargs
        yield FAKE_RESPONSE_EVENT


class FakeSession:
    def __init__(self):
        self.resources = FakeResources()
        self.knowledge_store = self.resources.knowledge_store
        self.llm = self.resources.llm_service
        self.user_name = "ada"
        self.session_id = "session-1"
        self.project_id = "project-1"
        self.model = None
        self.agent_id = None
        self.enabled_tools = None
        self.document_focus = None
        self.document_service = None
        self.project = SimpleNamespace(
            entities=object(),
            compiled_domain=DomainConfig.from_mapping(
                {
                    "version": 1,
                    "topics": {
                        "Research": {"active": True},
                        "Identity": {"active": True},
                    },
                    "entity_types": {
                        "Identity": {"topic": "Identity", "labels": ["identity"]}
                    },
                }
            ).compile(),
        )


def make_orchestrator(context):
    return AgentOrchestrator(
        AgentManager(context.resources, context.user_name),
        config_provider=FakeConfigManager,
    )


@pytest.fixture(autouse=True)
def reset_fake_executor():
    FakeExecutor.instances = []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_resolves_durable_agent_identity():
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

    identity = await make_orchestrator(context)._resolve_agent_identity(
        agent_id="agent-1",
        name_override=None,
        persona_override=None,
    )

    assert identity.config.id == "agent-1"
    assert identity.name == "Researcher"
    assert "Careful" in identity.persona


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

    identity = await make_orchestrator(context)._resolve_agent_identity(
        agent_id=None,
        name_override="Custom",
        persona_override="Direct",
    )

    assert identity.config.id == "agent-1"
    assert identity.name == "Custom"
    assert identity.persona == "Direct"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_compiles_selected_research_mode_into_same_agent_run(
    monkeypatch,
):
    context = FakeSession()
    context.resources.postgres.upsert_agent(
        AgentConfig(
            id="agent-1",
            name="Researcher",
            persona="Careful",
            is_default=True,
        )
    )
    tools = FakeTools()
    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FakeExecutor)

    async def fake_bootstrap_services(self, context_arg, agent_id, document_focus):
        return tools

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services)

    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query="Investigate this",
            context=context,
            research_mode="research",
        )
    ]

    assert events == [FAKE_RESPONSE_EVENT]
    run = FakeExecutor.instances[0].ctx
    assert run.research_profile.mode == "research"
    assert run.limits.max_calls == FakeLimits.max_tool_calls * 2
    assert run.limits.max_attempts == FakeLimits.max_attempts * 2


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
        enabled_tools=["episode_check"],
    )
    context.resources.postgres.upsert_agent(agent)

    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FakeExecutor)

    async def fake_bootstrap_services(self, context_arg, agent_id, document_focus):
        assert context_arg is context
        assert agent_id == "agent-1"
        return tools

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services)

    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query="hello",
            context=context,
            agent_id="agent-1",
            conversation_history=[{"role": "user", "content": "prior"}],
        )
    ]

    assert events == [FAKE_RESPONSE_EVENT]
    executor = FakeExecutor.instances[0]
    assert executor.ctx.user_query == "hello"
    assert executor.ctx.history == [{"role": "user", "content": "prior"}]
    assert executor.ctx.limits.max_calls == 9
    assert executor.ctx.limits.tool_timeout == 1.5
    assert executor.ctx.limits.get_tool_limit("search_entity") == 4
    assert executor.ctx.hot_topics == []
    assert executor.ctx.active_topics == ["Research", "Identity"]
    assert executor.ctx.hot_topic_context == {}
    assert tools.hot_topic_calls == []
    assert executor.ctx.model == "agent-model"
    assert executor.ctx.temperature == 0.25
    assert "Use memory" in executor.ctx.brain
    assert executor.ctx.enabled_tools == ("episode_check",)
    assert executor.execute_kwargs == {
        "user_timezone": None,
        "simulated_date": None,
    }
    assert tools.closed is True


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_resolves_request_then_session_then_agent_config(
    monkeypatch,
):
    context = FakeSession()
    context.model = "session-model"
    context.enabled_tools = ["message_search"]
    tools = FakeTools()
    context.resources.postgres.upsert_agent(
        AgentConfig(
            id="agent-1",
            name="Researcher",
            persona="Careful",
            model="agent-model",
            enabled_tools=["episode_check"],
            is_default=True,
        )
    )

    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FakeExecutor)

    async def fake_bootstrap_services(self, context_arg, agent_id, document_focus):
        assert context_arg is context
        assert agent_id == "agent-1"
        assert document_focus is None
        return tools

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services)

    async for _ in make_orchestrator(context).run_stream(
        user_query="hello",
        context=context,
        model="request-model",
        enabled_tools=["graph_query"],
    ):
        pass
    request_run = FakeExecutor.instances[-1]

    async for _ in make_orchestrator(context).run_stream(
        user_query="hello",
        context=context,
    ):
        pass
    session_run = FakeExecutor.instances[-1]

    assert request_run.ctx.model == "request-model"
    assert request_run.ctx.enabled_tools == ("graph_query",)
    assert session_run.ctx.model == "session-model"
    assert session_run.ctx.enabled_tools == ("message_search",)
    assert request_run.execute_kwargs == {
        "user_timezone": None,
        "simulated_date": None,
    }
    assert session_run.execute_kwargs == {
        "user_timezone": None,
        "simulated_date": None,
    }


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_hides_unexpected_error_details(monkeypatch):
    context = FakeSession()

    async def fail_bootstrap_services(*_args):
        raise RuntimeError("postgres://internal-host/knoggin")

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fail_bootstrap_services)

    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query="hello",
            context=context,
        )
    ]

    assert events == [
        {
            "event": "error",
            "data": {
                "message": "The agent couldn't complete this request. Please try again."
            },
        }
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_preserves_an_explicit_empty_tool_allowlist(
    monkeypatch,
):
    context = FakeSession()
    tools = FakeTools()
    agent = AgentConfig(
        id="agent-1",
        name="Researcher",
        persona="Careful",
        enabled_tools=[],
    )
    context.resources.postgres.upsert_agent(agent)

    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FakeExecutor)

    async def fake_bootstrap_services(self, context_arg, agent_id, document_focus):
        assert context_arg is context
        assert agent_id == "agent-1"
        return tools

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services)

    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query="hello",
            context=context,
            agent_id="agent-1",
        )
    ]

    assert events == [FAKE_RESPONSE_EVENT]
    assert FakeExecutor.instances[0].ctx.enabled_tools == ()


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_does_not_inject_maintenance_candidates(
    monkeypatch,
):
    context = FakeSession()
    tools = FakeTools()
    agent = AgentConfig(
        id="agent-1",
        name="Researcher",
        persona="Careful",
        enabled_tools=["check_graph_health"],
    )
    context.resources.postgres.upsert_agent(agent)

    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FakeExecutor)

    async def fake_bootstrap_services(self, context_arg, agent_id, document_focus):
        return tools

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services)

    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query="hello",
            context=context,
            agent_id="agent-1",
        )
    ]

    assert events == [FAKE_RESPONSE_EVENT]
    assert not hasattr(FakeExecutor.instances[0].ctx, "maintenance_candidates")


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

    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FakeExecutor)

    async def fake_bootstrap_services(self, context_arg, agent_id, document_focus):
        return tools

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services)

    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query="hello",
            context=context,
            hot_topics=["Identity", "General", "Identity"],
        )
    ]

    assert events == [FAKE_RESPONSE_EVENT]
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
async def test_orchestrator_resolves_session_document_focus_without_querying_postgres():
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

    context.document_focus = focus
    loaded = await make_orchestrator(context)._resolve_document_focus(
        context,
        context.document_focus,
    )

    assert loaded is not None
    assert loaded.mode == "pinned"
    assert loaded.target_type == "subtree"
    assert loaded.folder_root_id == "folder-1"
    assert loaded.path_prefix == "src"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_ignores_stale_document_focus():
    context = FakeSession()

    class MissingFocusService:
        async def resolve_focus_target(self, **kwargs):
            raise FileNotFoundError("Document focus target not found")

    context.document_service = MissingFocusService()
    context.document_focus = {
        "mode": "pinned",
        "target_type": "folder_upload",
        "folder_root_id": "missing-folder",
        "created_at": "2026-06-22T12:00:00+00:00",
    }

    assert await make_orchestrator(context)._resolve_document_focus(
        context,
        context.document_focus,
    ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_preserves_canonical_request_document_selection():
    context = FakeSession()

    class FocusDocumentService:
        async def resolve_focus_target(self, **kwargs):
            assert kwargs == {
                "session_id": "session-1",
                "document_id": "document-1",
                "folder_root_id": None,
                "path_prefix": None,
            }
            return {
                "target_type": "document",
                "document_id": "document-1",
                "relative_path": "docs/notes.py",
            }

    context.document_service = FocusDocumentService()
    focus = {
        "mode": "request",
        "target_type": "document",
        "document_id": "document-1",
        "relative_path": "docs/notes.py",
        "selection": {
            "content_hash": "a" * 64,
            "locator": {"kind": "code_lines", "start_line": 2, "end_line": 3},
        },
        "created_at": "2026-06-22T12:00:00+00:00",
    }

    loaded = await make_orchestrator(context)._resolve_document_focus(context, focus)

    assert loaded is not None
    assert loaded.mode == "request"
    assert loaded.selection is not None
    assert loaded.selection.locator.kind == "code_lines"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_seeds_pasted_text_candidates_from_canonical_turn(
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
    user_query = "Use this source: revenue increased 18%."
    excerpt = "revenue increased 18%"
    start_char = user_query.index(excerpt)

    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FakeExecutor)

    async def fake_bootstrap_services(self, context_arg, agent_id, document_focus):
        return tools

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services)

    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query=user_query,
            context=context,
            user_message_id=42,
            pasted_text_spans=[
                {"start_char": start_char, "end_char": start_char + len(excerpt)}
            ],
        )
    ]

    assert events == [FAKE_RESPONSE_EVENT]
    candidates = FakeExecutor.instances[0].ctx.initial_source_candidates
    assert len(candidates) == 1
    assert candidates[0].source_message_id == 42
    assert candidates[0].excerpt == excerpt
