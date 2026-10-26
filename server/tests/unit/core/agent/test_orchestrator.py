from types import SimpleNamespace

import pytest

from common.conf.domain_config import DomainConfig
from common.exceptions import LLMBudgetExceededError, WorkspaceConflictError
from common.schema.agent.identity import AgentConfig
from common.schema.agent.settings import PromptMemorySettings
from core.agent.agent_manager import AgentManager
from core.agent.orchestrator import AgentOrchestrator
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


def _resolved_response_event(agent_id="agent-1"):
    return {
        **FAKE_RESPONSE_EVENT,
        "data": {
            **FAKE_RESPONSE_EVENT["data"],
            "resolved_agent_id": agent_id,
        },
    }


def _after_budget(events):
    assert events[0]["event"] == "prompt_budget_selected"
    assert events[0]["data"]["agent_id"] == "agent-1"
    assert events[0]["data"]["budget"]["input_tokens"] == 12_000
    return events[1:]


class FakeLimits:
    max_tool_calls = 9
    tool_timeout = 1.5
    max_attempts = 8
    max_accumulated_messages = 12
    max_accumulated_profiles = 8
    max_accumulated_graph = 16
    max_accumulated_paths = 4
    max_accumulated_episodes = 6
    max_accumulated_documents = 10
    max_accumulated_web_discoveries = 5
    max_accumulated_web_reads = 5
    max_accumulated_actions = 6
    max_accumulated_next_steps = 6
    max_accumulated_summary_chars = 2000
    max_consecutive_errors = 2
    tool_limit_overrides = {"search_knowledge_entities": 4}


class FakeConfig:
    developer_settings = type(
        "DeveloperSettings",
        (),
        {
            "limits": FakeLimits(),
            "prompt_memory": PromptMemorySettings(),
            "search": type("Search", (), {"model_dump": lambda self: {}})(),
        },
    )()
    search = type("SearchKeys", (), {"model_dump": lambda self: {}})()


class FakeConfigManager:
    config = FakeConfig()


class FakeTools:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


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
        config_manager=FakeConfigManager(),
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
        enabled_tools=["search_knowledge_entities"],
    )
    context.resources.postgres.upsert_agent(agent)

    identity = await make_orchestrator(context)._resolve_agent_identity(
        agent_id="agent-1",
    )

    assert identity.config.id == "agent-1"
    assert identity.name == "Researcher"
    assert "Careful" in identity.persona


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_uses_durable_agent_identity():
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
    )

    assert identity.config.id == "agent-1"
    assert identity.name == "Researcher"
    assert "Careful" in identity.persona


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

    assert _after_budget(events) == [_resolved_response_event()]
    run = FakeExecutor.instances[0].ctx
    assert run.research_profile.mode == "research"
    assert run.limits.max_calls == FakeLimits.max_tool_calls * 2
    assert run.limits.max_attempts == FakeLimits.max_attempts * 2
    assert run.project_briefing.mode == "adaptive"
    assert run.project_briefing.initial_reason == "research_mode"


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
        enabled_tools=["search_episodes"],
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

    assert _after_budget(events) == [_resolved_response_event()]
    executor = FakeExecutor.instances[0]
    assert executor.ctx.user_query == "hello"
    assert executor.ctx.history == [{"role": "user", "content": "prior"}]
    assert executor.ctx.limits.max_calls == 9
    assert executor.ctx.limits.tool_timeout == 1.5
    assert executor.ctx.limits.get_tool_limit("search_knowledge_entities") == 4
    assert not hasattr(executor.ctx, "active_topics")
    assert executor.ctx.model == "agent-model"
    assert executor.ctx.temperature == 0.25
    assert "Use memory" in executor.ctx.brain
    assert executor.ctx.enabled_tools == ("search_episodes",)
    assert executor.ctx.project_briefing.mode == "adaptive"
    assert executor.ctx.project_briefing.initial_reason is None
    assert executor.execute_kwargs == {"user_timezone": None}
    assert executor.on_successful_completion is None
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
            enabled_tools=["search_episodes"],
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
    assert request_run.execute_kwargs == {"user_timezone": None}
    assert session_run.execute_kwargs == {"user_timezone": None}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_admitted_model_and_prompt_budget_ignore_later_session_change(monkeypatch):
    context = FakeSession()
    context.model = "later-model"
    context.resources.postgres.upsert_agent(AgentConfig(
        id="agent-1", name="Researcher", persona="Careful", is_default=True,
    ))
    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FakeExecutor)

    async def fake_bootstrap_services(self, context_arg, agent_id, document_focus):
        return FakeTools()

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services)
    captured = SimpleNamespace(
        config=FakeConfig(), model="captured-model", agent_id=None,
        enabled_tools=None, document_focus=None,
    )
    events = [event async for event in make_orchestrator(context).run_stream(
        user_query="hello", context=context, run_settings=captured,
    )]

    assert events[0]["event"] == "prompt_budget_selected"
    assert events[0]["data"]["model"] == "captured-model"
    assert FakeExecutor.instances[-1].ctx.model == "captured-model"


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
@pytest.mark.parametrize(
    ("error", "expected_data"),
    [
        (
            LLMBudgetExceededError("private provider budget details"),
            {
                "message": "The model budget is exhausted.",
                "code": "llm_budget_exhausted",
                "retryable": False,
            },
        ),
        (
            WorkspaceConflictError("private revision details"),
            {
                "message": "The workspace changed before the request could be applied.",
                "code": "workspace_conflict",
                "retryable": False,
            },
        ),
    ],
)
async def test_orchestrator_translates_expected_runtime_failures(
    monkeypatch, error, expected_data
):
    context = FakeSession()
    tools = FakeTools()
    context.resources.postgres.upsert_agent(
        AgentConfig(
            id="agent-1", name="Researcher", persona="Careful", is_default=True
        )
    )

    class FailingExecutor(FakeExecutor):
        async def execute(self, **kwargs):
            self.execute_kwargs = kwargs
            if False:
                yield FAKE_RESPONSE_EVENT
            raise error

    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FailingExecutor)

    async def fake_bootstrap_services(*_args):
        return tools

    monkeypatch.setattr(
        AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services
    )

    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query="hello",
            context=context,
        )
    ]

    assert _after_budget(events) == [{"event": "error", "data": expected_data}]
    assert tools.closed is True


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

    assert _after_budget(events) == [_resolved_response_event()]
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
        enabled_tools=["inspect_duplicate_entities"],
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

    assert _after_budget(events) == [_resolved_response_event()]
    assert not hasattr(FakeExecutor.instances[0].ctx, "maintenance_candidates")

@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_resolves_session_document_focus_without_querying_postgres():
    context = FakeSession()
    focus = {
        "mode": "pinned",
        "target_type": "subtree",
        "path_prefix": "src",
        "created_at": "2026-06-22T12:00:00+00:00",
    }

    class FocusDocumentService:
        async def resolve_focus_target(self, **kwargs):
            assert kwargs == {
                "document_id": None,
                "path_prefix": "src",
            }
            return {
                "target_type": "subtree",
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
    assert loaded.path_prefix == "src"


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize("mode", ["pinned", "request"])
async def test_orchestrator_ignores_stale_preferred_document_focus(mode):
    context = FakeSession()

    class MissingFocusService:
        async def resolve_focus_target(self, **kwargs):
            raise FileNotFoundError("Document focus target not found")

    context.document_service = MissingFocusService()
    context.document_focus = {
        "mode": mode,
        "behavior": "prefer",
        "target_type": "subtree",
        "path_prefix": "missing-folder",
        "created_at": "2026-06-22T12:00:00+00:00",
    }

    assert await make_orchestrator(context)._resolve_document_focus(
        context,
        context.document_focus,
    ) is None


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("mode", "behavior"),
    [("request", "restrict"), ("pinned", "restrict")],
)
async def test_orchestrator_stops_when_a_required_focus_is_stale(
    monkeypatch, mode, behavior
):
    context = FakeSession()
    context.resources.postgres.upsert_agent(
        AgentConfig(id="agent-1", name="Researcher", persona="Careful", is_default=True)
    )

    class MissingFocusService:
        async def resolve_focus_target(self, **kwargs):
            raise FileNotFoundError("Document focus target not found")

    context.document_service = MissingFocusService()
    focus = {
        "mode": mode,
        "behavior": behavior,
        "target_type": "subtree",
        "path_prefix": "missing-folder",
        "created_at": "2026-06-22T12:00:00+00:00",
    }

    async def unexpected_bootstrap(*args, **kwargs):
        raise AssertionError("Unrestricted tools must not be created")

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", unexpected_bootstrap)
    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query="hello",
            context=context,
            request_document_focus=focus,
        )
    ]

    assert events[0]["event"] == "error"
    assert events[0]["data"]["code"] == "workspace_conflict"
    assert FakeExecutor.instances == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_stops_restricted_focus_without_document_service():
    context = FakeSession()
    focus = {
        "mode": "request",
        "behavior": "restrict",
        "target_type": "subtree",
        "path_prefix": "docs",
        "created_at": "2026-06-22T12:00:00+00:00",
    }

    with pytest.raises(WorkspaceConflictError):
        await make_orchestrator(context)._resolve_document_focus(context, focus)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_preserves_canonical_request_document_selection():
    context = FakeSession()

    class FocusDocumentService:
        async def resolve_focus_target(self, **kwargs):
            assert kwargs == {
                "document_id": "document-1",
                "path_prefix": None,
            }
            return {
                "target_type": "document",
                "document_id": "document-1",
                "relative_path": "docs/notes.py",
            }

        async def resolve_document_selection(self, **kwargs):
            return {
                "document_id": "document-1",
                "project_id": "project-1",
                "relative_path": "docs/notes.py",
                "document_name": "notes.py",
                "extension": ".py",
                "chunk_index": "lines:2-3",
                "content_hash": "a" * 64,
                "parse_snapshot_id": "snapshot-1",
                "locator": {
                    "kind": "code_lines",
                    "start_line": 2,
                    "end_line": 3,
                },
                "content": "2: def useful():\n3:     return 42",
            }

    context.document_service = FocusDocumentService()
    focus = {
        "mode": "request",
        "target_type": "document",
        "document_id": "document-1",
        "relative_path": "docs/notes.py",
        "selection": {
            "content_hash": "a" * 64,
            "parse_snapshot_id": "snapshot-1",
            "locator": {"kind": "code_lines", "start_line": 2, "end_line": 3},
        },
        "created_at": "2026-06-22T12:00:00+00:00",
    }

    loaded = await make_orchestrator(context)._resolve_document_focus(context, focus)

    assert loaded is not None
    assert loaded.mode == "request"
    assert loaded.selection is not None
    assert loaded.selection.locator.kind == "code_lines"
    selection_context = await make_orchestrator(context)._resolve_document_selection_context(
        context,
        loaded,
    )
    assert selection_context == {
        "document_id": "document-1",
        "project_id": "project-1",
        "relative_path": "docs/notes.py",
        "document_name": "notes.py",
        "extension": ".py",
        "chunk_index": "lines:2-3",
        "content_hash": "a" * 64,
        "parse_snapshot_id": "snapshot-1",
        "locator": {"kind": "code_lines", "start_line": 2, "end_line": 3},
        "excerpt": "2: def useful():\n3:     return 42",
    }

    candidate = make_orchestrator(context)._initial_source_candidates(
        context=context,
        run_id="run-1",
        user_message_id=None,
        message_content="Use this passage",
        pasted_text_spans=None,
        selection_context=selection_context,
    )[0]
    assert candidate.parse_snapshot_id == "snapshot-1"
    assert candidate.excerpt == "2: def useful():\n3:     return 42"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_orchestrator_runs_with_a_selected_document_passage(monkeypatch):
    context = FakeSession()
    context.resources.postgres.upsert_agent(
        AgentConfig(id="agent-1", name="Researcher", persona="Careful", is_default=True)
    )

    class FocusDocumentService:
        async def resolve_focus_target(self, **kwargs):
            return {
                "target_type": "document",
                "document_id": "document-1",
                "relative_path": "docs/notes.md",
            }

        async def resolve_document_selection(self, **kwargs):
            return {
                "document_id": "document-1",
                "project_id": "project-1",
                "relative_path": "docs/notes.md",
                "document_name": "notes.md",
                "extension": ".md",
                "chunk_index": "lines:2-3",
                "content_hash": "a" * 64,
                "parse_snapshot_id": "snapshot-1",
                "locator": {"kind": "text_lines", "start_line": 2, "end_line": 3},
                "content": "2: Useful note\n3: Another note",
            }

    context.document_service = FocusDocumentService()
    monkeypatch.setattr("core.agent.orchestrator.AgentExecutor", FakeExecutor)

    async def fake_bootstrap_services(self, context_arg, agent_id, document_focus):
        return FakeTools()

    monkeypatch.setattr(AgentOrchestrator, "_bootstrap_services", fake_bootstrap_services)
    focus = {
        "mode": "request",
        "behavior": "restrict",
        "target_type": "document",
        "document_id": "document-1",
        "relative_path": "docs/notes.md",
        "selection": {
            "content_hash": "a" * 64,
            "parse_snapshot_id": "snapshot-1",
            "locator": {"kind": "text_lines", "start_line": 2, "end_line": 3},
        },
        "created_at": "2026-06-22T12:00:00+00:00",
    }

    events = [
        event
        async for event in make_orchestrator(context).run_stream(
            user_query="Use this passage",
            context=context,
            request_document_focus=focus,
        )
    ]

    assert _after_budget(events) == [_resolved_response_event()]
    candidate = FakeExecutor.instances[0].ctx.initial_source_candidates[0]
    assert candidate.parse_snapshot_id == "snapshot-1"
    assert candidate.excerpt == "2: Useful note\n3: Another note"


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

    assert _after_budget(events) == [_resolved_response_event()]
    candidates = FakeExecutor.instances[0].ctx.initial_source_candidates
    assert len(candidates) == 1
    assert candidates[0].source_message_id == 42
    assert candidates[0].excerpt == excerpt
