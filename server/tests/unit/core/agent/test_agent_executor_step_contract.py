from types import SimpleNamespace

import pytest

from common.exceptions import LLMProviderError
from core.agent.executor import AgentExecutor
from core.agent.maintenance import MaintenanceCandidate
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits


class StreamingLLM:
    agent_model = "architect-model"
    extraction_model = "librarian-model"

    def __init__(self, chunks=None, *, raises=None):
        self.chunks = chunks or []
        self.raises = raises
        self.calls = []

    async def stream_with_tools(self, **kwargs):
        self.calls.append(kwargs)
        if self.raises:
            raise self.raises
        for chunk in self.chunks:
            yield chunk


def make_executor(llm):
    ctx = AgentRun.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        user_query="What changed in profile behavior?",
        run_id="run-1",
        agent=AgentIdentity(
            config=SimpleNamespace(id="agent-1"),
            name="STELLA",
            persona="Careful memory assistant",
        ),
        limits=AgentRunLimits(),
        model="test-model",
        temperature=0.2,
        brain="Use citations",
        directives="Required:\n- stay grounded",
        enabled_tools=["search_messages"],
        active_topics=["Identity", "Testing"],
    )
    tools = SimpleNamespace(document_service=None)
    return AgentExecutor(ctx, llm, tools)


@pytest.mark.no_network
async def test_executor_loads_missing_or_unreadable_project_context_non_fatally():
    executor = make_executor(StreamingLLM())
    assert await executor._load_project_context() == ""

    async def fail_reader():
        raise RuntimeError("workspace unavailable")

    executor.tools.workspace_service = SimpleNamespace(
        read_project_context=fail_reader
    )
    assert await executor._load_project_context() == ""


@pytest.mark.no_network
async def test_executor_loads_canonical_project_context_directly():
    executor = make_executor(StreamingLLM())

    async def read_context():
        return "# Project\nUse the repository conventions."

    executor.tools.workspace_service = SimpleNamespace(
        read_project_context=read_context
    )
    assert await executor._load_project_context() == (
        "# Project\nUse the repository conventions."
    )


@pytest.mark.no_network
async def test_step_forwards_standard_stream_events(monkeypatch):
    chunks = [
        {"event": "token", "data": {"content": "I should search first. "}},
        {"event": "thinking", "data": {"content": "Need direct evidence"}},
        {
            "event": "tool_calls",
            "data": {
                "content": "Authoritative tool reasoning.",
                "calls": [
                    {
                        "name": "search_messages",
                        "arguments": '{"query": "profile behavior", "limit": 3}',
                        "id": "call-1",
                    },
                    {
                        "name": "get_recent_activity",
                        "arguments": '{"entity_name": "Knoggin", "hours": "24",}',
                        "id": "call-2",
                    },
                ],
            },
        },
        {
            "event": "step_completed",
            "data": {
                "content": "Authoritative tool reasoning.",
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                    "approximate": False,
                },
            },
        },
    ]
    llm = StreamingLLM(chunks)
    executor = make_executor(llm)
    prompt_calls = []

    def fake_agent_prompt(*args, **kwargs):
        prompt_calls.append((args, kwargs))
        return "SYSTEM PROMPT"

    monkeypatch.setattr(
        "core.agent.executor.get_agent_prompt",
        fake_agent_prompt,
    )
    client_tool = {
        "type": "function",
        "function": {
            "name": "client_tool",
            "capability": "read",
            "parameters": {"type": "object"},
        },
    }
    executor.ctx.additional_tool_schemas = (client_tool,)

    events = [
        event
        async for event in executor._step(
            date="2026-02-03 04:05 UTC",
            model="test-model",
            reasoning="high",
            current_mode="Architect",
            documents_context="- file.md",
            document_focus_context="",
            last_result=None,
        )
    ]

    assert events[0] == {
        "event": "token",
        "data": {"content": "I should search first. "},
    }
    assert events[1] == {
        "event": "thinking",
        "data": {"content": "Need direct evidence"},
    }
    assert events[2] == chunks[2]
    assert events[3] == chunks[3]

    calls = executor._parse_tool_calls(
        events[2]["data"]["calls"],
        events[2]["data"]["content"],
    )
    assert [call.name for call in calls] == ["search_messages", "get_recent_activity"]
    assert calls[0].args == {"query": "profile behavior", "limit": 3}
    assert calls[0].thinking == "Authoritative tool reasoning."
    assert calls[0].call_id == "call-1"
    assert calls[1].args == {"entity_name": "Knoggin", "hours": "24"}

    llm_call = llm.calls[0]
    assert llm_call["system"] == "SYSTEM PROMPT"
    assert llm_call["model"] == "test-model"
    assert llm_call["temperature"] == 0.2
    assert llm_call["reasoning"] == "high"
    tool_names = [schema["function"]["name"] for schema in llm_call["tools"]]
    assert "search_messages" in tool_names
    assert "client_tool" in tool_names
    assert "search_entity" not in tool_names

    _, prompt_kwargs = prompt_calls[0]
    assert prompt_kwargs["documents_context"] == "- file.md"
    assert prompt_kwargs["agent_directives"] == "Required:\n- stay grounded"
    assert prompt_kwargs["agent_brain"] == "Use citations"
    assert prompt_kwargs["current_mode"] == "Architect"


@pytest.mark.no_network
async def test_step_presents_maintenance_as_optional_when_tool_is_enabled(
    monkeypatch,
):
    llm = StreamingLLM(
        [
            {
                "event": "tool_calls",
                "data": {"content": "I can answer.", "calls": []},
            }
        ]
    )
    executor = make_executor(llm)
    executor.ctx.maintenance_candidates = [
        MaintenanceCandidate(
            id="graph_merge_scan:project-1",
            kind="graph_merge_scan",
            reason="Merge queue has 2 candidate entities.",
            suggested_tool="check_graph_health",
        )
    ]
    prompt_calls = []

    def fake_agent_prompt(*args, **kwargs):
        prompt_calls.append(kwargs)
        return "SYSTEM PROMPT"

    monkeypatch.setattr(
        "core.agent.executor.get_agent_prompt",
        fake_agent_prompt,
    )
    executor.ctx.enabled_tools = ("check_graph_health",)

    events = [
        event
        async for event in executor._step(
            date="now",
            model="model",
            reasoning="high",
            current_mode="Architect",
            documents_context="",
            document_focus_context="",
            last_result=None,
        )
    ]

    instruction = prompt_calls[0]["runtime_instructions"]
    assert "Optional maintenance is available" in instruction
    assert "You may handle one candidate" in instruction
    assert "MUST" not in instruction
    assert "`check_graph_health`" in instruction
    assert events[0]["event"] == "tool_calls"


@pytest.mark.no_network
async def test_step_does_not_auto_add_disabled_maintenance_tool(monkeypatch):
    llm = StreamingLLM(
        [
            {
                "event": "tool_calls",
                "data": {"content": "I can answer.", "calls": []},
            }
        ]
    )
    executor = make_executor(llm)
    executor.ctx.maintenance_candidates = [
        MaintenanceCandidate(
            id="graph_merge_scan:project-1",
            kind="graph_merge_scan",
            reason="Merge queue has 2 candidate entities.",
            suggested_tool="check_graph_health",
        )
    ]
    prompt_calls = []

    def fake_agent_prompt(*args, **kwargs):
        prompt_calls.append(kwargs)
        return "SYSTEM PROMPT"

    monkeypatch.setattr(
        "core.agent.executor.get_agent_prompt",
        fake_agent_prompt,
    )
    executor.ctx.enabled_tools = ("search_messages",)

    await anext(
        executor._step(
            date="now",
            model="model",
            reasoning="high",
            current_mode="Architect",
            documents_context="",
            document_focus_context="",
            last_result=None,
        )
    )

    tool_names = [schema["function"]["name"] for schema in llm.calls[0]["tools"]]
    assert "search_messages" in tool_names
    assert "check_graph_health" not in tool_names
    assert prompt_calls[0]["runtime_instructions"] == ""


@pytest.mark.no_network
async def test_step_marks_invalid_arguments_for_later_tool_error():
    executor = make_executor(StreamingLLM())
    tool_call = executor._parse_tool_calls(
        [
            {
                "name": "search_messages",
                "arguments": "{not json",
                "id": "call-bad",
            }
        ],
        "",
    )[0]

    assert tool_call.args["_parse_error"] is True
    assert tool_call.args["_raw"] == "{not json"


@pytest.mark.no_network
async def test_step_completed_without_tool_calls_yields_formatting_step_error():
    llm = StreamingLLM(
        [
            {"event": "token", "data": {"content": "raw answer"}},
            {
                "event": "step_completed",
                "data": {
                    "content": "raw answer",
                    "usage": {
                        "prompt_tokens": 4,
                        "completion_tokens": 2,
                        "total_tokens": 6,
                        "approximate": True,
                    },
                },
            },
        ]
    )
    executor = make_executor(llm)

    events = [
        event
        async for event in executor._step(
            date="now",
            model="model",
            reasoning="high",
            current_mode="Architect",
            documents_context="",
            document_focus_context="",
            last_result=None,
        )
    ]

    assert events[-1]["event"] == "step_error"
    assert events[-1]["data"]["kind"] == "formatting"
    assert "must either call" in events[-1]["data"]["message"]
    assert events[-1]["data"]["usage"]["approximate"] is True


@pytest.mark.no_network
async def test_step_forwards_llm_error_chunk_and_exceptions():
    chunk_error = StreamingLLM(
        [
            {
                "event": "step_error",
                "data": {"kind": "provider", "message": "provider said no"},
            }
        ]
    )
    exception_error = StreamingLLM(raises=LLMProviderError("stream broke"))

    chunk_events = [
        event
        async for event in make_executor(chunk_error)._step(
            date="now",
            model="model",
            reasoning="high",
            current_mode="Architect",
            documents_context="",
            document_focus_context="",
            last_result=None,
        )
    ]
    exception_events = [
        event
        async for event in make_executor(exception_error)._step(
            date="now",
            model="model",
            reasoning="high",
            current_mode="Architect",
            documents_context="",
            document_focus_context="",
            last_result=None,
        )
    ]

    assert chunk_events == [
        {
            "event": "step_error",
            "data": {"kind": "provider", "message": "provider said no"},
        }
    ]
    assert exception_events == [
        {
            "event": "step_error",
            "data": {
                "kind": "provider",
                "message": "LLM API failure: stream broke",
            },
        }
    ]
