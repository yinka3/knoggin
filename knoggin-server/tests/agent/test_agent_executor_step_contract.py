from types import SimpleNamespace

import pytest

from knoggin_server.agent.executor import AgentExecutor
from knoggin_server.agent.types import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
)


class StreamingLLM:
    agent_model = "architect-model"
    extraction_model = "librarian-model"

    def __init__(self, chunks=None, *, raises=None):
        self.chunks = chunks or []
        self.raises = raises
        self.calls = []

    async def call_llm_with_tools_streaming(self, **kwargs):
        self.calls.append(kwargs)
        if self.raises:
            raise self.raises
        for chunk in self.chunks:
            yield chunk


def make_executor(llm):
    ctx = AgentContext(
        config=AgentRunConfig(),
        state=AgentState(),
        evidence=RetrievedEvidence(),
        user_name="ada",
        user_query="What changed in profile behavior?",
        session_id="session-1",
        run_id="run-1",
        agent_name="STELLA",
        agent_persona="Careful memory assistant",
        active_topics=["Identity", "Testing"],
    )
    tools = SimpleNamespace(file_rag=None)
    return AgentExecutor(ctx, llm, tools, memory_mgr=None)


@pytest.mark.no_network
async def test_step_streams_tokens_thinking_and_parses_tool_calls(monkeypatch):
    chunks = [
        {"type": "token", "content": "I should search first. "},
        {"type": "thinking", "content": "Need direct evidence"},
        {
            "type": "tool_calls",
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
        {
            "type": "done",
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
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
        "knoggin_server.agent.executor.get_agent_prompt",
        fake_agent_prompt,
    )
    client_tool = {
        "type": "function",
        "function": {"name": "client_tool", "parameters": {"type": "object"}},
    }

    events = [
        event
        async for event in executor._step(
            date="2026-02-03 04:05 UTC",
            model="test-model",
            reasoning="high",
            current_mode="Architect",
            enabled_tools=["search_messages"],
            memory_context="[Identity]\n- memory",
            files_context="- file.md",
            directives="Required:\n- stay grounded",
            temp=0.2,
            agent_instructions="Use citations",
            last_result=None,
            client_tools=[client_tool],
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
    assert events[2]["event"] == "done"
    calls = events[2]["data"]
    assert [call.name for call in calls] == ["search_messages", "get_recent_activity"]
    assert calls[0].args == {"query": "profile behavior", "limit": 3}
    assert calls[0].thinking == "I should search first."
    assert calls[0].call_id == "call-1"
    assert calls[1].args == {"entity_name": "Knoggin", "hours": "24"}
    assert executor.ctx.state.usage == {
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "total_tokens": 15,
    }

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
    assert prompt_kwargs["memory_context"] == "[Identity]\n- memory"
    assert prompt_kwargs["files_context"] == "- file.md"
    assert prompt_kwargs["agent_directives"] == "Required:\n- stay grounded"
    assert prompt_kwargs["instructions"] == "Use citations"
    assert prompt_kwargs["current_mode"] == "Architect"
    assert prompt_kwargs["active_topics"] == ["Identity", "Testing"]


@pytest.mark.no_network
async def test_step_marks_invalid_arguments_for_later_tool_error():
    llm = StreamingLLM(
        [
            {
                "type": "tool_calls",
                "calls": [
                    {
                        "name": "search_messages",
                        "arguments": "{not json",
                        "id": "call-bad",
                    }
                ],
            },
            {"type": "done"},
        ]
    )
    executor = make_executor(llm)

    events = [
        event
        async for event in executor._step(
            date="now",
            model="model",
            reasoning="medium",
            current_mode="Librarian",
            enabled_tools=None,
            memory_context="",
            files_context="",
            directives="",
            temp=0.7,
            agent_instructions="",
            last_result=None,
        )
    ]

    tool_call = events[-1]["data"][0]
    assert tool_call.args["_parse_error"] is True
    assert tool_call.args["_raw"] == "{not json"


@pytest.mark.no_network
async def test_step_done_without_tool_calls_yields_formatting_error():
    llm = StreamingLLM(
        [
            {"type": "token", "content": "raw answer"},
            {"type": "done"},
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
            enabled_tools=None,
            memory_context="",
            files_context="",
            directives="",
            temp=0.7,
            agent_instructions="",
            last_result=None,
        )
    ]

    assert events[-1]["event"] == "formatting_error"
    assert "must either call" in events[-1]["data"]


@pytest.mark.no_network
async def test_step_forwards_llm_error_chunk_and_exceptions():
    chunk_error = StreamingLLM([{"type": "error", "message": "provider said no"}])
    exception_error = StreamingLLM(raises=RuntimeError("stream broke"))

    chunk_events = [
        event
        async for event in make_executor(chunk_error)._step(
            date="now",
            model="model",
            reasoning="high",
            current_mode="Architect",
            enabled_tools=None,
            memory_context="",
            files_context="",
            directives="",
            temp=0.7,
            agent_instructions="",
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
            enabled_tools=None,
            memory_context="",
            files_context="",
            directives="",
            temp=0.7,
            agent_instructions="",
            last_result=None,
        )
    ]

    assert chunk_events == [
        {"event": "error", "data": {"message": "provider said no"}}
    ]
    assert exception_events == [
        {
            "event": "error",
            "data": {"message": "LLM API failure: stream broke"},
        }
    ]
