import asyncio
from types import SimpleNamespace

import pytest

from common.exceptions import LLMProviderError
from common.schema.agent.identity import AgentConfig
from core.agent.executor import AgentExecutor
from core.agent.executor import _ToolCall as ToolCall
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits


def make_run(*, limits=None):
    return AgentRun.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        user_query="What changed?",
        run_id="run-1",
        agent=AgentIdentity(
            config=AgentConfig(
                id="agent-1",
                name="Researcher",
                persona={
                    "attention_bias": "evidence",
                    "reasoning_style": "methodical",
                    "social_temperament": "calm",
                    "communication_signature": "clear",
                    "productive_flaw": "overexplains",
                },
            ),
            name="Researcher",
            persona="Careful and evidence-led",
        ),
        limits=limits or AgentRunLimits(max_attempts=3, max_calls=4),
    )


class ScriptedLLM:
    agent_model = "architect"
    extraction_model = "librarian"

    def __init__(self, steps):
        self.steps = list(steps)
        self.calls = []

    async def stream_with_tools(self, **kwargs):
        self.calls.append(kwargs)
        for event in self.steps.pop(0):
            yield event

    def count_tokens(self, text):
        return len(text.split())

    async def generate_text(self, **_kwargs):
        return "A concise evidence summary."


def tool_call_event(name, arguments, call_id):
    return {
        "event": "tool_calls",
        "data": {
            "content": f"Calling {name}",
            "calls": [{"name": name, "arguments": arguments, "id": call_id}],
        },
    }


def completed_event():
    return {
        "event": "step_completed",
        "data": {
            "usage": {
                "prompt_tokens": 3,
                "completion_tokens": 2,
                "total_tokens": 5,
                "approximate": False,
            }
        },
    }


@pytest.mark.no_network
async def test_executor_loop_accumulates_context_across_reasoning_attempts(
    monkeypatch,
):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_messages",
                    '{"query": "profile", "limit": 3}',
                    "call-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "get_recent_activity",
                    '{"entity_name": "Knoggin", "hours": 24}',
                    "call-2",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "The profile changed."}',
                    "submit-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "The profile changed."}',
                    "submit-2",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(limits=AgentRunLimits(max_attempts=4, max_calls=4))
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    async def fake_execute(_tools, name, _args):
        if name == "search_messages":
            return {
                "data": [
                    {"id": "message-1", "message": "Profile changed", "score": 0.9}
                ]
            }
        return {"data": [{"source": "Knoggin", "target": "Profile"}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert events[-1]["data"]["content"] == "The profile changed."
    assert len(llm.calls) == 4
    assert [call["model"] for call in llm.calls] == [
        "architect",
        "librarian",
        "librarian",
        "architect",
    ]
    assert [call["reasoning"] for call in llm.calls] == [
        "high",
        "medium",
        "medium",
        "high",
    ]
    assert "CURRENT EXECUTION PHASE: PLAN" in llm.calls[0]["system"]
    assert "CURRENT EXECUTION PHASE: EXECUTE" in llm.calls[1]["system"]
    assert "CURRENT EXECUTION PHASE: SYNTHESIZE" in llm.calls[-1]["system"]
    assert run.attempt_count == 4
    assert run.call_count == 2
    assert run.messages == [
        {"id": "message-1", "message": "Profile changed", "score": 0.9}
    ]
    assert run.graph == [{"source": "Knoggin", "target": "Profile"}]
    assert run.usage["total_tokens"] == 20
    assert run.sealed is True
    run.release()
    assert run.released is True


@pytest.mark.no_network
async def test_executor_automatically_replans_after_empty_evidence(monkeypatch):
    llm = ScriptedLLM(
        [
            [tool_call_event("search_messages", '{"query": "missing"}', "search-1"), completed_event()],
            [tool_call_event("submit_answer", '{"content": "Still looking."}', "submit-1"), completed_event()],
            [tool_call_event("submit_answer", '{"content": "No matching evidence."}', "submit-2"), completed_event()],
        ]
    )
    run = make_run(
        limits=AgentRunLimits(
            max_attempts=3,
            max_calls=2,
            empty_result_replan_threshold=1,
        )
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    async def empty_result(*_args):
        return {"data": []}

    monkeypatch.setattr("core.agent.executor.execute_tool", empty_result)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["data"]["content"] == "No matching evidence."
    assert [call["model"] for call in llm.calls] == [
        "architect",
        "architect",
        "architect",
    ]
    assert "CURRENT EXECUTION PHASE: PLAN" in llm.calls[1]["system"]
    assert "CURRENT EXECUTION PHASE: SYNTHESIZE" in llm.calls[2]["system"]


@pytest.mark.no_network
async def test_executor_loop_enforces_duplicate_tool_and_global_limits(
    monkeypatch,
):
    run = make_run(
        limits=AgentRunLimits(
            max_calls=4,
            tool_limits=(("search_messages", 2),),
        )
    )
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace())

    async def fake_execute(_tools, _name, args):
        return {"data": [{"id": args["query"], "message": args["query"]}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)

    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall("search_messages", {"query": "one"}, call_id="one"),
                ToolCall("search_messages", {"query": "one"}, call_id="dup"),
                ToolCall("search_messages", {"query": "two"}, call_id="two"),
                ToolCall("search_messages", {"query": "three"}, call_id="three"),
            ],
            results,
        )
    ]

    errors = [event for event in events if event["event"] == "tool_error"]
    assert len(errors) == 2
    assert errors[0]["data"]["error"] == "Duplicate call skipped"
    assert errors[1]["data"]["error"] == "Call limit reached for search_messages"
    assert run.call_count == 2
    assert run.tool_call_counts == {"search_messages": 2}
    assert [item["id"] for item in run.messages] == ["one", "two"]


@pytest.mark.no_network
async def test_executor_loop_recovers_from_invalid_arguments_and_tool_exceptions(
    monkeypatch,
):
    run = make_run(limits=AgentRunLimits(max_calls=3))
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace())
    calls = []

    async def fake_execute(_tools, name, _args):
        calls.append(name)
        if name == "search_messages":
            raise RuntimeError("backend exploded")
        return {"data": [{"id": "ok", "message": "usable"}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)

    invalid = executor._parse_tool_calls(
        [{"name": "search_messages", "arguments": "{bad", "id": "bad"}],
        "",
    )[0]
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [invalid, ToolCall("search_entity", {"query": "Ada"}, call_id="ok")],
            results,
        )
    ]

    assert calls == ["search_entity"]
    assert [event["event"] for event in events] == [
        "tool_start",
        "tool_error",
        "tool_start",
        "tool_end",
    ]
    assert "Argument parse failure" in events[1]["data"]["error"]
    assert results[-1]["result"]["data"] == [{"id": "ok", "message": "usable"}]
    assert run.call_count == 2

    error_results = []
    error_events = [
        event
        async for event in executor._execute_tools(
            [ToolCall("search_messages", {"query": "explode"}, call_id="err")],
            error_results,
        )
    ]
    assert error_events[-1]["event"] == "tool_error"
    assert error_events[-1]["data"]["error"] == "Internal tool failure"
    assert run.consecutive_errors == 1


@pytest.mark.no_network
async def test_executor_loop_timeout_restores_tool_state_and_records_failure(
    monkeypatch,
):
    run = make_run(limits=AgentRunLimits(tool_timeout=0.01))
    tools = SimpleNamespace(short_uuid_references={"entity_1": "actual-1"})
    executor = AgentExecutor(run, ScriptedLLM([]), tools)

    async def slow_execute(*_args):
        await asyncio.sleep(1)
        return {"data": []}

    monkeypatch.setattr("core.agent.executor.execute_tool", slow_execute)

    events = [
        event
        async for event in executor._execute_tools(
            [ToolCall("search_messages", {"query": "slow"}, call_id="slow")],
            [],
        )
    ]

    assert events[-1]["event"] == "tool_error"
    assert "timed out" in events[-1]["data"]["error"]
    assert run.consecutive_errors == 1
    assert tools.short_uuid_references == {"entity_1": "actual-1"}


@pytest.mark.no_network
async def test_executor_cancellation_restores_tool_state_and_propagates(monkeypatch):
    started = asyncio.Event()
    run = make_run()
    tools = SimpleNamespace(short_uuid_references={"entity_1": "actual-1"})
    executor = AgentExecutor(run, ScriptedLLM([]), tools)

    async def blocking_execute(*_args):
        started.set()
        await asyncio.Event().wait()

    async def consume():
        return [
            event
            async for event in executor._execute_tools(
                [ToolCall("search_messages", {"query": "cancel"}, call_id="cancel")],
                [],
            )
        ]

    monkeypatch.setattr("core.agent.executor.execute_tool", blocking_execute)
    task = asyncio.create_task(consume())
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert tools.short_uuid_references == {"entity_1": "actual-1"}
    assert run.call_count == 1


@pytest.mark.no_network
async def test_executor_generator_closure_releases_model_only_state():
    gate = asyncio.Event()

    class BlockingLLM(ScriptedLLM):
        async def stream_with_tools(self, **kwargs):
            self.calls.append(kwargs)
            yield {"event": "token", "data": {"content": "thinking"}}
            await gate.wait()

    run = make_run()
    executor = AgentExecutor(
        run,
        BlockingLLM([]),
        SimpleNamespace(document_service=None),
    )
    stream = executor.execute()

    first = await anext(stream)
    assert first["event"] == "token"
    await stream.aclose()

    assert run.released is True
    assert run.short_uuid_references == {}


@pytest.mark.no_network
async def test_executor_provider_failure_reaches_terminal_error_and_releases():
    class FailingLLM(ScriptedLLM):
        async def stream_with_tools(self, **kwargs):
            self.calls.append(kwargs)
            raise LLMProviderError("provider unavailable")
            yield  # pragma: no cover

    run = make_run(limits=AgentRunLimits(max_attempts=3, max_consecutive_errors=1))
    executor = AgentExecutor(
        run,
        FailingLLM([]),
        SimpleNamespace(document_service=None),
    )

    events = [event async for event in executor.execute()]

    assert events[-1]["event"] == "error"
    assert events[-1]["data"]["message"] == (
        "The agent couldn't complete this request. Please try again."
    )
    assert run.sealed is True
    assert run.released is True


@pytest.mark.no_network
async def test_executor_no_response_terminal_state_emits_error():
    llm = ScriptedLLM(
        [
            [
                {"event": "token", "data": {"content": "not enough"}},
                completed_event(),
            ]
        ]
    )
    run = make_run(limits=AgentRunLimits(max_attempts=1))
    executor = AgentExecutor(
        run,
        llm,
        SimpleNamespace(document_service=None),
    )

    events = [event async for event in executor.execute()]

    assert events[-1]["event"] == "error"
    assert events[-1]["data"]["message"] == (
        "The agent couldn't complete this request. Please try again."
    )
    assert run.final_content is None
    assert run.sealed is True
    assert run.released is True
