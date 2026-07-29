import json

import pytest

from common.exceptions import LLMProviderError, ToolExecutionError
from common.schema.contracts import EngineScope
from core.agent.executor import AgentExecutor
from core.agent.types import (
    AgentContext,
    AgentRunIdentity,
    AgentRunConfig,
    AgentState,
    MaintenanceCandidate,
    RetrievedEvidence,
    ToolCall,
)
from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeRedis


class FakeTools:
    def __init__(self, *, files=None, redis=None, project_id="project-1"):
        self.document_service = object() if files is not None else None
        self.files = files or []
        self.redis = redis
        self.project_id = project_id

    async def get_document_manifest(self):
        return self.files


class FakeLLM:
    agent_model = "architect-model"
    extraction_model = "librarian-model"

    def __init__(
        self,
        *,
        token_counts=None,
        summary="fallback summary",
        raises=False,
        stream_events=None,
    ):
        self.token_counts = list(token_counts or [])
        self.summary = summary
        self.raises = raises
        self.call_llm_calls = []
        self.stream_events = list(stream_events or [])
        self.stream_calls = []

    async def stream_with_tools(self, **kwargs):
        self.stream_calls.append(kwargs)
        events = self.stream_events.pop(0)
        for event in events:
            if isinstance(event, tuple):
                for nested_event in event:
                    yield nested_event
            else:
                yield event

    def count_tokens(self, text):
        if self.token_counts:
            return self.token_counts.pop(0)
        return len(text.split())

    async def generate_text(self, **kwargs):
        self.call_llm_calls.append(kwargs)
        if self.raises:
            raise LLMProviderError("summary failed")
        return self.summary


class ScriptedExecutor(AgentExecutor):
    def __init__(self, *args, step_events=None, tool_results=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.step_events = list(step_events or [])
        self.tool_results = list(tool_results or [])
        self.step_calls = []
        self.tool_batches = []
        self.emitted_llm_calls = []
        self.manage_context_calls = 0

    async def _emit_llm_call(self, model, reasoning):
        self.emitted_llm_calls.append((model, reasoning, self.ctx.state.attempt_count))

    async def _step(
        self,
        date,
        model,
        reasoning,
        current_mode,
        enabled_tools,
        documents_context,
        document_focus_context,
        directives,
        temp,
        agent_brain,
        last_result,
        client_tools=None,
    ):
        self.step_calls.append(
            {
                "date": date,
                "model": model,
                "reasoning": reasoning,
                "current_mode": current_mode,
                "enabled_tools": enabled_tools,
                "documents_context": documents_context,
                "document_focus_context": document_focus_context,
                "directives": directives,
                "temp": temp,
                "agent_brain": agent_brain,
                "last_result": last_result,
                "client_tools": client_tools,
            }
        )
        events = self.step_events.pop(0)
        for event in events:
            if isinstance(event, tuple):
                for nested_event in event:
                    yield nested_event
            else:
                yield event

    async def _execute_tools(self, tool_calls, results_out):
        self.tool_batches.append(tool_calls)
        for call in tool_calls:
            result = self.tool_results.pop(0)
            results_out.append({"tool": call.name, "result": result})
            self.ctx.state.consecutive_errors = 0
            self.ctx.state.last_error = None
            yield {
                "event": "tool_end",
                "data": {"tool": call.name, "result": "scripted"},
            }

    async def _manage_context_size(self):
        self.manage_context_calls += 1


def make_ctx(*, config=None, evidence=None):
    return AgentContext(
        config=config or AgentRunConfig(max_attempts=5, max_consecutive_errors=2),
        state=AgentState(),
        evidence=evidence or RetrievedEvidence(),
        scope=EngineScope(
            user_name="ada", session_id="session-1", project_id="project-1"
        ),
        agent=AgentRunIdentity(
            config=SimpleNamespace(id="agent-1"),
            name="STELLA",
            persona="Careful memory assistant",
        ),
        user_query="What changed in profile behavior?",
        run_id="run-1",
        hot_topics=["Identity"],
        active_topics=["Identity", "Testing"],
    )


def done_with(*tool_calls):
    content = next(
        (call.thinking for call in tool_calls if call.thinking),
        "",
    )
    return (
        {
            "event": "tool_calls",
            "data": {
                "content": content,
                "calls": [
                    {
                        "name": call.name,
                        "arguments": json.dumps(call.args),
                        **({"id": call.call_id} if call.call_id else {}),
                    }
                    for call in tool_calls
                ],
            },
        },
        {
            "event": "step_completed",
            "data": {
                "content": content,
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "approximate": False,
                },
            },
        },
    )


def step_error(message, *, kind="provider"):
    return {
        "event": "step_error",
        "data": {"kind": kind, "message": message},
    }


@pytest.mark.no_network
async def test_execute_runs_architect_librarian_and_final_synthesis_modes():
    tools = FakeTools(
        files=[
            {
                "original_name": "profile-plan.md",
                "size_bytes": 2048,
                "chunk_count": 3,
            }
        ]
    )
    executor = ScriptedExecutor(
        make_ctx(),
        FakeLLM(),
        tools,
        step_events=[
            [
                done_with(
                    ToolCall(
                        name="search_messages",
                        args={"query": "profile behavior"},
                    )
                )
            ],
            [
                done_with(
                    ToolCall(name="submit_answer", args={"content": "draft answer"})
                )
            ],
            [
                done_with(
                    ToolCall(name="submit_answer", args={"content": "final answer"})
                )
            ],
        ],
        tool_results=[
            {
                "data": [
                    {
                        "id": "msg_1",
                        "user_name": "ada",
                        "session_id": "session-1",
                        "message": "profile behavior evidence",
                        "score": 0.9,
                    }
                ]
            }
        ],
    )

    events = [
        event
        async for event in executor.execute(
            simulated_date="2026-02-03 04:05 UTC",
            agent_temperature=0.2,
            agent_brain="Use citations",
            agent_directives="Required:\n- override directive",
        )
    ]

    assert events == [
        {
            "event": "tool_end",
            "data": {"tool": "search_messages", "result": "scripted"},
        },
        {
            "event": "response",
            "data": {
                "content": "final answer",
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "approximate": False,
                },
                "sources": None,
            },
        },
    ]
    assert executor.emitted_llm_calls == [
        ("architect-model", "high", 1),
        ("librarian-model", "medium", 2),
        ("architect-model", "high", 3),
    ]
    assert [call["current_mode"] for call in executor.step_calls] == [
        "Architect",
        "Librarian",
        "Architect",
    ]
    first_call = executor.step_calls[0]
    assert first_call["date"] == "2026-02-03 04:05 UTC"
    assert first_call["documents_context"] == "- profile-plan.md (2KB, 3 chunks)"
    assert first_call["directives"] == "Required:\n- override directive"
    assert first_call["temp"] == 0.2
    assert first_call["agent_brain"] == "Use citations"
    assert executor.step_calls[1]["last_result"] == [
        {
            "tool": "search_messages",
            "result": {
                "data": [
                    {
                        "id": "msg_1",
                        "user_name": "ada",
                        "session_id": "session-1",
                        "message": "profile behavior evidence",
                        "score": 0.9,
                    }
                ]
            },
        }
    ]
    assert executor.manage_context_calls == 1


@pytest.mark.no_network
async def test_execute_model_override_applies_to_all_modes():
    executor = ScriptedExecutor(
        make_ctx(),
        FakeLLM(),
        FakeTools(),
        step_events=[
            [
                done_with(
                    ToolCall(name="submit_answer", args={"content": "manual model"})
                )
            ]
        ],
    )

    events = [event async for event in executor.execute(model="manual-model")]

    assert events[0]["data"]["content"] == "manual model"
    assert executor.emitted_llm_calls == [("manual-model", "high", 1)]
    assert executor.step_calls[0]["model"] == "manual-model"


@pytest.mark.no_network
async def test_execute_clarification_and_replanning_short_circuit_paths():
    clarification_executor = ScriptedExecutor(
        make_ctx(),
        FakeLLM(),
        FakeTools(),
        step_events=[
            [
                done_with(
                    ToolCall(
                        name="request_clarification",
                        args={"question": "Which project?"},
                    )
                )
            ]
        ],
    )

    clarification_events = [
        event async for event in clarification_executor.execute()
    ]

    assert clarification_events == [
        {
            "event": "clarification",
            "data": {
                "question": "Which project?",
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "approximate": False,
                },
            },
        }
    ]

    replanning_executor = ScriptedExecutor(
        make_ctx(),
        FakeLLM(),
        FakeTools(),
        step_events=[
            [
                done_with(
                    ToolCall(
                        name="request_replanning",
                        args={"reason": "empty search"},
                    )
                )
            ],
            [
                done_with(
                    ToolCall(name="submit_answer", args={"content": "new plan answer"})
                )
            ],
        ],
    )

    replanning_events = [event async for event in replanning_executor.execute()]

    assert replanning_events[0]["data"]["content"] == "new plan answer"
    assert [call["current_mode"] for call in replanning_executor.step_calls] == [
        "Architect",
        "Architect",
    ]


@pytest.mark.no_network
async def test_execute_consecutive_empty_results_force_replanning():
    config = AgentRunConfig(
        max_attempts=4,
        empty_result_replan_threshold=2,
    )
    executor = ScriptedExecutor(
        make_ctx(config=config),
        FakeLLM(),
        FakeTools(),
        step_events=[
            [done_with(ToolCall(name="search_messages", args={"query": "one"}))],
            [done_with(ToolCall(name="search_messages", args={"query": "two"}))],
            [done_with(ToolCall(name="submit_answer", args={"content": "replanned"}))],
        ],
        tool_results=[{"data": []}, {"data": []}],
    )

    events = [event async for event in executor.execute()]

    assert events[-1]["data"]["content"] == "replanned"
    assert [call["current_mode"] for call in executor.step_calls] == [
        "Architect",
        "Librarian",
        "Architect",
    ]
    assert executor.ctx.state.consecutive_empty_results == 0


@pytest.mark.no_network
async def test_execute_hides_transient_step_errors_and_resets_after_tool_success():
    executor = ScriptedExecutor(
        make_ctx(),
        FakeLLM(),
        FakeTools(),
        step_events=[
            [step_error("temporary provider failure")],
            [done_with(ToolCall(name="search_messages", args={"query": "profile"}))],
            [
                done_with(
                    ToolCall(name="submit_answer", args={"content": "draft answer"})
                )
            ],
            [
                done_with(
                    ToolCall(name="submit_answer", args={"content": "final answer"})
                )
            ],
        ],
        tool_results=[{"data": [{"id": "msg-1"}]}],
    )

    events = [event async for event in executor.execute()]

    assert [event["event"] for event in events] == ["tool_end", "response"]
    assert events[-1]["data"]["content"] == "final answer"
    assert executor.ctx.state.consecutive_errors == 0
    assert len(executor.step_calls) == 4


@pytest.mark.no_network
async def test_execute_continues_to_response_after_maintenance_failure(monkeypatch):
    redis = FakeRedis()
    candidate = MaintenanceCandidate(
        id="topic_evaluation:project-1",
        kind="topic_evaluation",
        reason="Heartbeat reached threshold.",
        suggested_tool="update_topics",
    )
    ctx = make_ctx()
    ctx.maintenance_candidates = [candidate]
    llm = FakeLLM(
        stream_events=[
            [done_with(ToolCall(name="update_topics", args={}))],
            [done_with(ToolCall(name="submit_answer", args={"content": "draft"}))],
            [done_with(ToolCall(name="submit_answer", args={"content": "answered"}))],
        ]
    )
    executor = AgentExecutor(ctx, llm, FakeTools(redis=redis))

    async def fail_maintenance_tool(*_args):
        raise ToolExecutionError("update_topics", "topic write failed")

    monkeypatch.setattr(
        "core.agent.executor.execute_tool",
        fail_maintenance_tool,
    )

    events = [
        event async for event in executor.execute(enabled_tools=["update_topics"])
    ]

    assert [event["event"] for event in events] == [
        "tool_start",
        "tool_error",
        "response",
    ]
    assert events[-1]["data"]["content"] == "answered"
    assert ctx.state.consecutive_errors == 0
    assert ctx.maintenance_candidates == []
    assert await redis.get(
        RedisKeys.maintenance_attempts("ada", "project-1", candidate.id)
    ) == "1"
    assert await redis.get(
        RedisKeys.maintenance_cooldown("ada", "project-1", candidate.id)
    )


@pytest.mark.no_network
@pytest.mark.parametrize("kind", ["provider", "formatting"])
async def test_execute_emits_one_terminal_error_after_consecutive_step_failures(kind):
    config = AgentRunConfig(max_attempts=5, max_consecutive_errors=2)
    executor = ScriptedExecutor(
        make_ctx(config=config),
        FakeLLM(),
        FakeTools(),
        step_events=[
            [step_error("first failure", kind=kind)],
            [step_error("second failure", kind=kind)],
        ],
    )

    events = [event async for event in executor.execute()]

    assert events == [
        {
            "event": "error",
            "data": {
                "message": (
                    "Agent stopped after 2 consecutive errors: second failure"
                )
            },
        }
    ]
    assert len(executor.step_calls) == 2


@pytest.mark.no_network
async def test_execute_preserves_approximate_usage_in_final_response():
    tool_events = done_with(
        ToolCall(name="submit_answer", args={"content": "final answer"})
    )
    tool_events[1]["data"]["usage"] = {
        "prompt_tokens": 10,
        "completion_tokens": 4,
        "total_tokens": 14,
        "approximate": True,
    }
    executor = ScriptedExecutor(
        make_ctx(),
        FakeLLM(),
        FakeTools(),
        step_events=[[tool_events]],
    )

    events = [event async for event in executor.execute()]

    assert events == [
        {
            "event": "response",
            "data": {
                "content": "final answer",
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 4,
                    "total_tokens": 14,
                    "approximate": True,
                },
                "sources": None,
            },
        }
    ]


@pytest.mark.no_network
async def test_fallback_without_evidence_asks_for_rephrase():
    executor = AgentExecutor(
        make_ctx(),
        FakeLLM(),
        FakeTools(),
    )

    result = await executor._fallback()

    assert result == {
        "event": "clarification",
        "data": {
            "question": "I'm having trouble with that. Could you rephrase?",
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "approximate": False,
            },
            "fallback": True,
        },
    }


@pytest.mark.no_network
async def test_fallback_with_evidence_generates_summary_and_preserves_sources():
    evidence = RetrievedEvidence(
        profiles=[{"id": 1, "canonical_name": "Ada"}],
        messages=[
            {
                "id": "msg_1",
                "score": 0.9,
                "context": [
                    {
                        "role": "user",
                        "timestamp": "2026-01-01T10:00:00+00:00",
                        "content": "profile evidence",
                        "is_hit": True,
                    }
                ],
            }
        ],
        graph=[{"source": "Ada", "target": "Knoggin", "connection_strength": 1}],
        sources=[{"url": "https://example.test/source"}],
    )
    llm = FakeLLM(summary="summarized fallback")
    executor = AgentExecutor(
        make_ctx(evidence=evidence),
        llm,
        FakeTools(),
    )

    result = await executor._fallback()

    assert result == {
        "event": "response",
        "data": {
            "content": "summarized fallback",
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "approximate": False,
            },
            "sources": [{"url": "https://example.test/source"}],
            "fallback": True,
        },
    }
    assert llm.call_llm_calls[0]["temperature"] == 0.3
    assert "profile evidence" in llm.call_llm_calls[0]["user"]


@pytest.mark.no_network
async def test_manage_context_size_below_threshold_leaves_evidence_untouched():
    evidence = RetrievedEvidence(messages=[{"id": "msg_1"}])
    executor = AgentExecutor(
        make_ctx(evidence=evidence),
        FakeLLM(token_counts=[9999]),
        FakeTools(),
    )

    await executor._manage_context_size()

    assert evidence.token_count == 9999
    assert evidence.messages == [{"id": "msg_1"}]
    assert evidence.summary is None


@pytest.mark.no_network
async def test_manage_context_size_summarizes_and_trims_large_evidence():
    evidence = RetrievedEvidence(
        messages=[{"id": f"msg_{index}", "score": index} for index in range(8)],
        profiles=[
            {"id": index, "canonical_name": f"Entity {index}"}
            for index in range(8)
        ],
        graph=[
            {"source": "Ada", "target": f"Entity {index}"}
            for index in range(20)
        ],
        episodes=[{"resolution": "exact"}],
        paths=[{"entity_a": "Ada", "entity_b": "Knoggin"}],
        hierarchy=[{"entity": "Knoggin"}],
    )
    llm = FakeLLM(token_counts=[12000, 4], summary="compact evidence summary")
    executor = AgentExecutor(make_ctx(evidence=evidence), llm, FakeTools())

    await executor._manage_context_size()

    assert evidence.summary == "compact evidence summary"
    assert evidence.token_count == 4
    assert [msg["id"] for msg in evidence.messages] == [
        "msg_3",
        "msg_4",
        "msg_5",
        "msg_6",
        "msg_7",
    ]
    assert [profile["id"] for profile in evidence.profiles] == [3, 4, 5, 6, 7]
    assert len(evidence.graph) == 15
    assert evidence.episodes == []
    assert evidence.paths == []
    assert evidence.hierarchy == []
    assert llm.call_llm_calls[0]["temperature"] == 0.0


@pytest.mark.no_network
async def test_manage_context_size_truncates_when_summary_generation_fails():
    evidence = RetrievedEvidence(
        messages=[{"id": f"msg_{index}", "score": index} for index in range(7)],
        profiles=[{"id": index} for index in range(7)],
        graph=[
            {"source": "Ada", "target": f"Entity {index}"}
            for index in range(18)
        ],
        episodes=[{"resolution": "exact"}],
        paths=[{"entity_a": "Ada", "entity_b": "Knoggin"}],
        hierarchy=[{"entity": "Knoggin"}],
    )
    llm = FakeLLM(token_counts=[15000, 123], raises=True)
    executor = AgentExecutor(make_ctx(evidence=evidence), llm, FakeTools())

    await executor._manage_context_size()

    assert evidence.summary is None
    assert evidence.token_count == 123
    assert [msg["id"] for msg in evidence.messages] == [
        "msg_2",
        "msg_3",
        "msg_4",
        "msg_5",
        "msg_6",
    ]
    assert [profile["id"] for profile in evidence.profiles] == [2, 3, 4, 5, 6]
    assert len(evidence.graph) == 15
    assert evidence.episodes == []
    assert evidence.paths == []
    assert evidence.hierarchy == []
