import pytest

from knoggin_server.agent.executor import AgentExecutor
from knoggin_server.agent.types import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
    ToolCall,
)


class FakeMemoryManager:
    def __init__(self):
        self.calls = []

    async def load_prompt_strings(self, hot_topics):
        self.calls.append(list(hot_topics))
        return (
            "[Identity]\n- remembers stable profile preferences",
            "memory rule",
            "memory preference",
            "memory ick",
        )


class FakeTools:
    def __init__(self, *, files=None):
        self.file_rag = object() if files is not None else None
        self.files = files or []

    def get_file_manifest(self):
        return self.files


class FakeLLM:
    agent_model = "architect-model"
    extraction_model = "librarian-model"

    def __init__(self, *, token_counts=None, summary="fallback summary", raises=False):
        self.token_counts = list(token_counts or [])
        self.summary = summary
        self.raises = raises
        self.call_llm_calls = []

    def count_tokens(self, text):
        if self.token_counts:
            return self.token_counts.pop(0)
        return len(text.split())

    async def call_llm(self, **kwargs):
        self.call_llm_calls.append(kwargs)
        if self.raises:
            raise RuntimeError("summary failed")
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
        memory_context,
        files_context,
        rules,
        prefs,
        icks,
        temp,
        agent_instructions,
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
                "memory_context": memory_context,
                "files_context": files_context,
                "rules": rules,
                "prefs": prefs,
                "icks": icks,
                "temp": temp,
                "agent_instructions": agent_instructions,
                "last_result": last_result,
                "client_tools": client_tools,
            }
        )
        events = self.step_events.pop(0)
        for event in events:
            yield event

    async def _execute_tools(self, tool_calls, results_out):
        self.tool_batches.append(tool_calls)
        for call in tool_calls:
            result = self.tool_results.pop(0)
            results_out.append({"tool": call.name, "result": result})
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
        user_name="ada",
        user_query="What changed in profile behavior?",
        session_id="session-1",
        run_id="run-1",
        hot_topics=["Identity"],
        active_topics=["Identity", "Testing"],
        agent_name="STELLA",
        agent_persona="Careful memory assistant",
    )


def done_with(*tool_calls):
    return {"event": "done", "data": list(tool_calls)}


@pytest.mark.no_network
async def test_execute_runs_architect_librarian_and_final_synthesis_modes():
    memory = FakeMemoryManager()
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
        memory,
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
            agent_instructions="Use citations",
            agent_rules=["override rule"],
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
                },
                "sources": None,
            },
        },
    ]
    assert memory.calls == [["Identity"]]
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
    assert first_call["memory_context"] == (
        "[Identity]\n- remembers stable profile preferences"
    )
    assert first_call["files_context"] == "- profile-plan.md (2KB, 3 chunks)"
    assert first_call["rules"] == "override rule"
    assert first_call["prefs"] == "memory preference"
    assert first_call["icks"] == "memory ick"
    assert first_call["temp"] == 0.2
    assert first_call["agent_instructions"] == "Use citations"
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
        memory_mgr=None,
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
        memory_mgr=None,
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
                },
            },
        }
    ]

    replanning_executor = ScriptedExecutor(
        make_ctx(),
        FakeLLM(),
        FakeTools(),
        memory_mgr=None,
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
        memory_mgr=None,
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
async def test_fallback_without_evidence_asks_for_rephrase():
    executor = AgentExecutor(
        make_ctx(),
        FakeLLM(),
        FakeTools(),
        memory_mgr=None,
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
        memory_mgr=None,
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
        memory_mgr=None,
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
        facts=[{"resolution": "exact"}],
        paths=[{"entity_a": "Ada", "entity_b": "Knoggin"}],
        hierarchy=[{"entity": "Knoggin"}],
    )
    llm = FakeLLM(token_counts=[12000, 4], summary="compact evidence summary")
    executor = AgentExecutor(make_ctx(evidence=evidence), llm, FakeTools(), None)

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
    assert evidence.facts == []
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
        facts=[{"resolution": "exact"}],
        paths=[{"entity_a": "Ada", "entity_b": "Knoggin"}],
        hierarchy=[{"entity": "Knoggin"}],
    )
    llm = FakeLLM(token_counts=[15000, 123], raises=True)
    executor = AgentExecutor(make_ctx(evidence=evidence), llm, FakeTools(), None)

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
    assert evidence.facts == []
    assert evidence.paths == []
    assert evidence.hierarchy == []
