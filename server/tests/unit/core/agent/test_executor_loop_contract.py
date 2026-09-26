import asyncio
from types import SimpleNamespace

import pytest

from common.exceptions import (
    LLMBudgetExceededError,
    LLMProviderError,
    ToolExecutionError,
    WorkspaceConflictError,
)
from common.schema.agent.identity import AgentConfig
from common.schema.agent.research import resolve_research_profile
from core.agent.executor import AgentExecutor
from core.agent.executor import _ToolCall as ToolCall
from core.agent.prompt_context import build_evidence_context
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.sources.document_selection import build_document_selection_candidate
from core.agent.sources.pasted_text import build_pasted_text_candidates


def make_run(
    *,
    limits=None,
    research_profile=None,
    initial_source_candidates=None,
    user_query="What changed?",
    document_selection_context=None,
):
    return AgentRun.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        user_query=user_query,
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
        research_profile=research_profile,
        initial_source_candidates=initial_source_candidates,
        document_selection_context=document_selection_context,
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
    return tool_calls_event([(name, arguments, call_id)])


def tool_calls_event(calls):
    return {
        "event": "tool_calls",
        "data": {
            "content": "Calling tools",
            "calls": [
                {"name": name, "arguments": arguments, "id": call_id}
                for name, arguments, call_id in calls
            ],
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


def install_counted_project_briefing(monkeypatch, executor):
    counts = {"brief": 0, "context": 0}

    async def load_brief():
        counts["brief"] += 1
        return "BRIEF_PAYLOAD"

    async def load_context():
        counts["context"] += 1
        return "CONTEXT_PAYLOAD"

    monkeypatch.setattr(executor, "_load_project_brief", load_brief)
    monkeypatch.setattr(executor, "_load_project_context", load_context)
    return counts


@pytest.mark.no_network
async def test_adaptive_greeting_skips_project_briefing_and_answers_directly(
    monkeypatch,
):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Hey! What would you like to work on?"}',
                    "submit-1",
                ),
                completed_event(),
            ]
        ]
    )
    run = make_run(user_query="Hey")
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    counts = install_counted_project_briefing(monkeypatch, executor)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert counts == {"brief": 0, "context": 0}
    assert "<project_brief>" not in llm.calls[0]["system"]
    assert "<project_context>" not in llm.calls[0]["system"]
    assert run.project_briefing.loaded is False
    assert run.project_briefing.transition_count == 0
    assert run.project_briefing.content_token_count == 0


@pytest.mark.no_network
async def test_adaptive_project_memory_request_loads_briefing_before_first_step(
    monkeypatch,
):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "The decision was retained."}',
                    "submit-1",
                ),
                completed_event(),
            ]
        ]
    )
    run = make_run(user_query="What did we decide about the ingestion project?")
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    counts = install_counted_project_briefing(monkeypatch, executor)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert counts == {"brief": 1, "context": 1}
    assert "BRIEF_PAYLOAD" in llm.calls[0]["system"]
    assert "CONTEXT_PAYLOAD" in llm.calls[0]["system"]
    assert run.project_briefing.initial_reason == "explicit_project_memory_intent"
    assert run.project_briefing.load_count == 1
    assert run.project_briefing.content_token_count == 2


@pytest.mark.no_network
async def test_research_mode_loads_briefing_before_the_first_step(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_knowledge_messages",
                    '{"query": "source"}',
                    "search-1",
                ),
                completed_event(),
            ]
        ]
    )
    run = make_run(
        user_query="Investigate the source.",
        limits=AgentRunLimits(max_attempts=1, max_calls=1),
        research_profile=resolve_research_profile("research"),
    )
    assert run.set_research_plan(["What changed?"]) is None
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    counts = install_counted_project_briefing(monkeypatch, executor)

    async def searched_evidence(*_args):
        return {"data": [{"id": "message-1", "message": "SOURCE_FACT"}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", searched_evidence)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert counts == {"brief": 1, "context": 1}
    assert run.project_briefing.initial_reason == "research_mode"
    assert "BRIEF_PAYLOAD" in llm.calls[0]["system"]


@pytest.mark.no_network
async def test_document_selection_loads_briefing_before_the_first_step(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "The selected passage is clear."}',
                    "submit-1",
                ),
                completed_event(),
            ]
        ]
    )
    run = make_run(
        user_query="hey",
        document_selection_context={"excerpt": "selected passage"},
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    counts = install_counted_project_briefing(monkeypatch, executor)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert counts == {"brief": 1, "context": 1}
    assert run.project_briefing.initial_reason == "document_selection"
    assert "BRIEF_PAYLOAD" in llm.calls[0]["system"]


@pytest.mark.no_network
async def test_always_mode_loads_briefing_for_a_greeting(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Hey!"}',
                    "submit-1",
                ),
                completed_event(),
            ]
        ]
    )
    run = make_run(
        user_query="hey",
        limits=AgentRunLimits(
            max_attempts=1,
            max_calls=1,
            project_briefing_mode="always",
        ),
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    counts = install_counted_project_briefing(monkeypatch, executor)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert counts == {"brief": 1, "context": 1}
    assert run.project_briefing.initial_reason == "always"
    assert "BRIEF_PAYLOAD" in llm.calls[0]["system"]


@pytest.mark.no_network
async def test_briefing_policy_compares_persistent_prompt_cost_and_steps(monkeypatch):
    async def run_case(mode, query):
        llm = ScriptedLLM(
            [
                [
                    tool_call_event(
                        "submit_answer",
                        '{"content": "Acknowledged."}',
                        "submit-1",
                    ),
                    completed_event(),
                ]
            ]
        )
        run = make_run(
            user_query=query,
            limits=AgentRunLimits(
                max_attempts=1,
                max_calls=1,
                project_briefing_mode=mode,
            ),
        )
        manifest_calls = 0

        async def get_document_manifest():
            nonlocal manifest_calls
            manifest_calls += 1
            return [
                {
                    "original_name": "project-notes.md",
                    "size_bytes": 2048,
                    "chunk_count": 3,
                }
            ]

        executor = AgentExecutor(
            run,
            llm,
            SimpleNamespace(
                document_service=object(),
                get_document_manifest=get_document_manifest,
            ),
        )
        briefing_counts = install_counted_project_briefing(monkeypatch, executor)

        events = [event async for event in executor._execute_run()]
        assert events[-1]["event"] == "response"
        assert len(llm.calls) == 1
        return {
            "briefing_counts": briefing_counts,
            "manifest_calls": manifest_calls,
            "steps": len(llm.calls),
            "prompt_tokens": llm.count_tokens(
                f"{llm.calls[0]['system']}\n{llm.calls[0]['user']}"
            ),
            "system": llm.calls[0]["system"],
        }

    adaptive_greeting = await run_case("adaptive", "Hey")
    always_greeting = await run_case("always", "Hey")
    adaptive_project_question = await run_case(
        "adaptive",
        "What did we decide about this project?",
    )
    always_project_question = await run_case(
        "always",
        "What did we decide about this project?",
    )

    assert adaptive_greeting["briefing_counts"] == {"brief": 0, "context": 0}
    assert adaptive_greeting["manifest_calls"] == 0
    assert "<project_brief>" not in adaptive_greeting["system"]
    assert "<project_context>" not in adaptive_greeting["system"]
    assert "<uploaded_documents>" not in adaptive_greeting["system"]
    assert always_greeting["briefing_counts"] == {"brief": 1, "context": 1}
    assert always_greeting["manifest_calls"] == 1
    assert "<uploaded_documents>" in always_greeting["system"]
    assert adaptive_greeting["prompt_tokens"] < always_greeting["prompt_tokens"]
    assert adaptive_greeting["steps"] == always_greeting["steps"] == 1

    assert adaptive_project_question["briefing_counts"] == {"brief": 1, "context": 1}
    assert adaptive_project_question["manifest_calls"] == 1
    assert (
        adaptive_project_question["prompt_tokens"]
        == always_project_question["prompt_tokens"]
    )
    assert adaptive_project_question["steps"] == always_project_question["steps"] == 1


@pytest.mark.no_network
async def test_adaptive_tool_followup_loads_cached_briefing_once(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_knowledge_messages",
                    '{"query": "scope"}',
                    "search-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Draft from the retrieved result."}',
                    "draft-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Final from the retrieved result."}',
                    "final-1",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(
        user_query="Nice, go to the next one",
        limits=AgentRunLimits(max_attempts=3, max_calls=2),
    )
    manifest_calls = 0

    async def get_document_manifest():
        nonlocal manifest_calls
        manifest_calls += 1
        return [
            {
                "original_name": "project-notes.md",
                "size_bytes": 2048,
                "chunk_count": 3,
            }
        ]

    executor = AgentExecutor(
        run,
        llm,
        SimpleNamespace(
            document_service=object(),
            get_document_manifest=get_document_manifest,
        ),
    )
    counts = install_counted_project_briefing(monkeypatch, executor)

    async def searched_evidence(*_args):
        return {"data": [{"id": "message-1", "message": "SCOPE_FACT"}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", searched_evidence)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert counts == {"brief": 1, "context": 1}
    assert "<project_brief>" not in llm.calls[0]["system"]
    assert "BRIEF_PAYLOAD" in llm.calls[1]["system"]
    assert "BRIEF_PAYLOAD" in llm.calls[2]["system"]
    assert "<uploaded_documents>" not in llm.calls[0]["system"]
    assert "project-notes.md" in llm.calls[1]["system"]
    assert "project-notes.md" in llm.calls[2]["system"]
    assert manifest_calls == 1
    assert run.project_briefing.transition_count == 1
    assert run.project_briefing.load_count == 1


@pytest.mark.no_network
async def test_llm_call_telemetry_records_briefing_cost_and_transition(monkeypatch):
    run = make_run(user_query="hey")
    run.request_project_briefing_transition()
    run.record_project_briefing_loaded(
        brief="Project Brief",
        context="Project Context",
        content_token_count=4,
    )
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace())
    emitted = []

    async def capture_emit(*args, **kwargs):
        emitted.append((args, kwargs))

    monkeypatch.setattr("core.agent.executor.emit", capture_emit)

    await executor._emit_llm_call("architect", "high")

    args, kwargs = emitted[0]
    assert args[:3] == ("session-1", "agent", "llm_call")
    assert args[3]["project_briefing"] == {
        "mode": "adaptive",
        "reason": "tool_followup",
        "loaded": True,
        "load_count": 1,
        "transition_count": 1,
        "content_token_count": 4,
    }
    assert kwargs == {"verbose_only": True}


@pytest.mark.no_network
async def test_executor_loop_accumulates_context_across_reasoning_attempts(
    monkeypatch,
):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_knowledge_messages",
                    '{"query": "profile", "limit": 3}',
                    "call-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "get_entity_recent_activity",
                    '{"entity_name": "Knoggin", "hours": 24}',
                    "call-2",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Both facts are retained."}',
                    "submit-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Both facts are retained."}',
                    "submit-2",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(limits=AgentRunLimits(max_attempts=4, max_calls=4))
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    async def fake_execute(_tools, name, _args):
        if name == "search_knowledge_messages":
            return {
                "data": [
                    {
                        "id": "message-1",
                        "message": "LAUNCH_FACT_VIOLET",
                        "score": 0.9,
                    }
                ]
            }
        return {
            "data": [
                {
                    "entity_id": 7,
                    "entity": "Knoggin",
                    "time": 1_700_000_000_000,
                    "evidence": [
                        {
                            "id": "message-2",
                            "session_id": "session-1",
                            "message": "OWNER_FACT_ADA",
                        }
                    ],
                }
            ]
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert events[-1]["data"]["content"] == "Both facts are retained."
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
    assert [schema["function"]["name"] for schema in llm.calls[-1]["tools"]] == [
        "request_clarification",
        "submit_answer",
    ]
    assert "LAUNCH_FACT_VIOLET" in llm.calls[-1]["user"]
    assert "OWNER_FACT_ADA" in llm.calls[-1]["user"]
    assert (
        "Activities:\n- ACT1 E1 Knoggin at 1700000000000 (evidence: M2)"
        in llm.calls[-1]["user"]
    )
    assert "Messages:\n- M1: LAUNCH_FACT_VIOLET" in llm.calls[-1]["user"]
    assert run.attempt_count == 4
    assert run.call_count == 2
    assert run.notebook.section_items("messages") == (
        {"id": "message-1", "message": "LAUNCH_FACT_VIOLET", "score": 0.9},
        {
            "id": "message-2",
            "session_id": "session-1",
            "message": "OWNER_FACT_ADA",
        },
    )
    assert run.notebook.section_items("activities") == (
        {
            "entity_id": 7,
            "entity": "Knoggin",
            "time": 1_700_000_000_000,
            "evidence_refs": ["message::session-1:message-2"],
        },
    )
    assert run.usage["total_tokens"] == 20
    assert run.sealed is True
    run.release()
    assert run.released is True


@pytest.mark.no_network
async def test_final_synthesis_receives_each_admitted_evidence_kind(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_episodes",
                    '{"query": "launch history"}',
                    "episode-a",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "search_knowledge_messages",
                    '{"query": "launch decision"}',
                    "message-b",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "find_relationship_path",
                    '{"entity_a": "Ada", "entity_b": "Knoggin"}',
                    "path-c",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "read_project_document",
                    '{"document_id": "doc-d"}',
                    "document-d",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "read_web_page",
                    '{"url": "https://example.test/release"}',
                    "web-d",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Draft from all admitted evidence."}',
                    "submit-draft",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Final answer from all admitted evidence."}',
                    "submit-final",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(limits=AgentRunLimits(max_attempts=6, max_calls=5))
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    async def fake_execute(_tools, name, _args):
        if name == "search_episodes":
            return {
                "data": {
                    "resolution": "exact",
                    "results": [
                        {
                            "entity_name": "Knoggin",
                            "episodes": [
                                {
                                    "episode_id": "a3f91c84-1111-4444-8888-111111111111",
                                    "summary": "EPISODE_A",
                                    "first_message_at": "2026-01-01T10:00:00+00:00",
                                    "last_message_at": "2026-01-02T10:00:00+00:00",
                                }
                            ],
                        }
                    ],
                }
            }
        if name == "search_knowledge_messages":
            return {"data": [{"id": "message-b", "message": "MESSAGE_B"}]}
        if name == "find_relationship_path":
            return {"data": [{"entity_a": "Ada", "entity_b": "Knoggin"}]}
        if name == "read_project_document":
            return {
                "data": [
                    {
                        "document_id": "doc-d",
                        "document_name": "evidence.md",
                        "chunk_index": "lines:1-2",
                        "content": "DOCUMENT_D",
                    }
                ]
            }
        if name == "read_web_page":
            return {
                "data": [
                    {
                        "title": "Release source",
                        "url": "https://example.test/release",
                        "content": "WEB_D",
                        "start_line": 1,
                        "end_line": 1,
                        "content_hash": "d" * 64,
                    }
                ]
            }
        raise AssertionError(f"unexpected tool: {name}")

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["data"]["content"] == "Final answer from all admitted evidence."
    assert len(llm.calls) == 7
    assert "CURRENT EXECUTION PHASE: SYNTHESIZE" in llm.calls[-1]["system"]
    final_prompt = llm.calls[-1]["user"]
    assert "ep_a3f91c: EPISODE_A" in final_prompt
    assert (
        "chronology: 2026-01-01T10:00:00+00:00 to 2026-01-02T10:00:00+00:00"
        in final_prompt
    )
    assert "Messages:\n- M1: MESSAGE_B" in final_prompt
    assert "Paths:\n- P1: Ada -> Knoggin" in final_prompt
    assert "Documents:\n- D1 evidence.md: DOCUMENT_D" in final_prompt
    assert (
        "Web reads:\n- WR1 Release source: https://example.test/release" in final_prompt
    )
    assert "read passage: WEB_D" in final_prompt
    assert all(
        "Result was not added to the run notebook"
        not in event["data"].get("result", "")
        for event in events
        if event["event"] == "tool_end"
    )


@pytest.mark.no_network
async def test_final_synthesis_receives_episode_reversal_chronology(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_episodes",
                    '{"query": "current deployment policy"}',
                    "episodes-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Draft based on the later policy."}',
                    "submit-draft",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "The current supported policy is the later replacement."}',
                    "submit-final",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(limits=AgentRunLimits(max_attempts=2, max_calls=1))
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    async def episode_reversal(_tools, name, _args):
        assert name == "search_episodes"
        return {
            "data": {
                "resolution": "exact",
                "results": [
                    {
                        "entity_name": "Knoggin",
                        "episodes": [
                            {
                                "episode_id": "11111111-1111-4444-8888-111111111111",
                                "summary": "OLDER_POLICY: deploy manually.",
                                "first_message_at": "2026-01-01T10:00:00+00:00",
                                "last_message_at": "2026-01-01T10:00:00+00:00",
                            },
                            {
                                "episode_id": "22222222-1111-4444-8888-111111111111",
                                "summary": "LATER_POLICY: deploy through CI.",
                                "first_message_at": "2026-02-01T10:00:00+00:00",
                                "last_message_at": "2026-02-01T10:00:00+00:00",
                            },
                        ],
                    }
                ],
            }
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", episode_reversal)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["data"]["content"] == (
        "The current supported policy is the later replacement."
    )
    final_prompt = llm.calls[-1]["user"]
    assert final_prompt.index("OLDER_POLICY: deploy manually.") < final_prompt.index(
        "LATER_POLICY: deploy through CI."
    )
    assert "chronology: 2026-01-01T10:00:00+00:00" in final_prompt
    assert "chronology: 2026-02-01T10:00:00+00:00" in final_prompt
    assert (
        "When multiple retrieved Episodes describe a change or reversal"
        in llm.calls[-1]["system"]
    )
    assert (
        "A later, supported state is the best available current state"
        in llm.calls[-1]["system"]
    )


@pytest.mark.no_network
async def test_capacity_rejection_records_no_sources_and_allows_a_narrow_retry(
    monkeypatch,
):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_web",
                    '{"query": "wide release history"}',
                    "wide-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "search_web",
                    '{"query": "narrow release history"}',
                    "narrow-2",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Draft from the narrow result."}',
                    "submit-draft",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Final from the narrow result."}',
                    "submit-final",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(
        limits=AgentRunLimits(
            max_attempts=4,
            max_calls=3,
            max_accumulated_web_discoveries=1,
        )
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    calls = []

    def web_result(title, url, excerpt, *, query, rank, content_hash):
        return {
            "title": title,
            "url": url,
            "snippet": excerpt,
            "provider": "brave",
            "query": query,
            "rank": rank,
            "source_kind": "web_search_result",
            "source_context": {
                "source_kind": "web_search_result",
                "canonical_url": url,
                "content_hash": content_hash,
                "locator": {
                    "kind": "search_result",
                    "provider": "brave",
                    "query": query,
                    "rank": rank,
                },
                "excerpt": excerpt,
                "metadata": {"title": title, "discovery_snippet": True},
            },
        }

    async def fake_execute(_tools, name, args):
        calls.append((name, args))
        if args["query"] == "wide release history":
            return {
                "data": [
                    web_result(
                        "Wide result one",
                        "https://example.test/wide-one",
                        "WIDE_RESULT_ONE",
                        query=args["query"],
                        rank=1,
                        content_hash="a" * 64,
                    ),
                    web_result(
                        "Wide result two",
                        "https://example.test/wide-two",
                        "WIDE_RESULT_TWO",
                        query=args["query"],
                        rank=2,
                        content_hash="b" * 64,
                    ),
                ]
            }
        return {
            "data": [
                web_result(
                    "Narrow result",
                    "https://example.test/narrow",
                    "NARROW_RESULT",
                    query=args["query"],
                    rank=1,
                    content_hash="c" * 64,
                )
            ]
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)

    events = [event async for event in executor._execute_run()]

    rejection = next(
        event
        for event in events
        if event["event"] == "tool_end" and event["data"].get("call_id") == "wide-1"
    )
    assert rejection == {
        "event": "tool_end",
        "data": {
            "tool": "search_web",
            "result": (
                "Result was not added to the run notebook because it exceeds the "
                "evidence capacity. Try a narrower query or read a smaller document "
                "or web range."
            ),
            "call_id": "wide-1",
        },
    }
    assert "Result was not added to the run notebook" in llm.calls[1]["user"]
    assert "Wide result one" not in llm.calls[1]["user"]
    assert "WIDE_RESULT_ONE" not in llm.calls[1]["user"]
    assert "Found 2 items" not in llm.calls[1]["user"]
    assert "CURRENT EXECUTION PHASE: PLAN" in llm.calls[1]["system"]
    assert calls == [
        ("search_web", {"query": "wide release history"}),
        ("search_web", {"query": "narrow release history"}),
    ]
    assert [candidate.tool_call_id for candidate in run.source_candidates] == [
        "narrow-2"
    ]
    assert [candidate.result_position for candidate in run.source_candidates] == [0]
    assert [item["url"] for item in run.notebook.section_items("web_discoveries")] == [
        "https://example.test/narrow"
    ]
    assert events[-1]["event"] == "response"
    assert [
        source["tool_call_id"] for source in events[-1]["data"]["sources_consulted"]
    ] == ["narrow-2"]


@pytest.mark.no_network
async def test_executor_automatically_replans_after_empty_evidence(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event("search_knowledge_messages", '{"query": "missing"}', "search-1"),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer", '{"content": "Still looking."}', "submit-1"
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer", '{"content": "No matching evidence."}', "submit-2"
                ),
                completed_event(),
            ],
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

    assert events[-1]["data"]["content"] == "Still looking."
    assert [call["model"] for call in llm.calls] == ["architect", "architect"]
    assert "CURRENT EXECUTION PHASE: PLAN" in llm.calls[1]["system"]


@pytest.mark.no_network
@pytest.mark.parametrize("mode", ["research", "deep_research"])
async def test_research_modes_reject_ungrounded_terminal_answers(mode):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Ungrounded answer."}',
                    "submit-ungrounded",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "request_clarification",
                    '{"question": "Which evidence should I investigate?"}',
                    "clarify-after-rejection",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(
        limits=AgentRunLimits(max_attempts=2, max_calls=1),
        research_profile=resolve_research_profile(mode),
    )
    assert run.set_research_plan(["What changed?"]) is None
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    events = [event async for event in executor._execute_run()]

    assert events == [
        {
            "event": "clarification",
            "data": {
                "question": "Which evidence should I investigate?",
                "usage": run.usage,
            },
        }
    ]
    assert run.call_count == 0
    assert len(llm.calls) == 2
    assert all("CURRENT EXECUTION PHASE: PLAN" in call["system"] for call in llm.calls)
    assert (
        "Research mode requires grounded investigation evidence before submit_answer."
        in llm.calls[1]["user"]
    )


@pytest.mark.no_network
async def test_research_requires_read_content_after_document_listing(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event("list_project_documents", "{}", "list-documents"),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "The listing is enough."}',
                    "submit-metadata",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "read_project_document",
                    '{"document_id": "document-1"}',
                    "read-document",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Draft from the read passage.", "research_coverage": [{"subquestion": "What changed?", "supporting_references": [], "unresolved_gap": "The exact notebook reference was not retained."}]}',
                    "submit-draft",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Final answer from the read passage.", "research_coverage": [{"subquestion": "What changed?", "supporting_references": [], "unresolved_gap": "The exact notebook reference was not retained."}]}',
                    "submit-final",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(
        limits=AgentRunLimits(max_attempts=4, max_calls=2),
        research_profile=resolve_research_profile("research"),
    )
    assert run.set_research_plan(["What changed?"]) is None
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    dispatched = []

    async def document_results(_tools, name, args):
        dispatched.append((name, args))
        if name == "list_project_documents":
            return {
                "data": [
                    {
                        "document_id": "document-1",
                        "document_name": "brief.md",
                    }
                ]
            }
        if name == "read_project_document":
            return {
                "data": [
                    {
                        "document_id": "document-1",
                        "document_name": "brief.md",
                        "content": "The read document contains the answer.",
                    }
                ]
            }
        raise AssertionError(f"unexpected tool: {name}")

    monkeypatch.setattr("core.agent.executor.execute_tool", document_results)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert events[-1]["data"]["content"] == "Final answer from the read passage."
    assert events[-1]["data"]["artifact"]["kind"] == "research_brief"
    assert dispatched == [
        ("list_project_documents", {}),
        ("read_project_document", {"document_id": "document-1"}),
    ]
    assert len(llm.calls) == 5
    assert (
        "Research mode requires grounded investigation evidence before submit_answer."
        in llm.calls[2]["user"]
    )


@pytest.mark.no_network
@pytest.mark.parametrize("mode", ["research", "deep_research"])
async def test_research_fallback_requires_grounded_investigation_evidence(mode):
    llm = ScriptedLLM([])
    summary_calls = []

    async def generate_summary(**kwargs):
        summary_calls.append(kwargs)
        return "This should not become a research answer."

    llm.generate_text = generate_summary
    run = make_run(research_profile=resolve_research_profile(mode))
    run.notebook.apply("edit_agent_brain", {"data": {"success": True}})
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    event = await executor._fallback()

    assert event == {
        "event": "clarification",
        "data": {
            "question": (
                "I couldn't complete the research because I didn't gather usable "
                "evidence. Which source or detail should I investigate?"
            ),
            "usage": run.usage,
            "fallback": True,
        },
    }
    assert summary_calls == []
    assert run.final_content is None
    assert run.sealed is True


@pytest.mark.no_network
async def test_research_fallback_summarizes_grounded_evidence(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_knowledge_messages",
                    '{"query": "release"}',
                    "search-release",
                ),
                completed_event(),
            ]
        ]
    )
    run = make_run(
        limits=AgentRunLimits(max_attempts=1, max_calls=1),
        research_profile=resolve_research_profile("research"),
    )
    assert run.set_research_plan(["What changed?"]) is None
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    async def grounded_result(*_args):
        return {
            "data": [
                {
                    "id": "message-1",
                    "message": "The release notes describe the change.",
                }
            ]
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", grounded_result)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert events[-1]["data"]["fallback"] is True
    assert events[-1]["data"]["artifact"]["kind"] == "research_brief"
    assert run.final_content == "A concise evidence summary."


def _validated_initial_source_candidates(kind):
    if kind == "pasted_text":
        return build_pasted_text_candidates(
            project_id="project-1",
            session_id="session-1",
            source_message_id=1,
            message_content="Evidence supplied by the user:\n```text\nRelevant fact.\n```",
            agent_run_id="run-1",
        )
    return [
        build_document_selection_candidate(
            project_id="project-1",
            session_id="session-1",
            agent_run_id="run-1",
            selection_context={
                "document_id": "document-1",
                "project_id": "project-1",
                "document_name": "brief.md",
                "relative_path": "notes/brief.md",
                "extension": ".md",
                "content_hash": "a" * 64,
                "parse_snapshot_id": "snapshot-1",
                "locator": {"kind": "text_lines", "start_line": 1, "end_line": 1},
                "excerpt": "Relevant selected document fact.",
            },
        )
    ]


@pytest.mark.no_network
@pytest.mark.parametrize("source_kind", ["pasted_text", "document_selection"])
async def test_research_accepts_validated_supplied_evidence_without_dispatch(
    source_kind,
):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Answer from supplied evidence.", "research_coverage": [{"subquestion": "What changed?", "supporting_references": [], "unresolved_gap": "Evidence was supplied outside the run notebook."}]}',
                    "submit-supplied",
                ),
                completed_event(),
            ]
        ]
    )
    run = make_run(
        limits=AgentRunLimits(max_attempts=1, max_calls=1),
        research_profile=resolve_research_profile("research"),
        initial_source_candidates=_validated_initial_source_candidates(source_kind),
    )
    assert run.set_research_plan(["What changed?"]) is None
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert events[-1]["data"]["artifact"]["kind"] == "research_brief"
    assert run.call_count == 0
    assert len(llm.calls) == 1
    assert [
        event["data"]["tool"] for event in events if event["event"] == "tool_start"
    ] == []
    assert events[-1]["data"]["sources_consulted"][0][
        "encounter_kind"
    ] == source_kind.replace("pasted_text", "user_pasted_text")


@pytest.mark.no_network
async def test_research_plan_is_frozen_before_completion():
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "set_research_plan",
                    '{"subquestions": ["What changed?", "Why did it change?"]}',
                    "set-plan",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Incomplete answer.", "research_coverage": '
                    '[{"subquestion": "What changed?", "supporting_references": [], '
                    '"unresolved_gap": "Supplied evidence was outside the notebook."}]}',
                    "submit-incomplete",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(
        limits=AgentRunLimits(max_attempts=2, max_calls=1),
        research_profile=resolve_research_profile("research"),
        initial_source_candidates=_validated_initial_source_candidates("pasted_text"),
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "error"
    assert run.research_subquestions == ("What changed?", "Why did it change?")
    assert {
        schema["function"]["name"] for schema in llm.calls[0]["tools"]
    } == {"request_clarification", "set_research_plan"}
    assert "set_research_plan" not in {
        schema["function"]["name"] for schema in llm.calls[1]["tools"]
    }


@pytest.mark.no_network
async def test_invalid_research_plan_is_rejected_before_investigation():
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "set_research_plan",
                    '{"subquestions": []}',
                    "invalid-plan",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "request_clarification",
                    '{"question": "What should the research focus on?"}',
                    "clarify-plan",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(
        limits=AgentRunLimits(max_attempts=2, max_calls=1),
        research_profile=resolve_research_profile("research"),
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "clarification"
    assert run.research_subquestions == ()
    assert "requires 1-12 non-empty" in llm.calls[1]["user"]


@pytest.mark.no_network
async def test_deep_research_performs_one_gap_review_before_synthesis(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "read_web_page",
                    '{"url": "https://primary.example.test/release"}',
                    "read-primary",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Evidence is sufficient after review.", "research_coverage": [{"subquestion": "What changed?", "supporting_references": [], "unresolved_gap": "Final notebook reference selection remains."}]}',
                    "submit-gap-review",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Final deep-research answer.", "research_coverage": [{"subquestion": "What changed?", "supporting_references": [], "unresolved_gap": "No material gap remains beyond the cited answer."}]}',
                    "submit-synthesis",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(
        limits=AgentRunLimits(max_attempts=2, max_calls=1),
        research_profile=resolve_research_profile("deep_research"),
    )
    assert run.set_research_plan(["What changed?"]) is None
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    dispatched = []

    async def primary_source(_tools, name, args):
        dispatched.append((name, args))
        return {
            "data": [
                {
                    "title": "Official primary release note",
                    "url": "https://primary.example.test/release",
                    "content": "The primary source directly answers the question.",
                    "source_context": {
                        "source_kind": "web_page",
                        "canonical_url": "https://primary.example.test/release",
                        "content_hash": "b" * 64,
                        "locator": {
                            "kind": "text_lines",
                            "start_line": 1,
                            "end_line": 1,
                        },
                        "excerpt": "The primary source directly answers the question.",
                        "metadata": {"title": "Official primary release note"},
                    },
                }
            ]
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", primary_source)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "response"
    assert events[-1]["data"]["artifact"]["kind"] == "research_report"
    assert dispatched == [
        ("read_web_page", {"url": "https://primary.example.test/release"})
    ]
    assert run.deep_research_gap_review_count == 1
    assert len(llm.calls) == 3
    assert [call["model"] for call in llm.calls] == [
        "architect",
        "architect",
        "architect",
    ]
    assert [call["reasoning"] for call in llm.calls] == ["high", "high", "high"]
    assert "CURRENT EXECUTION PHASE: PLAN" in llm.calls[0]["system"]
    assert "CURRENT EXECUTION PHASE: PLAN" in llm.calls[1]["system"]
    assert "<deep_research_gap_review>" in llm.calls[1]["system"]
    assert "CURRENT EXECUTION PHASE: SYNTHESIZE" in llm.calls[2]["system"]


@pytest.mark.no_network
async def test_executor_reserves_one_synthesis_attempt_after_normal_budget(
    monkeypatch,
):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_knowledge_messages",
                    '{"query": "profile", "limit": 3}',
                    "search-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Draft answer."}',
                    "submit-draft",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Final answer."}',
                    "submit-final",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(limits=AgentRunLimits(max_attempts=2, max_calls=2))
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    async def evidence_result(*_args):
        return {
            "data": [{"id": "message-1", "message": "Profile changed", "score": 0.9}]
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", evidence_result)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["data"]["content"] == "Final answer."
    assert [call["model"] for call in llm.calls] == [
        "architect",
        "librarian",
        "architect",
    ]
    assert "CURRENT EXECUTION PHASE: SYNTHESIZE" in llm.calls[-1]["system"]
    assert run.attempt_count == 3
    assert run.synthesis_attempt_count == 1


@pytest.mark.no_network
async def test_executor_replans_after_mixed_terminal_batch_without_dispatch(
    monkeypatch,
):
    secret = "RAW_SENSITIVE_MIXED_BATCH_VALUE"
    llm = ScriptedLLM(
        [
            [
                tool_calls_event(
                    [
                        (
                            "submit_answer",
                            '{"content": "This answer must not be accepted."}',
                            "submit-mixed",
                        ),
                        (
                            "search_knowledge_messages",
                            f'{{"query": "{secret}"}}',
                            "search-mixed",
                        ),
                    ]
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "request_clarification",
                    '{"question": "Which profile should I use?"}',
                    "clarify-after-mixed",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(limits=AgentRunLimits(max_attempts=2, max_calls=2))
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    dispatched = []

    async def fake_execute(_tools, name, args):
        dispatched.append((name, args))
        return {"data": []}

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)

    events = [event async for event in executor._execute_run()]

    assert [event["event"] for event in events] == ["clarification"]
    assert events[-1]["event"] == "clarification"
    assert dispatched == []
    assert run.call_count == 0
    assert run.tool_call_counts == {}
    assert len(llm.calls) == 2
    assert all("CURRENT EXECUTION PHASE: PLAN" in call["system"] for call in llm.calls)
    assert "Terminal protocol tools must be called alone." in llm.calls[1]["user"]
    assert secret not in llm.calls[1]["user"]


@pytest.mark.no_network
async def test_executor_carries_admitted_sources_into_a_clarification():
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "request_clarification",
                    '{"question": "Which part should I verify?"}',
                    "clarify-with-source",
                ),
                completed_event(),
            ]
        ]
    )
    run = make_run(
        initial_source_candidates=_validated_initial_source_candidates("pasted_text")
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    events = [event async for event in executor._execute_run()]

    assert [event["event"] for event in events] == ["clarification"]
    assert events[0]["data"]["sources_consulted"][0]["source_kind"] == (
        "user_pasted_text"
    )


@pytest.mark.no_network
async def test_executor_rejects_hidden_synthesis_write_without_dispatch(monkeypatch):
    secret = "RAW_SENSITIVE_SYNTHESIS_VALUE"
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_knowledge_messages",
                    '{"query": "profile", "limit": 3}',
                    "search-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Draft answer."}',
                    "submit-draft",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "edit_agent_brain",
                    (
                        '{"section": "Role", "content": "'
                        f"{secret}"
                        '", "expected_revision": 1}'
                    ),
                    "edit-hidden",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "request_clarification",
                    '{"question": "Which detail should I verify?"}',
                    "clarify-after-synthesis",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(limits=AgentRunLimits(max_attempts=4, max_calls=2))
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    dispatched = []

    async def fake_execute(_tools, name, args):
        dispatched.append((name, args))
        if name == "search_knowledge_messages":
            return {
                "data": [
                    {
                        "id": "message-1",
                        "message": "Profile changed",
                        "score": 0.9,
                    }
                ]
            }
        return {"data": {"success": True}}

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["event"] == "clarification"
    assert [
        event["data"]["tool"] for event in events if event["event"] == "tool_start"
    ] == ["search_knowledge_messages"]
    assert dispatched == [("search_knowledge_messages", {"query": "profile", "limit": 3})]
    assert run.call_count == 1
    assert len(llm.calls) == 4
    assert "CURRENT EXECUTION PHASE: SYNTHESIZE" in llm.calls[2]["system"]
    assert [schema["function"]["name"] for schema in llm.calls[2]["tools"]] == [
        "request_clarification",
        "submit_answer",
    ]
    assert "CURRENT EXECUTION PHASE: PLAN" in llm.calls[3]["system"]
    assert "Returned tool is not allowed during SYNTHESIZE." in llm.calls[3]["user"]
    assert secret not in llm.calls[3]["user"]


@pytest.mark.no_network
async def test_topic_context_evidence_triggers_final_synthesis(monkeypatch):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "load_project_topic_context",
                    '{"topics": ["Work", "Finance"]}',
                    "topics-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Draft answer."}',
                    "submit-draft",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "submit_answer",
                    '{"content": "Final answer."}',
                    "submit-final",
                ),
                completed_event(),
            ],
        ]
    )
    run = make_run(limits=AgentRunLimits(max_attempts=3, max_calls=2))
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    async def topic_context_result(*_args):
        return {
            "data": {
                "Work": {
                    "entities": [{"name": "Acme"}],
                    "messages": [
                        {
                            "id": "msg_7",
                            "message": "The offer changes compensation.",
                        }
                    ],
                }
            }
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", topic_context_result)

    events = [event async for event in executor._execute_run()]

    assert events[-1]["data"]["content"] == "Final answer."
    assert "load_project_topic_context" in [
        schema["function"]["name"] for schema in llm.calls[0]["tools"]
    ]
    assert "CURRENT EXECUTION PHASE: SYNTHESIZE" in llm.calls[-1]["system"]
    messages = run.notebook.section_items("messages")
    assert messages[0]["id"] == "msg_7"
    assert messages[0]["context"][0]["content"] == ("The offer changes compensation.")


@pytest.mark.no_network
async def test_executor_loop_enforces_duplicate_tool_and_global_limits(
    monkeypatch,
):
    run = make_run(
        limits=AgentRunLimits(
            max_calls=4,
            tool_limits=(("search_knowledge_messages", 2),),
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
                ToolCall("search_knowledge_messages", {"query": "one"}, call_id="one"),
                ToolCall("search_knowledge_messages", {"query": "one"}, call_id="dup"),
                ToolCall("search_knowledge_messages", {"query": "two"}, call_id="two"),
                ToolCall("search_knowledge_messages", {"query": "three"}, call_id="three"),
            ],
            results,
        )
    ]

    errors = [event for event in events if event["event"] == "tool_error"]
    assert len(errors) == 2
    assert errors[0]["data"]["error"] == "Duplicate call skipped"
    assert errors[1]["data"]["error"] == (
        "Call limit reached for search_knowledge_messages"
    )
    assert run.call_count == 2
    assert run.tool_call_counts == {"search_knowledge_messages": 2}
    assert [item["id"] for item in run.notebook.section_items("messages")] == [
        "one",
        "two",
    ]


@pytest.mark.no_network
async def test_fallback_summary_uses_all_canonical_evidence_categories():
    llm = ScriptedLLM([])
    run = make_run()
    run.notebook.apply(
        "search_episodes",
        {
            "data": {
                "resolution": "direct",
                "results": [
                    {
                        "entity_name": "Ada",
                        "episodes": [{"episode_id": "ep-1", "summary": "Found clue"}],
                    }
                ],
            }
        },
    )
    run.notebook.apply(
        "find_relationship_path",
        {"data": [{"entity_a": "Ada", "entity_b": "Knoggin", "step": 0}]},
    )
    run.notebook.apply(
        "search_web",
        {
            "data": [
                {
                    "title": "Useful source",
                    "url": "https://example.test/source",
                    "snippet": "Useful evidence.",
                    "source_kind": "web_search_result",
                }
            ]
        },
    )
    run.notebook.set_summary("Previously compacted evidence.")
    prompts = []

    async def capture_summary(**kwargs):
        prompts.append(kwargs["user"])
        return "Fallback answer."

    llm.generate_text = capture_summary
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    event = await executor._fallback()

    assert event["data"]["content"] == "Fallback answer."
    assert prompts
    prompt = prompts[0]
    assert "RUN NOTEBOOK" in prompt
    assert "Summary: Previously compacted evidence." in prompt
    assert "Found clue" in prompt
    assert "Ada -> Knoggin" in prompt
    assert "Useful source" in prompt
    assert "discovery snippet: Useful evidence." in prompt


@pytest.mark.no_network
async def test_executor_reports_the_notebook_owned_bounded_token_count():
    llm = ScriptedLLM([])
    run = make_run()
    run.notebook.apply(
        "search_knowledge_messages",
        {"data": [{"id": "m1", "message": "A retained message"}]},
    )
    run.notebook.apply(
        "search_knowledge_entities",
        {"data": [{"id": "p1", "canonical_name": "Ada"}]},
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    generation = run.notebook.generation
    executor._refresh_evidence_token_count()

    assert build_evidence_context(run) == run.notebook.render()
    assert run.evidence_token_count == llm.count_tokens(build_evidence_context(run))
    assert run.notebook.generation == generation


@pytest.mark.no_network
async def test_executor_loop_recovers_from_invalid_arguments_and_tool_exceptions(
    monkeypatch,
):
    run = make_run(limits=AgentRunLimits(max_calls=3))
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace())
    calls = []

    async def fake_execute(_tools, name, _args):
        calls.append(name)
        if name == "search_knowledge_messages":
            raise RuntimeError("backend exploded")
        return {"data": [{"id": "ok", "message": "usable"}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)

    invalid = executor._parse_tool_calls(
        [{"name": "search_knowledge_messages", "arguments": "{bad", "id": "bad"}],
        "",
    )[0]
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [invalid, ToolCall("search_knowledge_entities", {"query": "Ada"}, call_id="ok")],
            results,
        )
    ]

    assert calls == ["search_knowledge_entities"]
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
            [ToolCall("search_knowledge_messages", {"query": "explode"}, call_id="err")],
            error_results,
        )
    ]
    assert error_events[-1]["event"] == "tool_error"
    assert error_events[-1]["data"]["error"] == "Internal tool failure"
    assert run.consecutive_errors == 1


@pytest.mark.no_network
async def test_executor_loop_timeout_keeps_run_scoped_tool_references(
    monkeypatch,
):
    run = make_run(limits=AgentRunLimits(tool_timeout=0.01))
    run.short_uuid_references["entity_2"] = "run-actual-2"
    tools = SimpleNamespace(short_uuid_references={"entity_1": "actual-1"})
    executor = AgentExecutor(run, ScriptedLLM([]), tools)
    assert tools.short_uuid_references is run.short_uuid_references

    async def slow_execute(*_args):
        await asyncio.sleep(1)
        return {"data": []}

    monkeypatch.setattr("core.agent.executor.execute_tool", slow_execute)

    events = [
        event
        async for event in executor._execute_tools(
            [ToolCall("search_knowledge_messages", {"query": "slow"}, call_id="slow")],
            [],
        )
    ]

    assert events[-1]["event"] == "tool_error"
    assert "timed out" in events[-1]["data"]["error"]
    assert run.consecutive_errors == 1
    assert tools.short_uuid_references == {"entity_2": "run-actual-2"}


@pytest.mark.no_network
async def test_executor_cancellation_keeps_run_scoped_tool_references(monkeypatch):
    started = asyncio.Event()
    run = make_run()
    run.short_uuid_references["entity_2"] = "run-actual-2"
    tools = SimpleNamespace(short_uuid_references={"entity_1": "actual-1"})
    executor = AgentExecutor(run, ScriptedLLM([]), tools)
    assert tools.short_uuid_references is run.short_uuid_references

    async def blocking_execute(*_args):
        started.set()
        await asyncio.Event().wait()

    async def consume():
        return [
            event
            async for event in executor._execute_tools(
                [ToolCall("search_knowledge_messages", {"query": "cancel"}, call_id="cancel")],
                [],
            )
        ]

    monkeypatch.setattr("core.agent.executor.execute_tool", blocking_execute)
    task = asyncio.create_task(consume())
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert tools.short_uuid_references == {"entity_2": "run-actual-2"}
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
async def test_executor_budget_exhaustion_is_terminal_without_step_retries():
    class BudgetExhaustedLLM(ScriptedLLM):
        async def stream_with_tools(self, **kwargs):
            self.calls.append(kwargs)
            raise LLMBudgetExceededError("private budget details")
            yield  # pragma: no cover

    llm = BudgetExhaustedLLM([])
    run = make_run(limits=AgentRunLimits(max_attempts=3, max_consecutive_errors=3))
    executor = AgentExecutor(
        run,
        llm,
        SimpleNamespace(document_service=None),
    )

    events = [event async for event in executor.execute()]

    assert len(llm.calls) == 1
    assert events[-1] == {
        "event": "error",
        "data": {
            "message": "The agent couldn't complete this request. Please try again.",
            "code": "llm_budget_exhausted",
            "retryable": False,
        },
    }
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


@pytest.mark.no_network
async def test_parallel_safe_reads_overlap_and_preserve_request_order(monkeypatch):
    run = make_run(limits=AgentRunLimits(max_attempts=1, max_calls=4))
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace(document_service=None))
    entered = 0
    both_entered = asyncio.Event()
    release = asyncio.Event()

    async def controlled_read(_tools, name, args):
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        await release.wait()
        return {"data": [{"id": args["query"], "message": name}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", controlled_read)
    results = []
    events = []

    async def consume():
        async for event in executor._execute_tools(
            [
                ToolCall("search_knowledge_messages", {"query": "first"}, call_id="first-call"),
                ToolCall("search_knowledge_messages", {"query": "second"}, call_id="second-call"),
            ],
            results,
        ):
            events.append(event)

    task = asyncio.create_task(consume())
    await asyncio.wait_for(both_entered.wait(), timeout=1)
    release.set()
    await task

    assert [event["data"]["call_id"] for event in events] == [
        "first-call",
        "second-call",
        "first-call",
        "second-call",
    ]
    assert [result["result"]["data"][0]["id"] for result in results] == [
        "first",
        "second",
    ]


@pytest.mark.no_network
async def test_one_parallel_read_failure_keeps_sibling_success(monkeypatch):
    run = make_run(limits=AgentRunLimits(max_attempts=1, max_calls=4))
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace(document_service=None))

    async def mixed_result(_tools, _name, args):
        if args["query"] == "broken":
            raise ToolExecutionError("search_knowledge_messages", "one read failed")
        return {"data": [{"id": "kept", "message": "usable"}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", mixed_result)
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall("search_knowledge_messages", {"query": "broken"}, call_id="bad"),
                ToolCall("search_knowledge_messages", {"query": "working"}, call_id="good"),
            ],
            results,
        )
    ]

    assert [event["event"] for event in events] == [
        "tool_start",
        "tool_start",
        "tool_error",
        "tool_end",
    ]
    assert results[0]["error"] == (
        "Tool 'search_knowledge_messages' failed: one read failed"
    )
    assert results[1]["result"]["data"][0]["id"] == "kept"


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("failure", "expected_error", "expected_code", "retryable"),
    [
        (TimeoutError(), "Tool execution timed out", "tool_failed", True),
        (RuntimeError("private failure"), "Internal tool failure", "tool_failed", False),
        (
            WorkspaceConflictError("stale hash"),
            "Workspace changed before the operation could be applied",
            "workspace_conflict",
            False,
        ),
    ],
)
async def test_parallel_read_normalizes_timeout_and_internal_failures(
    monkeypatch, failure, expected_error, expected_code, retryable
):
    run = make_run(limits=AgentRunLimits(max_attempts=1, max_calls=4))
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace(document_service=None))

    async def mixed_result(_tools, _name, args):
        if args["query"] == "broken":
            raise failure
        return {"data": [{"id": "kept", "message": "usable"}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", mixed_result)
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall("search_knowledge_messages", {"query": "broken"}, call_id="bad"),
                ToolCall("search_knowledge_messages", {"query": "working"}, call_id="good"),
            ],
            results,
        )
    ]

    assert events[2]["event"] == "tool_error"
    assert events[2]["data"]["error"].startswith(expected_error)
    assert events[2]["data"]["code"] == expected_code
    assert events[2]["data"]["retryable"] is retryable
    assert results[1]["result"]["data"][0]["id"] == "kept"


@pytest.mark.no_network
async def test_parallel_local_reference_failure_emits_the_same_diagnostic(monkeypatch):
    run = make_run(limits=AgentRunLimits(max_attempts=1, max_calls=4))
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace(document_service=None))
    diagnostics = []

    async def mixed_result(_tools, _name, args):
        if args["query"] == "broken":
            raise ToolExecutionError(
                "search_knowledge_messages",
                "Unknown local ID 'message_9'",
            )
        return {"data": [{"id": "kept", "message": "usable"}]}

    async def capture_emit(*args, **kwargs):
        diagnostics.append((args, kwargs))

    monkeypatch.setattr("core.agent.executor.execute_tool", mixed_result)
    monkeypatch.setattr("core.agent.executor.emit", capture_emit)
    events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall("search_knowledge_messages", {"query": "broken"}, call_id="bad"),
                ToolCall("search_knowledge_messages", {"query": "working"}, call_id="good"),
            ],
            [],
        )
    ]

    assert events[2]["data"]["code"] == "tool_failed"
    assert diagnostics[0][0][2] == "local_reference_resolution_failed"
    assert diagnostics[0][0][3]["reference_type"] == "tool_argument"


@pytest.mark.no_network
async def test_parallel_read_reports_notebook_capacity_rejection(monkeypatch):
    run = make_run(
        limits=AgentRunLimits(
            max_attempts=1,
            max_calls=4,
            max_accumulated_messages=1,
        )
    )
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace(document_service=None))

    async def result_for_query(_tools, name, args):
        return {"data": [{"id": args["query"], "message": name}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", result_for_query)
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall("search_knowledge_messages", {"query": "first"}, call_id="first"),
                ToolCall("search_knowledge_messages", {"query": "second"}, call_id="second"),
            ],
            results,
        )
    ]

    assert events[-1]["event"] == "tool_end"
    assert "exceeds the evidence capacity" in events[-1]["data"]["result"]
    admission = results[-1]["result"]["notebook_admission"]
    assert admission["accepted"] is False
    assert admission["reason"] == "capacity"
    assert [item["id"] for item in run.notebook.section_items("messages")] == [
        "first"
    ]


@pytest.mark.no_network
async def test_parallel_batch_cancellation_awaits_every_child(monkeypatch):
    run = make_run(limits=AgentRunLimits(max_attempts=1, max_calls=4))
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace(document_service=None))
    entered = asyncio.Event()
    active = 0
    finished = 0

    async def blocked_read(_tools, _name, _args):
        nonlocal active, finished
        active += 1
        if active == 2:
            entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finished += 1

    monkeypatch.setattr("core.agent.executor.execute_tool", blocked_read)

    async def consume():
        async for _ in executor._execute_tools(
            [
                ToolCall("search_knowledge_messages", {"query": "first"}, call_id="first"),
                ToolCall("search_knowledge_messages", {"query": "second"}, call_id="second"),
            ],
            [],
        ):
            pass

    task = asyncio.create_task(consume())
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert finished == 2


@pytest.mark.no_network
async def test_parallel_batch_propagates_process_level_failures_and_cleans_up(
    monkeypatch,
):
    class FatalToolFailure(BaseException):
        pass

    run = make_run(limits=AgentRunLimits(max_attempts=1, max_calls=4))
    executor = AgentExecutor(run, ScriptedLLM([]), SimpleNamespace(document_service=None))
    sibling_started = asyncio.Event()
    release_failure = asyncio.Event()
    sibling_finished = False

    async def fatal_and_blocked(_tools, _name, args):
        nonlocal sibling_finished
        if args["query"] == "fatal":
            await sibling_started.wait()
            await release_failure.wait()
            raise FatalToolFailure()
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            sibling_finished = True

    monkeypatch.setattr("core.agent.executor.execute_tool", fatal_and_blocked)

    async def consume():
        async for _ in executor._execute_tools(
            [
                ToolCall("search_knowledge_messages", {"query": "fatal"}, call_id="fatal"),
                ToolCall("search_knowledge_messages", {"query": "blocked"}, call_id="blocked"),
            ],
            [],
        ):
            pass

    task = asyncio.create_task(consume())
    await asyncio.wait_for(sibling_started.wait(), timeout=1)
    release_failure.set()
    with pytest.raises(FatalToolFailure):
        await task

    assert sibling_finished is True
