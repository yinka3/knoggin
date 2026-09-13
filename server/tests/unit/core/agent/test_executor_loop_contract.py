import asyncio
from types import SimpleNamespace

import pytest

from common.exceptions import LLMProviderError
from common.schema.agent.identity import AgentConfig
from core.agent.executor import AgentExecutor
from core.agent.executor import _ToolCall as ToolCall
from core.agent.prompt_context import build_evidence_context
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
        if name == "search_messages":
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
                    "source": "Knoggin",
                    "target": "Profile",
                    "observed_relationship_label": "OWNER_FACT_ADA",
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
        "Relationships:\n- R1 Knoggin -> Profile: OWNER_FACT_ADA\n"
        in llm.calls[-1]["user"]
    )
    assert "Messages:\n- M1: LAUNCH_FACT_VIOLET" in llm.calls[-1]["user"]
    assert "observed evidence, not a current-state claim" in llm.calls[-1]["user"]
    assert run.attempt_count == 4
    assert run.call_count == 2
    assert run.notebook.section_items("messages") == (
        {"id": "message-1", "message": "LAUNCH_FACT_VIOLET", "score": 0.9},
    )
    assert run.notebook.section_items("relationships") == (
        {
            "source": "Knoggin",
            "target": "Profile",
            "observed_relationship_label": "OWNER_FACT_ADA",
        },
    )
    assert run.usage["total_tokens"] == 20
    assert run.sealed is True
    run.release()
    assert run.released is True


@pytest.mark.no_network
async def test_capacity_rejection_records_no_sources_and_allows_a_narrow_retry(
    monkeypatch,
):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "web_search",
                    '{"query": "wide release history"}',
                    "wide-1",
                ),
                completed_event(),
            ],
            [
                tool_call_event(
                    "web_search",
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
            "tool": "web_search",
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
        ("web_search", {"query": "wide release history"}),
        ("web_search", {"query": "narrow release history"}),
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
                tool_call_event("search_messages", '{"query": "missing"}', "search-1"),
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
async def test_executor_reserves_one_synthesis_attempt_after_normal_budget(
    monkeypatch,
):
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_messages",
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
                            "search_messages",
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
    assert run.tools_used == []
    assert len(llm.calls) == 2
    assert all("CURRENT EXECUTION PHASE: PLAN" in call["system"] for call in llm.calls)
    assert "Terminal protocol tools must be called alone." in llm.calls[1]["user"]
    assert secret not in llm.calls[1]["user"]


@pytest.mark.no_network
async def test_executor_rejects_hidden_synthesis_write_without_dispatch(monkeypatch):
    secret = "RAW_SENSITIVE_SYNTHESIS_VALUE"
    llm = ScriptedLLM(
        [
            [
                tool_call_event(
                    "search_messages",
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
                    "edit_brain",
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
        if name == "search_messages":
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
    ] == ["search_messages"]
    assert dispatched == [("search_messages", {"query": "profile", "limit": 3})]
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
                    "load_topic_context",
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
    assert "load_topic_context" in [
        schema["function"]["name"] for schema in llm.calls[0]["tools"]
    ]
    assert "CURRENT EXECUTION PHASE: SYNTHESIZE" in llm.calls[-1]["system"]
    messages = run.notebook.model_view()["messages"]
    assert messages[0]["id"] == "msg_7"
    assert messages[0]["context"][0]["content"] == ("The offer changes compensation.")


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
    assert [item["id"] for item in run.notebook.model_view()["messages"]] == [
        "one",
        "two",
    ]


@pytest.mark.no_network
async def test_fallback_summary_uses_all_canonical_evidence_categories():
    llm = ScriptedLLM([])
    run = make_run()
    run.notebook.apply(
        "episode_check",
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
        "find_path",
        {"data": [{"entity_a": "Ada", "entity_b": "Knoggin", "step": 0}]},
    )
    run.notebook.apply(
        "web_search",
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
async def test_compaction_token_count_matches_post_compaction_context(monkeypatch):
    llm = ScriptedLLM([])
    run = make_run()
    run.notebook.apply(
        "search_messages",
        {"data": [{"id": "m1", "message": "A retained message"}]},
    )
    run.notebook.apply(
        "search_entity",
        {"data": [{"id": "p1", "canonical_name": "Ada"}]},
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))

    async def summarize(_evidence):
        return "Condensed evidence."

    monkeypatch.setattr(executor, "_generate_evidence_summary", summarize)
    monkeypatch.setattr("core.agent.executor.MAX_TOKEN_CHUNK_SIZE", 1)

    await executor._manage_context_size()

    assert build_evidence_context(run) == run.notebook.render()
    assert run.evidence_token_count == llm.count_tokens(build_evidence_context(run))


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
            [ToolCall("search_messages", {"query": "slow"}, call_id="slow")],
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
