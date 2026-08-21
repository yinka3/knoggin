from types import SimpleNamespace

import pytest

from common.schema.agent.identity import AgentConfig
from core.agent.executor import AgentExecutor
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits


def make_agent_config() -> AgentConfig:
    return AgentConfig(
        id="agent-1",
        name="Researcher",
        persona={
            "attention_bias": "evidence",
            "reasoning_style": "methodical",
            "social_temperament": "calm",
            "communication_signature": "clear",
            "productive_flaw": "overexplains",
        },
    )


def make_run(**overrides) -> AgentRun:
    values = {
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-1",
        "user_query": "What changed?",
        "agent": AgentIdentity(
            config=make_agent_config(),
            name="Researcher",
            persona="Careful and evidence-led",
        ),
        "limits": AgentRunLimits(max_calls=2, max_attempts=2),
        "run_id": "run-1",
    }
    values.update(overrides)
    return AgentRun.open(**values)


@pytest.mark.no_network
def test_agent_run_owns_scope_limits_identity_and_effective_policy():
    run = make_run(
        model="run-model",
        temperature=0.2,
        brain="Use evidence.",
        directives="Be concise.",
        enabled_tools=["search_messages"],
        additional_tool_schemas=[
            {
                "type": "function",
                "function": {
                    "name": "community_tool",
                    "capability": "read",
                    "parameters": {"type": "object"},
                },
            }
        ],
    )

    assert run.user_name == "ada"
    assert run.project_id == "project-1"
    assert run.session_id == "session-1"
    assert run.limits.max_calls == 2
    assert run.agent.config.id == "agent-1"
    assert run.model == "run-model"
    assert run.temperature == 0.2
    assert run.brain == "Use evidence."
    assert run.directives == "Be concise."
    assert run.enabled_tools == ("search_messages",)
    assert run.additional_tool_schemas == (
        {
            "type": "function",
            "function": {
                "name": "community_tool",
                "capability": "read",
                "parameters": {"type": "object"},
            },
        },
    )
    assert run.tool_runtime.permissions.allowed_tools >= {
        "search_messages",
        "request_clarification",
        "submit_answer",
        "community_tool",
    }
    with pytest.raises(AttributeError):
        run.limits.max_calls = 4


@pytest.mark.no_network
def test_agent_run_enforces_attempt_and_tool_call_invariants():
    run = make_run()

    assert run.begin_attempt() is True
    assert run.begin_attempt() is True
    assert run.begin_attempt() is False
    assert run.can_call_tool("search_messages", {"query": "Ada"})

    run.record_tool_call("search_messages", {"query": "Ada"})

    assert run.call_count == 1
    assert not run.can_call_tool("search_messages", {"query": "Ada"})
    with pytest.raises(ValueError, match="not permitted"):
        run.record_tool_call("search_messages", {"query": "Ada"})


@pytest.mark.no_network
def test_agent_run_snapshots_tool_runtime_once_at_construction():
    run = make_run(enabled_tools=["search_messages"])
    runtime = run.tool_runtime

    run.enabled_tools = ("search_entity",)
    run.additional_tool_schemas = ()

    assert run.tool_runtime is runtime
    assert runtime.permissions.allowed_tools >= {
        "search_messages",
        "request_clarification",
        "submit_answer",
    }
    assert "search_entity" not in runtime.permissions.allowed_tools


@pytest.mark.no_network
def test_agent_run_records_runtime_diagnostics_and_releases_handles():
    run = make_run()

    run.record_error("temporary failure")
    run.record_usage({"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5})
    run.short_uuid_references["e1"] = "entity-1"

    assert run.consecutive_errors == 1
    assert run.usage["total_tokens"] == 5

    run.release()

    assert run.released is True
    assert run.short_uuid_references == {}
    with pytest.raises(RuntimeError, match="released"):
        run.record_error("should fail")


@pytest.mark.no_network
def test_agent_run_finalization_seals_direct_state_and_rejects_more_work():
    run = make_run()
    run.record_tool_result({"messages": [{"id": "message-1"}]})

    run.finalize("A concise answer.")

    assert run.final_content == "A concise answer."
    assert run.sealed is True
    with pytest.raises(RuntimeError, match="finalized"):
        run.record_error("too late")


class CompletingLLM:
    agent_model = "architect"
    extraction_model = "librarian"

    async def stream_with_tools(self, **_kwargs):
        yield {
            "event": "tool_calls",
            "data": {
                "content": "I have the answer.",
                "calls": [
                    {
                        "name": "submit_answer",
                        "arguments": '{"content": "Done"}',
                        "id": "submit-1",
                    }
                ],
            },
        }
        yield {
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
async def test_executor_finalizes_an_agent_run():
    run = make_run()
    executor = AgentExecutor(
        run,
        CompletingLLM(),
        SimpleNamespace(document_service=None),
    )

    events = [event async for event in executor.execute()]

    assert events[0]["event"] == "response"
    assert events[0]["data"]["content"] == "Done"
    assert run.final_content == "Done"
    assert run.sealed is True
    assert run.released is True
    # The executor performs a dedicated final synthesis after the first
    # structured answer, so both model turns contribute usage.
    assert run.usage["total_tokens"] == 10


@pytest.mark.no_network
def test_agent_run_can_finish_without_a_final_response():
    run = make_run()

    run.finish_without_response()

    assert run.sealed is True
    assert run.final_content is None
    with pytest.raises(RuntimeError, match="finalized"):
        run.record_error("too late")
