from types import SimpleNamespace

import pytest

from common.schema.agent.community_tools import AAC_SPECIFIC_SCHEMAS
from common.schema.agent.identity import AgentConfig
from common.schema.agent.research import resolve_research_profile
from core.agent.executor import AgentExecutor
from core.agent.run import (
    AAC_DIAGNOSTIC_PROJECT_ID,
    AgentIdentity,
    AgentRun,
    AgentRunLimits,
)


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
        enabled_tools=["search_messages"],
        additional_tool_schemas=[
            next(
                schema
                for schema in AAC_SPECIFIC_SCHEMAS
                if schema["function"]["name"] == "search_insights"
            )
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
    assert run.enabled_tools == ("search_messages",)
    assert run.additional_tool_schemas[0]["function"]["name"] == "search_insights"
    assert run.tool_runtime.permissions.allowed_tools >= {
        "search_messages",
        "request_clarification",
        "submit_answer",
        "search_insights",
    }
    with pytest.raises(AttributeError):
        run.limits.max_calls = 4


@pytest.mark.no_network
def test_agent_run_rejects_unregistered_additional_tool_schema():
    with pytest.raises(ValueError, match="no registered implementation"):
        make_run(
            additional_tool_schemas=[
                {
                    "type": "function",
                    "function": {
                        "name": "client_tool",
                        "capability": "read",
                        "parameters": {"type": "object"},
                    },
                }
            ]
        )


@pytest.mark.no_network
def test_research_profile_scales_existing_run_budget_without_new_executor():
    profile = resolve_research_profile("deep_research")
    limits = AgentRunLimits(
        max_calls=4,
        max_attempts=5,
        max_accumulated_web_discoveries=6,
        max_accumulated_web_reads=5,
        tool_limits=(("search_messages", 2),),
    )
    scaled = limits.for_research_profile(profile)
    run = make_run(
        limits=scaled,
        research_profile=profile,
    )

    assert run.research_profile.mode == "deep_research"
    assert run.limits.max_calls == 12
    assert run.limits.max_attempts == 15
    assert run.limits.max_accumulated_web_discoveries == 18
    assert run.limits.max_accumulated_web_reads == 15
    assert run.limits.get_tool_limit("search_messages") == 6


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
def test_agent_run_opens_aac_scope_without_a_durable_project_audit_owner():
    run = AgentRun.open_aac(
        user_name="ada",
        session_id="aac:discussion-1",
        user_query="Explore a disagreement.",
        agent=AgentIdentity(
            config=make_agent_config(),
            name="Researcher",
            persona="Careful and evidence-led",
        ),
        limits=AgentRunLimits(),
    )

    assert run.project_id == AAC_DIAGNOSTIC_PROJECT_ID
    assert run.tool_runtime.permissions.project_id == AAC_DIAGNOSTIC_PROJECT_ID
    assert run.tool_runtime.permissions.audit_project_id is None


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


class ArtifactCompletingLLM(CompletingLLM):
    async def stream_with_tools(self, **_kwargs):
        yield {
            "event": "tool_calls",
            "data": {
                "content": "I have the answer.",
                "calls": [
                    {
                        "name": "submit_answer",
                        "arguments": (
                            '{"content": "Done", "artifact": '
                            '{"kind": "general", "title": "Saved", '
                            '"blocks": [{"kind": "markdown", "content": "Saved"}]}}'
                        ),
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
    # A direct answer with no newly gathered evidence finalizes immediately.
    assert run.usage["total_tokens"] == 5


@pytest.mark.no_network
async def test_research_profile_supplies_default_report_artifact_at_synthesis():
    profile = resolve_research_profile("deep_research")
    run = make_run(
        limits=AgentRunLimits(max_calls=6, max_attempts=6),
        research_profile=profile,
    )
    executor = AgentExecutor(
        run,
        CompletingLLM(),
        SimpleNamespace(document_service=None),
    )

    events = [event async for event in executor.execute()]

    response = next(event for event in events if event["event"] == "response")
    assert response["data"]["research_mode"] == "deep_research"
    assert response["data"]["artifact"]["kind"] == "research_report"
    assert response["data"]["artifact"]["title"] == "Research report"


@pytest.mark.no_network
async def test_model_supplied_artifact_is_preserved_through_final_synthesis():
    run = make_run()
    executor = AgentExecutor(
        run,
        ArtifactCompletingLLM(),
        SimpleNamespace(document_service=None),
    )

    events = [event async for event in executor.execute()]

    response = next(event for event in events if event["event"] == "response")
    assert response["data"]["artifact"]["title"] == "Saved"


@pytest.mark.no_network
async def test_executor_records_only_successful_turn_completion():
    run = make_run()
    completed_agents = []

    async def record_completion(agent_id):
        completed_agents.append(agent_id)

    executor = AgentExecutor(
        run,
        CompletingLLM(),
        SimpleNamespace(document_service=None),
        on_successful_completion=record_completion,
    )

    _ = [event async for event in executor.execute()]

    assert completed_agents == ["agent-1"]


@pytest.mark.no_network
def test_agent_run_can_finish_without_a_final_response():
    run = make_run()

    run.finish_without_response()

    assert run.sealed is True
    assert run.final_content is None
    with pytest.raises(RuntimeError, match="finalized"):
        run.record_error("too late")
