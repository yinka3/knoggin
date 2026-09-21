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
from core.agent.sources.pasted_text import build_pasted_text_candidates


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
@pytest.mark.parametrize(
    "user_query",
    [
        "Hey",
        "nice",
        "Nice, go to the next one",
        "go to the next one",
    ],
)
def test_adaptive_briefing_uses_a_narrow_conversational_fast_path(user_query):
    run = make_run(user_query=user_query)

    assert run.project_briefing.mode == "adaptive"
    assert run.project_briefing.initial_reason is None
    assert run.project_briefing.needs_load is False


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("overrides", "expected_reason"),
    [
        (
            {"user_query": "What did we decide about the ingestion project?"},
            "explicit_project_memory_intent",
        ),
        (
            {"user_query": "Explain this architecture."},
            "conservative_default",
        ),
        (
            {"research_profile": resolve_research_profile("research")},
            "research_mode",
        ),
        (
            {"document_selection_context": {"excerpt": "selected passage"}},
            "document_selection",
        ),
        (
            {"limits": AgentRunLimits(project_briefing_mode="always")},
            "always",
        ),
    ],
)
def test_agent_run_freezes_observable_briefing_signals(
    overrides,
    expected_reason,
):
    run = make_run(**overrides)

    assert run.project_briefing.initial_reason == expected_reason
    assert run.project_briefing.requested_reason == expected_reason
    assert run.project_briefing.needs_load is True


@pytest.mark.no_network
def test_adaptive_briefing_allows_one_deferred_transition_and_caches_content():
    run = make_run(user_query="hey")

    assert run.request_project_briefing_transition() is True
    assert run.request_project_briefing_transition() is False
    assert run.project_briefing.transition_count == 1
    assert run.project_briefing.requested_reason == "tool_followup"

    run.record_project_briefing_loaded(
        brief="Project Brief",
        context="Project Context",
        documents_context="- project-notes.md (2KB, 3 chunks)",
        content_token_count=9,
    )
    run.record_project_briefing_loaded(
        brief="later brief",
        context="later context",
        content_token_count=8,
    )

    assert run.project_briefing.loaded is True
    assert run.project_briefing.load_count == 1
    assert run.project_briefing.brief == "Project Brief"
    assert run.project_briefing.context == "Project Context"
    assert run.project_briefing.documents_context == "- project-notes.md (2KB, 3 chunks)"
    assert run.project_briefing.content_token_count == 9

    run.release()

    assert run.project_briefing.brief == ""
    assert run.project_briefing.context == ""
    assert run.project_briefing.documents_context == ""


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
def test_agent_run_distinguishes_grounded_evidence_from_actions_and_validates_input():
    run = make_run()

    run.notebook.apply("edit_brain", {"data": {"success": True}})

    assert run.has_any() is True
    assert run.has_grounded_investigation_evidence() is False

    run.notebook.apply(
        "search_messages",
        {"data": [{"id": "message-1", "message": "Grounded evidence."}]},
    )

    assert run.has_grounded_investigation_evidence() is True

    pasted_candidates = build_pasted_text_candidates(
        project_id="project-1",
        session_id="session-1",
        source_message_id=1,
        message_content="Use this:\n```text\nSource-backed detail.\n```",
        agent_run_id="run-2",
    )
    sourced_run = make_run(
        run_id="run-2",
        initial_source_candidates=pasted_candidates,
    )

    assert sourced_run.has_grounded_investigation_evidence() is True
    with pytest.raises(TypeError, match="validated source references"):
        make_run(initial_source_candidates=[object()])


@pytest.mark.no_network
def test_agent_run_requires_read_content_after_document_or_web_discovery():
    run = make_run()

    run.notebook.apply(
        "list_documents",
        {
            "data": [
                {
                    "document_id": "document-1",
                    "document_name": "brief.md",
                }
            ]
        },
    )
    run.notebook.apply(
        "web_search",
        {
            "data": [
                {
                    "title": "Release notes",
                    "url": "https://example.test/release-notes",
                    "snippet": "A promising discovery snippet.",
                }
            ]
        },
    )

    assert run.has_any() is True
    assert run.has_grounded_investigation_evidence() is False

    run.notebook.apply(
        "read_document",
        {
            "data": [
                {
                    "document_id": "document-1",
                    "document_name": "brief.md",
                    "content": "The read passage supports the answer.",
                }
            ]
        },
    )

    assert run.has_grounded_investigation_evidence() is True


@pytest.mark.no_network
def test_research_coverage_requires_each_material_part_to_have_evidence_or_a_gap():
    run = make_run(research_profile=resolve_research_profile("research"))
    applied = run.accumulate_tool_result(
        "search_messages",
        {"data": [{"id": "message-1", "message": "Grounded evidence."}]},
    )
    reference = applied.references[0]

    assert run.validate_research_coverage(
        [
            {
                "subquestion": "What changed?",
                "supporting_references": [reference],
            },
            {
                "subquestion": "Why did it change?",
                "supporting_references": [],
                "unresolved_gap": "The available evidence does not explain why.",
            },
        ]
    ) is None
    assert "needs evidence or an unresolved gap" in run.validate_research_coverage(
        [
            {
                "subquestion": "Why did it change?",
                "supporting_references": [],
            }
        ]
    )


@pytest.mark.no_network
def test_research_coverage_rejects_discovery_only_and_unknown_references():
    run = make_run(research_profile=resolve_research_profile("research"))
    discovery = run.accumulate_tool_result(
        "web_search",
        {"data": [{"title": "Result", "url": "https://example.test", "snippet": "Lead"}]},
    ).references[0]

    for reference in (discovery, "message:missing"):
        error = run.validate_research_coverage(
            [
                {
                    "subquestion": "What changed?",
                    "supporting_references": [reference],
                }
            ]
        )
        assert "unknown or discovery-only" in error


@pytest.mark.no_network
def test_cosmetically_repeated_empty_query_forces_early_replan():
    run = make_run(
        limits=AgentRunLimits(empty_result_replan_threshold=3),
    )

    assert run.record_empty_result([("search_messages", {"query": "Project Alpha?"})]) is False
    assert run.record_empty_result([("search_messages", {"query": " project   alpha "})]) is True


@pytest.mark.no_network
def test_deep_research_gap_review_is_due_once_after_grounded_evidence():
    run = make_run(research_profile=resolve_research_profile("deep_research"))

    assert run.needs_deep_research_gap_review() is False
    run.notebook.apply(
        "search_messages",
        {"data": [{"id": "message-1", "message": "Grounded evidence."}]},
    )

    assert run.needs_deep_research_gap_review() is True
    assert run.begin_deep_research_gap_review() is True
    assert run.has_completed_deep_research_gap_review() is True
    assert run.deep_research_gap_review_count == 1
    assert run.needs_deep_research_gap_review() is False
    assert run.begin_deep_research_gap_review() is False


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
                        "arguments": (
                            '{"content": "Done", "research_coverage": ['
                            '{"subquestion": "What changed?", '
                            '"supporting_references": [], '
                            '"unresolved_gap": "No notebook evidence was needed."}]}'
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
        initial_source_candidates=build_pasted_text_candidates(
            project_id="project-1",
            session_id="session-1",
            source_message_id=1,
            message_content="Use this:\n```text\nGrounded evidence.\n```",
            agent_run_id="run-1",
        ),
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
    assert run.deep_research_gap_review_count == 1


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
