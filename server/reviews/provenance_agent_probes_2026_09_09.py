"""Historical provenance and agent review probes.

Each assertion describes a defect observed at acc0cac. The file remains
outside normal test discovery; PA1–PA7 are skipped historical records because
normal regressions now assert their desired behavior.
"""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from common.schema.agent.research import DEFAULT_RESEARCH_PROFILES
from core.agent.executor import AgentExecutor
from core.agent.executor import _ToolCall as ToolCall
from core.agent.prompt_context import build_user_message
from core.knowledge.db.writers.source_reference_writer import SourceReferenceWriter
from core.knowledge.retrieval import KnowledgeRetrieval
from tests.contract.storage.test_source_reference_storage_contract import (
    DOCUMENT_ID,
    _seed_scope,
    document_candidate,
)
from tests.unit.core.agent.test_executor_loop_contract import (
    ScriptedLLM,
    completed_event,
    make_run,
    tool_call_event,
)

pytest_plugins = ["tests.contract.storage.conftest"]


def step(name, args, identifier):
    return [tool_call_event(name, args, identifier), completed_event()]


@pytest.mark.skip(reason="PA1 is covered by the canonical notebook prompt regression.")
def test_episode_payload_is_retained_but_missing_from_model_prompt():
    run = make_run()
    result = {
        "data": {
            "resolution": "exact",
            "results": [
                {
                    "entity_name": "Knoggin",
                    "episodes": [
                        {
                            "episode_id": "episode-123",
                            "summary": "The durable launch phrase is violet.",
                            "sources_consulted": [
                                {"excerpt": "The launch phrase is violet."}
                            ],
                        }
                    ],
                }
            ],
        }
    }
    run.accumulate_tool_result("episode_check", result)
    assert run.notebook.section_items("episodes")[0]["summary"]
    prompt = build_user_message(run, [{"tool": "episode_check", "result": result}])
    assert "Found 1 episode(s)" in prompt
    assert "No contextual episodes recorded" in prompt
    assert "episode-123" not in prompt
    assert "violet" not in prompt


@pytest.mark.no_network
@pytest.mark.skip(
    reason="PA2 is covered by the final synthesis accumulated-evidence regression."
)
async def test_earlier_fact_disappears_from_final_synthesis_prompt(monkeypatch):
    run = make_run()
    llm = ScriptedLLM(
        [
            step("search_messages", '{"query":"launch"}', "one"),
            step("search_messages", '{"query":"owner"}', "two"),
            step("submit_answer", '{"content":"Draft."}', "draft"),
            step("submit_answer", '{"content":"Final."}', "final"),
        ]
    )

    async def execute(_tools, _name, args):
        text = "LAUNCH_FACT_VIOLET" if args["query"] == "launch" else "OWNER_FACT_ADA"
        return {
            "data": [
                {
                    "id": text,
                    "message": text,
                    "score": 1.0,
                    "context": [{"role": "user", "content": text, "is_hit": True}],
                }
            ]
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", execute)
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    events = [event async for event in executor._execute_run()]
    assert events[-1]["event"] == "response"
    assert "LAUNCH_FACT_VIOLET" in llm.calls[1]["user"]
    assert "SYNTHESIZE" in llm.calls[-1]["system"]
    assert "OWNER_FACT_ADA" in llm.calls[-1]["user"]
    assert "LAUNCH_FACT_VIOLET" not in llm.calls[-1]["user"]
    assert len(run.notebook.section_items("messages")) == 2


@pytest.mark.no_network
@pytest.mark.skip(
    reason=(
        "Historical PA5 reproduction resolved by Stage 6; desired behavior is "
        "covered by the executor capacity-admission and source-candidate regressions"
    ),
)
async def test_capacity_rejection_is_reported_as_success_and_keeps_sources(monkeypatch):
    run = make_run()
    run.notebook.capacity = replace(run.notebook.capacity, max_documents=1)
    candidates = [document_candidate(result_position=i) for i in range(2)]
    result = {
        "data": [
            {
                "document_id": DOCUMENT_ID,
                "chunk_index": i,
                "content": f"DISTINCT_PASSAGE_{i}",
                "document_name": "report.pdf",
                "source_context": candidate.model_dump(mode="json"),
            }
            for i, candidate in enumerate(candidates)
        ]
    }

    async def execute(*_args):
        return result

    monkeypatch.setattr("core.agent.executor.execute_tool", execute)
    executor = AgentExecutor(
        run, ScriptedLLM([]), SimpleNamespace(document_service=None)
    )
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [ToolCall("search_documents", {"query": "passage"}, call_id="doc-search")],
            results,
        )
    ]
    assert not run.notebook.last_apply_result.accepted
    assert run.notebook.last_apply_result.reason == "capacity"
    assert run.notebook.section_items("documents") == ()
    assert len(run.source_candidates) == 2
    assert events[-1]["event"] == "tool_end"
    prompt = build_user_message(run, results)
    assert "Found 2 items" in prompt
    assert "DISTINCT_PASSAGE" not in prompt


@pytest.mark.no_network
@pytest.mark.skip(
    reason=(
        "Historical PA6 reproduction resolved by Stage 6; desired behavior is "
        "covered by SYNTHESIZE tool-allowlist and executor-reset regressions"
    ),
)
async def test_synthesis_dispatches_hidden_investigative_tool(monkeypatch):
    run = make_run()
    llm = ScriptedLLM(
        [
            step("search_messages", '{"query":"first"}', "one"),
            step("submit_answer", '{"content":"Draft."}', "draft"),
            step("search_messages", '{"query":"during synthesis"}', "hidden"),
            step("submit_answer", '{"content":"Final."}', "final"),
        ]
    )
    queries = []

    async def execute(_tools, _name, args):
        queries.append(args["query"])
        return {"data": [{"id": args["query"], "message": "Evidence", "score": 1.0}]}

    monkeypatch.setattr("core.agent.executor.execute_tool", execute)
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    [event async for event in executor._execute_run()]
    assert "SYNTHESIZE" in llm.calls[2]["system"]
    assert "search_messages" not in [
        s["function"]["name"] for s in llm.calls[2]["tools"]
    ]
    assert "during synthesis" in queries


@pytest.mark.no_network
@pytest.mark.skip(
    reason=(
        "Historical PA7 reproduction resolved by the research-grounding contract; "
        "desired behavior is covered by research readiness and fallback regressions"
    ),
)
async def test_deep_research_can_finish_without_investigation():
    run = make_run()
    run.research_profile = DEFAULT_RESEARCH_PROFILES["deep_research"]
    llm = ScriptedLLM(
        [step("submit_answer", '{"content":"Unsupported report."}', "answer")]
    )
    executor = AgentExecutor(run, llm, SimpleNamespace(document_service=None))
    events = [event async for event in executor._execute_run()]
    assert len(llm.calls) == 1
    assert run.call_count == 0
    assert events[-1]["event"] == "response"
    assert events[-1]["data"]["artifact"]["kind"] == "research_report"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
@pytest.mark.skip(
    reason=(
        "Historical PA4 reproduction resolved by a6396d5; desired behavior is "
        "covered by the Stage 4 source-reference and finalization regressions"
    ),
)
async def test_document_replaced_after_capture_rejects_provenance(real_postgres_client):
    await _seed_scope(real_postgres_client)
    captured = document_candidate()
    await real_postgres_client.execute(
        "UPDATE public.project_documents SET content_hash = %s WHERE document_id = %s",
        ("c" * 64, DOCUMENT_ID),
    )
    writer = SourceReferenceWriter(real_postgres_client)
    with pytest.raises(ValueError):
        await writer.write_for_assistant_message(
            101,
            [captured],
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
            readable_project_ids=["project-1"],
        )
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.message_source_refs"
    ) == {"count": 0}


@pytest.mark.no_network
@pytest.mark.skip(
    reason=(
        "Historical PA3 reference shape predates 8ea0136; GraphReader now "
        "emits typed observation references covered by Stage 4 graph regressions"
    ),
)
async def test_path_observation_references_are_discarded_by_message_hydration():
    # The exact keys emitted by GraphReader._relationship_observation_refs.
    observation_ref = {
        "project_id": "project-1",
        "user_name": "ada",
        "semantic_window_id": "window-1",
        "observation_id": 123,
    }
    retrieval = KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=object(),
        embedding_service=None,
        knowledge_store=object(),
        postgres=object(),
    )
    assert (
        retrieval._normalize_evidence_ref(observation_ref, session_id="session-1")
        is None
    )
    result = await retrieval._hydrate_result_evidence(
        [{"source": "Ada", "target": "Knoggin", "evidence_refs": [observation_ref]}],
        session_id="session-1",
    )
    assert result[0]["evidence"] == []
    assert "evidence_refs" not in result[0]
