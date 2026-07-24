import json
from types import SimpleNamespace

import pytest

from core.agent.executor import AgentExecutor
from core.agent.internals import (
    build_user_message,
    execute_tool,
    localize_agent_tool_result,
    resolve_agent_tool_arguments,
    update_accumulators,
)
from core.agent.types import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
)


def make_ctx() -> AgentContext:
    return AgentContext(
        config=AgentRunConfig(),
        state=AgentState(),
        evidence=RetrievedEvidence(),
        user_name="ada",
        user_query="What changed?",
        session_id="session-1",
        project_id="project-1",
        run_id="run-1",
    )


class RecordingEpisodeReader:
    def __init__(self, short_uuid_references):
        self.short_uuid_references = short_uuid_references
        self.calls = []

    async def read_episode(self, episode_id):
        self.calls.append(episode_id)
        return [{"id": episode_id, "message": "expanded source"}]


@pytest.mark.no_network
def test_episode_results_use_compact_uuid_handles_and_keep_numeric_evidence():
    ctx = make_ctx()
    raw_episode_id = "3d1e54e8-5f56-4c24-8da8-761e7b6c734b"
    result = localize_agent_tool_result(
        ctx,
        "episode_check",
        {
            "data": {
                "resolution": "exact",
                "results": [
                    {
                        "entity_name": "Ada",
                        "episodes": [
                            {
                                "episode_id": raw_episode_id,
                                "summary": "Ada selected the compact UUID design.",
                                "entities": [{"entity_id": 17}],
                                "relationships": [{"relationship_id": "rel-1"}],
                                "evidence": [
                                    {
                                        "message_id": 83,
                                        "content": "Use short UUID handles.",
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        },
    )

    episode = result["data"]["results"][0]["episodes"][0]
    serialized = json.dumps(result)
    assert episode["episode_id"] == "ep_3d1e54"
    assert episode["entities"] == [{"entity_id": 17}]
    assert episode["evidence"][0]["message_id"] == 83
    assert raw_episode_id not in serialized
    assert '"relationship_id"' not in serialized

    update_accumulators(ctx, "episode_check", result)
    prompt = build_user_message(ctx, [{"tool": "episode_check", "result": result}])
    assert "[ep_3d1e54] Ada selected the compact UUID design." in prompt
    assert raw_episode_id not in prompt
    assert "focus entities:" not in prompt
    assert "evidence MSG_" not in prompt


@pytest.mark.no_network
def test_document_and_folder_handles_resolve_only_within_the_active_context():
    ctx = make_ctx()
    raw_document_id = "a574d8f7-d997-4e8a-a557-6ec4c8451a55"
    raw_folder_id = "ba78b2f4-5d5b-4a21-af2d-8a9633761c54"
    result = localize_agent_tool_result(
        ctx,
        "list_documents",
        {
            "data": [
                {
                    "document_id": raw_document_id,
                    "folder_root_id": raw_folder_id,
                    "project_id": "project-1",
                    "session_id": "session-1",
                    "relative_path": "plans/local-references.md",
                }
            ]
        },
    )

    item = result["data"][0]
    assert item["document_id"] == "doc_a574d8"
    assert item["folder_root_id"] == "folder_ba78b2"
    assert "project_id" not in item
    assert "session_id" not in item

    prompt = build_user_message(ctx, [{"tool": "list_documents", "result": result}])
    assert '"document_id": "doc_a574d8"' in prompt
    assert '"folder_root_id": "folder_ba78b2"' in prompt
    assert raw_document_id not in prompt
    assert raw_folder_id not in prompt

    tools = SimpleNamespace(short_uuid_references=ctx.state.short_uuid_references)
    assert resolve_agent_tool_arguments(
        tools,
        "read_document",
        {"document_id": "doc_a574d8", "start_line": 1},
    ) == {"document_id": raw_document_id, "start_line": 1}
    assert resolve_agent_tool_arguments(
        tools,
        "list_folder_tree",
        {"folder_root_id": "folder_ba78b2"},
    ) == {"folder_root_id": raw_folder_id}

    with pytest.raises(ValueError, match="Expected a doc_ UUID handle"):
        resolve_agent_tool_arguments(
            tools,
            "read_document",
            {"document_id": "folder_ba78b2"},
        )

    ctx.state.clear_short_uuid_references()
    with pytest.raises(ValueError, match="Unknown local ID 'doc_a574d8'"):
        resolve_agent_tool_arguments(
            tools,
            "read_document",
            {"document_id": "doc_a574d8"},
        )


@pytest.mark.no_network
async def test_tool_dispatch_resolves_episode_handle_after_schema_validation():
    ctx = make_ctx()
    raw_episode_id = "82fcbe9a-2a2f-48cf-a934-0c38e4b2f1d1"
    localize_agent_tool_result(
        ctx,
        "episode_check",
        {"data": {"results": [{"episodes": [{"episode_id": raw_episode_id}]}]}},
    )
    tools = RecordingEpisodeReader(ctx.state.short_uuid_references)

    result = await execute_tool(
        tools,
        "read_episode",
        {"episode_id": "ep_82fcbe"},
    )

    assert tools.calls == [raw_episode_id]
    assert result == {
        "data": [{"id": raw_episode_id, "message": "expanded source"}]
    }


@pytest.mark.no_network
def test_merge_candidates_keep_numeric_ids_and_resolve_episode_handles():
    ctx = make_ctx()
    raw_episode_id = "23b9e8a1-1d8a-4720-a7a0-4d051278724d"
    result = localize_agent_tool_result(
        ctx,
        "check_graph_health",
        {
            "data": {
                "suggestions": [
                    {
                        "primary_id": 9,
                        "secondary_id": 3,
                        "evidence": [
                            {"message_id": 44, "session_id": "session-1"},
                            {"episode_id": raw_episode_id},
                        ],
                    }
                ]
            }
        },
    )

    suggestion = result["data"]["suggestions"][0]
    assert suggestion["primary_id"] == 9
    assert suggestion["secondary_id"] == 3
    assert suggestion["evidence"] == [
        {"message_id": 44},
        {"episode_id": "ep_23b9e8"},
    ]

    tools = SimpleNamespace(short_uuid_references=ctx.state.short_uuid_references)
    assert resolve_agent_tool_arguments(
        tools,
        "propose_entity_merge",
        {
            "primary_id": 9,
            "duplicate_id": 3,
            "evidence_message_ids": [44],
            "evidence_episode_ids": ["ep_23b9e8"],
        },
    ) == {
        "primary_id": 9,
        "duplicate_id": 3,
        "evidence_message_ids": [44],
        "evidence_episode_ids": [raw_episode_id],
    }

    with pytest.raises(ValueError, match="Duplicate local ep references"):
        resolve_agent_tool_arguments(
            tools,
            "propose_entity_merge",
            {"evidence_episode_ids": ["ep_23b9e8", "ep_23b9e8"]},
        )


@pytest.mark.no_network
def test_compact_uuid_handles_do_not_cross_agent_runs():
    first_ctx = make_ctx()
    localize_agent_tool_result(
        first_ctx,
        "episode_check",
        {
                "data": {
                    "results": [
                    {
                        "episodes": [
                            {"episode_id": "f9036df8-5555-4444-8888-555555555555"}
                        ]
                    }
                ]
            }
        },
    )

    second_tools = SimpleNamespace(
        short_uuid_references=make_ctx().state.short_uuid_references
    )
    with pytest.raises(ValueError, match="Unknown local ID 'ep_f9036d'"):
        resolve_agent_tool_arguments(
            second_tools,
            "read_episode",
            {"episode_id": "ep_f9036d"},
        )


@pytest.mark.no_network
async def test_agent_execution_cleanup_discards_short_uuid_references():
    ctx = make_ctx()
    ctx.config = AgentRunConfig(max_attempts=0)
    localize_agent_tool_result(
        ctx,
        "episode_check",
        {
            "data": {
                "results": [
                    {
                        "episodes": [
                            {"episode_id": "e5da1be0-4444-4444-8888-444444444444"}
                        ]
                    }
                ]
            }
        },
    )
    executor = AgentExecutor(
        ctx,
        llm=object(),
        tools=SimpleNamespace(document_service=None),
    )

    events = [event async for event in executor.execute()]

    assert events[-1]["event"] == "clarification"
    assert ctx.state.short_uuid_references == {}
