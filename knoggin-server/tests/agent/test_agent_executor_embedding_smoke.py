import json

import pytest

from knoggin_server.agent.executor import AgentExecutor
from knoggin_server.agent.tools.search import SearchTools
from knoggin_server.agent.types import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
)
from tests.fixtures.fakes import FakeRedis
from tests.knowledge.test_retrieval_embedding_smoke import (
    cosine,
    load_local_embedding_service,
)


class FakeMemoryManager:
    def __init__(self):
        self.calls = []

    async def load_prompt_strings(self, hot_topics):
        self.calls.append(list(hot_topics))
        return (
            "[Testing]\n- Agent tests use fake LLMs and real local embeddings.",
            "Keep tool evidence grounded.",
            "Prefer subsystem-sized coverage.",
            "Do not hide retrieval failures.",
        )


class ScriptedLLM:
    agent_model = "architect-model"
    extraction_model = "librarian-model"

    def __init__(self):
        self.stream_calls = []

    def count_tokens(self, text):
        return len(text.split())

    async def call_llm(self, **_kwargs):
        raise AssertionError("embedding smoke should not call generation LLM")

    async def call_llm_with_tools_streaming(self, **kwargs):
        self.stream_calls.append(kwargs)
        turn = len(self.stream_calls)

        if turn == 1:
            yield {
                "type": "thinking",
                "content": "Search for agent/tool evidence first.",
            }
            yield {
                "type": "tool_calls",
                "calls": [
                    {
                        "id": "call-search-agent",
                        "name": "search_messages",
                        "arguments": json.dumps(
                            {
                                "query": (
                                    "AgentExecutor tool behavior prompt context "
                                    "active topics fake LLM real embedding retrieval"
                                ),
                                "limit": 5,
                            }
                        ),
                    }
                ],
            }
            yield {
                "type": "done",
                "usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 10,
                    "total_tokens": 110,
                },
            }
            return

        if turn == 2:
            yield {
                "type": "tool_calls",
                "calls": [
                    {
                        "id": "call-draft-answer",
                        "name": "submit_answer",
                        "arguments": json.dumps(
                            {"content": "draft based on retrieved evidence"}
                        ),
                    }
                ],
            }
            yield {
                "type": "done",
                "usage": {
                    "prompt_tokens": 80,
                    "completion_tokens": 8,
                    "total_tokens": 88,
                },
            }
            return

        yield {
            "type": "tool_calls",
            "calls": [
                {
                    "id": "call-final-answer",
                    "name": "submit_answer",
                    "arguments": json.dumps(
                        {
                            "content": (
                                "Agent/tool smoke final answer from retrieved "
                                "executor, tool behavior, and prompt context evidence."
                            )
                        }
                    ),
                }
            ],
        }
        yield {
            "type": "done",
            "usage": {
                "prompt_tokens": 70,
                "completion_tokens": 7,
                "total_tokens": 77,
            },
        }


class QuietExecutor(AgentExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.llm_calls = []

    async def _emit_llm_call(self, model, reasoning):
        self.llm_calls.append((model, reasoning, self.ctx.state.attempt_count))


class FakeGraphClient:
    def __init__(self, messages):
        self.messages = dict(messages)
        self.fts_calls = []

    async def search_messages_fts(
        self,
        query,
        limit,
        user_name,
        session_ids=None,
        project_ids=None,
    ):
        self.fts_calls.append(
            (
                query,
                limit,
                user_name,
                list(session_ids or []),
                list(project_ids or []),
            )
        )
        return [
            (6, 1.0, "session-agent"),
            (7, 0.95, "session-agent"),
            (1, 0.75, "session-agent"),
            (2, 0.70, "session-agent"),
            (3, 0.65, "session-agent"),
            (4, 0.60, "session-agent"),
            (5, 0.55, "session-agent"),
            (8, 0.50, "session-agent"),
            (9, 0.45, "session-agent"),
            (10, 0.40, "session-agent"),
        ][:limit]


def make_agent_context():
    return AgentContext(
        config=AgentRunConfig(max_attempts=4, max_calls=4),
        state=AgentState(),
        evidence=RetrievedEvidence(),
        user_name="ada",
        user_query=(
            "What did we decide about AgentExecutor tool behavior and prompt "
            "context testing?"
        ),
        session_id="session-agent",
        run_id="run-agent-smoke",
        hot_topics=["Testing"],
        active_topics=["Testing"],
        agent_name="STELLA",
        agent_persona="Careful test assistant",
    )


def make_agent_messages():
    return [
        (
            1,
            "AgentExecutor chunk 8 smoke testing should use a fake LLM but real "
            "embedding retrieval to search messages about tool behavior, prompt "
            "context, active topics, and final answer synthesis.",
        ),
        (
            2,
            "For AgentExecutor _step, test streaming tokens, thinking chunks, "
            "tool call JSON parsing, usage accounting, enabled tool schemas, and "
            "client tools.",
        ),
        (
            3,
            "Tool behavior coverage includes duplicate tool calls, malformed JSON "
            "arguments, per-tool limits, global call limits, ToolExecutionError, "
            "and unexpected failures.",
        ),
        (
            4,
            "Prompt context must include active hot topics, memory context, "
            "uploaded file context, community participants, and current Architect "
            "or Librarian mode.",
        ),
        (
            5,
            "When search_messages returns empty evidence twice, AgentExecutor "
            "should request replanning and escalate back to Architect.",
        ),
        (
            6,
            "The website hero layout needs better mobile spacing, calmer visual "
            "hierarchy, and direct product screenshots above the fold.",
        ),
        (
            7,
            "Scheduler heartbeat tests should inspect job leases, stale locks, "
            "retry windows, and background worker timing.",
        ),
        (
            8,
            "Community profile refinement should avoid over-updating stable user "
            "preferences from weak or one-off evidence.",
        ),
        (
            9,
            "Dinner plans for Friday include buying coffee filters, renewing the "
            "library book, and sending Maya the itinerary.",
        ),
        (
            10,
            "FileRAG folder upload will change later, so deep document chunking "
            "coverage should wait until the redesign settles.",
        ),
    ]


def make_search_tool(service, graph_client, messages):
    tool = SearchTools()
    tool.redis = FakeRedis()
    tool.graph_client = graph_client
    tool.embedding_service = service
    tool.search_cfg = {
        "fts_limit": 10,
        "rerank_candidates": 10,
        "default_message_limit": 5,
    }
    tool.file_rag = None
    tool.user_name = "ada"
    tool.session_id = "session-agent"
    tool.active_topics = ["Testing"]
    tool.readable_project_ids = None

    message_by_id = dict(messages)

    async def visible_session_ids():
        return ["session-agent"]

    async def hydrate(refs):
        return [
            {
                "session_id": ref["session_id"],
                "id": f"msg_{ref['message_id']}",
                "message": message_by_id[ref["message_id"]],
            }
            for ref in refs
        ]

    async def surrounding_context(msg_id, session_id=None):
        numerical_id = int(msg_id.split("_", 1)[1])
        return [
            {
                "id": msg_id,
                "role": "user",
                "content": message_by_id[numerical_id],
                "timestamp": "2026-04-05T10:30:00+00:00",
                "is_hit": True,
            }
        ]

    tool._get_visible_session_ids = visible_session_ids
    tool._hydrate_evidence = hydrate
    tool._get_surrounding_context = surrounding_context
    return tool


@pytest.mark.slow
@pytest.mark.no_network
async def test_real_embedding_agent_loop_retrieves_agent_tool_context():
    service = await load_local_embedding_service()
    messages = make_agent_messages()
    graph = FakeGraphClient(messages)
    tools = make_search_tool(service, graph, messages)
    memory = FakeMemoryManager()
    llm = ScriptedLLM()
    executor = QuietExecutor(make_agent_context(), llm, tools, memory)

    query = (
        "AgentExecutor tool behavior prompt context active topics fake LLM "
        "real embedding retrieval"
    )
    target_text = dict(messages)[1]
    unrelated_text = dict(messages)[9]

    try:
        query_vector = await service.encode_single(query)
        target_vector = await service.encode_single(target_text)
        unrelated_vector = await service.encode_single(unrelated_text)

        events = [
            event
            async for event in executor.execute(
                simulated_date="2026-04-05 10:30 UTC"
            )
        ]
    except Exception as exc:
        pytest.skip(f"Local agent embedding smoke could not run: {exc}")
    finally:
        service.cleanup()

    assert cosine(query_vector, target_vector) > cosine(query_vector, unrelated_vector)
    assert graph.fts_calls == [
        (
            query,
            10,
            "ada",
            ["session-agent"],
            [],
        )
    ]
    assert memory.calls == [["Testing"]]
    assert executor.llm_calls == [
        ("architect-model", "high", 1),
        ("librarian-model", "medium", 2),
        ("architect-model", "high", 3),
    ]

    response = events[-1]
    assert response["event"] == "response"
    assert response["data"]["content"].startswith(
        "Agent/tool smoke final answer from retrieved executor"
    )

    retrieved_ids = [message["id"] for message in executor.ctx.evidence.messages]
    target_ids = {"msg_1", "msg_2", "msg_3", "msg_4", "msg_5"}
    assert retrieved_ids[0] in target_ids
    assert len(target_ids.intersection(retrieved_ids[:4])) >= 3
    assert "msg_9" not in retrieved_ids[:2]

    second_turn_user_message = llm.stream_calls[1]["user"]
    assert "AgentExecutor chunk 8 smoke testing" in second_turn_user_message
    assert "`search_messages`: Found" in second_turn_user_message
    assert "Active topics you can categorize memories under: Testing" in (
        llm.stream_calls[0]["system"]
    )
