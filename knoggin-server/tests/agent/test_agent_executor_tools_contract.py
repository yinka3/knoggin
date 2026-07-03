from types import SimpleNamespace

import pytest

from common.exceptions import ToolExecutionError
from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.executor import AgentExecutor
from knoggin_server.agent.types import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    MaintenanceCandidate,
    RetrievedEvidence,
    ToolCall,
)
from tests.fixtures.fakes import FakeRedis


def make_executor(*, config=None, state=None, redis=None):
    ctx = AgentContext(
        config=config
        or AgentRunConfig(max_calls=4, tool_limits=(("search_messages", 2),)),
        state=state or AgentState(),
        evidence=RetrievedEvidence(),
        user_name="ada",
        user_query="Find profile evidence",
        session_id="session-1",
        project_id="project-1",
        run_id="run-1",
    )
    return AgentExecutor(
        ctx,
        llm=object(),
        tools=SimpleNamespace(redis=redis, project_id="project-1"),
    )


@pytest.mark.no_network
def test_agent_run_config_uses_cached_tool_limits_with_wildcard_and_default():
    config = AgentRunConfig(
        tool_limits=(
            ("search_messages", 2),
            ("read_*", 4),
        )
    )

    assert config.get_tool_limit("search_messages") == 2
    assert config.get_tool_limit("read_document") == 4
    assert config.get_tool_limit("unknown_tool", default=7) == 7


@pytest.mark.no_network
async def test_execute_tools_success_records_state_and_accumulates_evidence(
    monkeypatch,
):
    executor = make_executor()
    execute_calls = []

    async def fake_execute_tool(tools, name, args):
        execute_calls.append((tools, name, args))
        return {
            "data": [
                {
                    "id": "msg_1",
                    "user_name": "ada",
                    "session_id": "session-1",
                    "message": "profile evidence",
                    "score": 0.9,
                }
            ]
        }

    monkeypatch.setattr(
        "knoggin_server.agent.executor.execute_tool",
        fake_execute_tool,
    )
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall(
                    name="search_messages",
                    args={"query": "profile", "limit": 2},
                    thinking="Need exact messages",
                    call_id="call-1",
                )
            ],
            results,
        )
    ]

    assert events == [
        {
            "event": "tool_start",
            "data": {
                "tool": "search_messages",
                "args": {"query": "profile", "limit": 2},
                "thinking": "Need exact messages",
                "call_id": "call-1",
            },
        },
        {
            "event": "tool_end",
            "data": {
                "tool": "search_messages",
                "result": "Found 1 results",
                "call_id": "call-1",
            },
        },
    ]
    assert execute_calls == [
        (executor.tools, "search_messages", {"query": "profile", "limit": 2})
    ]
    assert executor.ctx.state.call_count == 1
    assert executor.ctx.state.tool_call_counts == {"search_messages": 1}
    assert executor.ctx.state.tools_used == ["search_messages"]
    assert executor.ctx.state.consecutive_errors == 0
    assert executor.ctx.evidence.messages == [
        {
            "id": "msg_1",
            "user_name": "ada",
            "session_id": "session-1",
            "message": "profile evidence",
            "score": 0.9,
        }
    ]
    assert results == [
        {
            "tool": "search_messages",
            "result": {
                "data": [
                    {
                        "id": "msg_1",
                        "user_name": "ada",
                        "session_id": "session-1",
                        "message": "profile evidence",
                        "score": 0.9,
                    }
                ]
            },
        }
    ]


@pytest.mark.no_network
async def test_execute_tools_global_limit_blocks_all_calls(monkeypatch):
    state = AgentState(call_count=2)
    executor = make_executor(config=AgentRunConfig(max_calls=2), state=state)

    async def fail_execute_tool(*_args):
        raise AssertionError("tool should not execute after global limit")

    monkeypatch.setattr("knoggin_server.agent.executor.execute_tool", fail_execute_tool)
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [ToolCall(name="search_messages", args={"query": "profile"})],
            results,
        )
    ]

    assert events == [
        {
            "event": "tool_error",
            "data": {"tool": "all", "error": "Global call limit reached (2)"},
        }
    ]
    assert results == [{"tool": "all", "error": "Global call limit reached (2)"}]


@pytest.mark.no_network
async def test_execute_tools_tool_limit_duplicate_and_parse_error_skip_methods(
    monkeypatch,
):
    config = AgentRunConfig(
        max_calls=10,
        tool_limits=(("search_messages", 1), ("fact_check", 3)),
    )
    state = AgentState()
    state.record_call("search_messages", {"query": "already used"})
    state.record_call("fact_check", {"entity_name": "Ada", "query": "profile"})
    executor = make_executor(config=config, state=state)

    async def fail_execute_tool(*_args):
        raise AssertionError("invalid calls should not execute")

    monkeypatch.setattr("knoggin_server.agent.executor.execute_tool", fail_execute_tool)
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall(name="search_messages", args={"query": "new"}),
                ToolCall(
                    name="fact_check",
                    args={"entity_name": "Ada", "query": "profile"},
                ),
                ToolCall(
                    name="get_connections",
                    args={"_parse_error": True, "_raw": "{bad"},
                ),
            ],
            results,
        )
    ]

    assert events == [
        {
            "event": "tool_start",
            "data": {
                "tool": "search_messages",
                "args": {"query": "new"},
                "thinking": None,
                "call_id": None,
            },
        },
        {
            "event": "tool_error",
            "data": {
                "tool": "search_messages",
                "error": "Call limit reached for search_messages",
            },
        },
        {
            "event": "tool_start",
            "data": {
                "tool": "fact_check",
                "args": {"entity_name": "Ada", "query": "profile"},
                "thinking": None,
                "call_id": None,
            },
        },
        {
            "event": "tool_error",
            "data": {"tool": "fact_check", "error": "Duplicate call skipped"},
        },
        {
            "event": "tool_start",
            "data": {
                "tool": "get_connections",
                "args": {"_parse_error": True, "_raw": "{bad"},
                "thinking": None,
                "call_id": None,
            },
        },
        {
            "event": "tool_error",
            "data": {
                "tool": "get_connections",
                "error": "Argument parse failure",
            },
        },
    ]
    assert results == [
        {
            "tool": "search_messages",
            "error": "Tool 'search_messages' has reached its call limit",
        },
        {
            "tool": "fact_check",
            "error": "Duplicate call to 'fact_check' with same arguments",
        },
        {
            "tool": "get_connections",
            "error": "Failed to parse arguments for 'get_connections'",
        },
    ]
    assert (
        executor.ctx.state.last_error
        == "Failed to parse arguments for 'get_connections'"
    )
    assert executor.ctx.state.call_count == 3
    assert executor.ctx.state.tools_used == [
        "search_messages",
        "fact_check",
        "get_connections",
    ]


@pytest.mark.no_network
async def test_execute_tools_clarification_emits_and_stops(monkeypatch):
    executor = make_executor()

    async def fail_execute_tool(*_args):
        raise AssertionError("clarification should not execute as normal tool")

    monkeypatch.setattr("knoggin_server.agent.executor.execute_tool", fail_execute_tool)
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall(
                    name="request_clarification",
                    args={"question": "Which project?"},
                ),
                ToolCall(name="search_messages", args={"query": "profile"}),
            ],
            results,
        )
    ]

    assert events == [
        {
            "event": "tool_start",
            "data": {
                "tool": "request_clarification",
                "args": {"question": "Which project?"},
                "thinking": None,
                "call_id": None,
            },
        },
        {"event": "clarification", "data": {"question": "Which project?"}},
    ]
    assert results == []
    assert executor.ctx.state.tools_used == ["request_clarification"]


@pytest.mark.no_network
async def test_execute_tools_tool_errors_increment_and_success_resets(monkeypatch):
    executor = make_executor()
    outcomes = [
        ToolExecutionError("search_messages", "backend unavailable"),
        {"data": [{"id": "msg_1", "score": 0.5}]},
        RuntimeError("unexpected"),
    ]

    async def fake_execute_tool(*_args):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(
        "knoggin_server.agent.executor.execute_tool",
        fake_execute_tool,
    )
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [
                ToolCall(name="search_messages", args={"query": "one"}),
                ToolCall(name="search_messages", args={"query": "two"}),
                ToolCall(name="fact_check", args={"entity_name": "Ada", "query": "q"}),
            ],
            results,
        )
    ]

    assert [event["event"] for event in events] == [
        "tool_start",
        "tool_error",
        "tool_start",
        "tool_end",
        "tool_start",
        "tool_error",
    ]
    assert "backend unavailable" in events[1]["data"]["error"]
    assert events[3]["data"]["result"] == "Found 1 results"
    assert events[5]["data"] == {
        "tool": "fact_check",
        "error": "Internal tool failure",
    }
    assert executor.ctx.state.consecutive_errors == 1


@pytest.mark.no_network
async def test_successful_maintenance_tool_clears_attempts_and_cooldown(monkeypatch):
    redis = FakeRedis()
    executor = make_executor(redis=redis)
    candidate = MaintenanceCandidate(
        id="graph_merge_scan:project-1",
        kind="graph_merge_scan",
        reason="Merge queue has candidates.",
        suggested_tool="check_graph_health",
    )
    executor.ctx.maintenance_candidates = [candidate]
    await redis.set(
        RedisKeys.maintenance_attempts("ada", "project-1", candidate.id),
        "1",
    )
    await redis.set(
        RedisKeys.maintenance_cooldown("ada", "project-1", candidate.id),
        "2000",
    )

    async def fake_execute_tool(*_args):
        return {"data": {"message": "Graph is healthy."}}

    monkeypatch.setattr(
        "knoggin_server.agent.executor.execute_tool",
        fake_execute_tool,
    )
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [ToolCall(name="check_graph_health", args={})],
            results,
        )
    ]

    assert events[-1]["event"] == "tool_end"
    assert (
        await redis.get(
            RedisKeys.maintenance_attempts("ada", "project-1", candidate.id)
        )
        is None
    )
    assert (
        await redis.get(
            RedisKeys.maintenance_cooldown("ada", "project-1", candidate.id)
        )
        is None
    )
    assert executor.ctx.maintenance_candidates == []


@pytest.mark.no_network
async def test_failed_maintenance_tool_records_cooldown_without_terminal_error(
    monkeypatch,
):
    redis = FakeRedis()
    executor = make_executor(redis=redis)
    candidate = MaintenanceCandidate(
        id="topic_evaluation:project-1",
        kind="topic_evaluation",
        reason="Heartbeat reached threshold.",
        suggested_tool="update_topics",
    )
    executor.ctx.maintenance_candidates = [candidate]

    async def fake_execute_tool(*_args):
        raise ToolExecutionError("update_topics", "topic write failed")

    monkeypatch.setattr(
        "knoggin_server.agent.executor.execute_tool",
        fake_execute_tool,
    )
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [ToolCall(name="update_topics", args={})],
            results,
        )
    ]

    assert events[-1]["event"] == "tool_error"
    assert await redis.get(
        RedisKeys.maintenance_attempts("ada", "project-1", candidate.id)
    ) == "1"
    assert await redis.get(
        RedisKeys.maintenance_cooldown("ada", "project-1", candidate.id)
    )
    assert executor.ctx.state.consecutive_errors == 0
    assert results[0]["tool"] == "update_topics"
    assert "topic write failed" in results[0]["error"]
    assert executor.ctx.maintenance_candidates == []


@pytest.mark.no_network
async def test_maintenance_tool_error_result_records_cooldown(monkeypatch):
    redis = FakeRedis()
    executor = make_executor(redis=redis)
    candidate = MaintenanceCandidate(
        id="topic_evaluation:project-1",
        kind="topic_evaluation",
        reason="Heartbeat reached threshold.",
        suggested_tool="update_topics",
    )
    executor.ctx.maintenance_candidates = [candidate]

    async def fake_execute_tool(*_args):
        return {"data": {"error": "topic validation failed"}}

    monkeypatch.setattr(
        "knoggin_server.agent.executor.execute_tool",
        fake_execute_tool,
    )
    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [ToolCall(name="update_topics", args={})],
            results,
        )
    ]

    assert events[-1]["event"] == "tool_end"
    assert await redis.get(
        RedisKeys.maintenance_attempts("ada", "project-1", candidate.id)
    ) == "1"
    assert await redis.get(
        RedisKeys.maintenance_cooldown("ada", "project-1", candidate.id)
    )
    assert executor.ctx.maintenance_candidates == []
