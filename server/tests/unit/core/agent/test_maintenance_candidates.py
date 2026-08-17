import pytest

from core.agent.maintenance import (
    GRAPH_MERGE_SCAN_CANDIDATE,
    active_tool_names,
    build_maintenance_candidates,
)
from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeRedis


@pytest.mark.no_network
def test_active_tool_names_uses_default_schema_filtering():
    default_tools = active_tool_names(None)

    assert "check_graph_health" in default_tools
    assert active_tool_names(["search_messages"]) == frozenset(
        {
            "request_clarification",
            "request_replanning",
            "submit_answer",
            "search_messages",
        }
    )


@pytest.mark.no_network
async def test_graph_merge_queue_creates_graph_scan_candidate():
    redis = FakeRedis()
    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "2", "3")

    candidates = await build_maintenance_candidates(
        redis=redis,
        user_name="ada",
        project_id="project-1",
        enabled_tools=None,
    )

    graph = next(c for c in candidates if c.kind == GRAPH_MERGE_SCAN_CANDIDATE)
    assert graph.id == "graph_merge_scan:project-1"
    assert graph.suggested_tool == "check_graph_health"
    assert graph.priority == "low"
    assert graph.metadata == {"merge_queue_count": 2}


@pytest.mark.no_network
async def test_graph_scan_candidate_is_skipped_when_tool_disabled():
    redis = FakeRedis()
    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "2")

    candidates = await build_maintenance_candidates(
        redis=redis,
        user_name="ada",
        project_id="project-1",
        enabled_tools=["search_messages"],
    )

    assert candidates == []
