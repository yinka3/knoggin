from types import SimpleNamespace

import pytest

from common.utils.time_utils import frozen_time
from core.agent.maintenance import (
    GRAPH_MERGE_SCAN_CANDIDATE,
    TOPIC_EVALUATION_CANDIDATE,
    active_tool_names,
    build_maintenance_candidates,
    candidate_id,
)
from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeRedis


@pytest.mark.no_network
def test_active_tool_names_uses_default_schema_filtering():
    default_tools = active_tool_names(None)

    assert "update_topics" in default_tools
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
async def test_heartbeat_counter_creates_topic_candidate():
    redis = FakeRedis()
    await redis.set(RedisKeys.project_heartbeat_counter("ada", "project-1"), "40")

    candidates = await build_maintenance_candidates(
        redis=redis,
        user_name="ada",
        project_id="project-1",
        enabled_tools=None,
        topic_settings=SimpleNamespace(enabled=True, interval_msgs=40),
    )

    topic = next(c for c in candidates if c.kind == TOPIC_EVALUATION_CANDIDATE)
    assert topic.id == "topic_evaluation:project-1"
    assert topic.suggested_tool == "update_topics"
    assert topic.priority == "normal"
    assert topic.metadata == {"heartbeat_count": 40, "interval_msgs": 40}


@pytest.mark.no_network
async def test_topic_candidate_is_skipped_when_tool_disabled():
    redis = FakeRedis()
    await redis.set(RedisKeys.project_heartbeat_counter("ada", "project-1"), "40")

    candidates = await build_maintenance_candidates(
        redis=redis,
        user_name="ada",
        project_id="project-1",
        enabled_tools=["search_messages"],
        topic_settings=SimpleNamespace(enabled=True, interval_msgs=40),
    )

    assert candidates == []


@pytest.mark.no_network
async def test_graph_merge_queue_creates_graph_scan_candidate():
    redis = FakeRedis()
    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "2", "3")

    candidates = await build_maintenance_candidates(
        redis=redis,
        user_name="ada",
        project_id="project-1",
        enabled_tools=None,
        topic_settings=SimpleNamespace(enabled=False, interval_msgs=40),
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
        topic_settings=SimpleNamespace(enabled=False, interval_msgs=40),
    )

    assert candidates == []


@pytest.mark.no_network
async def test_cooldown_suppresses_candidate_creation():
    redis = FakeRedis()
    cid = candidate_id(TOPIC_EVALUATION_CANDIDATE, "project-1")
    await redis.set(RedisKeys.project_heartbeat_counter("ada", "project-1"), "40")
    await redis.set(RedisKeys.maintenance_cooldown("ada", "project-1", cid), "2000")

    with frozen_time(1000):
        candidates = await build_maintenance_candidates(
            redis=redis,
            user_name="ada",
            project_id="project-1",
            enabled_tools=None,
            topic_settings=SimpleNamespace(enabled=True, interval_msgs=40),
        )

    assert candidates == []


@pytest.mark.no_network
async def test_max_attempts_suppress_candidate_creation():
    redis = FakeRedis()
    cid = candidate_id(TOPIC_EVALUATION_CANDIDATE, "project-1")
    await redis.set(RedisKeys.project_heartbeat_counter("ada", "project-1"), "40")
    await redis.set(RedisKeys.maintenance_attempts("ada", "project-1", cid), "3")

    candidates = await build_maintenance_candidates(
        redis=redis,
        user_name="ada",
        project_id="project-1",
        enabled_tools=None,
        topic_settings=SimpleNamespace(enabled=True, interval_msgs=40),
    )

    assert candidates == []
