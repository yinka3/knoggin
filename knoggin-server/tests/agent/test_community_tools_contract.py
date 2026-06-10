import json
from types import SimpleNamespace

import pytest

from common.schema.aac_schema import AAC_DEFAULT_ENABLED_TOOLS
from common.utils.time_utils import frozen_time
from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.tools.community_tools import (
    MAX_SPAWNED_SPECIALISTS,
    CommunityTools,
)
from tests.fixtures.fakes import FakeRedis

FROZEN_AT = "2026-02-03T04:05:06+00:00"


class RecordingCommunityStore:
    def __init__(self):
        self.messages = []
        self.spawns = []

    async def add_message(self, discussion_id, agent_id, content, role="agent"):
        self.messages.append(
            {
                "discussion_id": discussion_id,
                "agent_id": agent_id,
                "content": content,
                "role": role,
            }
        )

    async def register_agent_spawn(self, parent_id, child_id, detail=""):
        self.spawns.append(
            {
                "parent_id": parent_id,
                "child_id": child_id,
                "detail": detail,
            }
        )


def make_base_tools(redis):
    entities = SimpleNamespace(
        embedding_service=object(),
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    return SimpleNamespace(
        entities=entities,
        session_id="session-1",
        topic_config=SimpleNamespace(active_topics=["General"]),
        search_cfg={},
        file_rag=None,
        graph_client=SimpleNamespace(),
        redis=redis,
    )


def make_tool(*, redis=None, participants=None):
    redis = redis or FakeRedis()
    store = RecordingCommunityStore()
    participants = participants if participants is not None else ["agent-1"]
    tool = CommunityTools(
        user_name="ada",
        base_tools=make_base_tools(redis),
        community_store=store,
        discussion_id="disc-1",
        agent_id="agent-1",
        memory_mgr=object(),
        participants=participants,
    )
    return tool, store, redis, participants


def patch_community_tool_config(monkeypatch, *, events=None):
    root = SimpleNamespace(
        config=SimpleNamespace(llm=SimpleNamespace(agent_model="test-agent-model"))
    )
    monkeypatch.setattr(
        "knoggin_server.agent.tools.community_tools.ConfigManager.get",
        staticmethod(lambda: root),
    )

    async def fake_emit_community(*args):
        if events is not None:
            events.append(args)

    monkeypatch.setattr(
        "knoggin_server.agent.tools.community_tools.emit_community",
        fake_emit_community,
    )


@pytest.mark.no_network
async def test_community_tools_save_insight_writes_insight_message():
    tool, store, _, _ = make_tool()

    try:
        result = await tool.save_insight("Ada prefers precise regression tests")
    finally:
        await tool.close()

    assert result == {"saved": True, "type": "insight"}
    assert store.messages == [
        {
            "discussion_id": "disc-1",
            "agent_id": "system",
            "content": "INSIGHT: Ada prefers precise regression tests",
            "role": "insight",
        }
    ]


@pytest.mark.no_network
async def test_community_tools_save_memory_stores_discussion_scoped_payload():
    tool, _, redis, _ = make_tool()

    try:
        with frozen_time(FROZEN_AT):
            result = await tool.save_memory(
                "Check profile drift before broadening summaries",
                topic="Testing",
            )
    finally:
        await tool.close()

    assert result["saved"] is True
    assert result["memory_id"].startswith("comm_mem_")
    key = RedisKeys.community_agent_memory("ada", "agent-1")
    entries = await redis.hgetall(key)
    assert list(entries) == [result["memory_id"]]
    assert json.loads(entries[result["memory_id"]]) == {
        "content": "Check profile drift before broadening summaries",
        "topic": "Testing",
        "created_at": FROZEN_AT,
        "discussion_id": "disc-1",
    }


@pytest.mark.no_network
async def test_community_tools_save_memory_refuses_when_memory_is_full():
    redis = FakeRedis()
    key = RedisKeys.community_agent_memory("ada", "agent-1")
    for index in range(10):
        await redis.hset(key, f"existing-{index}", "{}")
    tool, _, _, _ = make_tool(redis=redis)

    try:
        result = await tool.save_memory("one too many")
    finally:
        await tool.close()

    assert result == {"error": "Memory full (10/10). No new memories can be saved."}
    assert await redis.hlen(key) == 10


@pytest.mark.no_network
async def test_community_tools_spawn_specialist_creates_agent_and_seed_memory(
    monkeypatch,
):
    events = []
    patch_community_tool_config(monkeypatch, events=events)
    participants = ["agent-1"]
    tool, store, redis, participants = make_tool(participants=participants)

    try:
        with frozen_time(FROZEN_AT):
            result = await tool.spawn_specialist(
                name="Evidence Steward",
                persona="Tracks profile evidence and weak claims.",
                initial_rules=["Never invent source messages."],
                initial_preferences=["Prefer direct evidence."],
                initial_icks=["Unscoped profile claims."],
            )
    finally:
        await tool.close()

    assert result["id"].startswith("spawned_")
    assert result["seeded_memory"] == {
        "rules": 1,
        "preferences": 1,
        "icks": 1,
    }
    assert participants == ["agent-1", result["id"]]

    raw_agent = await redis.hget(RedisKeys.agents("ada"), result["id"])
    agent = json.loads(raw_agent)
    assert agent["id"] == result["id"]
    assert agent["name"] == "Evidence Steward"
    assert agent["persona"] == "Tracks profile evidence and weak claims."
    assert agent["model"] == "test-agent-model"
    assert agent["enabled_tools"] == AAC_DEFAULT_ENABLED_TOOLS
    assert agent["is_spawned"] is True
    assert agent["spawned_by"] == "agent-1"

    assert store.spawns == [
        {
            "parent_id": "agent-1",
            "child_id": result["id"],
            "detail": "Tracks profile evidence and weak claims.",
        }
    ]
    assert events[0][2] == "agent_spawned"
    assert events[0][3]["agent_id"] == result["id"]
    assert events[0][3]["seeded_memory"] == result["seeded_memory"]

    seeded = {}
    for category in ("rules", "preferences", "icks"):
        memory_key = RedisKeys.agent_working_memory(result["id"], category)
        values = list((await redis.hgetall(memory_key)).values())
        seeded[category] = [json.loads(value) for value in values]

    assert seeded["rules"] == [
        {
            "content": "Never invent source messages.",
            "created_at": FROZEN_AT,
            "seeded_by": "agent-1",
        }
    ]
    assert seeded["preferences"][0]["content"] == "Prefer direct evidence."
    assert seeded["icks"][0]["content"] == "Unscoped profile claims."


@pytest.mark.no_network
async def test_community_tools_spawn_specialist_blocks_at_limit(monkeypatch):
    patch_community_tool_config(monkeypatch)
    redis = FakeRedis()
    participants = [f"spawned-{index}" for index in range(MAX_SPAWNED_SPECIALISTS)]
    for agent_id in participants:
        await redis.hset(
            RedisKeys.agents("ada"),
            agent_id,
            json.dumps(
                {
                    "id": agent_id,
                    "name": agent_id,
                    "persona": "spawned",
                    "is_spawned": True,
                }
            ),
        )
    tool, store, _, participants = make_tool(redis=redis, participants=participants)

    try:
        result = await tool.spawn_specialist("Extra", "Should not join")
    finally:
        await tool.close()

    assert result == {
        "error": "Spawn limit reached. Max 10 sub-agents per discussion."
    }
    assert len(participants) == MAX_SPAWNED_SPECIALISTS
    assert store.spawns == []


@pytest.mark.no_network
async def test_community_tools_count_spawned_ignores_missing_and_malformed_agents():
    redis = FakeRedis()
    await redis.hset(
        RedisKeys.agents("ada"),
        "spawned-good",
        json.dumps(
            {
                "id": "spawned-good",
                "name": "Spawned Good",
                "persona": "valid",
                "is_spawned": True,
            }
        ),
    )
    await redis.hset(RedisKeys.agents("ada"), "malformed", "{not json")
    await redis.hset(
        RedisKeys.agents("ada"),
        "regular-agent",
        json.dumps(
            {
                "id": "regular-agent",
                "name": "Regular",
                "persona": "not spawned",
                "is_spawned": False,
            }
        ),
    )
    tool, _, _, _ = make_tool(
        redis=redis,
        participants=["spawned-good", "malformed", "missing-agent", "regular-agent"],
    )

    try:
        assert await tool._count_spawned_participants() == 1
    finally:
        await tool.close()
