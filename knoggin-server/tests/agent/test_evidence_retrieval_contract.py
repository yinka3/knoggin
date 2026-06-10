import asyncio
import json

import pytest

from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.tools.search import SearchTools
from tests.fixtures.fakes import FakeRedis


def make_search_tool(redis=None, graph_client=None):
    tool = SearchTools()
    tool.redis = redis or FakeRedis()
    tool.graph_client = graph_client or object()
    tool.user_name = "ada"
    tool.session_id = "session-1"
    return tool


@pytest.mark.no_network
async def test_surrounding_context_uses_msg_lookup_and_skips_malformed_turns():
    redis = FakeRedis()
    tool = make_search_tool(redis=redis)

    lookup_key = RedisKeys.msg_to_turn_lookup("ada", "session-1")
    recent_key = RedisKeys.recent_conversation("ada", "session-1")
    conv_key = RedisKeys.conversation("ada", "session-1")

    await redis.hset(lookup_key, "7", "turn_2")
    await redis.zadd(
        recent_key,
        {
            "turn_1": 1,
            "turn_2": 2,
            "turn_bad": 3,
            "turn_3": 4,
        },
    )
    await redis.hset(
        conv_key,
        "turn_1",
        json.dumps(
            {
                "role": "assistant",
                "content": "before",
                "timestamp": "2026-01-01T10:00:00+00:00",
            }
        ),
    )
    await redis.hset(
        conv_key,
        "turn_2",
        json.dumps(
            {
                "role": "user",
                "content": "hit",
                "timestamp": "2026-01-01T10:01:00+00:00",
            }
        ),
    )
    await redis.hset(conv_key, "turn_bad", "{not-json")
    await redis.hset(
        conv_key,
        "turn_3",
        json.dumps(
            {
                "role": "assistant",
                "content": "after",
                "timestamp": "2026-01-01T10:02:00+00:00",
            }
        ),
    )

    context = await tool._get_surrounding_context(
        "msg_7", forward=2, target_total=4
    )

    assert context == [
        {
            "role": "assistant",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "content": "before",
            "id": "turn_1",
        },
        {
            "role": "user",
            "timestamp": "2026-01-01T10:01:00+00:00",
            "content": "hit",
            "id": "turn_2",
            "is_hit": True,
        },
        {
            "role": "assistant",
            "timestamp": "2026-01-01T10:02:00+00:00",
            "content": "after",
            "id": "turn_3",
        },
    ]


@pytest.mark.no_network
async def test_surrounding_context_falls_back_to_graph_when_redis_rank_is_missing():
    class FakeGraphClient:
        def __init__(self):
            self.calls = []

        async def get_surrounding_messages(
            self, msg_id, forward, target_total, user_name, session_id
        ):
            self.calls.append((msg_id, forward, target_total, user_name, session_id))
            return [
                {
                    "id": 4,
                    "role": "assistant",
                    "content": "before",
                    "timestamp": 1767261600000,
                },
                {
                    "id": 5,
                    "role": "user",
                    "content": "hit",
                    "timestamp": 1767261660000,
                },
            ]

    graph = FakeGraphClient()
    tool = make_search_tool(redis=FakeRedis(), graph_client=graph)

    context = await tool._get_surrounding_context(
        "msg_5", forward=1, target_total=3, session_id="session-2"
    )

    assert graph.calls == [(5, 1, 3, "ada", "session-2")]
    assert [item["id"] for item in context] == ["msg_4", "msg_5"]
    assert context[1]["is_hit"] is True


@pytest.mark.no_network
async def test_search_messages_drops_results_without_hit_context():
    tool = make_search_tool(redis=FakeRedis())
    tool.search_cfg = {"default_message_limit": 8}

    async def search(query, limit):
        return [("msg_7", 0.5, "session-1")]

    async def context_without_hit(msg_id, session_id=None):
        return [{"id": "msg_6", "content": "nearby", "role": "user"}]

    tool._search_messages = search
    tool._get_surrounding_context = context_without_hit

    assert await tool.search_messages("query") == []


@pytest.mark.no_network
async def test_hydrate_evidence_uses_explicit_scope_and_legacy_default_scope():
    redis = FakeRedis()
    await redis.hset(
        RedisKeys.message_content("grace", "session-9"),
        "msg_7",
        json.dumps(
            {
                "message": "explicit scoped message",
                "timestamp": "2026-01-01T10:00:00+00:00",
            }
        ),
    )
    await redis.hset(
        RedisKeys.message_content("ada", "session-1"),
        "msg_8",
        json.dumps(
            {
                "content": "legacy default message",
                "timestamp": "2026-01-01T10:01:00+00:00",
            }
        ),
    )
    await redis.hset(
        RedisKeys.conversation("ada", "session-1"),
        "turn_2",
        json.dumps(
            {
                "content": "turn evidence",
                "timestamp": "2026-01-01T10:02:00+00:00",
            }
        ),
    )
    tool = make_search_tool(redis=redis)

    evidence = await tool._hydrate_evidence(
        [
            {"user_name": "grace", "session_id": "session-9", "message_id": 7},
            8,
            "turn_2",
            {"message_id": "msg_nope"},
        ]
    )

    assert evidence == [
        {
            "id": "msg_7",
            "user_name": "grace",
            "session_id": "session-9",
            "message": "explicit scoped message",
            "timestamp": "2026-01-01T10:00:00+00:00",
        },
        {
            "id": "msg_8",
            "user_name": "ada",
            "session_id": "session-1",
            "message": "legacy default message",
            "timestamp": "2026-01-01T10:01:00+00:00",
        },
        {
            "id": "turn_2",
            "user_name": "ada",
            "session_id": "session-1",
            "message": "turn evidence",
            "timestamp": "2026-01-01T10:02:00+00:00",
        },
    ]


@pytest.mark.no_network
async def test_hydrate_evidence_falls_back_to_scoped_graph_lookup_on_redis_miss():
    class FakeGraphClient:
        def __init__(self):
            self.calls = []

        async def get_messages_by_ids(self, message_ids, user_name, session_ids):
            self.calls.append((message_ids, user_name, session_ids))
            return [
                {
                    "id": 11,
                    "user_name": user_name,
                    "session_id": session_ids[0],
                    "content": "from graph",
                    "timestamp": 1767261600000,
                }
            ]

    graph = FakeGraphClient()
    tool = make_search_tool(redis=FakeRedis(), graph_client=graph)

    evidence = await tool._hydrate_evidence(
        [{"user_name": "ada", "session_id": "session-2", "message_id": 11}]
    )

    assert graph.calls == [([11], "ada", ["session-2"])]
    assert evidence == [
        {
            "id": "msg_11",
            "user_name": "ada",
            "session_id": "session-2",
            "message": "from graph",
            "timestamp": "2026-01-01T10:00:00+00:00",
        }
    ]


@pytest.mark.no_network
async def test_hydrate_evidence_returns_empty_on_redis_timeout():
    class TimeoutPipeline:
        def hget(self, key, field):
            return self

        async def execute(self):
            raise asyncio.TimeoutError

    class TimeoutRedis:
        def pipeline(self):
            return TimeoutPipeline()

    tool = make_search_tool(redis=TimeoutRedis())

    assert await tool._hydrate_evidence([7]) == []
