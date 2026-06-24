import json
from contextlib import asynccontextmanager

import pytest

from common.utils.time_utils import frozen_time
from knoggin_server.community.community_store import CommunityStore

FROZEN_AT = "2026-02-03T04:05:06+00:00"


class RecordingCursor:
    def __init__(self, client):
        self.client = client

    async def execute(self, query, params=None):
        self.client.cursor_execute_calls.append((query, params))

    async def fetchone(self):
        return self.client.fetchone_result


class RecordingPostgresClient:
    def __init__(
        self,
        *,
        read_results=None,
        read_exception=None,
        write_exception=None,
        fetchone_result=None,
    ):
        self.build_calls = []
        self.read_calls = []
        self.write_calls = []
        self.cursor_execute_calls = []
        self.read_results = list(read_results or [])
        self.read_exception = read_exception
        self.write_exception = write_exception
        self.fetchone_result = fetchone_result
        self.transaction_enters = 0
        self.transaction_exits = 0
        self.cursor_enters = 0
        self.cursor_exits = 0

    def build_cypher(
        self,
        cypher_query,
        return_types="result agtype",
        graph_name="knoggin_graph",
    ):
        self.build_calls.append((cypher_query, return_types, graph_name))
        return f"cypher<{graph_name}|{return_types}>:{cypher_query}"

    async def execute(self, query, params=None):
        self.write_calls.append((query, params))
        if self.write_exception:
            raise self.write_exception

    async def fetch_all(self, query, params=None):
        self.read_calls.append((query, params))
        if self.read_exception:
            raise self.read_exception
        if not self.read_results:
            return []
        return self.read_results.pop(0)

    @asynccontextmanager
    async def transaction(self):
        self.transaction_enters += 1
        self.cursor_enters += 1
        try:
            yield RecordingCursor(self)
        finally:
            self.cursor_exits += 1
            self.transaction_exits += 1


def only_payload(call):
    _, params = call
    return json.loads(params[0])


@pytest.mark.storage
@pytest.mark.no_network
async def test_community_store_write_methods_record_expected_payloads():
    client = RecordingPostgresClient()
    store = CommunityStore(client)

    with frozen_time(FROZEN_AT):
        await store.create_discussion("disc-1", "Profile drift review", ["a1", "a2"])
        await store.add_message("disc-1", "a1", "Community note", "assistant")
        await store.close_discussion("disc-1")
        await store.register_agent_spawn("a1", "spawned-1", "Evidence specialist")

    assert len(client.write_calls) == 4
    assert "CREATE (d:AAC_Discussion" in client.write_calls[0][0]
    assert "status: 'active'" in client.write_calls[0][0]
    assert only_payload(client.write_calls[0]) == {
        "id": "disc-1",
        "topic": "Profile drift review",
        "agent_ids": ["a1", "a2"],
        "ts": FROZEN_AT,
    }

    assert "CREATE (m:AAC_Message" in client.write_calls[1][0]
    assert only_payload(client.write_calls[1]) == {
        "discussion_id": "disc-1",
        "agent_id": "a1",
        "content": "Community note",
        "role": "assistant",
        "ts": FROZEN_AT,
    }

    assert "SET d.status = 'closed'" in client.write_calls[2][0]
    assert only_payload(client.write_calls[2]) == {
        "id": "disc-1",
        "ts": FROZEN_AT,
    }

    assert "CREATE (p)-[:SPAWNED" in client.write_calls[3][0]
    assert only_payload(client.write_calls[3]) == {
        "parent_id": "a1",
        "child_id": "spawned-1",
        "detail": "Evidence specialist",
        "ts": FROZEN_AT,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_community_store_get_discussions_normalizes_rows():
    client = RecordingPostgresClient(
        read_results=[
            [
                {
                    "id": '"disc-1"',
                    "topic": '"Profile drift review"',
                    "status": '"active"',
                    "created_at": '"2026-02-03T04:05:06+00:00"',
                    "closed_at": None,
                    "agent_ids": ["a1", "a2"],
                }
            ]
        ]
    )
    store = CommunityStore(client)

    discussions = await store.get_discussions()

    assert discussions == [
        {
            "id": "disc-1",
            "topic": "Profile drift review",
            "status": "active",
            "created_at": "2026-02-03T04:05:06+00:00",
            "agent_ids": ["a1", "a2"],
        }
    ]
    assert "ORDER BY d.created_at DESC" in client.read_calls[0][0]
    assert client.read_calls[0][1] == ("{}",)


@pytest.mark.storage
@pytest.mark.no_network
async def test_community_store_get_discussion_history_normalizes_rows():
    client = RecordingPostgresClient(
        read_results=[
            [
                {
                    "agent_id": '"a1"',
                    "content": '"First"',
                    "role": '"assistant"',
                    "timestamp": '"2026-02-03T04:05:06+00:00"',
                },
                {
                    "agent_id": '"system"',
                    "content": '"INSIGHT: second"',
                    "role": '"insight"',
                    "timestamp": '"2026-02-03T04:06:06+00:00"',
                },
            ]
        ]
    )
    store = CommunityStore(client)

    history = await store.get_discussion_history("disc-1")

    assert history == [
        {
            "agent_id": "a1",
            "content": "First",
            "role": "assistant",
            "timestamp": "2026-02-03T04:05:06+00:00",
        },
        {
            "agent_id": "system",
            "content": "INSIGHT: second",
            "role": "insight",
            "timestamp": "2026-02-03T04:06:06+00:00",
        },
    ]
    assert json.loads(client.read_calls[0][1][0]) == {"discussion_id": "disc-1"}
    assert "ORDER BY m.timestamp ASC" in client.read_calls[0][0]


@pytest.mark.storage
@pytest.mark.no_network
async def test_community_store_get_agent_hierarchy_normalizes_rows():
    client = RecordingPostgresClient(
        read_results=[
            [
                {
                    "parent": '"a1"',
                    "child": '"spawned-1"',
                    "detail": '"Evidence specialist"',
                    "timestamp": '"2026-02-03T04:05:06+00:00"',
                }
            ]
        ]
    )
    store = CommunityStore(client)

    hierarchy = await store.get_agent_hierarchy()

    assert hierarchy == [
        {
            "parent": "a1",
            "child": "spawned-1",
            "detail": "Evidence specialist",
            "timestamp": "2026-02-03T04:05:06+00:00",
        }
    ]
    assert client.read_calls[0][1] == ("{}",)


@pytest.mark.storage
@pytest.mark.no_network
async def test_community_store_get_recent_discussions_passes_limit():
    client = RecordingPostgresClient(
        read_results=[
            [
                {
                    "id": '"disc-1"',
                    "topic": '"Profile drift review"',
                    "status": '"closed"',
                    "created_at": '"2026-02-03T04:05:06+00:00"',
                    "closed_at": '"2026-02-03T04:10:06+00:00"',
                    "message_count": "3",
                }
            ]
        ]
    )
    store = CommunityStore(client)

    discussions = await store.get_recent_discussions(limit=7)

    assert discussions == [
        {
            "id": "disc-1",
            "topic": "Profile drift review",
            "status": "closed",
            "created_at": "2026-02-03T04:05:06+00:00",
            "closed_at": "2026-02-03T04:10:06+00:00",
            "message_count": "3",
        }
    ]
    assert json.loads(client.read_calls[0][1][0]) == {"limit": 7}
    assert "message_count" in client.read_calls[0][0]


@pytest.mark.storage
@pytest.mark.no_network
async def test_community_store_get_discussion_insights_passes_limit():
    client = RecordingPostgresClient(
        read_results=[
            [
                {
                    "content": '"INSIGHT: Ada prefers precise tests"',
                    "timestamp": '"2026-02-03T04:05:06+00:00"',
                    "discussion_topic": '"Testing plan"',
                }
            ]
        ]
    )
    store = CommunityStore(client)

    insights = await store.get_discussion_insights(limit=4)

    assert insights == [
        {
            "content": "INSIGHT: Ada prefers precise tests",
            "timestamp": "2026-02-03T04:05:06+00:00",
            "discussion_topic": "Testing plan",
        }
    ]
    assert json.loads(client.read_calls[0][1][0]) == {"limit": 4}
    assert "WHERE m.role = 'insight'" in client.read_calls[0][0]


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("method_name", "args"),
    [
        ("get_discussions", ()),
        ("get_discussion_history", ("disc-1",)),
        ("get_agent_hierarchy", ()),
        ("get_recent_discussions", (5,)),
        ("get_discussion_insights", (5,)),
    ],
)
async def test_community_store_read_failures_return_empty_list(method_name, args):
    client = RecordingPostgresClient(read_exception=RuntimeError("read failed"))
    store = CommunityStore(client)

    assert await getattr(store, method_name)(*args) == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_community_store_write_failures_reraise():
    client = RecordingPostgresClient(write_exception=RuntimeError("write failed"))
    store = CommunityStore(client)

    with pytest.raises(RuntimeError, match="write failed"):
        await store.create_discussion("disc-1", "Topic", ["a1"])


@pytest.mark.storage
@pytest.mark.no_network
async def test_community_store_delete_old_discussions_uses_pool_and_returns_count():
    client = RecordingPostgresClient(fetchone_result={"deleted_discussions": "2"})
    store = CommunityStore(client)

    with frozen_time("2026-02-10T04:05:06+00:00"):
        deleted = await store.delete_old_discussions(retention_days=7)

    assert deleted == 2
    assert client.transaction_enters == 1
    assert client.cursor_enters == 1
    query, params = client.cursor_execute_calls[0]
    assert "DETACH DELETE d, m" in query
    assert json.loads(params[0]) == {"cutoff": "2026-02-03T04:05:06+00:00"}
