import json

import pytest

from core.knowledge.db.readers.graph_reader import GraphReader
from tests.fixtures.fakes import RecordingPostgresClient


class FakePostgresReaderClient:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.read_calls = []

    def build_cypher(
        self,
        cypher_query,
        return_types="result agtype",
        graph_name="knoggin_graph",
    ):
        return f"{graph_name}:{return_types}:{cypher_query}"

    async def fetch_all(self, query, params=None):
        self.read_calls.append((query, params))
        return self.rows


class VectorLike:
    def __init__(self, values):
        self.values = values

    def tolist(self):
        return list(self.values)


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_refuses_message_lookup_without_scope():
    client = FakePostgresReaderClient()
    reader = GraphReader(client)

    with pytest.raises(ValueError, match="requires user_name scope"):
        await reader.get_messages_by_ids(
            [1],
            user_name=None,
            session_ids=["s"],
            visible_project_ids=["project-1"],
        )
    with pytest.raises(ValueError, match="requires session_id scope"):
        await reader.get_message_text(
            1,
            user_name="ada",
            session_id="",
            visible_project_ids=["project-1"],
        )
    assert client.read_calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_get_message_text_uses_user_session_scope():
    client = RecordingPostgresClient(
        fetch_one_results=[{"content": "hello graph"}]
    )
    reader = GraphReader(client)

    assert await reader.get_message_text(
        7,
        user_name="ada",
        session_id="session-1",
        visible_project_ids=["project-1"],
    ) == "hello graph"

    assert "FROM messages" in client.calls[0][1]
    assert client.calls[0][2] == ("ada", "session-1", 7, ["project-1"])


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_get_messages_by_ids_empty_list_skips_db():
    client = RecordingPostgresClient()
    reader = GraphReader(client)

    assert (
        await reader.get_messages_by_ids(
            [],
            user_name="ada",
            session_ids=["s"],
            visible_project_ids=["project-1"],
        )
        == []
    )
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_message_lookup_uses_structured_scope_params():
    client = FakePostgresReaderClient(
        rows=[
            {
                "id": "1",
                "user_name": '"ada"',
                "session_id": '"session-1"',
                "role": '"user"',
                "content": '"hello"',
                "timestamp": 123,
            }
        ]
    )
    reader = GraphReader(client)

    rows = await reader.get_messages_by_ids(
        [1],
        user_name="ada",
        session_ids=["session-1"],
        visible_project_ids=["project-1"],
    )

    assert rows == [
        {
            "id": 1,
            "user_name": "ada",
            "session_id": "session-1",
            "role": "user",
            "content": "hello",
            "timestamp": 123,
        }
    ]
    query, params = client.read_calls[0]
    assert "FROM messages" in query
    assert params == ([1], "ada", ["session-1"], ["project-1"])


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_recent_project_messages_use_project_scope():
    client = FakePostgresReaderClient(
        rows=[
            {
                "id": "3",
                "user_name": '"ada"',
                "session_id": '"session-b"',
                "role": '"user"',
                "content": '"newer"',
                "timestamp": 456,
            },
            {
                "id": "2",
                "user_name": '"ada"',
                "session_id": '"session-a"',
                "role": '"user"',
                "content": '"older"',
                "timestamp": 123,
            },
        ]
    )
    reader = GraphReader(client)

    rows = await reader.get_recent_project_messages(
        "ada", "project-1", 2, before_message_id=3
    )

    assert rows == [
        {
            "id": 2,
            "user_name": "ada",
            "session_id": "session-a",
            "role": "user",
            "content": "older",
            "timestamp": 123,
        },
        {
            "id": 3,
            "user_name": "ada",
            "session_id": "session-b",
            "role": "user",
            "content": "newer",
            "timestamp": 456,
        },
    ]
    query, params = client.read_calls[0]
    assert "project_id = %s" in query
    assert "message_id <= %s" in query
    assert params == ("ada", "project-1", 3, 2)


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_surrounding_messages_uses_scoped_lookups():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "id": "2",
                    "user_name": '"ada"',
                    "session_id": '"session-1"',
                    "role": '"user"',
                    "content": '"target"',
                    "timestamp": 200,
                }
            ],
            [
                {
                    "id": "1",
                    "user_name": '"ada"',
                    "session_id": '"session-1"',
                    "role": '"user"',
                    "content": '"before"',
                    "timestamp": 100,
                }
            ],
            [
                {
                    "id": "3",
                    "user_name": '"ada"',
                    "session_id": '"session-1"',
                    "role": '"assistant"',
                    "content": '"after"',
                    "timestamp": 300,
                }
            ],
        ]
    )
    reader = GraphReader(client)

    messages = await reader.get_surrounding_messages(
        2,
        forward=1,
        target_total=3,
        user_name="ada",
        session_id="session-1",
        visible_project_ids=["project-1"],
    )

    assert [message["content"] for message in messages] == [
        "before",
        "target",
        "after",
    ]
    assert len(client.calls) == 3
    assert client.calls[0][2] == (
        [2],
        "ada",
        ["session-1"],
        ["project-1"],
    )
    assert client.calls[1][2] == (
        200,
        2,
        "ada",
        "session-1",
        ["project-1"],
        1,
    )
    assert client.calls[2][2] == (
        200,
        2,
        "ada",
        "session-1",
        ["project-1"],
        1,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_neighbor_ids_batch_empty_list_skips_db():
    client = RecordingPostgresClient()
    reader = GraphReader(client)

    assert await reader.get_neighbor_ids_batch(
        [], visible_project_ids=["project-1"]
    ) == {}
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_parent_child_and_neighbor_entities_use_params():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "id": "10",
                    "canonical_name": "Parent",
                    "type": "topic",
                    "facts": ["parent fact"],
                }
            ],
            [{"id": "11", "name": "Neighbor"}],
            [
                {
                    "id": "12",
                    "canonical_name": "Child",
                    "type": "project",
                    "facts": ["child fact"],
                }
            ],
        ]
    )
    reader = GraphReader(client)

    assert await reader.get_parent_entities(
        2, visible_project_ids=["project-1"]
    ) == [
        {
            "id": 10,
            "canonical_name": "Parent",
            "type": "topic",
            "facts": ["parent fact"],
        }
    ]
    assert await reader.get_neighbor_entities(
        2, visible_project_ids=["project-1"], limit=3
    ) == [
        {"id": 11, "name": "Neighbor"}
    ]
    assert await reader.get_child_entities(
        2, visible_project_ids=["project-1"]
    ) == [
        {
            "id": 12,
            "canonical_name": "Child",
            "type": "project",
            "facts": ["child fact"],
        }
    ]
    assert "FROM hierarchy_edges edge" in client.calls[0][1]
    assert "f.project_id = ANY(%s)" in client.calls[0][1]
    assert client.calls[0][2] == (
        ["project-1"],
        2,
        ["project-1"],
        ["project-1"],
        1,
    )
    assert json.loads(client.calls[1][2][0]) == {
        "entity_id": 2,
        "limit": 3,
        "visible_project_ids": ["project-1"],
        "identity_entity_id": 1,
    }
    assert "FROM hierarchy_edges edge" in client.calls[2][1]
    assert "f.project_id = ANY(%s)" in client.calls[2][1]
    assert client.calls[2][2] == (
        ["project-1"],
        2,
        ["project-1"],
        ["project-1"],
        1,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_hierarchy_candidates_attach_embeddings():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "parent_id": "2",
                    "parent_name": "Area",
                    "parent_type": "area",
                    "child_id": "3",
                    "child_name": "Task",
                    "child_type": "task",
                    "weight": "4",
                }
            ],
            [
                {"entity_id": 2, "embedding": VectorLike([0.1, 0.2])},
                {"entity_id": 3, "embedding": [0.3, 0.4]},
            ],
        ]
    )
    reader = GraphReader(client)

    candidates = await reader.get_hierarchy_candidates(
        "project-1",
        "Work",
        "area",
        ["task"],
        min_weight=2,
    )

    assert candidates == [
        {
            "parent_id": 2,
            "parent_name": "Area",
            "parent_type": "area",
            "parent_embedding": [0.1, 0.2],
            "child_id": 3,
            "child_name": "Task",
            "child_type": "task",
            "child_embedding": [0.3, 0.4],
            "weight": "4",
        }
    ]
    assert "FROM relationships rel" in client.calls[0][1]
    assert "FROM hierarchy_edges edge" in client.calls[0][1]
    assert "rel.project_id = %s" in client.calls[0][1]
    assert client.calls[0][2] == (
        "project-1",
        "Work",
        "Work",
        "area",
        ["task"],
        2,
    )
    assert set(client.calls[1][2][0]) == {2, 3}
    assert client.calls[1][2][1] == "project-1"


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_hierarchy_candidates_skip_embedding_query_when_empty():
    client = RecordingPostgresClient(fetch_all_results=[[]])
    reader = GraphReader(client)

    assert (
        await reader.get_hierarchy_candidates(
            "project-1", "Work", "area", ["task"]
        )
        == []
    )
    assert len(client.calls) == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_hierarchy_candidates_require_project_scope():
    client = RecordingPostgresClient()
    reader = GraphReader(client)

    with pytest.raises(ValueError, match="requires project_id scope"):
        await reader.get_hierarchy_candidates("", "Work", "area", ["task"])

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_get_merge_topic_strength_reads_canonical_sql():
    row = {
        "p_topic": "People",
        "p_conf": "0.8",
        "p_last": "100",
        "s_topic": "Projects",
        "s_conf": "0.9",
        "s_last": "200",
        "p_fact_count": "1",
        "s_fact_count": "2",
        "p_relationship_count": "3",
        "s_relationship_count": "4",
    }
    client = RecordingPostgresClient(fetch_one_results=[row])
    reader = GraphReader(client)

    strength = await reader.get_merge_topic_strength(2, 3, "project-1")

    assert strength == row
    query, params = client.calls[0][1], client.calls[0][2]
    assert "FROM entities p" in query
    assert "JOIN entities s" in query
    assert "invalid_at IS NULL" in query
    assert "FROM relationships" in query
    assert "AND NOT" in query
    assert params == (
        "project-1",
        2,
        "project-1",
        3,
        "project-1",
        2,
        2,
        2,
        3,
        3,
        2,
        "project-1",
        3,
        3,
        2,
        3,
        3,
        2,
        3,
        "project-1",
        2,
        "project-1",
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_get_merge_topic_strength_requires_project_scope():
    client = RecordingPostgresClient()
    reader = GraphReader(client)

    with pytest.raises(ValueError, match="requires project_id scope"):
        await reader.get_merge_topic_strength(2, 3, "")

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_get_graph_stats_hydrates_counts_and_defaults_empty():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {"entities": "2", "facts": "3", "relationships": "4"},
            None,
        ]
    )
    reader = GraphReader(client)

    assert await reader.get_graph_stats(
        visible_project_ids=["project-1"]
    ) == {
        "entities": 2,
        "facts": 3,
        "relationships": 4,
    }
    assert "FROM entities" in client.calls[0][1]
    assert "FROM facts" in client.calls[0][1]
    assert "FROM relationships" in client.calls[0][1]
    assert client.calls[0][2] == (
        ["project-1"],
        1,
        ["project-1"],
        ["project-1"],
    )
    assert await reader.get_graph_stats(
        visible_project_ids=["project-1"]
    ) == {
        "entities": 0,
        "facts": 0,
        "relationships": 0,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_neighbor_ids_batch_hydrates_sets_for_all_requested_ids():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {"entity_id": "2", "neighbor_ids": ["3", "4"]},
                {"entity_id": "5", "neighbor_ids": []},
            ]
        ]
    )
    reader = GraphReader(client)

    assert await reader.get_neighbor_ids_batch(
        [2, 5, 9], visible_project_ids=["project-1"]
    ) == {
        2: {3, 4},
        5: set(),
        9: set(),
    }
    assert json.loads(client.calls[0][2][0]) == {
        "ids": [2, 5, 9],
        "visible_project_ids": ["project-1"],
        "identity_entity_id": 1,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_reader_rejects_empty_visibility_before_empty_input():
    client = RecordingPostgresClient()
    reader = GraphReader(client)

    with pytest.raises(ValueError, match="requires visible_project_ids scope"):
        await reader.get_neighbor_ids_batch([], visible_project_ids=[])

    assert client.calls == []
