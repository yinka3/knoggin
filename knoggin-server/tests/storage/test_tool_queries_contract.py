import json

import pytest

from common.scoping import IDENTITY_ENTITY_ID
from common.utils.time_utils import reset_clock, set_test_clock
from knoggin_server.knowledge.db.tool_queries import ToolQueries
from tests.fixtures.fakes import RecordingPostgresClient


class FakeToolClient:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.read_calls = []

    async def execute_read(self, query, params=None):
        self.read_calls.append((query, params))
        return self.rows


@pytest.mark.storage
@pytest.mark.no_network
def test_tool_queries_sanitizes_fts_queries():
    sanitized = ToolQueries._sanitize_fts_query('alpha + beta:"gamma"')

    assert sanitized == "alpha | beta | gamma"
    assert ToolQueries._sanitize_fts_query('?! : ""') == ""
    assert ToolQueries._sanitize_fts_query(" alpha\t(beta)\n~gamma* ") == (
        "alpha | beta | gamma"
    )
    assert ToolQueries._sanitize_fts_query(
        "alpha'); DROP TABLE message_search; -- beta"
    ) == "alpha | DROP | TABLE | message_search | beta"


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_messages_fts_adds_user_session_scope_when_provided():
    client = FakeToolClient(
        rows=[{"message_id": "4", "score": "0.5", "session_id": "s1"}]
    )
    queries = ToolQueries(client)

    result = await queries.search_messages_fts(
        "alpha beta",
        limit=7,
        user_name="ada",
        session_ids=["s1", "s2"],
    )

    assert result == [(4, 0.5, "s1")]
    sql, params = client.read_calls[0]
    assert "AND user_name = %s AND session_id = ANY(%s)" in sql
    assert params == ("alpha | beta", "alpha | beta", "ada", ["s1", "s2"], 7)


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_messages_fts_prefers_project_scope_when_provided():
    client = FakeToolClient(
        rows=[{"message_id": "4", "score": "0.5", "session_id": "s1"}]
    )
    queries = ToolQueries(client)

    result = await queries.search_messages_fts(
        "alpha beta",
        limit=7,
        user_name="ada",
        session_ids=["s1"],
        project_ids=["project-1", "archive-1"],
    )

    assert result == [(4, 0.5, "s1")]
    sql, params = client.read_calls[0]
    assert "AND user_name = %s AND project_id = ANY(%s)" in sql
    assert params == (
        "alpha | beta",
        "alpha | beta",
        "ada",
        ["project-1", "archive-1"],
        7,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_messages_fts_ignores_empty_queries():
    client = FakeToolClient()
    queries = ToolQueries(client)

    assert await queries.search_messages_fts("?!") == []
    assert client.read_calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_entity_ignores_empty_clean_query_without_db_access():
    client = RecordingPostgresClient()
    queries = ToolQueries(client)

    assert await queries.search_entity("?!") == []
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_tool_queries_skip_empty_inputs_without_db_access():
    client = RecordingPostgresClient()
    queries = ToolQueries(client)

    assert await queries.get_hot_topic_context_with_messages([]) == {}
    assert await queries.get_related_entities([]) == []
    assert await queries.get_recent_activity(" ") == []
    assert await queries.find_path_filtered("", "Grace") == ([], False)
    assert await queries.find_path_filtered("Ada", "") == ([], False)
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_messages_fts_runs_without_optional_scope_when_absent():
    client = FakeToolClient(
        rows=[{"message_id": "4", "score": "0.5", "session_id": "s1"}]
    )
    queries = ToolQueries(client)

    result = await queries.search_messages_fts("alpha beta", limit=7)

    assert result == [(4, 0.5, "s1")]
    sql, params = client.read_calls[0]
    assert "AND user_name = %s AND session_id = ANY(%s)" not in sql
    assert params == ("alpha | beta", "alpha | beta", 7)


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_entity_applies_sql_and_cypher_visible_project_scope():
    client = RecordingPostgresClient(
        execute_read_results=[
            [{"entity_id": "2"}],
            [
                {
                    "id": "2",
                    "canonical_name": '"Ada Lovelace"',
                    "aliases": ["Ada"],
                    "type": '"person"',
                    "topic": '"Identity"',
                    "last_mentioned": 123,
                    "last_updated": 456,
                    "facts": ["writes algorithms"],
                    "conn_name": None,
                    "conn_aliases": None,
                    "conn_weight": None,
                    "evidence_refs": None,
                    "conn_context": None,
                    "conn_facts": None,
                    "parent_name": None,
                    "children_count": 0,
                }
            ],
        ]
    )
    queries = ToolQueries(client)

    results = await queries.search_entity(
        "Ada",
        active_topics=["Identity"],
        limit=1,
        visible_project_ids=["project-1"],
    )

    assert results[0]["canonical_name"] == "Ada Lovelace"
    sql_call, graph_call = client.calls
    assert "AND (project_id = ANY(%s) OR entity_id = %s)" in sql_call[1]
    assert sql_call[2] == ("%Ada%", ["project-1"], IDENTITY_ENTITY_ID, 2)
    graph_params = json.loads(graph_call[2][0])
    assert graph_params["ids"] == [2]
    assert graph_params["filter_projects"] is True
    assert graph_params["visible_project_ids"] == ["project-1"]
    assert graph_params["identity_entity_id"] == IDENTITY_ENTITY_ID
    assert graph_params["filter_topics"] is True
    assert graph_params["active_topics"] == ["Identity"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_hot_topic_context_with_messages_groups_dedupes_and_scopes():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "topic": '"Work"',
                    "name": '"Ada"',
                    "aliases": ["A"],
                    "facts": ["fact 1"],
                    "msg_ids": [
                        {
                            "user_name": "ada",
                            "session_id": "s1",
                            "message_id": 1,
                        },
                        {
                            "user_name": "ada",
                            "session_id": "s1",
                            "message_id": 1,
                        },
                    ],
                },
                {
                    "topic": '"Work"',
                    "name": '"Ada"',
                    "aliases": ["Duplicate"],
                    "facts": ["duplicate"],
                    "msg_ids": [
                        {
                            "user_name": "ada",
                            "session_id": "s1",
                            "message_id": 2,
                        }
                    ],
                },
                {
                    "topic": '"Work"',
                    "name": '"Grace"',
                    "aliases": [],
                    "facts": ["fact 2"],
                    "msg_ids": ["legacy-ref"],
                },
            ]
        ]
    )
    queries = ToolQueries(client)

    result = await queries.get_hot_topic_context_with_messages(
        ["Work"],
        msg_limit=2,
        visible_project_ids=["project-1"],
    )

    assert result == {
        "Work": {
            "entities": [
                {"name": "Ada", "aliases": ["A"], "facts": ["fact 1"]},
                {"name": "Grace", "aliases": [], "facts": ["fact 2"]},
            ],
            "message_refs": [
                {"user_name": "ada", "session_id": "s1", "message_id": 1},
                {"user_name": "ada", "session_id": "s1", "message_id": 2},
            ],
        }
    }
    params = json.loads(client.calls[0][2][0])
    assert params == {
        "hot_topics": ["Work"],
        "filter_projects": True,
        "visible_project_ids": ["project-1"],
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_hot_topic_context_slim_omits_facts():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "topic": '"Work"',
                    "name": '"Ada"',
                    "aliases": [],
                    "facts": ["hidden"],
                    "msg_ids": None,
                }
            ]
        ]
    )
    queries = ToolQueries(client)

    result = await queries.get_hot_topic_context_with_messages(["Work"], slim=True)

    assert result == {
        "Work": {
            "entities": [{"name": "Ada", "aliases": []}],
            "message_refs": [],
        }
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_related_entities_applies_topic_and_project_scope():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "source": '"Ada"',
                    "target": '"Grace"',
                    "target_facts": ["compiler"],
                    "connection_strength": "2",
                    "evidence_refs": [{"message_id": 7}],
                    "confidence": "0.75",
                    "last_seen": "123",
                    "context": '"worked with"',
                }
            ]
        ]
    )
    queries = ToolQueries(client)

    result = await queries.get_related_entities(
        ["Ada"],
        active_topics=["Identity"],
        limit=3,
        visible_project_ids=["project-1"],
    )

    assert result == [
        {
            "source": "Ada",
            "target": "Grace",
            "target_facts": ["compiler"],
            "connection_strength": 2.0,
            "evidence_refs": [{"message_id": 7}],
            "confidence": 0.75,
            "last_seen": "123",
            "context": "worked with",
        }
    ]
    params = json.loads(client.calls[0][2][0])
    assert params == {
        "names": ["Ada"],
        "filter_topics": True,
        "active_topics": ["Identity"],
        "limit": 3,
        "filter_projects": True,
        "visible_project_ids": ["project-1"],
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_recent_activity_applies_filters_and_hydrates_rows():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "entity": '"Grace"',
                    "evidence_refs": [{"message_id": 7}],
                    "time": "123",
                }
            ]
        ]
    )
    queries = ToolQueries(client)
    set_test_clock(1000)

    try:
        result = await queries.get_recent_activity(
            "Ada",
            active_topics=["Identity"],
            hours=2,
            visible_project_ids=["project-1"],
        )
    finally:
        reset_clock()

    assert result == [
        {
            "entity": "Grace",
            "evidence_refs": [{"message_id": 7}],
            "time": "123",
        }
    ]
    params = json.loads(client.calls[0][2][0])
    assert params == {
        "name": "Ada",
        "cutoff": int((1000 - (2 * 3600)) * 1000),
        "filter_topics": True,
        "active_topics": ["Identity"],
        "filter_projects": True,
        "visible_project_ids": ["project-1"],
        "identity_entity_id": IDENTITY_ENTITY_ID,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_find_path_filtered_falls_back_to_active_only_path():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "names": ["Ada", "Dormant", "Grace"],
                    "node_topics": ["Identity", "Archive", "Identity"],
                    "evidence_refs": [["m1"], ["m2"]],
                    "has_inactive": True,
                }
            ],
            [
                {
                    "names": ["Ada", "Grace"],
                    "node_topics": ["Identity", "Identity"],
                    "evidence_refs": [["m3"]],
                }
            ],
        ]
    )
    queries = ToolQueries(client)

    path, filtered = await queries.find_path_filtered(
        "Ada",
        "Grace",
        active_topics=["Identity"],
        visible_project_ids=["project-1"],
    )

    assert filtered is True
    assert path == [
        {
            "step": 0,
            "entity_a": "Ada",
            "entity_b": "Grace",
            "topic_a": "Identity",
            "topic_b": "Identity",
            "evidence_refs": ["m3"],
        }
    ]
    assert len(client.calls) == 2
    shortest_params = json.loads(client.calls[0][2][0])
    active_params = json.loads(client.calls[1][2][0])
    assert shortest_params["filter_projects"] is True
    assert shortest_params["visible_project_ids"] == ["project-1"]
    assert active_params["active_topics"] == ["Identity"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_find_path_filtered_returns_shortest_path_when_no_inactive_topics():
    client = RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "names": ["Ada", "Grace"],
                    "node_topics": ["Identity", "Identity"],
                    "evidence_refs": [["m1"]],
                    "has_inactive": False,
                }
            ]
        ]
    )
    queries = ToolQueries(client)

    path, filtered = await queries.find_path_filtered(
        "Ada",
        "Grace",
        active_topics=["Identity"],
        visible_project_ids=["project-1"],
    )

    assert filtered is False
    assert path == [
        {
            "step": 0,
            "entity_a": "Ada",
            "entity_b": "Grace",
            "topic_a": "Identity",
            "topic_b": "Identity",
            "evidence_refs": ["m1"],
        }
    ]
    assert len(client.calls) == 1
    params = json.loads(client.calls[0][2][0])
    assert params["start_name"] == "Ada"
    assert params["end_name"] == "Grace"
    assert params["active_topics"] == ["Identity"]
    assert params["filter_projects"] is True


@pytest.mark.storage
@pytest.mark.no_network
async def test_find_path_filtered_returns_empty_when_no_path():
    client = RecordingPostgresClient(execute_read_results=[[]])
    queries = ToolQueries(client)

    assert await queries.find_path_filtered("Ada", "Grace") == ([], False)
    assert len(client.calls) == 1
