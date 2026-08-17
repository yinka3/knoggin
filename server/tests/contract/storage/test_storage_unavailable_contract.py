import pytest

from common.exceptions import StorageUnavailableError
from core.knowledge.db.readers.graph_reader import GraphReader
from core.knowledge.db.tool_queries import ToolQueries
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_read_failure_is_not_reported_as_missing_message():
    reader = GraphReader(
        RecordingPostgresClient(fetch_one_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageUnavailableError) as error:
        await reader.get_message_text(
            7,
            user_name="ada",
            session_id="session-1",
            visible_project_ids=["project-1"],
        )

    assert error.value.code == "storage_unavailable"
    assert error.value.details["operation"] == "get_message_text"


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_read_keeps_a_missing_message_as_normal_absence():
    reader = GraphReader(RecordingPostgresClient(fetch_one_results=[None]))

    assert await reader.get_message_text(
        7,
        user_name="ada",
        session_id="session-1",
        visible_project_ids=["project-1"],
    ) == ""


@pytest.mark.storage
@pytest.mark.no_network
async def test_tool_query_failure_is_not_reported_as_empty_search():
    queries = ToolQueries(
        RecordingPostgresClient(fetch_all_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageUnavailableError) as error:
        await queries.search_messages_fts(
            "release plan",
            user_name="ada",
            session_ids=["session-1"],
            visible_project_ids=["project-1"],
        )

    assert error.value.code == "storage_unavailable"
    assert error.value.details["operation"] == "search_messages_fts"
