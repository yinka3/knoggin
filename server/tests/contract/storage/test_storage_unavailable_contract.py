import pytest

from common.exceptions import StorageReadError, StorageWriteError
from core.knowledge.db.readers.graph_reader import GraphReader
from core.knowledge.db.tool_queries import ToolQueries
from core.knowledge.db.writers.graph_writer import GraphWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_read_failure_is_not_reported_as_missing_message():
    reader = GraphReader(
        RecordingPostgresClient(fetch_one_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageReadError) as error:
        await reader.get_message_text(
            7,
            user_name="ada",
            session_id="session-1",
            visible_project_ids=["project-1"],
        )

    assert error.value.code == "storage_read_error"
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

    with pytest.raises(StorageReadError) as error:
        await queries.search_messages_fts(
            "release plan",
            user_name="ada",
            session_ids=["session-1"],
            visible_project_ids=["project-1"],
        )

    assert error.value.code == "storage_read_error"
    assert error.value.details["operation"] == "search_messages_fts"


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_write_failure_is_not_reported_as_false_result():
    writer = GraphWriter(
        RecordingPostgresClient(cursor_execute_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageWriteError) as error:
        await writer.delete_relationship(
            2,
            3,
            relationship_type="related_to",
            project_id="project-1",
        )

    assert error.value.code == "storage_write_error"
    assert error.value.details["operation"] == "delete_relationship"
