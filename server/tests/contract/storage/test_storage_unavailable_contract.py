import pytest
from psycopg import OperationalError

from common.conf.domain_config import DomainConfig
from common.exceptions import StorageReadError, StorageWriteError
from core.knowledge.db.readers.message_reader import MessageReader
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.writers.episode_writer import EpisodeWriter
from core.knowledge.db.writers.message_writer import MessageWriter
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.db.writers.relationship_reclassification_writer import (
    RelationshipReclassificationWriter,
)
from tests.fixtures.fakes import RecordingPostgresClient


def _reclassification_domain():
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"General": {"active": True}},
            "entity_types": {
                "Concept": {"topic": "General", "labels": ["concept"]},
            },
            "relationships": {
                "RELATED_TO": {
                    "source_types": ["Concept"],
                    "target_types": ["Concept"],
                },
            },
        }
    ).compile()


@pytest.mark.storage
@pytest.mark.no_network
async def test_message_read_failure_is_not_reported_as_missing_message():
    reader = MessageReader(
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
async def test_message_read_keeps_a_missing_message_as_normal_absence():
    reader = MessageReader(RecordingPostgresClient(fetch_one_results=[None]))

    assert await reader.get_message_text(
        7,
        user_name="ada",
        session_id="session-1",
        visible_project_ids=["project-1"],
    ) == ""


@pytest.mark.storage
@pytest.mark.no_network
async def test_message_search_failure_is_not_reported_as_empty_search():
    reader = MessageReader(
        RecordingPostgresClient(fetch_all_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageReadError) as error:
        await reader.search_fts(
            "release plan",
            user_name="ada",
            session_ids=["session-1"],
            visible_project_ids=["project-1"],
        )

    assert error.value.code == "storage_read_error"
    assert error.value.details["operation"] == "search_fts"


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_context_read_failure_is_not_reported_as_missing_context():
    reader = ProjectContextReader(
        RecordingPostgresClient(fetch_one_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageReadError) as error:
        await reader.get_current_revision(user_name="ada", project_id="project-1")

    assert error.value.details["operation"] == "get_current_revision"


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_context_list_failure_is_not_reported_as_empty_context():
    reader = ProjectContextReader(
        RecordingPostgresClient(fetch_all_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageReadError) as error:
        await reader.get_revision_blocks(
            user_name="ada",
            project_id="project-1",
            revision_id="00000000-0000-0000-0000-000000000001",
        )

    assert error.value.details["operation"] == "get_revision_blocks"


@pytest.mark.storage
@pytest.mark.no_network
async def test_visible_session_failure_is_not_reported_as_no_sessions():
    reader = MessageReader(
        RecordingPostgresClient(fetch_all_exceptions=[RuntimeError("database down")])
    )

    with pytest.raises(StorageReadError) as error:
        await reader.get_visible_session_ids(
            user_name="ada", visible_project_ids=["project-1"]
        )

    assert error.value.details["operation"] == "get_visible_session_ids"


@pytest.mark.storage
@pytest.mark.no_network
async def test_message_write_failure_is_standardized():
    writer = MessageWriter(
        RecordingPostgresClient(
            cursor_execute_exceptions=[OperationalError("database down")]
        )
    )

    with pytest.raises(StorageWriteError) as error:
        await writer.save_message_logs(
            [
                {
                    "id": 7,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "role": "user",
                    "content": "Hello",
                }
            ]
        )

    assert error.value.details["operation"] == "save_message_logs"


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_episode_write_failure_is_standardized():
    writer = EpisodeWriter(
        RecordingPostgresClient(
            cursor_execute_exceptions=[OperationalError("database down")]
        )
    )

    with pytest.raises(StorageWriteError) as error:
        await writer.write_project_semantic_window_episodes(
            window_id="11111111-1111-4111-8111-111111111111",
            episodes=[],
            window_messages=[{"message_id": 7, "session_id": "session-1"}],
            user_name="ada",
            project_id="project-1",
        )

    assert error.value.details["operation"] == "write_project_semantic_window_episodes"


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_deletion_write_failure_is_standardized():
    writer = ProjectDeletionWriter(
        RecordingPostgresClient(
            cursor_execute_exceptions=[OperationalError("database down")]
        )
    )

    with pytest.raises(StorageWriteError) as error:
        await writer.delete_project(user_name="ada", project_id="project-1")

    assert error.value.details["operation"] == "delete_project"


@pytest.mark.storage
@pytest.mark.no_network
async def test_relationship_reclassification_write_failure_is_standardized():
    writer = RelationshipReclassificationWriter(
        RecordingPostgresClient(
            fetch_all_exceptions=[OperationalError("database down")]
        )
    )

    with pytest.raises(StorageWriteError) as error:
        await writer.reclassify(
            user_name="ada",
            project_id="project-1",
            domain=_reclassification_domain(),
        )

    assert error.value.details["operation"] == (
        "fetch_relationship_reclassification_observations"
    )
