import pytest

from common.schema.episode.models import EpisodeCheckpoint
from core.knowledge.db.readers.episode_reader import EpisodeReader
from core.knowledge.db.writers.episode_writer import EpisodeWriter


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_project_episode_checkpoint_does_not_skip_backdated_message_ids(
    real_postgres_client,
):
    """The cursor must use the reader's chronological sort, not max(message_id)."""

    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content, timestamp_ms
        )
        VALUES
            ('ada', 'session-1', 101, 'project-1', 'user', 'Late ID.', 3000),
            ('ada', 'session-1', 102, 'project-1', 'user', 'First.', 1000),
            ('ada', 'session-1', 103, 'project-1', 'user', 'Second.', 2000)
        """
    )
    await real_postgres_client.execute(
        """
        UPDATE messages
        SET episode_eligible = TRUE,
            ingestion_state = 'processed'
        WHERE message_id = ANY(ARRAY[101, 102, 103])
        """
    )

    reader = EpisodeReader(real_postgres_client)
    writer = EpisodeWriter(real_postgres_client)
    scope = {
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-1",
    }

    initial_checkpoint = await reader.get_episode_checkpoint(**scope)
    assert initial_checkpoint == EpisodeCheckpoint()
    first_window = await reader.get_next_project_episode_window(
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        message_count=2,
    )
    assert [message["message_id"] for message in first_window] == [102, 103]

    assert await writer.write_project_episode_window(
        [],
        first_window,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
    )
    second_checkpoint = await reader.get_episode_checkpoint(**scope)
    assert second_checkpoint == EpisodeCheckpoint(
        last_evaluated_message_id=103,
        last_evaluated_timestamp_ms=2000,
    )

    second_window = await reader.get_next_project_episode_window(
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        message_count=1,
    )
    assert [message["message_id"] for message in second_window] == [101]
