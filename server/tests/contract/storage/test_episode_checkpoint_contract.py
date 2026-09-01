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
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, user_msg_id, lifecycle_state, ingestion_state
        )
        VALUES
            ('ada', 'session-1', 101, 'project-1', 'user', 'Late ID.', 3000, 101, 'sealed', 'processed'),
            ('ada', 'session-1', 104, 'project-1', 'assistant', 'Late response.', 3001, 101, 'sealed', 'excluded'),
            ('ada', 'session-1', 102, 'project-1', 'user', 'First.', 1000, 102, 'sealed', 'processed'),
            ('ada', 'session-1', 105, 'project-1', 'assistant', 'First response.', 1001, 102, 'sealed', 'excluded'),
            ('ada', 'session-1', 103, 'project-1', 'user', 'Second.', 2000, 103, 'sealed', 'processed'),
            ('ada', 'session-1', 106, 'project-1', 'assistant', 'Second response.', 2001, 103, 'sealed', 'excluded')
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
        message_count=4,
    )
    assert [message["message_id"] for message in first_window] == [102, 105, 103, 106]

    assert await writer.write_project_episode_window(
        [],
        first_window,
        user_name=scope["user_name"],
        project_id=scope["project_id"],
    )
    second_checkpoint = await reader.get_episode_checkpoint(**scope)
    assert second_checkpoint == EpisodeCheckpoint(
        last_evaluated_message_id=106,
        last_evaluated_timestamp_ms=2001,
    )

    second_window = await reader.get_next_project_episode_window(
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        message_count=2,
    )
    assert [message["message_id"] for message in second_window] == [101, 104]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_project_episode_window_advances_each_source_session_cursor(
    real_postgres_client,
):
    """One project window may preserve evidence from multiple sessions."""

    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES
            ('session-project-a', 'ada', 'project-1'),
            ('session-project-b', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, user_msg_id, lifecycle_state, ingestion_state
        )
        VALUES
            ('ada', 'session-project-a', 201, 'project-1', 'user',
             'Project session A user turn.', 1000, 201, 'sealed', 'processed'),
            ('ada', 'session-project-a', 202, 'project-1', 'assistant',
             'Project session A assistant turn.', 1001, 201, 'sealed', 'excluded'),
            ('ada', 'session-project-b', 203, 'project-1', 'user',
             'Project session B user turn.', 2000, 203, 'sealed', 'processed'),
            ('ada', 'session-project-b', 204, 'project-1', 'assistant',
             'Project session B assistant turn.', 2001, 203, 'sealed', 'excluded')
        """
    )

    reader = EpisodeReader(real_postgres_client)
    writer = EpisodeWriter(real_postgres_client)
    window = await reader.get_next_project_episode_window(
        user_name="ada",
        project_id="project-1",
        message_count=4,
    )

    assert [(message["session_id"], message["message_id"]) for message in window] == [
        ("session-project-a", 201),
        ("session-project-a", 202),
        ("session-project-b", 203),
        ("session-project-b", 204),
    ]
    assert await reader.has_ready_project_episode_window(
        user_name="ada",
        project_id="project-1",
        message_count=4,
    )
    assert await writer.write_project_episode_window(
        [],
        window,
        user_name="ada",
        project_id="project-1",
    )
    assert await reader.get_episode_checkpoint(
        user_name="ada",
        project_id="project-1",
        session_id="session-project-a",
    ) == EpisodeCheckpoint(
        last_evaluated_message_id=202,
        last_evaluated_timestamp_ms=1001,
    )
    assert await reader.get_episode_checkpoint(
        user_name="ada",
        project_id="project-1",
        session_id="session-project-b",
    ) == EpisodeCheckpoint(
        last_evaluated_message_id=204,
        last_evaluated_timestamp_ms=2001,
    )
    assert not await reader.has_ready_project_episode_window(
        user_name="ada",
        project_id="project-1",
        message_count=1,
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_user_episode_edit_marks_the_narrative_as_user_modified(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO episodes (
            episode_id, project_id, summary, source_message_count,
            user_modified, created_at, updated_at
        ) VALUES (
            'episode-user-edit', 'project-1', 'Generated summary', 0,
            FALSE, NOW(), NOW()
        )
        """
    )
    writer = EpisodeWriter(real_postgres_client)

    await writer.edit_episode(
        episode_id="episode-user-edit",
        user_name="ada",
        project_id="project-1",
        summary="User-curated summary",
        new_developments=["The user clarified the decision."],
        updates=[],
        unresolved=["The implementation date is open."],
    )

    row = await real_postgres_client.fetch_one(
        """
        SELECT summary, new_developments, unresolved, user_modified
        FROM episodes
        WHERE episode_id = 'episode-user-edit'
        """
    )
    assert row == {
        "summary": "User-curated summary",
        "new_developments": ["The user clarified the decision."],
        "unresolved": ["The implementation date is open."],
        "user_modified": True,
    }
