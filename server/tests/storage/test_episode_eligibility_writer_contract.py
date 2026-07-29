import pytest

from common.schema.contracts import EngineScope, EpisodeEligibility
from core.knowledge.db.writers.entity_writer import EntityWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_eligibility_marks_message_and_persists_optional_type():
    client = RecordingPostgresClient(
        fetch_all_results=[[{"message_id": 7}, {"message_id": 8}]]
    )
    writer = EntityWriter(client)
    scope = EngineScope(
        user_name="ada",
        session_id="session-1",
        project_id="project-1",
    )

    async with client.transaction() as cur:
        await writer._mark_episode_eligible_messages(
            cur,
            [
                EpisodeEligibility(message_id=8),
                EpisodeEligibility(message_id=7, episode_type="decision"),
            ],
            scope,
        )

    updates = [call for call in client.calls if "UPDATE messages" in call[1]]
    assert len(updates) == 2
    assert all("episode_eligible = TRUE" in call[1] for call in updates)
    assert all(
        "episode_type = COALESCE(%s, episode_type)" in call[1]
        for call in updates
    )
    assert updates[0][2] == ("decision", 7, "ada", "session-1", "project-1")
    assert updates[1][2] == (None, 8, "ada", "session-1", "project-1")
