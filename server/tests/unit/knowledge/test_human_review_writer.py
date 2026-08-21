import pytest

from core.knowledge.db.writers.human_review_writer import HumanReviewWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.unit
@pytest.mark.no_network
async def test_open_reopens_a_stable_subject_review_and_resolve_only_changes_status():
    client = RecordingPostgresClient()
    writer = HumanReviewWriter(client)

    first_id = await writer.open(
        user_name="ada",
        project_id="project-1",
        kind="relationship_advisory",
        subject_type="relationship_advisory",
        subject_id="advisory-1",
        title="Relationship advisory",
    )
    second_id = await writer.open(
        user_name="ada",
        project_id="project-1",
        kind="relationship_advisory",
        subject_type="relationship_advisory",
        subject_id="advisory-1",
        title="Relationship advisory",
    )
    await writer.resolve(
        user_name="ada",
        project_id="project-1",
        kind="relationship_advisory",
        subject_id="advisory-1",
    )

    assert first_id == second_id
    calls = [call for call in client.calls if call[0] == "execute_command"]
    assert "ON CONFLICT (user_name, project_id, kind, subject_id)" in calls[0][1]
    assert "UPDATE public.human_reviews" in calls[2][1]
