from datetime import datetime, timezone
from uuid import UUID

import pytest

from common.schema.artifacts import ArtifactDraft, MarkdownArtifactBlock
from core.knowledge.db.readers.artifact_reader import ArtifactReader
from core.knowledge.db.writers.artifact_writer import ArtifactWriter
from tests.fixtures.fakes import RecordingPostgresClient


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []

    async def execute(self, query, params):
        self.calls.append((query, params))

    async def fetchone(self):
        return self.rows.pop(0)


@pytest.mark.no_network
async def test_artifact_writer_inserts_reference_and_first_revision_on_supplied_cursor():
    artifact_id = UUID("11111111-1111-1111-1111-111111111111")
    created_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    cursor = Cursor(
        [
            {
                "artifact_id": artifact_id,
                "project_id": "project-1",
                "session_id": "session-1",
                "originating_message_id": 9,
                "kind": "general",
                "title": "Reusable note",
                "status": "complete",
                "current_revision": 1,
                "created_at": created_at,
                "updated_at": created_at,
            }
        ]
    )
    artifact = ArtifactDraft(
        title="Reusable note",
        blocks=(MarkdownArtifactBlock(content="Durable content"),),
    )

    reference = await ArtifactWriter(object()).write_for_assistant_message(
        9,
        artifact,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        cursor=cursor,
    )

    assert reference.artifact_id == artifact_id
    assert reference.originating_message_id == 9
    assert len(cursor.calls) == 2
    assert "project_artifacts" in cursor.calls[0][0]
    assert "project_artifact_revisions" in cursor.calls[1][0]
    assert cursor.calls[1][1][0] == str(artifact_id)


@pytest.mark.storage
@pytest.mark.no_network
async def test_artifact_reader_resolves_message_artifact_with_full_scope():
    artifact_id = UUID("22222222-2222-2222-2222-222222222222")
    created_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    client = RecordingPostgresClient(
        fetch_one_results=[
            {
                "artifact_id": artifact_id,
                "project_id": "project-1",
                "session_id": "session-1",
                "originating_message_id": 43,
                "kind": "research_report",
                "title": "Research report",
                "status": "complete",
                "current_revision": 1,
                "created_at": created_at,
                "updated_at": created_at,
            }
        ]
    )

    artifact = await ArtifactReader(client).get_for_assistant_message(
        43,
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert artifact is not None
    assert artifact.artifact_id == artifact_id
    query, params = client.calls[0][1], client.calls[0][2]
    assert "artifact.originating_message_id = %s" in query
    assert "artifact.session_id = %s" in query
    assert params == (43, "ada", "project-1", "session-1", "ada")


@pytest.mark.storage
@pytest.mark.requires_postgres
async def test_postgres_artifact_round_trip_is_visible_through_project_and_message_reads(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-artifact', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content,
            lifecycle_state
        ) VALUES ('ada', 'session-artifact', 9001, 'project-1', 'assistant',
                  'A durable answer', 'sealed')
        """
    )
    draft = ArtifactDraft(
        kind="research_report",
        title="Durable report",
        blocks=(MarkdownArtifactBlock(content="Evidence"),),
    )
    reference = await ArtifactWriter(real_postgres_client).write_for_assistant_message(
        9001,
        draft,
        user_name="ada",
        project_id="project-1",
        session_id="session-artifact",
    )

    reader = ArtifactReader(real_postgres_client)
    assert await reader.get_artifact(
        reference.artifact_id,
        user_name="ada",
        project_id="project-1",
    ) == reference
    assert await reader.get_for_assistant_message(
        9001,
        user_name="ada",
        project_id="project-1",
        session_id="session-artifact",
    ) == reference
    revision = await reader.get_revision(
        reference.artifact_id,
        1,
        user_name="ada",
        project_id="project-1",
        session_id="session-artifact",
    )
    assert revision is not None
    assert revision.markdown == "# Durable report\n\nEvidence\n"
