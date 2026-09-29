import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from psycopg import OperationalError

from common.artifact_retention import (
    ARTIFACT_RETENTION_BATCH_SIZE,
    ARTIFACT_RETENTION_PERIOD,
)
from common.exceptions import StorageWriteError
from common.schema.artifacts import ArtifactDraft, MarkdownArtifactBlock
from common.schema.public import to_public_error
from core.knowledge.db.readers.artifact_reader import ArtifactReader
from core.knowledge.db.writers.artifact_retention_writer import ArtifactRetentionWriter
from core.knowledge.db.writers.artifact_writer import ArtifactWriter
from core.knowledge.db.writers.session_deletion_writer import SessionDeletionWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.no_network
@pytest.mark.parametrize("operation", ["get_artifact", "list_project_artifacts", "get_for_assistant_message", "get_revision"])
async def test_every_artifact_read_uses_owned_retention_window(operation):
    client = RecordingPostgresClient()
    reader = ArtifactReader(client)
    scope = dict(user_name="ada", project_id="p1", session_id="s1")
    artifact_id = UUID("11111111-1111-1111-1111-111111111111")
    if operation == "get_artifact":
        await reader.get_artifact(artifact_id, **scope)
    elif operation == "get_revision":
        await reader.get_revision(artifact_id, 1, **scope)
    elif operation == "get_for_assistant_message":
        await reader.get_for_assistant_message(1, **scope)
    else:
        await reader.list_project_artifacts(**scope)
    _, query, params = client.calls[0]
    assert "artifact.user_name = %s" in query
    assert "artifact.project_id = %s" in query
    assert "session.user_name = %s" in query
    assert "session.status = 'open'" in query
    assert "session.status = 'deleted'" in query
    assert "session.deleted_at > statement_timestamp() - %s::interval" in query
    assert ARTIFACT_RETENTION_PERIOD in params
    assert ARTIFACT_RETENTION_PERIOD == timedelta(days=30)


@pytest.mark.no_network
async def test_purge_is_a_bounded_scoped_transaction_deleting_only_artifact_rows():
    client = RecordingPostgresClient()
    assert await ArtifactRetentionWriter(client).purge_expired_artifacts(user_name="ada") == 1
    assert client.transaction_enters == client.transaction_exits == 1
    assert len(client.calls) == 1
    _, query, params = client.calls[0]
    assert params == dict(user_name="ada", retention=timedelta(days=30), limit=ARTIFACT_RETENTION_BATCH_SIZE)
    for owner in ("artifact", "session", "project"):
        assert f"{owner}.user_name = %(user_name)s" in query
    assert "session.status = 'deleted'" in query
    assert "session.deleted_at <= statement_timestamp() - %(retention)s::interval" in query
    assert "LIMIT %(limit)s" in query
    assert "FOR UPDATE OF artifact SKIP LOCKED" in query
    assert query.count("DELETE FROM") == 1
    assert "DELETE FROM public.project_artifacts" in query


@pytest.mark.no_network
@pytest.mark.parametrize("limit", [0, -1, 501, True, 1.0, "1"])
async def test_invalid_purge_batch_never_opens_transaction(limit):
    client = RecordingPostgresClient()
    with pytest.raises(ValueError):
        await ArtifactRetentionWriter(client).purge_expired_artifacts(user_name="ada", limit=limit)
    assert client.transaction_enters == 0


@pytest.mark.no_network
async def test_blank_purge_owner_never_opens_transaction():
    client = RecordingPostgresClient()
    with pytest.raises(ValueError):
        await ArtifactRetentionWriter(client).purge_expired_artifacts(user_name=" ")
    assert client.transaction_enters == 0


@pytest.mark.no_network
async def test_failed_purge_is_typed_and_can_retry():
    client = RecordingPostgresClient(cursor_execute_exceptions=[OperationalError("SECRET SQL")])
    writer = ArtifactRetentionWriter(client)
    with pytest.raises(StorageWriteError) as failure:
        await writer.purge_expired_artifacts(user_name="ada")
    assert "SECRET" not in to_public_error(failure.value).model_dump_json()
    assert await writer.purge_expired_artifacts(user_name="ada") == 1
    assert client.transaction_enters == client.transaction_exits == 2


@pytest.mark.no_network
@pytest.mark.parametrize("cancel", [False, True])
async def test_transaction_exit_failure_or_cancellation_never_claims_a_deleted_count(cancel):
    error = asyncio.CancelledError() if cancel else OperationalError("SECRET commit failure")

    @asynccontextmanager
    async def transaction():
        yield SimpleNamespace(execute=AsyncMock(), rowcount=3)
        raise error

    writer = ArtifactRetentionWriter(SimpleNamespace(transaction=transaction))
    with pytest.raises(asyncio.CancelledError if cancel else StorageWriteError):
        await writer.purge_expired_artifacts(user_name="ada")


async def seed_artifact(postgres, *, index, project="project-1", user="ada", deleted_days=None):
    session_id = f"artifact-session-{index}"
    await postgres.execute(
        "INSERT INTO sessions (session_id, user_name, project_id) VALUES (%s, %s, %s)",
        (session_id, user, project),
    )
    await postgres.execute(
        """INSERT INTO messages (message_id, session_id, user_name, project_id, role, content, lifecycle_state)
           VALUES (%s, %s, %s, %s, 'assistant', 'Canonical answer', 'sealed')""",
        (index, session_id, user, project),
    )
    reference = await ArtifactWriter(postgres).write_for_assistant_message(
        index, ArtifactDraft(title=f"Artifact {index}", blocks=(MarkdownArtifactBlock(content="Retained output"),)),
        user_name=user, project_id=project, session_id=session_id,
    )
    # A second historical version must expire with the identity, not become an orphan.
    await postgres.execute(
        """INSERT INTO project_artifact_revisions (
               artifact_id, revision, schema_version, kind, title, status,
               blocks, markdown, content_hash, created_at
           )
           SELECT artifact_id, 2, schema_version, kind, title, status, blocks, markdown, content_hash, created_at
           FROM project_artifact_revisions WHERE artifact_id = %s AND revision = 1""",
        (str(reference.artifact_id),),
    )
    if deleted_days is not None:
        await SessionDeletionWriter(postgres).delete_session(user_name=user, session_id=session_id)
        await postgres.execute(
            "UPDATE sessions SET deleted_at = statement_timestamp() - %s::interval WHERE session_id = %s",
            (timedelta(days=deleted_days), session_id),
        )
    return reference


@pytest.mark.requires_postgres
@pytest.mark.storage
async def test_retained_reads_and_permanent_purge_preserve_other_owned_rows(real_postgres_client):
    postgres = real_postgres_client
    opened = await seed_artifact(postgres, index=801)
    recent = await seed_artifact(postgres, index=802, deleted_days=29)
    expired = await seed_artifact(postgres, index=803, deleted_days=31)
    archived = await seed_artifact(postgres, index=804, project="project-2", deleted_days=31)
    unknown = await seed_artifact(postgres, index=805, deleted_days=31)
    await postgres.execute("UPDATE sessions SET deleted_at = NULL WHERE session_id = 'artifact-session-805'")
    await postgres.execute("UPDATE projects SET status = 'archived' WHERE project_id = 'project-2'")
    await postgres.execute(
        """INSERT INTO projects (project_id, user_name, name, domain_config)
           SELECT 'foreign', 'bob', 'Foreign', domain_config FROM projects WHERE project_id = 'project-1'"""
    )
    foreign = await seed_artifact(postgres, index=806, project="foreign", user="bob", deleted_days=31)
    await postgres.execute(
        "UPDATE project_artifacts SET created_at = statement_timestamp() - interval '100 days' WHERE artifact_id = %s",
        (str(opened.artifact_id),),
    )
    before_messages = await postgres.fetch_all("SELECT * FROM messages ORDER BY message_id")
    before_sessions = await postgres.fetch_all("SELECT * FROM sessions ORDER BY session_id")
    reader = ArtifactReader(postgres)
    scope = dict(user_name="ada", project_id="project-1")
    assert await reader.get_artifact(recent.artifact_id, **scope) is not None
    assert await reader.get_revision(recent.artifact_id, 2, **scope) is not None
    assert await reader.get_for_assistant_message(802, session_id="artifact-session-802", **scope) is not None
    assert {row.artifact_id for row in await reader.list_project_artifacts(**scope)} == {opened.artifact_id, recent.artifact_id}
    for hidden in (expired, unknown):
        assert await reader.get_artifact(hidden.artifact_id, **scope) is None
        assert await reader.get_revision(hidden.artifact_id, 1, **scope) is None
        assert await reader.get_for_assistant_message(
            hidden.originating_message_id, session_id=hidden.session_id, **scope,
        ) is None
    assert await reader.get_artifact(recent.artifact_id, user_name="bob", project_id="project-1") is None
    assert await reader.get_artifact(recent.artifact_id, user_name="ada", project_id="project-2") is None
    assert await reader.get_artifact(recent.artifact_id, session_id="artifact-session-801", **scope) is None
    writer = ArtifactRetentionWriter(postgres)
    assert await writer.purge_expired_artifacts(user_name="ada", limit=1) == 1
    assert await writer.purge_expired_artifacts(user_name="ada", limit=1) == 1
    assert await writer.purge_expired_artifacts(user_name="ada") == 0
    assert await postgres.fetch_all("SELECT * FROM messages ORDER BY message_id") == before_messages
    assert await postgres.fetch_all("SELECT * FROM sessions ORDER BY session_id") == before_sessions
    surviving = await postgres.fetch_all("SELECT artifact_id FROM project_artifacts")
    assert {str(row["artifact_id"]) for row in surviving} == {
        str(item.artifact_id) for item in (opened, recent, unknown, foreign)
    }
    for removed in (expired, archived):
        assert await postgres.fetch_all(
            "SELECT * FROM project_artifact_revisions WHERE artifact_id = %s", (str(removed.artifact_id),),
        ) == []
    revisions = await postgres.fetch_one("SELECT count(*) AS count FROM project_artifact_revisions")
    assert revisions["count"] == 8


@pytest.mark.requires_postgres
@pytest.mark.storage
async def test_repeat_session_deletion_does_not_extend_retention(real_postgres_client):
    postgres = real_postgres_client
    reference = await seed_artifact(postgres, index=811, deleted_days=30)
    before = await postgres.fetch_one("SELECT deleted_at FROM sessions WHERE session_id = %s", (reference.session_id,))
    await SessionDeletionWriter(postgres).delete_session(user_name="ada", session_id=reference.session_id)
    after = await postgres.fetch_one("SELECT deleted_at FROM sessions WHERE session_id = %s", (reference.session_id,))
    assert after == before
    assert await ArtifactReader(postgres).get_artifact(reference.artifact_id, user_name="ada", project_id="project-1") is None
    assert await ArtifactRetentionWriter(postgres).purge_expired_artifacts(user_name="ada") == 1


@pytest.mark.requires_postgres
@pytest.mark.storage
async def test_sweep_skips_locked_candidates_then_removes_them_on_retry(real_postgres_client):
    postgres = real_postgres_client
    reference = await seed_artifact(postgres, index=831, deleted_days=31)
    writer = ArtifactRetentionWriter(postgres)
    async with postgres.transaction() as cursor:
        await cursor.execute(
            "SELECT artifact_id FROM project_artifacts WHERE artifact_id = %s FOR UPDATE",
            (str(reference.artifact_id),),
        )
        assert await asyncio.wait_for(writer.purge_expired_artifacts(user_name="ada"), timeout=3) == 0
    assert await writer.purge_expired_artifacts(user_name="ada") == 1


@pytest.mark.requires_postgres
@pytest.mark.storage
async def test_revision_delete_failure_rolls_back_identity_and_retries(real_postgres_client):
    postgres = real_postgres_client
    reference = await seed_artifact(postgres, index=821, deleted_days=31)
    await postgres.execute(
        """CREATE FUNCTION fail_artifact_expiry() RETURNS trigger LANGUAGE plpgsql AS $$
           BEGIN RAISE EXCEPTION 'injected expiry failure'; END; $$;
           CREATE TRIGGER fail_artifact_expiry_trigger BEFORE DELETE ON project_artifact_revisions
           FOR EACH ROW EXECUTE FUNCTION fail_artifact_expiry();"""
    )
    try:
        with pytest.raises(StorageWriteError):
            await ArtifactRetentionWriter(postgres).purge_expired_artifacts(user_name="ada")
        assert await postgres.fetch_one("SELECT artifact_id FROM project_artifacts WHERE artifact_id = %s", (str(reference.artifact_id),))
        assert len(await postgres.fetch_all("SELECT * FROM project_artifact_revisions WHERE artifact_id = %s", (str(reference.artifact_id),))) == 2
    finally:
        await postgres.execute("DROP TRIGGER fail_artifact_expiry_trigger ON project_artifact_revisions; DROP FUNCTION fail_artifact_expiry();")
    assert await ArtifactRetentionWriter(postgres).purge_expired_artifacts(user_name="ada") == 1
