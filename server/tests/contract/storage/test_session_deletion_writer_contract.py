from contextlib import asynccontextmanager

import pytest
from psycopg.errors import RaiseException

from core.knowledge.db.writers.session_deletion_writer import SessionDeletionWriter
from infrastructure.postgres_client import PostgresClient


class RecordingCursor:
    def __init__(self, *, fail_on_messages=False) -> None:
        self.fail_on_messages = fail_on_messages
        self.calls = []

    async def execute(self, query, params=None) -> None:
        self.calls.append((" ".join(query.split()), params))
        if self.fail_on_messages and "DELETE FROM public.messages" in query:
            raise RuntimeError("injected session message deletion failure")


class RecordingClient:
    build_cypher = staticmethod(PostgresClient.build_cypher)

    def __init__(self, **cursor_kwargs) -> None:
        self.cursor = RecordingCursor(**cursor_kwargs)
        self.transaction_exits = []

    @asynccontextmanager
    async def transaction(self):
        try:
            yield self.cursor
        except Exception:
            self.transaction_exits.append("rollback")
            raise
        else:
            self.transaction_exits.append("commit")


@pytest.mark.storage
@pytest.mark.no_network
async def test_session_deletion_tombstones_only_session_state_and_preserves_evidence():
    client = RecordingClient()
    writer = SessionDeletionWriter(client)

    await writer.delete_session(user_name="ada", session_id="session-1")

    assert client.transaction_exits == ["commit"]
    queries = [query for query, _ in client.cursor.calls]
    assert len(queries) == 1
    assert "UPDATE public.sessions" in queries[0]
    assert "status = 'deleted'" in queries[0]
    assert not any("DELETE FROM public.messages" in query for query in queries)
    assert not any("DELETE FROM public.sessions" in query for query in queries)
    assert not any("UPDATE public.messages" in query for query in queries)
    assert not any("project_documents" in query for query in queries)
    assert not any("document_chunks" in query for query in queries)
    assert not any("document_content" in query for query in queries)
    assert not any("document_folder_uploads" in query for query in queries)
    assert not any("document_workspace_sources" in query for query in queries)


@pytest.mark.storage
@pytest.mark.no_network
async def test_session_deletion_rolls_back_when_session_tombstone_fails():
    client = RecordingClient()
    writer = SessionDeletionWriter(client)

    original_execute = client.cursor.execute

    async def fail_session_tombstone(query, params=None):
        if "UPDATE public.sessions" in query and "status = 'deleted'" in query:
            raise RuntimeError("injected session tombstone failure")
        await original_execute(query, params)

    client.cursor.execute = fail_session_tombstone

    with pytest.raises(RuntimeError, match="injected session tombstone failure"):
        await writer.delete_session(user_name="ada", session_id="session-1")

    assert client.transaction_exits == ["rollback"]
    queries = [query for query, _ in client.cursor.calls]
    assert not any("UPDATE public.sessions" in query for query in queries)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_session_deletion_preserves_canonical_messages(
    real_postgres_client,
):
    writer = SessionDeletionWriter(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 101, 'project-1', 'user', 'Delete me.')
        """
    )
    await writer.delete_session(user_name="ada", session_id="session-1")

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM sessions WHERE session_id = 'session-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM messages WHERE message_id = 101"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT status FROM sessions WHERE session_id = 'session-1'"
    ) == {"status": "deleted"}

@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_session_deletion_preserves_project_library_rows(
    real_postgres_client,
):
    writer = SessionDeletionWriter(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES
            ('session-1', 'ada', 'project-1'),
            ('session-2', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_folder_uploads (
            folder_root_id, project_id, session_id, visibility_scope,
            folder_name, candidate_count, candidate_bytes, document_count,
            total_size_bytes, excluded_count, excluded_bytes,
            excluded_directory_count, scan_settings, indexed_at
        ) VALUES (
            '11111111-1111-4111-8111-111111111111', 'project-1', 'session-1',
            'session', 'Session folder', 0, 0, 0, 0, 0, 0, 0, '{}'::jsonb, NOW()
        )
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, session_id, visibility_scope, folder_root_id,
            source_kind,
            original_name, relative_path, extension, size_bytes, content_hash
        ) VALUES
            (
                '33333333-3333-4333-8333-333333333333', 'project-1', 'session-1',
                'session', '11111111-1111-4111-8111-111111111111',
                'folder_upload', 'session.txt', 'session.txt', '.txt', 7, 'session-hash'
            ),
            (
                '55555555-5555-4555-8555-555555555555', 'project-1', 'session-2',
                'session', NULL, 'manual_upload', 'other-session.txt',
                'other-session.txt', '.txt', 7, 'other-session-hash'
            ),
            (
                '66666666-6666-4666-8666-666666666666', 'project-1', NULL,
                'project', NULL, 'manual_upload', 'project-only.txt', 'project-only.txt',
                '.txt', 7, 'project-only-hash'
            )
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_content (document_id, content)
        VALUES ('33333333-3333-4333-8333-333333333333', 'session')
        """
    )

    await writer.delete_session(user_name="ada", session_id="session-1")

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.sessions WHERE session_id = 'session-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.sessions WHERE session_id = 'session-1'"
    ) == {"status": "deleted"}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_documents "
        "WHERE session_id = 'session-1' AND status <> 'deleted'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.document_content "
        "WHERE document_id = '33333333-3333-4333-8333-333333333333'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.document_folder_uploads "
        "WHERE session_id = 'session-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_documents "
        "WHERE session_id = 'session-1' AND folder_root_id IS NOT NULL"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_documents "
        "WHERE document_id IN (\n"
        "    '55555555-5555-4555-8555-555555555555',\n"
        "    '66666666-6666-4666-8666-666666666666'\n"
        ")"
    ) == {"count": 2}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_session_tombstone_rolls_back_when_durable_update_fails(
    real_postgres_client,
):
    writer = SessionDeletionWriter(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 101, 'project-1', 'user', 'Keep me.')
        """
    )
    await real_postgres_client.execute(
        """
        CREATE FUNCTION fail_session_tombstone() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'injected session deletion failure';
        END;
        $$;
        CREATE TRIGGER fail_session_tombstone_trigger
        BEFORE UPDATE ON sessions
        FOR EACH ROW EXECUTE FUNCTION fail_session_tombstone();
        """
    )
    try:
        with pytest.raises(RaiseException, match="injected session deletion failure"):
            await writer.delete_session(user_name="ada", session_id="session-1")
    finally:
        await real_postgres_client.execute(
            "DROP TRIGGER IF EXISTS fail_session_tombstone_trigger ON sessions"
        )
        await real_postgres_client.execute(
            "DROP FUNCTION IF EXISTS fail_session_tombstone()"
        )

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM sessions WHERE session_id = 'session-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM messages WHERE message_id = 101"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT status FROM sessions WHERE session_id = 'session-1'"
    ) == {"status": "open"}
