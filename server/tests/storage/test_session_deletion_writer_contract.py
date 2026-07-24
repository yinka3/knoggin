import json
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
async def test_session_deletion_removes_age_messages_and_relational_rows_together():
    client = RecordingClient()
    writer = SessionDeletionWriter(client)

    await writer.delete_session(user_name="ada", session_id="session-1")

    assert client.transaction_exits == ["commit"]
    queries = [query for query, _ in client.cursor.calls]
    assert "MATCH (m:Message)" in queries[0]
    assert "DETACH DELETE m" in queries[0]
    assert "DELETE FROM public.project_documents" in queries[1]
    assert "DELETE FROM public.document_folder_uploads" in queries[2]
    assert "DELETE FROM public.document_workspace_sources" in queries[3]
    assert "DELETE FROM public.messages" in queries[4]
    assert "DELETE FROM public.sessions" in queries[5]


@pytest.mark.storage
@pytest.mark.no_network
async def test_session_deletion_rolls_back_when_age_or_relational_cleanup_fails():
    client = RecordingClient(fail_on_messages=True)
    writer = SessionDeletionWriter(client)

    with pytest.raises(RuntimeError, match="injected session message deletion failure"):
        await writer.delete_session(user_name="ada", session_id="session-1")

    assert client.transaction_exits == ["rollback"]
    queries = [query for query, _ in client.cursor.calls]
    assert not any("DELETE FROM public.sessions" in query for query in queries)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_session_deletion_removes_canonical_and_age_message_projection(
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
    async with real_postgres_client.transaction() as cur:
        await writer.projection.project_messages(
            cur,
            [
                {
                    "id": 101,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "role": "user",
                    "content": "Delete me.",
                    "timestamp": 1,
                }
            ],
        )

    await writer.delete_session(user_name="ada", session_id="session-1")

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM sessions WHERE session_id = 'session-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM messages WHERE message_id = 101"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        real_postgres_client.build_cypher(
            """
            MATCH (m:Message)
            WHERE m.user_name = $user_name
              AND m.session_id = $session_id
            RETURN count(m)
            """,
            "count agtype",
        ),
        (json.dumps({"user_name": "ada", "session_id": "session-1"}),),
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_session_deletion_removes_all_document_roots_with_its_session_id(
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
        INSERT INTO public.document_workspace_sources (
            source_id, project_id, session_id, visibility_scope, display_name
        ) VALUES (
            '22222222-2222-4222-8222-222222222222', 'project-1', 'session-1',
            'session', 'Session workspace'
        )
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, session_id, visibility_scope, source_kind,
            original_name, relative_path, extension, size_bytes, content_hash
        ) VALUES
            (
                '33333333-3333-4333-8333-333333333333', 'project-1', 'session-1',
                'session', 'manual_upload', 'session.txt', 'session.txt', '.txt', 7,
                'session-hash'
            ),
            (
                '44444444-4444-4444-8444-444444444444', 'project-1', 'session-1',
                'project', 'manual_upload', 'project-visible.txt',
                'project-visible.txt', '.txt', 7, 'project-visible-hash'
            ),
            (
                '55555555-5555-4555-8555-555555555555', 'project-1', 'session-2',
                'session', 'manual_upload', 'other-session.txt',
                'other-session.txt', '.txt', 7, 'other-session-hash'
            ),
            (
                '66666666-6666-4666-8666-666666666666', 'project-1', NULL,
                'project', 'manual_upload', 'project-only.txt', 'project-only.txt',
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
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_documents "
        "WHERE session_id = 'session-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.document_content "
        "WHERE document_id = '33333333-3333-4333-8333-333333333333'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.document_folder_uploads "
        "WHERE session_id = 'session-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.document_workspace_sources "
        "WHERE session_id = 'session-1'"
    ) == {"count": 0}
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
async def test_session_deletion_rolls_back_age_cleanup_with_relational_failure(
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
    async with real_postgres_client.transaction() as cur:
        await writer.projection.project_messages(
            cur,
            [
                {
                    "id": 101,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "role": "user",
                    "content": "Keep me.",
                    "timestamp": 1,
                }
            ],
        )

    await real_postgres_client.execute(
        """
        CREATE FUNCTION fail_session_message_delete() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'injected session deletion failure';
        END;
        $$;
        CREATE TRIGGER fail_session_message_delete_trigger
        BEFORE DELETE ON messages
        FOR EACH ROW EXECUTE FUNCTION fail_session_message_delete();
        """
    )
    try:
        with pytest.raises(RaiseException, match="injected session deletion failure"):
            await writer.delete_session(user_name="ada", session_id="session-1")
    finally:
        await real_postgres_client.execute(
            "DROP TRIGGER IF EXISTS fail_session_message_delete_trigger ON messages"
        )
        await real_postgres_client.execute(
            "DROP FUNCTION IF EXISTS fail_session_message_delete()"
        )

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM sessions WHERE session_id = 'session-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM messages WHERE message_id = 101"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        real_postgres_client.build_cypher(
            """
            MATCH (m:Message)
            WHERE m.user_name = $user_name
              AND m.session_id = $session_id
            RETURN count(m)
            """,
            "count agtype",
        ),
        (json.dumps({"user_name": "ada", "session_id": "session-1"}),),
    ) == {"count": 1}
