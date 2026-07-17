from contextlib import asynccontextmanager

import pytest

from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from infrastructure.postgres_client import PostgresClient


class RecordingCursor:
    def __init__(self, *, project_exists=True, fail_on_table=None) -> None:
        self.project_exists = project_exists
        self.fail_on_table = fail_on_table
        self.calls = []
        self.rowcount = 0
        self._result = None

    async def execute(self, query, params=None) -> None:
        normalized = " ".join(query.split())
        self.calls.append((normalized, params))
        if (
            self.fail_on_table
            and f"DELETE FROM public.{self.fail_on_table}" in normalized
        ):
            raise RuntimeError("injected aggregate delete failure")
        if normalized.startswith("SELECT project_id FROM public.projects"):
            self._result = {"project_id": "project-1"} if self.project_exists else None
            self.rowcount = 1 if self.project_exists else 0
        elif normalized.startswith("DELETE FROM public.projects"):
            self._result = {"project_id": "project-1"}
            self.rowcount = 1
        else:
            self._result = None
            self.rowcount = 1

    async def fetchone(self):
        return self._result


class RecordingClient:
    def __init__(self, **cursor_kwargs) -> None:
        self.cursor = RecordingCursor(**cursor_kwargs)
        self.transaction_exits = []

    build_cypher = staticmethod(PostgresClient.build_cypher)

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
async def test_project_deletion_removes_all_relational_and_age_state_atomically():
    client = RecordingClient()
    writer = ProjectDeletionWriter(client)

    deleted = await writer.delete_project(user_name="ada", project_id="project-1")

    assert deleted is not None
    assert set(deleted) == {*writer._PROJECT_TABLES, "projects"}
    assert client.transaction_exits == ["commit"]

    queries = [query for query, _ in client.cursor.calls]
    assert queries[0].startswith("SELECT project_id FROM public.projects")
    assert any("DETACH DELETE n" in query for query in queries)
    assert any("MATCH (t:Topic)" in query for query in queries)
    for table in writer._PROJECT_TABLES:
        assert any(f"DELETE FROM public.{table}" in query for query in queries)
    assert queries[-1].startswith("DELETE FROM public.projects")


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_deletion_returns_none_without_mutating_missing_project():
    client = RecordingClient(project_exists=False)
    writer = ProjectDeletionWriter(client)

    assert await writer.delete_project(user_name="ada", project_id="missing") is None
    assert len(client.cursor.calls) == 1
    assert client.transaction_exits == ["commit"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_deletion_failure_escapes_atomic_transaction():
    client = RecordingClient(fail_on_table="episode_entities")
    writer = ProjectDeletionWriter(client)

    with pytest.raises(RuntimeError, match="injected aggregate delete failure"):
        await writer.delete_project(user_name="ada", project_id="project-1")

    assert client.transaction_exits == ["rollback"]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_project_deletion_executes_complete_aggregate_against_postgres(
    real_postgres_client,
):
    writer = ProjectDeletionWriter(real_postgres_client)
    document_id = "11111111-1111-4111-8111-111111111111"
    chunk_id = "22222222-2222-4222-8222-222222222222"
    embedding = "[" + ",".join(["0"] * 1024) + "]"

    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (
            entity_id, user_name, project_id, canonical_name, type, topic
        )
        VALUES (42, 'ada', 'project-1', 'Delete Me', 'concept', 'General')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, visibility_scope, original_name,
            relative_path, extension, size_bytes, content_hash
        )
        VALUES (%s, 'project-1', 'project', 'notes.md', 'notes.md', '.md', 5, 'hash')
        """,
        (document_id,),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_content (document_id, content)
        VALUES (%s, %s)
        """,
        (document_id, b"hello"),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_chunks (
            chunk_id, document_id, chunk_index, content, embedding
        )
        VALUES (%s, %s, 0, 'hello', %s::vector)
        """,
        (chunk_id, document_id, embedding),
    )

    deleted = await writer.delete_project(user_name="ada", project_id="project-1")

    assert deleted is not None
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.projects WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.entities WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_documents "
        "WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.document_content WHERE document_id = %s",
        (document_id,),
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.projects WHERE project_id = 'project-2'"
    ) == {"count": 1}
