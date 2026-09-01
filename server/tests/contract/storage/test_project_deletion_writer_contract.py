from contextlib import asynccontextmanager

import pytest

from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from infrastructure.postgres_client import PostgresClient


class RecordingCursor:
    def __init__(self, *, project_exists=True, fail_on_operation=None) -> None:
        self.project_exists = project_exists
        self.fail_on_operation = fail_on_operation
        self.calls = []
        self.rowcount = 0
        self._result = None
        self._results = []

    async def execute(self, query, params=None) -> None:
        normalized = " ".join(query.split())
        self.calls.append((normalized, params))
        if self.fail_on_operation and self.fail_on_operation in normalized:
            raise RuntimeError("injected project delete failure")
        if normalized.startswith("SELECT project_id FROM public.projects"):
            self._result = {"project_id": "project-1"} if self.project_exists else None
            self.rowcount = 1 if self.project_exists else 0
        elif normalized.startswith("DELETE FROM public.projects"):
            self._result = {"project_id": "project-1"}
            self.rowcount = 1
        elif normalized.startswith("SELECT entity_id FROM public.project_entity_contexts"):
            self._results = [{"entity_id": 42}]
            self.rowcount = 1
        elif normalized.startswith("DELETE FROM public.entities"):
            self._results = [{"entity_id": 42}]
            self.rowcount = 1
        else:
            self._result = None
            self.rowcount = 1

    async def fetchone(self):
        return self._result

    async def fetchall(self):
        return list(self._results)


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
async def test_project_deletion_uses_one_project_cascade_root_atomically():
    client = RecordingClient()
    writer = ProjectDeletionWriter(client)

    deleted = await writer.delete_project(user_name="ada", project_id="project-1")

    assert deleted is not None
    assert set(deleted) == {"entities", "projects"}
    assert client.transaction_exits == ["commit"]

    queries = [query for query, _ in client.cursor.calls]
    assert queries[0].startswith("SELECT project_id FROM public.projects")
    assert any("RELATED_TO" in query for query in queries)
    assert any("project_entity_contexts" in query for query in queries)
    assert any("DELETE FROM public.entities" in query for query in queries)
    assert not any("DELETE FROM public.messages" in query for query in queries)
    assert not any("DELETE FROM public.project_documents" in query for query in queries)
    assert any(query.startswith("DELETE FROM public.projects") for query in queries)


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
    client = RecordingClient(fail_on_operation="DELETE FROM public.entities")
    writer = ProjectDeletionWriter(client)

    with pytest.raises(RuntimeError, match="injected project delete failure"):
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
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (42, 'ada', 'Delete Me')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-1', 42, 'ada', 'concept', 'General')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, original_name,
            relative_path, extension, size_bytes, content_hash
        )
        VALUES (%s, 'project-1', 'notes.md', 'notes.md', '.md', 5, 'hash')
        """,
        (document_id,),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_extractions (
            document_id, extracted_text, extracted_content_hash
        ) VALUES (%s, %s, 'hash')
        """,
        (document_id, "hello"),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_chunks (
            chunk_id, document_id, chunk_index, content, relative_path, embedding
        )
        VALUES (%s, %s, 0, 'hello', 'notes.md', %s::vector)
        """,
        (chunk_id, document_id, embedding),
    )

    deleted = await writer.delete_project(user_name="ada", project_id="project-1")

    assert deleted is not None
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.projects WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_entity_contexts WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_documents "
        "WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.document_extractions WHERE document_id = %s",
        (document_id,),
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.projects WHERE project_id = 'project-2'"
    ) == {"count": 1}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_project_deletion_removes_episode_graph_search_and_source_aggregates(
    real_postgres_client,
):
    """Project deletion removes derived aggregates without touching a sibling project."""

    writer = ProjectDeletionWriter(real_postgres_client)
    embedding = "[" + ",".join(["0"] * 1024) + "]"

    async with real_postgres_client.transaction() as cur:
        await cur.execute(
            """
            INSERT INTO sessions (session_id, user_name, project_id)
            VALUES ('session-1', 'ada', 'project-1'), ('session-2', 'ada', 'project-2')
            """
        )
        await cur.execute(
            """
            INSERT INTO messages (
                user_name, session_id, message_id, project_id, role, content,
                timestamp_ms
            ) VALUES
                ('ada', 'session-1', 101, 'project-1', 'user', 'Delete project one', 1000),
                ('ada', 'session-2', 201, 'project-2', 'user', 'Keep project two', 2000, TRUE)
            """
        )
        await cur.execute(
            """
            INSERT INTO entities (entity_id, user_name, canonical_name) VALUES
                (42, 'ada', 'Project One'),
                (43, 'ada', 'Project One Detail'),
                (52, 'ada', 'Project Two'),
                (53, 'ada', 'Project Two Detail'),
                (60, 'ada', 'Shared identity');
            INSERT INTO project_entity_contexts (
                project_id, entity_id, user_name, entity_type, topic
            ) VALUES
                ('project-1', 42, 'ada', 'concept', 'General'),
                ('project-1', 43, 'ada', 'concept', 'General'),
                ('project-2', 52, 'ada', 'concept', 'General'),
                ('project-2', 53, 'ada', 'concept', 'General'),
                ('project-1', 60, 'ada', 'concept', 'General'),
                ('project-2', 60, 'ada', 'concept', 'General')
            """
        )
        await cur.execute(
            """
            INSERT INTO entity_aliases (entity_id, alias)
            VALUES (42, 'P1'), (52, 'P2'), (60, 'Shared')
            """
        )
        await cur.execute(
            """
        INSERT INTO relationships (
            relationship_id, user_name, project_id, entity_a_id, entity_b_id,
            relationship_type
        ) VALUES
            ('project-1:42:43:related', 'ada', 'project-1', 42, 43, 'related'),
            ('project-2:52:53:related', 'ada', 'project-2', 52, 53, 'related')
            """
        )
        await cur.execute(
            """
            INSERT INTO relationship_observations (
                relationship_id, project_id, user_name, session_id, message_id,
                source_entity_id, target_entity_id, observed_relationship_label,
                observed_at_ms
            ) VALUES
                (
                    'project-1:42:43:related', 'project-1', 'ada', 'session-1',
                    101, 42, 43, 'related to', 1000
                ),
                (
                    'project-2:52:53:related', 'project-2', 'ada', 'session-2',
                    201, 52, 53, 'related to', 2000
                )
            """
        )
        await cur.execute(
            """
            INSERT INTO episodes (
                episode_id, project_id, summary, source_message_count,
                first_message_at, last_message_at, created_at, updated_at
            ) VALUES
                (
                    'episode-1', 'project-1', 'Delete project one episode', 1,
                    TIMESTAMPTZ '2026-01-01 00:00:01+00', TIMESTAMPTZ '2026-01-01 00:00:01+00',
                    TIMESTAMPTZ '2026-01-01 00:00:01+00', TIMESTAMPTZ '2026-01-01 00:00:01+00'
                ),
                (
                    'episode-2', 'project-2', 'Keep project two episode', 1,
                    TIMESTAMPTZ '2026-01-02 00:00:01+00', TIMESTAMPTZ '2026-01-02 00:00:01+00',
                    TIMESTAMPTZ '2026-01-02 00:00:01+00', TIMESTAMPTZ '2026-01-02 00:00:01+00'
                )
            """
        )
        await cur.execute(
            """
            INSERT INTO episode_messages (
                episode_id, project_id, session_id, message_id,
                influence_weight, message_position
            ) VALUES
                ('episode-1', 'project-1', 'session-1', 101, 1.0, 0),
                ('episode-2', 'project-2', 'session-2', 201, 1.0, 0)
            """
        )
        await cur.execute(
            """
            INSERT INTO episode_entities (
                episode_id, project_id, entity_id, prominence_weight,
                is_focus_entity, source_message_count
            ) VALUES
                ('episode-1', 'project-1', 42, 1.0, TRUE, 1),
                ('episode-2', 'project-2', 52, 1.0, TRUE, 1),
                ('episode-2', 'project-2', 60, 0.5, FALSE, 1)
            """
        )
        await cur.execute(
            """
            INSERT INTO episode_relationships (
                episode_id, project_id, relationship_id, prominence_weight,
                is_central_relationship, source_message_count
            ) VALUES
                ('episode-1', 'project-1', 'project-1:42:43:related', 1.0, TRUE, 1),
                ('episode-2', 'project-2', 'project-2:52:53:related', 1.0, TRUE, 1)
            """
        )
        await cur.execute(
            """
            INSERT INTO episode_processing_checkpoints (
                project_id, session_id, last_evaluated_message_id
            ) VALUES ('project-1', 'session-1', 101), ('project-2', 'session-2', 201)
            """
        )
        await cur.execute(
            """
            INSERT INTO project_documents (
                document_id, project_id, original_name,
                relative_path, extension, size_bytes, content_hash
            ) VALUES
                (
                    '77777777-7777-4777-8777-777777777777', 'project-1',
                    'delete.md', 'delete.md', '.md', 6, repeat('1', 64)
                ),
                (
                    '88888888-8888-4888-8888-888888888888', 'project-2',
                    'keep.md', 'keep.md', '.md', 4, repeat('2', 64)
                )
            """
        )
        await cur.execute(
            """
            INSERT INTO document_extractions (
                document_id, extracted_text, extracted_content_hash
            )
            VALUES
                ('77777777-7777-4777-8777-777777777777', 'delete', repeat('1', 64)),
                ('88888888-8888-4888-8888-888888888888', 'keep', repeat('2', 64))
            """
        )
        await cur.execute(
            """
            INSERT INTO document_chunks (
                chunk_id, document_id, chunk_index, content, relative_path, embedding
            ) VALUES
                (
                    '99999999-9999-4999-8999-999999999999',
                    '77777777-7777-4777-8777-777777777777', 0, 'delete', 'delete.md', %s::vector
                ),
                (
                    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
                    '88888888-8888-4888-8888-888888888888', 0, 'keep', 'keep.md', %s::vector
                )
            """,
            (embedding, embedding),
        )
        await cur.execute(
            """
            INSERT INTO message_source_refs (
                source_ref_id, project_id, session_id, message_id, source_kind,
                document_id, source_project_id, content_hash, locator, excerpt, metadata, encounter_kind,
                agent_run_id, tool_call_id, result_position, idempotency_key
            ) VALUES
                (
                    'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 'project-1', 'session-1', 101,
                    'text_document', '77777777-7777-4777-8777-777777777777', 'project-1', repeat('1', 64),
                    '{"kind":"text_lines","start_line":1,"end_line":1}'::jsonb,
                    'delete', '{"document_name":"delete.md"}'::jsonb, 'document_search',
                    'run-delete', 'tool-delete', 0, 'source-delete'
                ),
                (
                    'cccccccc-cccc-4ccc-8ccc-cccccccccccc', 'project-2', 'session-2', 201,
                    'text_document', '88888888-8888-4888-8888-888888888888', 'project-2', repeat('2', 64),
                    '{"kind":"text_lines","start_line":1,"end_line":1}'::jsonb,
                    'keep', '{"document_name":"keep.md"}'::jsonb, 'document_search',
                    'run-keep', 'tool-keep', 0, 'source-keep'
                )
            """
        )

    deleted = await writer.delete_project(user_name="ada", project_id="project-1")

    assert deleted is not None
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM projects WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM projects WHERE project_id = 'project-2'"
    ) == {"count": 1}

    project_one_queries = {
        "sessions": "SELECT count(*) AS count FROM sessions WHERE project_id = 'project-1'",
        "messages": "SELECT count(*) AS count FROM messages WHERE project_id = 'project-1'",
        "episodes": "SELECT count(*) AS count FROM episodes WHERE project_id = 'project-1'",
        "episode_messages": "SELECT count(*) AS count FROM episode_messages WHERE project_id = 'project-1'",
        "episode_entities": "SELECT count(*) AS count FROM episode_entities WHERE project_id = 'project-1'",
        "episode_relationships": "SELECT count(*) AS count FROM episode_relationships WHERE project_id = 'project-1'",
        "checkpoints": "SELECT count(*) AS count FROM episode_processing_checkpoints WHERE project_id = 'project-1'",
        "contexts": "SELECT count(*) AS count FROM project_entity_contexts WHERE project_id = 'project-1'",
        "relationships": "SELECT count(*) AS count FROM relationships WHERE project_id = 'project-1'",
        "relationship_observations": "SELECT count(*) AS count FROM relationship_observations WHERE project_id = 'project-1'",
        "source_refs": "SELECT count(*) AS count FROM message_source_refs WHERE project_id = 'project-1'",
        "documents": "SELECT count(*) AS count FROM project_documents WHERE project_id = 'project-1'",
        "chunks": (
            "SELECT count(*) AS count FROM document_chunks c "
            "JOIN project_documents d ON d.document_id = c.document_id "
            "WHERE d.project_id = 'project-1'"
        ),
        "extractions": (
            "SELECT count(*) AS count FROM document_extractions c "
            "JOIN project_documents d ON d.document_id = c.document_id "
            "WHERE d.project_id = 'project-1'"
        ),
        "aliases": (
            "SELECT count(*) AS count FROM entity_aliases a "
            "JOIN project_entity_contexts context ON context.entity_id = a.entity_id "
            "WHERE context.project_id = 'project-1'"
        ),
    }
    for query in project_one_queries.values():
        assert await real_postgres_client.fetch_one(query) == {"count": 0}

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities WHERE entity_id = 60"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM project_entity_contexts "
        "WHERE entity_id = 60 AND project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM project_entity_contexts "
        "WHERE entity_id = 60 AND project_id = 'project-2'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM episode_entities "
        "WHERE entity_id = 60 AND project_id = 'project-2'"
    ) == {"count": 1}

    project_two_queries = {
        "sessions": "SELECT count(*) AS count FROM sessions WHERE project_id = 'project-2'",
        "messages": "SELECT count(*) AS count FROM messages WHERE project_id = 'project-2'",
        "episodes": "SELECT count(*) AS count FROM episodes WHERE project_id = 'project-2'",
        "episode_messages": "SELECT count(*) AS count FROM episode_messages WHERE project_id = 'project-2'",
        "episode_entities": "SELECT count(*) AS count FROM episode_entities WHERE project_id = 'project-2'",
        "episode_relationships": "SELECT count(*) AS count FROM episode_relationships WHERE project_id = 'project-2'",
        "checkpoints": "SELECT count(*) AS count FROM episode_processing_checkpoints WHERE project_id = 'project-2'",
        "contexts": "SELECT count(*) AS count FROM project_entity_contexts WHERE project_id = 'project-2'",
        "relationships": "SELECT count(*) AS count FROM relationships WHERE project_id = 'project-2'",
        "relationship_observations": "SELECT count(*) AS count FROM relationship_observations WHERE project_id = 'project-2'",
        "source_refs": "SELECT count(*) AS count FROM message_source_refs WHERE project_id = 'project-2'",
        "documents": "SELECT count(*) AS count FROM project_documents WHERE project_id = 'project-2'",
        "chunks": (
            "SELECT count(*) AS count FROM document_chunks c "
            "JOIN project_documents d ON d.document_id = c.document_id "
            "WHERE d.project_id = 'project-2'"
        ),
        "extractions": (
            "SELECT count(*) AS count FROM document_extractions c "
            "JOIN project_documents d ON d.document_id = c.document_id "
            "WHERE d.project_id = 'project-2'"
        ),
        "aliases": (
            "SELECT count(*) AS count FROM entity_aliases a "
            "JOIN project_entity_contexts context ON context.entity_id = a.entity_id "
            "WHERE context.project_id = 'project-2'"
        ),
    }
    for name, query in project_two_queries.items():
        expected_count = {
            "contexts": 3,
            "episode_entities": 2,
            "aliases": 2,
        }.get(name, 1)
        assert await real_postgres_client.fetch_one(query) == {"count": expected_count}
