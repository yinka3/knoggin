import json
import os

import psycopg
import pytest
from psycopg.rows import dict_row

from infrastructure.postgres_client import PostgresClient

DB_URL = os.environ.get(
    "KNOGGIN_TEST_DATABASE_URL",
    "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
)
REDIS_URL = os.environ.get("KNOGGIN_TEST_REDIS_URL", "redis://localhost:6379/1")

EXPECTED_TABLES = {
    "messages",
    "entities",
    "entity_aliases",
    "facts",
    "relationships",
    "relationship_evidence_refs",
    "hierarchy_edges",
    "entity_search",
    "message_search",
    "fact_search",
    "project_documents",
    "document_folder_uploads",
    "project_document_scan_settings",
    "document_chunks",
}

EXPECTED_SEQUENCES = {
    "entity_id_seq",
    "message_id_seq",
}

EXPECTED_INDEXES = {
    "messages_project_idx",
    "entities_project_idx",
    "entities_topic_idx",
    "entity_aliases_alias_idx",
    "facts_entity_active_idx",
    "facts_source_message_idx",
    "relationships_pair_idx",
    "relationships_entity_a_idx",
    "relationships_entity_b_idx",
    "relationship_evidence_refs_message_idx",
    "hierarchy_edges_child_idx",
    "hierarchy_edges_parent_idx",
    "hierarchy_edges_parent_entity_idx",
    "hierarchy_edges_child_entity_idx",
    "entity_search_embedding_idx",
    "message_search_fts_idx",
    "fact_search_embedding_idx",
    "entity_search_project_idx",
    "message_search_session_idx",
    "message_search_project_idx",
    "fact_search_project_idx",
    "project_documents_project_idx",
    "project_documents_visibility_idx",
    "project_documents_hash_idx",
    "project_documents_folder_root_idx",
    "document_folder_uploads_project_idx",
    "document_folder_uploads_visibility_idx",
    "document_chunks_document_idx",
    "document_chunks_embedding_idx",
}


def _execute_direct_read(query, params=None, load_age=True):
    with psycopg.connect(
        DB_URL,
        autocommit=True,
        row_factory=dict_row,
    ) as conn:
        if load_age:
            conn.execute("LOAD 'age';")
            conn.execute('SET search_path = ag_catalog, "$user", public;')
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_redis
@pytest.mark.requires_pgvector
@pytest.mark.slow
async def test_real_postgres_and_redis_connections_are_available():
    import redis.asyncio as redis

    redis_client = redis.from_url(REDIS_URL)

    try:
        assert _execute_direct_read("SELECT 1 AS ok", load_age=False) == [{"ok": 1}]
        assert await redis_client.ping() is True
    finally:
        await redis_client.aclose()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
async def test_real_postgres_extensions_search_path_and_graph_are_ready():
    extension_rows = _execute_direct_read(
        "SELECT extname FROM pg_extension WHERE extname IN ('age', 'vector')",
        load_age=False,
    )
    extensions = {row["extname"] for row in extension_rows}
    assert {"age", "vector"}.issubset(extensions)

    search_path_rows = _execute_direct_read("SHOW search_path")
    assert "ag_catalog" in search_path_rows[0]["search_path"]

    graph_rows = _execute_direct_read(
        "SELECT name FROM ag_graph WHERE name = %s",
        ("knoggin_graph",),
    )
    assert graph_rows == [{"name": "knoggin_graph"}]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
async def test_real_postgres_schema_tables_and_indexes_are_present():
    table_rows = _execute_direct_read(
        """
        SELECT table_name, to_regclass('public.' || table_name) AS regclass
        FROM (
            VALUES
                ('messages'),
                ('entities'),
                ('entity_aliases'),
                ('facts'),
                ('relationships'),
                ('relationship_evidence_refs'),
                ('hierarchy_edges'),
                ('entity_search'),
                ('message_search'),
                ('fact_search'),
                ('project_documents'),
                ('document_folder_uploads'),
                ('project_document_scan_settings'),
                ('document_chunks')
        ) AS expected(table_name)
        """,
        load_age=False,
    )
    present_tables = {row["table_name"] for row in table_rows if row["regclass"]}
    missing_tables = EXPECTED_TABLES - present_tables
    assert not missing_tables, (
        "Missing expected schema tables. If this is a Docker Postgres volume "
        "created before schema.sql was mounted, recreate the volume or apply "
        f"schema.sql manually. Missing: {sorted(missing_tables)}"
    )

    sequence_rows = _execute_direct_read(
        """
        SELECT sequence_name
        FROM information_schema.sequences
        WHERE sequence_schema = 'public'
          AND sequence_name = ANY(%s)
        """,
        (list(EXPECTED_SEQUENCES),),
        load_age=False,
    )
    present_sequences = {row["sequence_name"] for row in sequence_rows}
    assert not EXPECTED_SEQUENCES - present_sequences

    index_rows = _execute_direct_read(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = ANY(%s)
        """,
        (list(EXPECTED_INDEXES),),
        load_age=False,
    )
    present_indexes = {row["indexname"] for row in index_rows}
    missing_indexes = EXPECTED_INDEXES - present_indexes
    assert not missing_indexes, (
        "Missing expected schema indexes. If tables exist but indexes are missing, "
        f"re-apply schema.sql. Missing: {sorted(missing_indexes)}"
    )

    document_column_rows = _execute_direct_read(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'project_documents'
          AND column_name IN (
              'document_id',
              'folder_root_id',
              'source_kind',
              'indexed_at',
              'error_message'
          )
        """,
        load_age=False,
    )
    assert {row["column_name"] for row in document_column_rows} == {
        "document_id",
        "folder_root_id",
        "source_kind",
        "indexed_at",
        "error_message",
    }

    folder_column_rows = _execute_direct_read(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'document_folder_uploads'
          AND column_name IN (
              'folder_root_id',
              'excluded_reason_counts',
              'scan_settings',
              'indexed_at'
          )
        """,
        load_age=False,
    )
    assert {
        row["column_name"]: row["data_type"]
        for row in folder_column_rows
    } == {
        "folder_root_id": "uuid",
        "excluded_reason_counts": "jsonb",
        "scan_settings": "jsonb",
        "indexed_at": "timestamp with time zone",
    }

    scan_settings_column_rows = _execute_direct_read(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'project_document_scan_settings'
          AND column_name IN (
              'project_id',
              'settings',
              'created_at',
              'updated_at'
          )
        """,
        load_age=False,
    )
    assert {
        row["column_name"]: row["data_type"]
        for row in scan_settings_column_rows
    } == {
        "project_id": "text",
        "settings": "jsonb",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
    }

    document_constraint_rows = _execute_direct_read(
        """
        SELECT conname
        FROM pg_constraint
        WHERE connamespace = 'public'::regnamespace
          AND conname = ANY(%s)
        """,
        (
            [
                "project_documents_visibility_scope_check",
                "project_documents_session_visibility_check",
                "project_documents_status_check",
                "project_documents_source_kind_check",
                "project_documents_folder_source_check",
                "project_documents_size_check",
                "project_documents_folder_root_id_fkey",
                "document_folder_uploads_visibility_scope_check",
                "document_folder_uploads_session_visibility_check",
                "document_folder_uploads_counts_check",
                "document_chunks_document_index_unique",
                "document_chunks_index_check",
                "document_chunks_document_id_fkey",
            ],
        ),
        load_age=False,
    )
    assert {row["conname"] for row in document_constraint_rows} == {
        "project_documents_visibility_scope_check",
        "project_documents_session_visibility_check",
        "project_documents_status_check",
        "project_documents_source_kind_check",
        "project_documents_folder_source_check",
        "project_documents_size_check",
        "project_documents_folder_root_id_fkey",
        "document_folder_uploads_visibility_scope_check",
        "document_folder_uploads_session_visibility_check",
        "document_folder_uploads_counts_check",
        "document_chunks_document_index_unique",
        "document_chunks_index_check",
        "document_chunks_document_id_fkey",
    }


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
async def test_entity_dependencies_use_cascade_foreign_keys_and_checks():
    rows = _execute_direct_read(
        """
        SELECT conname, confdeltype
        FROM pg_constraint
        WHERE connamespace = 'public'::regnamespace
          AND conname = ANY(%s)
        """,
        (
            [
                "entity_aliases_entity_id_fkey",
                "facts_entity_id_fkey",
                "relationships_entity_a_id_fkey",
                "relationships_entity_b_id_fkey",
                "relationship_evidence_refs_relationship_id_fkey",
                "hierarchy_edges_parent_id_fkey",
                "hierarchy_edges_child_id_fkey",
                "entity_search_entity_id_fkey",
                "message_search_message_id_fkey",
                "fact_search_fact_id_fkey",
                "relationships_distinct_entities",
                "hierarchy_edges_distinct_entities",
            ],
        ),
        load_age=False,
    )
    constraints = {row["conname"]: row["confdeltype"] for row in rows}
    cascade_names = {
        "entity_aliases_entity_id_fkey",
        "facts_entity_id_fkey",
        "relationships_entity_a_id_fkey",
        "relationships_entity_b_id_fkey",
        "relationship_evidence_refs_relationship_id_fkey",
        "hierarchy_edges_parent_id_fkey",
        "hierarchy_edges_child_id_fkey",
        "entity_search_entity_id_fkey",
        "message_search_message_id_fkey",
        "fact_search_fact_id_fkey",
    }
    assert cascade_names <= constraints.keys()
    assert all(constraints[name] == "c" for name in cascade_names)
    assert "relationships_distinct_entities" in constraints
    assert "hierarchy_edges_distinct_entities" in constraints


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
async def test_message_search_is_project_scoped_and_keyed_by_message():
    rows = _execute_direct_read(
        """
        SELECT
            column_name,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'message_search'
          AND column_name = 'project_id'
        """,
        load_age=False,
    )
    assert rows == [{"column_name": "project_id", "is_nullable": "NO"}]

    primary_key_rows = _execute_direct_read(
        """
        SELECT a.attname AS column_name
        FROM pg_constraint c
        JOIN pg_attribute a
          ON a.attrelid = c.conrelid
         AND a.attnum = ANY(c.conkey)
        WHERE c.conrelid = 'public.message_search'::regclass
          AND c.contype = 'p'
        """,
        load_age=False,
    )
    assert primary_key_rows == [{"column_name": "message_id"}]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
async def test_direct_entity_delete_cascades_all_sql_dependencies():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO entities (
                    entity_id, user_name, project_id, canonical_name, type, topic
                )
                VALUES
                    (-910001, 'cascade-test', 'cascade-project', 'Parent', 'concept', 'General'),
                    (-910002, 'cascade-test', 'cascade-project', 'Target', 'concept', 'General')
                """
            )
            cur.execute(
                """
                INSERT INTO entity_aliases (entity_id, alias)
                VALUES (-910002, 'Target Alias')
                """
            )
            cur.execute(
                """
                INSERT INTO entity_search (
                    entity_id, canonical_name, user_name, project_id
                )
                VALUES (-910002, 'Target', 'cascade-test', 'cascade-project')
                """
            )
            cur.execute(
                """
                INSERT INTO facts (
                    fact_id, entity_id, user_name, project_id, content
                )
                VALUES (
                    'cascade-fact', -910002, 'cascade-test',
                    'cascade-project', 'temporary fact'
                )
                """
            )
            cur.execute(
                """
                INSERT INTO fact_search (
                    fact_id, entity_id, user_name, project_id
                )
                VALUES (
                    'cascade-fact', -910002, 'cascade-test', 'cascade-project'
                )
                """
            )
            cur.execute(
                """
                INSERT INTO relationships (
                    relationship_id, user_name, project_id,
                    entity_a_id, entity_b_id
                )
                VALUES (
                    'cascade-relationship', 'cascade-test', 'cascade-project',
                    -910001, -910002
                )
                """
            )
            cur.execute(
                """
                INSERT INTO relationship_evidence_refs (
                    relationship_id, user_name, session_id, message_id
                )
                VALUES (
                    'cascade-relationship', 'cascade-test', 'cascade-session', 1
                )
                """
            )
            cur.execute(
                """
                INSERT INTO hierarchy_edges (
                    project_id, parent_id, child_id, created_at_ms
                )
                VALUES ('cascade-project', -910001, -910002, 1)
                """
            )

            cur.execute("DELETE FROM entities WHERE entity_id = -910002")

            for table, predicate in (
                ("entities", "entity_id = -910002"),
                ("entity_aliases", "entity_id = -910002"),
                ("entity_search", "entity_id = -910002"),
                ("facts", "entity_id = -910002"),
                ("fact_search", "fact_id = 'cascade-fact'"),
                ("relationships", "relationship_id = 'cascade-relationship'"),
                (
                    "relationship_evidence_refs",
                    "relationship_id = 'cascade-relationship'",
                ),
                ("hierarchy_edges", "child_id = -910002"),
            ):
                cur.execute(f"SELECT count(*) AS count FROM {table} WHERE {predicate}")
                assert cur.fetchone()["count"] == 0

            cur.execute(
                "SELECT count(*) AS count FROM entities WHERE entity_id = -910001"
            )
            assert cur.fetchone()["count"] == 1
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.slow
async def test_real_age_cypher_query_executes_through_postgres_client():
    query = PostgresClient.build_cypher("RETURN 1 AS ok", "ok agtype")

    rows = _execute_direct_read(query, (json.dumps({}),))
    assert rows
    assert rows[0]["ok"] is not None


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
async def test_real_redis_server_metadata_is_available():
    import redis.asyncio as redis

    redis_client = redis.from_url(REDIS_URL)

    try:
        assert await redis_client.ping() is True
        server_info = await redis_client.info("server")
        assert server_info["redis_version"]
    finally:
        await redis_client.aclose()
