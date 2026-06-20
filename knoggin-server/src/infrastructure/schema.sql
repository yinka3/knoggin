
CREATE SCHEMA IF NOT EXISTS public;

-- Canonical knowledge storage.
-- These tables are the long-term source of truth. AGE remains the graph
-- traversal projection, while *_search tables below remain derived indexes.
-- The application is pre-release: recreate development database volumes after
-- constraint changes instead of carrying compatibility migrations.

CREATE SEQUENCE IF NOT EXISTS public.entity_id_seq
AS BIGINT
START WITH 2
MINVALUE 2;

CREATE SEQUENCE IF NOT EXISTS public.message_id_seq
AS BIGINT
START WITH 1
MINVALUE 1;

CREATE TABLE IF NOT EXISTS public.messages (
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id BIGINT NOT NULL UNIQUE,
    project_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    timestamp_ms BIGINT,
    PRIMARY KEY (user_name, session_id, message_id)
);

CREATE INDEX IF NOT EXISTS messages_project_idx
ON public.messages(user_name, project_id, message_id);

CREATE TABLE IF NOT EXISTS public.entities (
    entity_id BIGINT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT,
    canonical_name TEXT NOT NULL,
    type TEXT,
    topic TEXT NOT NULL DEFAULT 'General',
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    last_mentioned_ms BIGINT,
    last_updated_ms BIGINT,
    last_profiled_msg_id BIGINT
);

CREATE INDEX IF NOT EXISTS entities_project_idx
ON public.entities(user_name, project_id);

CREATE INDEX IF NOT EXISTS entities_topic_idx
ON public.entities(project_id, topic);

CREATE TABLE IF NOT EXISTS public.entity_aliases (
    entity_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    alias TEXT NOT NULL,
    PRIMARY KEY (entity_id, alias)
);

CREATE INDEX IF NOT EXISTS entity_aliases_alias_idx
ON public.entity_aliases(alias);

CREATE TABLE IF NOT EXISTS public.facts (
    fact_id TEXT PRIMARY KEY,
    entity_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    content TEXT NOT NULL,
    valid_at TIMESTAMPTZ,
    invalid_at TIMESTAMPTZ,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    source_msg_id BIGINT,
    source_user_name TEXT,
    source_session_id TEXT,
    source TEXT
);

CREATE INDEX IF NOT EXISTS facts_entity_active_idx
ON public.facts(entity_id, invalid_at);

CREATE INDEX IF NOT EXISTS facts_source_message_idx
ON public.facts(source_user_name, source_session_id, source_msg_id);

CREATE TABLE IF NOT EXISTS public.relationships (
    relationship_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    entity_a_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    entity_b_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    weight INTEGER NOT NULL DEFAULT 1,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    context TEXT,
    last_seen_ms BIGINT,
    CONSTRAINT relationships_distinct_entities
        CHECK (entity_a_id <> entity_b_id)
);

CREATE INDEX IF NOT EXISTS relationships_pair_idx
ON public.relationships(project_id, entity_a_id, entity_b_id);

CREATE INDEX IF NOT EXISTS relationships_entity_a_idx
ON public.relationships(entity_a_id);

CREATE INDEX IF NOT EXISTS relationships_entity_b_idx
ON public.relationships(entity_b_id);

CREATE TABLE IF NOT EXISTS public.relationship_evidence_refs (
    relationship_id TEXT NOT NULL
        REFERENCES public.relationships(relationship_id) ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id BIGINT NOT NULL,
    PRIMARY KEY (relationship_id, user_name, session_id, message_id)
);

CREATE INDEX IF NOT EXISTS relationship_evidence_refs_message_idx
ON public.relationship_evidence_refs(user_name, session_id, message_id);

CREATE TABLE IF NOT EXISTS public.hierarchy_edges (
    project_id TEXT NOT NULL,
    parent_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    child_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    created_at_ms BIGINT,
    PRIMARY KEY (project_id, parent_id, child_id),
    CONSTRAINT hierarchy_edges_distinct_entities
        CHECK (parent_id <> child_id)
);

CREATE INDEX IF NOT EXISTS hierarchy_edges_child_idx
ON public.hierarchy_edges(project_id, child_id);

CREATE INDEX IF NOT EXISTS hierarchy_edges_parent_idx
ON public.hierarchy_edges(project_id, parent_id);

CREATE INDEX IF NOT EXISTS hierarchy_edges_parent_entity_idx
ON public.hierarchy_edges(parent_id);

CREATE INDEX IF NOT EXISTS hierarchy_edges_child_entity_idx
ON public.hierarchy_edges(child_id);

-- 4. Entity and Message Vector/FTS search (Hybrid storage for the Graph)
-- Since AGE nodes don't support pgvector indexes directly inside `agtype`,
-- we store the heavy vectors and tsvectors in standard relational tables
-- and join them against the graph using the integer `id` property.

CREATE TABLE IF NOT EXISTS public.entity_search (
    entity_id BIGINT PRIMARY KEY REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    canonical_name TEXT NOT NULL,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    embedding vector(1024)
);

CREATE INDEX IF NOT EXISTS entity_search_embedding_idx 
ON public.entity_search USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS public.message_search (
    message_id BIGINT PRIMARY KEY REFERENCES public.messages(message_id)
        ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    content_tsvector tsvector
);

-- Index for Full-Text Search on messages
CREATE INDEX IF NOT EXISTS message_search_fts_idx 
ON public.message_search USING gin (content_tsvector);

CREATE TABLE IF NOT EXISTS public.fact_search (
    fact_id TEXT PRIMARY KEY REFERENCES public.facts(fact_id)
        ON DELETE CASCADE,
    entity_id BIGINT NOT NULL,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    embedding vector(1024),
    invalid_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS fact_search_embedding_idx 
ON public.fact_search USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS entity_search_project_idx
ON public.entity_search(user_name, project_id);

CREATE INDEX IF NOT EXISTS message_search_session_idx
ON public.message_search(user_name, session_id);

CREATE INDEX IF NOT EXISTS message_search_project_idx
ON public.message_search(user_name, project_id);

CREATE INDEX IF NOT EXISTS fact_search_project_idx
ON public.fact_search(user_name, project_id);

-- Project-owned source files and their derived retrieval chunks.
CREATE TABLE IF NOT EXISTS public.project_files (
    file_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL,
    session_id TEXT,
    visibility_scope TEXT NOT NULL,
    original_name TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    extension TEXT NOT NULL DEFAULT '',
    size_bytes BIGINT NOT NULL,
    content_hash TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'uploaded',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT project_files_visibility_scope_check
        CHECK (visibility_scope IN ('project', 'session')),
    CONSTRAINT project_files_session_visibility_check
        CHECK (visibility_scope <> 'session' OR session_id IS NOT NULL),
    CONSTRAINT project_files_status_check
        CHECK (status IN ('uploaded', 'indexed', 'failed')),
    CONSTRAINT project_files_size_check
        CHECK (size_bytes >= 0)
);

CREATE INDEX IF NOT EXISTS project_files_project_idx
ON public.project_files(project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS project_files_visibility_idx
ON public.project_files(project_id, visibility_scope, session_id);

CREATE INDEX IF NOT EXISTS project_files_hash_idx
ON public.project_files(project_id, content_hash);

CREATE TABLE IF NOT EXISTS public.file_chunks (
    chunk_id UUID PRIMARY KEY,
    file_id UUID NOT NULL REFERENCES public.project_files(file_id)
        ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    embedding vector(1024) NOT NULL,
    CONSTRAINT file_chunks_file_index_unique
        UNIQUE (file_id, chunk_index),
    CONSTRAINT file_chunks_index_check
        CHECK (chunk_index >= 0)
);

CREATE INDEX IF NOT EXISTS file_chunks_file_idx
ON public.file_chunks(file_id);

CREATE INDEX IF NOT EXISTS file_chunks_embedding_idx
ON public.file_chunks USING hnsw (embedding vector_cosine_ops);
