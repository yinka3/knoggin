CREATE SCHEMA IF NOT EXISTS public;

-- ==============================================================================
-- PROJECT STATE
-- ==============================================================================

CREATE TABLE IF NOT EXISTS public.projects (
    project_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    access_mode TEXT NOT NULL DEFAULT 'open',
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'archived', 'deleted')),
    topic_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    last_activity_at TIMESTAMPTZ,
    UNIQUE (user_name, project_id)
);

CREATE TABLE IF NOT EXISTS public.project_read_scopes (
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id) ON DELETE CASCADE,
    readable_project_id TEXT NOT NULL REFERENCES public.projects(project_id) ON DELETE CASCADE,
    PRIMARY KEY (user_name, project_id, readable_project_id),
    CHECK (project_id <> readable_project_id)
);

CREATE TABLE IF NOT EXISTS public.agents (
    agent_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT REFERENCES public.projects(project_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    persona TEXT,
    brain TEXT,
    model TEXT,
    temperature DOUBLE PRECISION,
    enabled_tools JSONB,
    is_default BOOLEAN NOT NULL DEFAULT false,
    is_spawned BOOLEAN NOT NULL DEFAULT false,
    spawned_by TEXT,
    brain_revision INTEGER NOT NULL DEFAULT 1 CHECK (brain_revision >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TABLE IF EXISTS public.agent_brain_revisions;

CREATE TABLE IF NOT EXISTS public.agent_brain_snapshots (
    agent_id TEXT NOT NULL REFERENCES public.agents(agent_id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    user_name TEXT NOT NULL,
    content TEXT NOT NULL,
    edited_by TEXT NOT NULL DEFAULT 'agent',
    change_type TEXT NOT NULL DEFAULT 'initial_seed',
    changed_section TEXT,
    change_summary TEXT NOT NULL DEFAULT 'Initial Brain',
    restored_from_revision INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (agent_id, revision)
);

CREATE INDEX IF NOT EXISTS agent_brain_snapshots_user_idx
ON public.agent_brain_snapshots(user_name, agent_id, revision DESC);

INSERT INTO public.agent_brain_snapshots (
    agent_id, revision, user_name, content, edited_by, change_type,
    change_summary
)
SELECT
    agent_id,
    brain_revision,
    user_name,
    COALESCE(brain, ''),
    'seed',
    'initial_seed',
    'Initial Brain'
FROM public.agents
ON CONFLICT (agent_id, revision) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.sessions (
    session_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id) ON DELETE CASCADE,
    model TEXT,
    agent_id TEXT REFERENCES public.agents(agent_id) ON DELETE SET NULL,
    enabled_tools JSONB,
    document_focus JSONB,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'closed', 'deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_active_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS sessions_project_idx
ON public.sessions(user_name, project_id, created_at);

-- ==============================================================================
-- KNOWLEDGE GRAPH
-- ==============================================================================

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
    project_id TEXT NOT NULL REFERENCES public.projects(project_id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    user_msg_id BIGINT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
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

-- Durable review boundary for destructive entity merges. Proposal records do
-- not use entity foreign keys because the duplicate entity is deleted after a
-- successful merge and the historical IDs must remain auditable.
CREATE TABLE IF NOT EXISTS public.entity_merge_proposals (
    proposal_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    primary_entity_id BIGINT NOT NULL,
    duplicate_entity_id BIGINT NOT NULL,
    evidence_fact_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    reasoning TEXT NOT NULL,
    model_confidence DOUBLE PRECISION,
    reviewed_state_hash TEXT NOT NULL,
    reviewed_state JSONB NOT NULL,
    policy_checks JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'confirmation_required',
    confirmation_token_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confirmed_at TIMESTAMPTZ,
    executed_at TIMESTAMPTZ,
    confirmed_by TEXT,
    failure_reason TEXT,
    CONSTRAINT entity_merge_proposals_distinct_entities
        CHECK (primary_entity_id <> duplicate_entity_id),
    CONSTRAINT entity_merge_proposals_status
        CHECK (
            status IN (
                'confirmation_required',
                'executing',
                'executed',
                'rejected',
                'failed'
            )
        )
);

CREATE INDEX IF NOT EXISTS entity_merge_proposals_project_idx
ON public.entity_merge_proposals(project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS entity_merge_proposals_status_idx
ON public.entity_merge_proposals(project_id, status);

CREATE TABLE IF NOT EXISTS public.entity_merge_audits (
    audit_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL
        REFERENCES public.entity_merge_proposals(proposal_id),
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    primary_entity_id BIGINT NOT NULL,
    duplicate_entity_id BIGINT NOT NULL,
    evidence_fact_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    reasoning TEXT NOT NULL,
    confirmed_by TEXT NOT NULL,
    before_state JSONB,
    after_state JSONB,
    status TEXT NOT NULL DEFAULT 'executing',
    failure_reason TEXT,
    rollback_status TEXT NOT NULL DEFAULT 'unavailable',
    rollback_expires_at TIMESTAMPTZ,
    rolled_back_at TIMESTAMPTZ,
    rolled_back_by TEXT,
    rollback_failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.entity_merge_audits
    ALTER COLUMN before_state DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS rollback_status TEXT NOT NULL DEFAULT 'unavailable',
    ADD COLUMN IF NOT EXISTS rollback_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rolled_back_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rolled_back_by TEXT,
    ADD COLUMN IF NOT EXISTS rollback_failure_reason TEXT;

CREATE INDEX IF NOT EXISTS entity_merge_audits_project_idx
ON public.entity_merge_audits(project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS entity_merge_audits_rollback_expiry_idx
ON public.entity_merge_audits(project_id, rollback_status, rollback_expires_at);

CREATE TABLE IF NOT EXISTS public.fact_change_audits (
    fact_change_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    session_id TEXT,
    actor TEXT NOT NULL,
    change_type TEXT NOT NULL,
    reason TEXT,
    source_msg_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    invalidated_fact_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    invalidated_fact_snapshots JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_fact_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    replacement_content TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'applied',
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fact_change_audits_change_type CHECK (
        change_type IN (
            'manual_remove',
            'manual_correction',
            'fact_merge',
            'bad_extraction_report',
            'profile_extraction',
            'admin_recovery'
        )
    ),
    CONSTRAINT fact_change_audits_status CHECK (
        status IN ('applying', 'applied', 'failed')
    )
);

CREATE INDEX IF NOT EXISTS fact_change_audits_entity_idx
ON public.fact_change_audits(user_name, project_id, entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS fact_change_audits_project_idx
ON public.fact_change_audits(user_name, project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.ingestion_candidate_suggestions (
    suggestion_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    msg_id BIGINT NOT NULL,
    mention TEXT NOT NULL,
    mention_type TEXT NOT NULL,
    mention_topic TEXT NOT NULL,
    candidate_id BIGINT NOT NULL,
    candidate_name TEXT NOT NULL,
    base_score DOUBLE PRECISION NOT NULL,
    reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_entity_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ingestion_candidate_suggestions_project_idx
ON public.ingestion_candidate_suggestions(user_name, project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ingestion_candidate_suggestions_candidate_idx
ON public.ingestion_candidate_suggestions(user_name, project_id, candidate_id);

CREATE INDEX IF NOT EXISTS ingestion_candidate_suggestions_created_entity_idx
ON public.ingestion_candidate_suggestions(user_name, project_id, created_entity_id);

-- Durable authorization and outcome trail for every model-initiated write.
CREATE TABLE IF NOT EXISTS public.agent_tool_audits (
    audit_id UUID PRIMARY KEY,
    user_name TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    capability TEXT NOT NULL,
    confirmation_state TEXT NOT NULL DEFAULT 'not_confirmed',
    arguments JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB,
    status TEXT NOT NULL,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT agent_tool_audits_capability_check CHECK (
        capability IN (
            'reversible_write',
            'configuration_write',
            'identity_write',
            'destructive_write'
        )
    ),
    CONSTRAINT agent_tool_audits_status_check CHECK (
        status IN ('started', 'succeeded', 'rejected', 'failed')
    )
);

CREATE INDEX IF NOT EXISTS agent_tool_audits_scope_idx
ON public.agent_tool_audits(
    user_name,
    project_id,
    created_at DESC
);

CREATE INDEX IF NOT EXISTS agent_tool_audits_run_idx
ON public.agent_tool_audits(run_id, created_at);

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

-- Project-owned folder upload batches.
CREATE TABLE IF NOT EXISTS public.document_folder_uploads (
    folder_root_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL,
    session_id TEXT,
    visibility_scope TEXT NOT NULL,
    folder_name TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    candidate_bytes BIGINT NOT NULL,
    document_count INTEGER NOT NULL,
    total_size_bytes BIGINT NOT NULL,
    excluded_count INTEGER NOT NULL,
    excluded_bytes BIGINT NOT NULL,
    excluded_directory_count INTEGER NOT NULL,
    excluded_reason_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    scan_settings JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    indexed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT document_folder_uploads_visibility_scope_check
        CHECK (visibility_scope IN ('project', 'session')),
    CONSTRAINT document_folder_uploads_session_visibility_check
        CHECK (visibility_scope <> 'session' OR session_id IS NOT NULL),
    CONSTRAINT document_folder_uploads_counts_check
        CHECK (
            candidate_count >= 0
            AND candidate_bytes >= 0
            AND document_count >= 0
            AND total_size_bytes >= 0
            AND excluded_count >= 0
            AND excluded_bytes >= 0
            AND excluded_directory_count >= 0
        )
);

CREATE INDEX IF NOT EXISTS document_folder_uploads_project_idx
ON public.document_folder_uploads(project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS document_folder_uploads_visibility_idx
ON public.document_folder_uploads(
    project_id,
    visibility_scope,
    session_id
);

CREATE TABLE IF NOT EXISTS public.project_document_scan_settings (
    project_id TEXT PRIMARY KEY,
    settings JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Project-owned source documents and their derived retrieval chunks.
CREATE TABLE IF NOT EXISTS public.project_documents (
    document_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL,
    session_id TEXT,
    visibility_scope TEXT NOT NULL,
    folder_root_id UUID REFERENCES public.document_folder_uploads(folder_root_id)
        ON DELETE CASCADE,
    source_kind TEXT NOT NULL DEFAULT 'manual_upload',
    original_name TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    extension TEXT NOT NULL DEFAULT '',
    size_bytes BIGINT NOT NULL,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'uploaded',
    indexed_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT project_documents_visibility_scope_check
        CHECK (visibility_scope IN ('project', 'session')),
    CONSTRAINT project_documents_session_visibility_check
        CHECK (visibility_scope <> 'session' OR session_id IS NOT NULL),
    CONSTRAINT project_documents_status_check
        CHECK (status IN ('uploaded', 'queued', 'indexing', 'indexed', 'failed')),
    CONSTRAINT project_documents_source_kind_check
        CHECK (source_kind IN ('manual_upload', 'folder_upload')),
    CONSTRAINT project_documents_folder_source_check
        CHECK (
            (source_kind = 'manual_upload' AND folder_root_id IS NULL)
            OR
            (source_kind = 'folder_upload' AND folder_root_id IS NOT NULL)
        ),
    CONSTRAINT project_documents_size_check
        CHECK (size_bytes >= 0)
);

-- Migration: drop storage_key if it exists from a previous schema version.
ALTER TABLE public.project_documents DROP COLUMN IF EXISTS storage_key;
ALTER TABLE public.project_documents
    DROP CONSTRAINT IF EXISTS project_documents_status_check;
ALTER TABLE public.project_documents
    ADD CONSTRAINT project_documents_status_check
    CHECK (status IN ('uploaded', 'queued', 'indexing', 'indexed', 'failed'));

CREATE INDEX IF NOT EXISTS project_documents_project_idx
ON public.project_documents(project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS project_documents_visibility_idx
ON public.project_documents(project_id, visibility_scope, session_id);

CREATE INDEX IF NOT EXISTS project_documents_hash_idx
ON public.project_documents(project_id, content_hash);

CREATE INDEX IF NOT EXISTS project_documents_folder_root_idx
ON public.project_documents(folder_root_id, relative_path);

-- Raw document bytes, stored separately to keep the project_documents table lean.
-- Deleted automatically when the parent project_documents row is removed.
CREATE TABLE IF NOT EXISTS public.document_content (
    document_id UUID PRIMARY KEY
        REFERENCES public.project_documents(document_id) ON DELETE CASCADE,
    content BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS public.document_chunks (
    chunk_id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES public.project_documents(document_id)
        ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    embedding vector(1024) NOT NULL,
    CONSTRAINT document_chunks_document_index_unique
        UNIQUE (document_id, chunk_index),
    CONSTRAINT document_chunks_index_check
        CHECK (chunk_index >= 0)
);

CREATE INDEX IF NOT EXISTS document_chunks_document_idx
ON public.document_chunks(document_id);

CREATE INDEX IF NOT EXISTS document_chunks_embedding_idx
ON public.document_chunks USING hnsw (embedding vector_cosine_ops);
