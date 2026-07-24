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

CREATE UNIQUE INDEX IF NOT EXISTS agents_one_default_per_user_idx
ON public.agents(user_name)
WHERE is_default;

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
    deleted_at TIMESTAMPTZ,
    CONSTRAINT sessions_id_project_key UNIQUE (session_id, project_id)
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
    episode_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    episode_type TEXT,
    PRIMARY KEY (user_name, session_id, message_id),
    CONSTRAINT messages_scope_project_key
        UNIQUE (user_name, session_id, message_id, project_id),
    CONSTRAINT messages_id_project_session_key
        UNIQUE (message_id, project_id, session_id),
    CONSTRAINT messages_session_project_fk
        FOREIGN KEY (session_id, project_id)
        REFERENCES public.sessions(session_id, project_id)
        ON DELETE CASCADE
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
    last_profiled_msg_id BIGINT,
    CONSTRAINT entities_id_project_key UNIQUE (entity_id, project_id),
    CONSTRAINT entities_id_user_project_key
        UNIQUE (entity_id, user_name, project_id)
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

CREATE TABLE IF NOT EXISTS public.message_entity_refs (
    message_id BIGINT NOT NULL REFERENCES public.messages(message_id)
        ON DELETE CASCADE,
    entity_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    PRIMARY KEY (message_id, entity_id)
);

CREATE INDEX IF NOT EXISTS message_entity_refs_entity_idx
ON public.message_entity_refs(entity_id, message_id);

CREATE TABLE IF NOT EXISTS public.relationships (
    relationship_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    entity_a_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    entity_b_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    relationship_type TEXT,
    weight INTEGER NOT NULL DEFAULT 1,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    context TEXT,
    last_seen_ms BIGINT,
    CONSTRAINT relationships_distinct_entities
        CHECK (entity_a_id <> entity_b_id),
    CONSTRAINT relationships_id_project_key UNIQUE (relationship_id, project_id)
);

CREATE INDEX IF NOT EXISTS relationships_pair_idx
ON public.relationships(project_id, entity_a_id, entity_b_id);

CREATE INDEX IF NOT EXISTS relationships_entity_a_idx
ON public.relationships(entity_a_id);

CREATE INDEX IF NOT EXISTS relationships_entity_b_idx
ON public.relationships(entity_b_id);

CREATE TABLE IF NOT EXISTS public.relationship_evidence_refs (
    relationship_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id BIGINT NOT NULL,
    PRIMARY KEY (relationship_id, user_name, session_id, message_id),
    CONSTRAINT relationship_evidence_refs_relationship_project_fk
        FOREIGN KEY (relationship_id, project_id)
        REFERENCES public.relationships(relationship_id, project_id)
        ON DELETE CASCADE,
    CONSTRAINT relationship_evidence_refs_message_scope_project_fk
        FOREIGN KEY (user_name, session_id, message_id, project_id)
        REFERENCES public.messages(user_name, session_id, message_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS relationship_evidence_refs_message_idx
ON public.relationship_evidence_refs(user_name, session_id, message_id);

ALTER TABLE public.relationship_evidence_refs
ADD COLUMN IF NOT EXISTS project_id TEXT;

UPDATE public.relationship_evidence_refs ref
SET project_id = relationship.project_id
FROM public.relationships relationship
WHERE relationship.relationship_id = ref.relationship_id
  AND ref.project_id IS NULL;

ALTER TABLE public.relationship_evidence_refs
ALTER COLUMN project_id SET NOT NULL;

ALTER TABLE public.relationship_evidence_refs
DROP CONSTRAINT IF EXISTS relationship_evidence_refs_relationship_id_fkey,
DROP CONSTRAINT IF EXISTS relationship_evidence_refs_message_scope_fk;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'messages_scope_project_key'
          AND conrelid = 'public.messages'::regclass
    ) THEN
        ALTER TABLE public.messages
        ADD CONSTRAINT messages_scope_project_key
        UNIQUE (user_name, session_id, message_id, project_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'relationships_id_project_key'
          AND conrelid = 'public.relationships'::regclass
    ) THEN
        ALTER TABLE public.relationships
        ADD CONSTRAINT relationships_id_project_key
        UNIQUE (relationship_id, project_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'relationship_evidence_refs_relationship_project_fk'
          AND conrelid = 'public.relationship_evidence_refs'::regclass
    ) THEN
        ALTER TABLE public.relationship_evidence_refs
        ADD CONSTRAINT relationship_evidence_refs_relationship_project_fk
        FOREIGN KEY (relationship_id, project_id)
        REFERENCES public.relationships(relationship_id, project_id)
        ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'relationship_evidence_refs_message_scope_project_fk'
          AND conrelid = 'public.relationship_evidence_refs'::regclass
    ) THEN
        ALTER TABLE public.relationship_evidence_refs
        ADD CONSTRAINT relationship_evidence_refs_message_scope_project_fk
        FOREIGN KEY (user_name, session_id, message_id, project_id)
        REFERENCES public.messages(user_name, session_id, message_id, project_id)
        ON DELETE CASCADE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.episodes (
    episode_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id)
        ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    summary TEXT NOT NULL,
    new_developments JSONB NOT NULL DEFAULT '[]'::jsonb,
    updates JSONB NOT NULL DEFAULT '[]'::jsonb,
    unresolved JSONB NOT NULL DEFAULT '[]'::jsonb,
    importance DOUBLE PRECISION NOT NULL DEFAULT 0.0
        CHECK (importance >= 0.0 AND importance <= 1.0),
    search_tsvector TSVECTOR GENERATED ALWAYS AS (
        to_tsvector(
            'simple',
            summary || ' ' || new_developments::text || ' ' || updates::text
            || ' ' || unresolved::text
        )
    ) STORED,
    source_message_count INTEGER NOT NULL DEFAULT 0
        CHECK (source_message_count >= 0),
    first_message_at TIMESTAMPTZ,
    last_message_at TIMESTAMPTZ,
    embedding vector(1024),
    generator_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    version_history JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT episodes_scope_key UNIQUE (episode_id, project_id, session_id),
    CONSTRAINT episodes_id_project_key UNIQUE (episode_id, project_id),
    CONSTRAINT episodes_session_project_fk
        FOREIGN KEY (session_id, project_id)
        REFERENCES public.sessions(session_id, project_id)
        ON DELETE CASCADE
);

ALTER TABLE public.relationships
ADD COLUMN IF NOT EXISTS relationship_type TEXT;

ALTER TABLE public.episodes
ADD COLUMN IF NOT EXISTS embedding vector(1024);

ALTER TABLE public.episodes
ADD COLUMN IF NOT EXISTS search_tsvector TSVECTOR GENERATED ALWAYS AS (
    to_tsvector(
        'simple',
        summary || ' ' || new_developments::text || ' ' || updates::text
        || ' ' || unresolved::text
    )
) STORED;

ALTER TABLE public.episodes
ADD COLUMN IF NOT EXISTS source_message_count INTEGER NOT NULL DEFAULT 0
    CHECK (source_message_count >= 0),
ADD COLUMN IF NOT EXISTS first_message_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_message_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS version_history JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS episodes_session_updated_idx
ON public.episodes(project_id, session_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS episodes_embedding_idx
ON public.episodes USING hnsw (embedding vector_cosine_ops)
WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS episodes_search_tsvector_idx
ON public.episodes USING GIN (search_tsvector);

CREATE TABLE IF NOT EXISTS public.episode_messages (
    episode_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id BIGINT NOT NULL,
    influence_weight DOUBLE PRECISION NOT NULL DEFAULT 0.0
        CHECK (influence_weight >= 0.0),
    influence_reason TEXT,
    message_position INTEGER NOT NULL CHECK (message_position >= 0),
    attached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (episode_id, message_id),
    UNIQUE (episode_id, message_position),
    CONSTRAINT episode_messages_episode_scope_fk
        FOREIGN KEY (episode_id, project_id, session_id)
        REFERENCES public.episodes(episode_id, project_id, session_id)
        ON DELETE CASCADE,
    CONSTRAINT episode_messages_message_scope_fk
        FOREIGN KEY (message_id, project_id, session_id)
        REFERENCES public.messages(message_id, project_id, session_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS episode_messages_message_idx
ON public.episode_messages(message_id, episode_id);

ALTER TABLE public.episode_messages
ADD COLUMN IF NOT EXISTS attached_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE public.episode_messages
ADD COLUMN IF NOT EXISTS project_id TEXT,
ADD COLUMN IF NOT EXISTS session_id TEXT;

UPDATE public.episode_messages episode_message
SET project_id = episode.project_id,
    session_id = episode.session_id
FROM public.episodes episode
WHERE episode.episode_id = episode_message.episode_id
  AND (
      episode_message.project_id IS NULL
      OR episode_message.session_id IS NULL
  );

ALTER TABLE public.episode_messages
ALTER COLUMN project_id SET NOT NULL,
ALTER COLUMN session_id SET NOT NULL;

ALTER TABLE public.episode_messages
DROP CONSTRAINT IF EXISTS episode_messages_episode_id_fkey,
DROP CONSTRAINT IF EXISTS episode_messages_message_id_fkey;

ALTER TABLE public.episodes
DROP CONSTRAINT IF EXISTS episodes_session_id_fkey;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'sessions_id_project_key'
          AND conrelid = 'public.sessions'::regclass
    ) THEN
        ALTER TABLE public.sessions
        ADD CONSTRAINT sessions_id_project_key
        UNIQUE (session_id, project_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'messages_id_project_session_key'
          AND conrelid = 'public.messages'::regclass
    ) THEN
        ALTER TABLE public.messages
        ADD CONSTRAINT messages_id_project_session_key
        UNIQUE (message_id, project_id, session_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'messages_session_project_fk'
          AND conrelid = 'public.messages'::regclass
    ) THEN
        ALTER TABLE public.messages
        ADD CONSTRAINT messages_session_project_fk
        FOREIGN KEY (session_id, project_id)
        REFERENCES public.sessions(session_id, project_id)
        ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episodes_scope_key'
          AND conrelid = 'public.episodes'::regclass
    ) THEN
        ALTER TABLE public.episodes
        ADD CONSTRAINT episodes_scope_key
        UNIQUE (episode_id, project_id, session_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episodes_id_project_key'
          AND conrelid = 'public.episodes'::regclass
    ) THEN
        ALTER TABLE public.episodes
        ADD CONSTRAINT episodes_id_project_key
        UNIQUE (episode_id, project_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'entities_id_project_key'
          AND conrelid = 'public.entities'::regclass
    ) THEN
        ALTER TABLE public.entities
        ADD CONSTRAINT entities_id_project_key
        UNIQUE (entity_id, project_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episodes_session_project_fk'
          AND conrelid = 'public.episodes'::regclass
    ) THEN
        ALTER TABLE public.episodes
        ADD CONSTRAINT episodes_session_project_fk
        FOREIGN KEY (session_id, project_id)
        REFERENCES public.sessions(session_id, project_id)
        ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episode_messages_episode_scope_fk'
          AND conrelid = 'public.episode_messages'::regclass
    ) THEN
        ALTER TABLE public.episode_messages
        ADD CONSTRAINT episode_messages_episode_scope_fk
        FOREIGN KEY (episode_id, project_id, session_id)
        REFERENCES public.episodes(episode_id, project_id, session_id)
        ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episode_messages_message_scope_fk'
          AND conrelid = 'public.episode_messages'::regclass
    ) THEN
        ALTER TABLE public.episode_messages
        ADD CONSTRAINT episode_messages_message_scope_fk
        FOREIGN KEY (message_id, project_id, session_id)
        REFERENCES public.messages(message_id, project_id, session_id)
        ON DELETE CASCADE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.episode_entities (
    episode_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    prominence_weight DOUBLE PRECISION NOT NULL DEFAULT 0.0
        CHECK (prominence_weight >= 0.0),
    role TEXT,
    is_focus_entity BOOLEAN NOT NULL DEFAULT FALSE,
    source_message_count INTEGER NOT NULL DEFAULT 0
        CHECK (source_message_count >= 0),
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    PRIMARY KEY (episode_id, entity_id),
    CONSTRAINT episode_entities_episode_project_fk
        FOREIGN KEY (episode_id, project_id)
        REFERENCES public.episodes(episode_id, project_id)
        ON DELETE CASCADE,
    CONSTRAINT episode_entities_entity_project_fk
        FOREIGN KEY (entity_id, project_id)
        REFERENCES public.entities(entity_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS episode_entities_focus_lookup_idx
ON public.episode_entities(entity_id, is_focus_entity, episode_id);

CREATE INDEX IF NOT EXISTS episode_entities_lookup_idx
ON public.episode_entities(entity_id, episode_id);

ALTER TABLE public.episode_entities
ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;

ALTER TABLE public.episode_entities
ADD COLUMN IF NOT EXISTS project_id TEXT;

UPDATE public.episode_entities episode_entity
SET project_id = episode.project_id
FROM public.episodes episode
WHERE episode.episode_id = episode_entity.episode_id
  AND episode_entity.project_id IS NULL;

ALTER TABLE public.episode_entities
ALTER COLUMN project_id SET NOT NULL;

ALTER TABLE public.episode_entities
DROP CONSTRAINT IF EXISTS episode_entities_episode_id_fkey,
DROP CONSTRAINT IF EXISTS episode_entities_entity_id_fkey;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episode_entities_episode_project_fk'
          AND conrelid = 'public.episode_entities'::regclass
    ) THEN
        ALTER TABLE public.episode_entities
        ADD CONSTRAINT episode_entities_episode_project_fk
        FOREIGN KEY (episode_id, project_id)
        REFERENCES public.episodes(episode_id, project_id)
        ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episode_entities_entity_project_fk'
          AND conrelid = 'public.episode_entities'::regclass
    ) THEN
        ALTER TABLE public.episode_entities
        ADD CONSTRAINT episode_entities_entity_project_fk
        FOREIGN KEY (entity_id, project_id)
        REFERENCES public.entities(entity_id, project_id)
        ON DELETE CASCADE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.episode_relationships (
    episode_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    relationship_id TEXT NOT NULL,
    prominence_weight DOUBLE PRECISION NOT NULL DEFAULT 0.0
        CHECK (prominence_weight >= 0.0),
    is_central_relationship BOOLEAN NOT NULL DEFAULT FALSE,
    source_message_count INTEGER NOT NULL DEFAULT 0
        CHECK (source_message_count >= 0),
    PRIMARY KEY (episode_id, relationship_id),
    CONSTRAINT episode_relationships_episode_project_fk
        FOREIGN KEY (episode_id, project_id)
        REFERENCES public.episodes(episode_id, project_id)
        ON DELETE CASCADE,
    CONSTRAINT episode_relationships_relationship_project_fk
        FOREIGN KEY (relationship_id, project_id)
        REFERENCES public.relationships(relationship_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS episode_relationships_lookup_idx
ON public.episode_relationships(relationship_id, episode_id);

ALTER TABLE public.episode_relationships
ADD COLUMN IF NOT EXISTS project_id TEXT;

UPDATE public.episode_relationships episode_relationship
SET project_id = episode.project_id
FROM public.episodes episode
WHERE episode.episode_id = episode_relationship.episode_id
  AND episode_relationship.project_id IS NULL;

ALTER TABLE public.episode_relationships
ALTER COLUMN project_id SET NOT NULL;

ALTER TABLE public.episode_relationships
DROP CONSTRAINT IF EXISTS episode_relationships_episode_id_fkey,
DROP CONSTRAINT IF EXISTS episode_relationships_relationship_id_fkey;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episode_relationships_episode_project_fk'
          AND conrelid = 'public.episode_relationships'::regclass
    ) THEN
        ALTER TABLE public.episode_relationships
        ADD CONSTRAINT episode_relationships_episode_project_fk
        FOREIGN KEY (episode_id, project_id)
        REFERENCES public.episodes(episode_id, project_id)
        ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episode_relationships_relationship_project_fk'
          AND conrelid = 'public.episode_relationships'::regclass
    ) THEN
        ALTER TABLE public.episode_relationships
        ADD CONSTRAINT episode_relationships_relationship_project_fk
        FOREIGN KEY (relationship_id, project_id)
        REFERENCES public.relationships(relationship_id, project_id)
        ON DELETE CASCADE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.episode_processing_checkpoints (
    project_id TEXT NOT NULL REFERENCES public.projects(project_id)
        ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES public.sessions(session_id)
        ON DELETE CASCADE,
    last_evaluated_message_id BIGINT NOT NULL DEFAULT 0
        CHECK (last_evaluated_message_id >= 0),
    last_evaluated_timestamp_ms BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, session_id)
);

ALTER TABLE public.episode_processing_checkpoints
ADD COLUMN IF NOT EXISTS last_evaluated_timestamp_ms BIGINT;

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
    evidence_message_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence_episode_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
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
    evidence_message_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence_episode_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
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

ALTER TABLE public.entity_merge_proposals
    ADD COLUMN IF NOT EXISTS evidence_message_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS evidence_episode_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS reviewed_state_hash TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS reviewed_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS policy_checks JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS confirmation_token_hash TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS executed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS confirmed_by TEXT,
    ADD COLUMN IF NOT EXISTS failure_reason TEXT;

ALTER TABLE public.entity_merge_audits
    ALTER COLUMN before_state DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS evidence_message_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS evidence_episode_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS rollback_status TEXT NOT NULL DEFAULT 'unavailable',
    ADD COLUMN IF NOT EXISTS rollback_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rolled_back_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rolled_back_by TEXT,
    ADD COLUMN IF NOT EXISTS rollback_failure_reason TEXT;

CREATE INDEX IF NOT EXISTS entity_merge_audits_project_idx
ON public.entity_merge_audits(project_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS entity_merge_audits_proposal_uidx
ON public.entity_merge_audits(proposal_id);

CREATE INDEX IF NOT EXISTS entity_merge_audits_rollback_expiry_idx
ON public.entity_merge_audits(project_id, rollback_status, rollback_expires_at);

-- ==============================================================================
-- DOMAIN INVARIANTS
-- ==============================================================================

ALTER TABLE public.entities
    DROP CONSTRAINT IF EXISTS entities_confidence_range_check;
ALTER TABLE public.entities
    ADD CONSTRAINT entities_confidence_range_check
    CHECK (confidence >= 0.0 AND confidence <= 1.0);

ALTER TABLE public.relationships
    DROP CONSTRAINT IF EXISTS relationships_weight_positive_check,
    DROP CONSTRAINT IF EXISTS relationships_confidence_range_check;
ALTER TABLE public.relationships
    ADD CONSTRAINT relationships_weight_positive_check CHECK (weight >= 1),
    ADD CONSTRAINT relationships_confidence_range_check
        CHECK (confidence >= 0.0 AND confidence <= 1.0);

ALTER TABLE public.entity_merge_audits
    DROP CONSTRAINT IF EXISTS entity_merge_audits_status_check,
    DROP CONSTRAINT IF EXISTS entity_merge_audits_rollback_status_check;
ALTER TABLE public.entity_merge_audits
    ADD CONSTRAINT entity_merge_audits_status_check
        CHECK (status IN ('executing', 'executed', 'failed')),
    ADD CONSTRAINT entity_merge_audits_rollback_status_check
        CHECK (rollback_status IN ('unavailable', 'available', 'rolled_back', 'expired', 'failed'));

ALTER TABLE public.agent_tool_audits
    DROP CONSTRAINT IF EXISTS agent_tool_audits_confirmation_state_check;
ALTER TABLE public.agent_tool_audits
    ADD CONSTRAINT agent_tool_audits_confirmation_state_check
        CHECK (confirmation_state IN ('not_confirmed', 'confirmed'));

ALTER TABLE public.episode_processing_checkpoints
    DROP CONSTRAINT IF EXISTS episode_processing_checkpoints_session_id_fkey;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'episode_processing_checkpoints_session_project_fk'
          AND conrelid = 'public.episode_processing_checkpoints'::regclass
    ) THEN
        ALTER TABLE public.episode_processing_checkpoints
        ADD CONSTRAINT episode_processing_checkpoints_session_project_fk
        FOREIGN KEY (session_id, project_id)
        REFERENCES public.sessions(session_id, project_id)
        ON DELETE CASCADE;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION public.enforce_message_entity_ref_scope()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    message_user_name TEXT;
    message_project_id TEXT;
BEGIN
    SELECT user_name, project_id
    INTO message_user_name, message_project_id
    FROM public.messages
    WHERE message_id = NEW.message_id;

    IF NOT FOUND OR NOT EXISTS (
        SELECT 1
        FROM public.entities entity
        WHERE entity.entity_id = NEW.entity_id
          AND entity.user_name = message_user_name
          AND (
              entity.project_id = message_project_id
              OR (
                  entity.entity_id = 1
                  AND entity.project_id = '__identity__'
              )
          )
    ) THEN
        RAISE EXCEPTION
            'message entity reference must use an entity visible to its message scope'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS message_entity_refs_scope_trigger
    ON public.message_entity_refs;
CREATE TRIGGER message_entity_refs_scope_trigger
BEFORE INSERT OR UPDATE OF message_id, entity_id
ON public.message_entity_refs
FOR EACH ROW EXECUTE FUNCTION public.enforce_message_entity_ref_scope();

CREATE OR REPLACE FUNCTION public.enforce_relationship_scope()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.entities entity
        WHERE entity.entity_id = NEW.entity_a_id
          AND entity.user_name = NEW.user_name
          AND (
              entity.project_id = NEW.project_id
              OR (
                  entity.entity_id = 1
                  AND entity.project_id = '__identity__'
              )
          )
    ) OR NOT EXISTS (
        SELECT 1
        FROM public.entities entity
        WHERE entity.entity_id = NEW.entity_b_id
          AND entity.user_name = NEW.user_name
          AND (
              entity.project_id = NEW.project_id
              OR (
                  entity.entity_id = 1
                  AND entity.project_id = '__identity__'
              )
          )
    ) THEN
        RAISE EXCEPTION
            'relationship endpoints must belong to the relationship user and project scope'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS relationships_scope_trigger ON public.relationships;
CREATE TRIGGER relationships_scope_trigger
BEFORE INSERT OR UPDATE OF user_name, project_id, entity_a_id, entity_b_id
ON public.relationships
FOR EACH ROW EXECUTE FUNCTION public.enforce_relationship_scope();

CREATE OR REPLACE FUNCTION public.enforce_hierarchy_edge_invariants()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    creates_cycle BOOLEAN;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.entities entity
        JOIN public.projects project ON project.project_id = NEW.project_id
        WHERE entity.entity_id = NEW.parent_id
          AND entity.project_id = NEW.project_id
          AND entity.user_name = project.user_name
    ) OR NOT EXISTS (
        SELECT 1
        FROM public.entities entity
        JOIN public.projects project ON project.project_id = NEW.project_id
        WHERE entity.entity_id = NEW.child_id
          AND entity.project_id = NEW.project_id
          AND entity.user_name = project.user_name
    ) THEN
        RAISE EXCEPTION
            'hierarchy endpoints must belong to the hierarchy project scope'
            USING ERRCODE = '23514';
    END IF;

    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.project_id, 0));

    IF TG_OP = 'UPDATE' THEN
        WITH RECURSIVE descendants(entity_id) AS (
            SELECT edge.child_id
            FROM public.hierarchy_edges edge
            WHERE edge.project_id = NEW.project_id
              AND edge.parent_id = NEW.child_id
              AND (edge.project_id, edge.parent_id, edge.child_id)
                  IS DISTINCT FROM (OLD.project_id, OLD.parent_id, OLD.child_id)

            UNION

            SELECT edge.child_id
            FROM public.hierarchy_edges edge
            JOIN descendants ON edge.parent_id = descendants.entity_id
            WHERE edge.project_id = NEW.project_id
              AND (edge.project_id, edge.parent_id, edge.child_id)
                  IS DISTINCT FROM (OLD.project_id, OLD.parent_id, OLD.child_id)
        )
        SELECT EXISTS (
            SELECT 1 FROM descendants WHERE entity_id = NEW.parent_id
        ) INTO creates_cycle;
    ELSE
        WITH RECURSIVE descendants(entity_id) AS (
            SELECT child_id
            FROM public.hierarchy_edges
            WHERE project_id = NEW.project_id
              AND parent_id = NEW.child_id

            UNION

            SELECT edge.child_id
            FROM public.hierarchy_edges edge
            JOIN descendants ON edge.parent_id = descendants.entity_id
            WHERE edge.project_id = NEW.project_id
        )
        SELECT EXISTS (
            SELECT 1 FROM descendants WHERE entity_id = NEW.parent_id
        ) INTO creates_cycle;
    END IF;

    IF creates_cycle THEN
        RAISE EXCEPTION 'hierarchy edge would create a cycle'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS hierarchy_edges_invariants_trigger
    ON public.hierarchy_edges;
CREATE TRIGGER hierarchy_edges_invariants_trigger
BEFORE INSERT OR UPDATE OF project_id, parent_id, child_id
ON public.hierarchy_edges
FOR EACH ROW EXECUTE FUNCTION public.enforce_hierarchy_edge_invariants();

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
    embedding vector(1024),
    CONSTRAINT entity_search_entity_scope_fk
        FOREIGN KEY (entity_id, user_name, project_id)
        REFERENCES public.entities(entity_id, user_name, project_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS entity_search_embedding_idx 
ON public.entity_search USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS public.message_search (
    message_id BIGINT PRIMARY KEY REFERENCES public.messages(message_id)
        ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    content_tsvector tsvector,
    CONSTRAINT message_search_message_scope_fk
        FOREIGN KEY (user_name, session_id, message_id, project_id)
        REFERENCES public.messages(user_name, session_id, message_id, project_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);

-- Index for Full-Text Search on messages
CREATE INDEX IF NOT EXISTS message_search_fts_idx 
ON public.message_search USING gin (content_tsvector);

CREATE INDEX IF NOT EXISTS entity_search_project_idx
ON public.entity_search(user_name, project_id);

CREATE INDEX IF NOT EXISTS message_search_session_idx
ON public.message_search(user_name, session_id);

CREATE INDEX IF NOT EXISTS message_search_project_idx
ON public.message_search(user_name, project_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'entities_id_user_project_key'
          AND conrelid = 'public.entities'::regclass
    ) THEN
        ALTER TABLE public.entities
        ADD CONSTRAINT entities_id_user_project_key
        UNIQUE (entity_id, user_name, project_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'entity_search_entity_scope_fk'
          AND conrelid = 'public.entity_search'::regclass
    ) THEN
        ALTER TABLE public.entity_search
        ADD CONSTRAINT entity_search_entity_scope_fk
        FOREIGN KEY (entity_id, user_name, project_id)
        REFERENCES public.entities(entity_id, user_name, project_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'message_search_message_scope_fk'
          AND conrelid = 'public.message_search'::regclass
    ) THEN
        ALTER TABLE public.message_search
        ADD CONSTRAINT message_search_message_scope_fk
        FOREIGN KEY (user_name, session_id, message_id, project_id)
        REFERENCES public.messages(user_name, session_id, message_id, project_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION public.enforce_entity_search_canonical_name()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    canonical_entity_name TEXT;
BEGIN
    -- A canonical scope change cascades into this projection before the
    -- canonical entity's AFTER trigger synchronizes its copied name.
    IF TG_OP = 'UPDATE'
       AND NEW.canonical_name IS NOT DISTINCT FROM OLD.canonical_name THEN
        RETURN NEW;
    END IF;

    SELECT canonical_name
    INTO canonical_entity_name
    FROM public.entities
    WHERE entity_id = NEW.entity_id;

    IF NOT FOUND OR NEW.canonical_name IS DISTINCT FROM canonical_entity_name THEN
        RAISE EXCEPTION
            'entity search canonical name must match its canonical entity'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS entity_search_canonical_name_trigger
    ON public.entity_search;
CREATE TRIGGER entity_search_canonical_name_trigger
BEFORE INSERT OR UPDATE OF entity_id, canonical_name, user_name, project_id
ON public.entity_search
FOR EACH ROW EXECUTE FUNCTION public.enforce_entity_search_canonical_name();

CREATE OR REPLACE FUNCTION public.sync_entity_search_canonical_name()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE public.entity_search
    SET canonical_name = NEW.canonical_name
    WHERE entity_id = NEW.entity_id
      AND canonical_name IS DISTINCT FROM NEW.canonical_name;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS entities_search_projection_sync_trigger
    ON public.entities;
CREATE TRIGGER entities_search_projection_sync_trigger
AFTER UPDATE OF canonical_name
ON public.entities
FOR EACH ROW EXECUTE FUNCTION public.sync_entity_search_canonical_name();

-- Search rebuilds generate embeddings outside a transaction.  These revisions
-- let the publisher reject a snapshot when its canonical inputs changed while
-- embedding was in progress, instead of overwriting the newer derived index.
CREATE TABLE IF NOT EXISTS public.project_search_revisions (
    project_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0)
);

CREATE TABLE IF NOT EXISTS public.identity_search_revisions (
    user_name TEXT PRIMARY KEY,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0)
);

CREATE OR REPLACE FUNCTION public.bump_search_index_revision()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    changed_project_id TEXT;
    changed_user_name TEXT;
    changed_entity_id BIGINT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        changed_project_id := OLD.project_id;
        IF TG_TABLE_NAME = 'entities' THEN
            changed_entity_id := OLD.entity_id;
            changed_user_name := OLD.user_name;
        END IF;
    ELSE
        changed_project_id := NEW.project_id;
        IF TG_TABLE_NAME = 'entities' THEN
            changed_entity_id := NEW.entity_id;
            changed_user_name := NEW.user_name;
        END IF;
    END IF;

    -- The reserved identity entity is shared by every project search rebuild.
    IF TG_TABLE_NAME = 'entities' AND changed_entity_id = 1 THEN
        INSERT INTO public.identity_search_revisions (user_name, revision)
        VALUES (changed_user_name, 1)
        ON CONFLICT (user_name) DO UPDATE
        SET revision = identity_search_revisions.revision + 1;
        RETURN NULL;
    END IF;

    INSERT INTO public.project_search_revisions (project_id, revision)
    VALUES (changed_project_id, 1)
    ON CONFLICT (project_id) DO UPDATE
    SET revision = project_search_revisions.revision + 1;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS messages_search_revision_trigger ON public.messages;
CREATE TRIGGER messages_search_revision_trigger
AFTER INSERT OR DELETE OR UPDATE OF content ON public.messages
FOR EACH ROW EXECUTE FUNCTION public.bump_search_index_revision();

DROP TRIGGER IF EXISTS entities_search_revision_trigger ON public.entities;
CREATE TRIGGER entities_search_revision_trigger
AFTER INSERT OR DELETE OR UPDATE OF canonical_name, type ON public.entities
FOR EACH ROW EXECUTE FUNCTION public.bump_search_index_revision();

DROP TRIGGER IF EXISTS episodes_search_revision_trigger ON public.episodes;
CREATE TRIGGER episodes_search_revision_trigger
AFTER INSERT OR DELETE
    OR UPDATE OF summary, new_developments, updates, unresolved ON public.episodes
FOR EACH ROW EXECUTE FUNCTION public.bump_search_index_revision();

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

-- A durable identity for a synchronizable local workspace.  Folder uploads
-- remain immutable snapshots; a workspace source will later own repeated
-- manifest syncs and incremental indexing.
CREATE TABLE IF NOT EXISTS public.document_workspace_sources (
    source_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL,
    session_id TEXT,
    visibility_scope TEXT NOT NULL,
    display_name TEXT NOT NULL,
    last_synced_at TIMESTAMPTZ,
    last_manifest_candidate_count INTEGER NOT NULL DEFAULT 0,
    last_manifest_included_count INTEGER NOT NULL DEFAULT 0,
    last_manifest_excluded_count INTEGER NOT NULL DEFAULT 0,
    last_manifest_excluded_reason_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT document_workspace_sources_visibility_scope_check
        CHECK (visibility_scope IN ('project', 'session')),
    CONSTRAINT document_workspace_sources_session_visibility_check
        CHECK (visibility_scope <> 'session' OR session_id IS NOT NULL),
    CONSTRAINT document_workspace_sources_manifest_counts_check
        CHECK (
            last_manifest_candidate_count >= 0
            AND last_manifest_included_count >= 0
            AND last_manifest_excluded_count >= 0
        )
);

ALTER TABLE public.document_workspace_sources
    ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ;
ALTER TABLE public.document_workspace_sources
    ADD COLUMN IF NOT EXISTS last_manifest_candidate_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE public.document_workspace_sources
    ADD COLUMN IF NOT EXISTS last_manifest_included_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE public.document_workspace_sources
    ADD COLUMN IF NOT EXISTS last_manifest_excluded_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE public.document_workspace_sources
    ADD COLUMN IF NOT EXISTS last_manifest_excluded_reason_counts JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE public.document_workspace_sources
    DROP CONSTRAINT IF EXISTS document_workspace_sources_manifest_counts_check;
ALTER TABLE public.document_workspace_sources
    ADD CONSTRAINT document_workspace_sources_manifest_counts_check
    CHECK (
        last_manifest_candidate_count >= 0
        AND last_manifest_included_count >= 0
        AND last_manifest_excluded_count >= 0
    );

CREATE INDEX IF NOT EXISTS document_workspace_sources_project_idx
ON public.document_workspace_sources(project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS document_workspace_sources_visibility_idx
ON public.document_workspace_sources(
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
    source_id UUID REFERENCES public.document_workspace_sources(source_id)
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
        CHECK (source_kind IN ('manual_upload', 'folder_upload', 'workspace')),
    CONSTRAINT project_documents_folder_source_check
        CHECK (
            (
                source_kind = 'manual_upload'
                AND folder_root_id IS NULL
                AND source_id IS NULL
            )
            OR
            (
                source_kind = 'folder_upload'
                AND folder_root_id IS NOT NULL
                AND source_id IS NULL
            )
            OR
            (
                source_kind = 'workspace'
                AND folder_root_id IS NULL
                AND source_id IS NOT NULL
            )
        ),
    CONSTRAINT project_documents_size_check
        CHECK (size_bytes >= 0)
);

-- Migration: drop storage_key if it exists from a previous schema version.
ALTER TABLE public.project_documents DROP COLUMN IF EXISTS storage_key;
ALTER TABLE public.project_documents
    ADD COLUMN IF NOT EXISTS source_id UUID
        REFERENCES public.document_workspace_sources(source_id) ON DELETE CASCADE;
ALTER TABLE public.project_documents
    DROP CONSTRAINT IF EXISTS project_documents_status_check;
ALTER TABLE public.project_documents
    DROP CONSTRAINT IF EXISTS project_documents_source_kind_check;
ALTER TABLE public.project_documents
    DROP CONSTRAINT IF EXISTS project_documents_folder_source_check;
ALTER TABLE public.project_documents
    ADD CONSTRAINT project_documents_status_check
    CHECK (status IN ('uploaded', 'queued', 'indexing', 'indexed', 'failed'));
ALTER TABLE public.project_documents
    ADD CONSTRAINT project_documents_source_kind_check
    CHECK (source_kind IN ('manual_upload', 'folder_upload', 'workspace'));
ALTER TABLE public.project_documents
    ADD CONSTRAINT project_documents_folder_source_check
    CHECK (
        (
            source_kind = 'manual_upload'
            AND folder_root_id IS NULL
            AND source_id IS NULL
        )
        OR
        (
            source_kind = 'folder_upload'
            AND folder_root_id IS NOT NULL
            AND source_id IS NULL
        )
        OR
        (
            source_kind = 'workspace'
            AND folder_root_id IS NULL
            AND source_id IS NOT NULL
        )
    );

CREATE INDEX IF NOT EXISTS project_documents_project_idx
ON public.project_documents(project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS project_documents_visibility_idx
ON public.project_documents(project_id, visibility_scope, session_id);

CREATE INDEX IF NOT EXISTS project_documents_hash_idx
ON public.project_documents(project_id, content_hash);

CREATE INDEX IF NOT EXISTS project_documents_folder_root_idx
ON public.project_documents(folder_root_id, relative_path);

CREATE INDEX IF NOT EXISTS project_documents_source_idx
ON public.project_documents(source_id, relative_path);

CREATE UNIQUE INDEX IF NOT EXISTS project_documents_workspace_path_unique
ON public.project_documents(source_id, relative_path)
WHERE source_id IS NOT NULL;

-- Raw document bytes, stored separately to keep the project_documents table lean.
-- Deleted automatically when the parent project_documents row is removed.
CREATE TABLE IF NOT EXISTS public.document_content (
    document_id UUID PRIMARY KEY
        REFERENCES public.project_documents(document_id) ON DELETE CASCADE,
    content BYTEA NOT NULL,
    extracted_text TEXT,
    extracted_content_hash TEXT
);

ALTER TABLE public.document_content
    ADD COLUMN IF NOT EXISTS extracted_text TEXT;
ALTER TABLE public.document_content
    ADD COLUMN IF NOT EXISTS extracted_content_hash TEXT;

CREATE TABLE IF NOT EXISTS public.document_chunks (
    chunk_id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES public.project_documents(document_id)
        ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    embedding vector(1024) NOT NULL,
    language TEXT,
    chunk_kind TEXT NOT NULL DEFAULT 'text',
    symbol_name TEXT,
    start_line INTEGER,
    end_line INTEGER,
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'simple',
            content || ' ' || relative_path || ' '
            || COALESCE(symbol_name, '') || ' '
            || COALESCE(language, '')
        )
    ) STORED,
    CONSTRAINT document_chunks_document_index_unique
        UNIQUE (document_id, chunk_index),
    CONSTRAINT document_chunks_index_check
        CHECK (chunk_index >= 0),
    CONSTRAINT document_chunks_line_range_check
        CHECK (
            (start_line IS NULL AND end_line IS NULL)
            OR (start_line >= 1 AND end_line >= start_line)
        )
);

ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS language TEXT;
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS relative_path TEXT;
UPDATE public.document_chunks AS dc
SET relative_path = pd.relative_path
FROM public.project_documents AS pd
WHERE pd.document_id = dc.document_id
  AND dc.relative_path IS NULL;
ALTER TABLE public.document_chunks
    ALTER COLUMN relative_path SET NOT NULL;
ALTER TABLE public.document_chunks
    ADD COLUMN IF NOT EXISTS chunk_kind TEXT NOT NULL DEFAULT 'text';
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS symbol_name TEXT;
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS start_line INTEGER;
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS end_line INTEGER;
ALTER TABLE public.document_chunks DROP COLUMN IF EXISTS search_vector;
ALTER TABLE public.document_chunks
    ADD COLUMN search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'simple',
            content || ' ' || relative_path || ' '
            || COALESCE(symbol_name, '') || ' '
            || COALESCE(language, '')
        )
    ) STORED;
ALTER TABLE public.document_chunks
    DROP CONSTRAINT IF EXISTS document_chunks_line_range_check;
ALTER TABLE public.document_chunks
    ADD CONSTRAINT document_chunks_line_range_check
    CHECK (
        (start_line IS NULL AND end_line IS NULL)
        OR (start_line >= 1 AND end_line >= start_line)
    );

CREATE INDEX IF NOT EXISTS document_chunks_document_idx
ON public.document_chunks(document_id);

CREATE INDEX IF NOT EXISTS document_chunks_embedding_idx
ON public.document_chunks USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS document_chunks_search_vector_idx
ON public.document_chunks USING gin (search_vector);
