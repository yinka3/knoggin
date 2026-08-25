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
    domain_config JSONB NOT NULL
        CHECK (jsonb_typeof(domain_config) = 'object'),
    episode_window_size INTEGER NOT NULL DEFAULT 24
        CHECK (episode_window_size BETWEEN 8 AND 72),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    last_activity_at TIMESTAMPTZ,
    UNIQUE (user_name, project_id)
);

ALTER TABLE public.projects
ADD COLUMN IF NOT EXISTS domain_config JSONB NOT NULL
    CHECK (jsonb_typeof(domain_config) = 'object');

ALTER TABLE public.projects
ADD COLUMN IF NOT EXISTS episode_window_size INTEGER NOT NULL DEFAULT 24
    CHECK (episode_window_size BETWEEN 8 AND 72);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.projects
        WHERE domain_config IS NULL OR domain_config = '{}'::jsonb
    ) THEN
        RAISE EXCEPTION
            'Cannot remove projects.topic_config while a project lacks domain_config';
    END IF;
END $$;

ALTER TABLE public.projects
ALTER COLUMN domain_config DROP DEFAULT;

ALTER TABLE public.projects
DROP COLUMN IF EXISTS topic_config;

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
    name TEXT NOT NULL,
    persona TEXT,
    brain TEXT,
    model TEXT,
    temperature DOUBLE PRECISION,
    enabled_tools JSONB,
    is_default BOOLEAN NOT NULL DEFAULT false,
    aac_enabled BOOLEAN NOT NULL DEFAULT false,
    spawned_by TEXT,
    brain_revision INTEGER NOT NULL DEFAULT 1 CHECK (brain_revision >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_turn_at TIMESTAMPTZ
);

-- AAC agents are application-owned. This unreleased schema may drop the
-- former project coupling and redundant spawned flag outright.
ALTER TABLE public.agents
    DROP COLUMN IF EXISTS project_id,
    DROP COLUMN IF EXISTS is_spawned,
    ADD COLUMN IF NOT EXISTS aac_enabled BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS last_turn_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS agents_one_default_per_user_idx
ON public.agents(user_name)
WHERE is_default;

-- AAC is durable user-level discussion state, intentionally separate from the
-- canonical knowledge graph and from any individual project lifecycle.
CREATE TABLE IF NOT EXISTS public.aac_discussions (
    discussion_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    token_budget BIGINT NOT NULL CHECK (token_budget >= 0),
    tokens_used BIGINT NOT NULL DEFAULT 0 CHECK (tokens_used >= 0),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    CONSTRAINT aac_discussions_status_check
        CHECK (status IN ('active', 'completed', 'stopped', 'interrupted', 'failed'))
);

CREATE INDEX IF NOT EXISTS aac_discussions_user_started_idx
ON public.aac_discussions(user_name, started_at DESC);

CREATE TABLE IF NOT EXISTS public.aac_timeline (
    timeline_id TEXT PRIMARY KEY,
    discussion_id TEXT NOT NULL REFERENCES public.aac_discussions(discussion_id)
        ON DELETE CASCADE,
    kind TEXT NOT NULL,
    agent_id TEXT,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT aac_timeline_kind_check
        CHECK (kind IN ('agent_message', 'system_event'))
);

CREATE INDEX IF NOT EXISTS aac_timeline_discussion_created_idx
ON public.aac_timeline(discussion_id, created_at, timeline_id);

CREATE TABLE IF NOT EXISTS public.aac_insights (
    insight_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    discussion_id TEXT REFERENCES public.aac_discussions(discussion_id)
        ON DELETE SET NULL,
    author_agent_id TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'shared',
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT aac_insights_visibility_check
        CHECK (visibility IN ('shared', 'private'))
);

CREATE INDEX IF NOT EXISTS aac_insights_user_visibility_created_idx
ON public.aac_insights(user_name, visibility, created_at DESC);

CREATE TABLE IF NOT EXISTS public.aac_insight_votes (
    insight_id TEXT NOT NULL REFERENCES public.aac_insights(insight_id)
        ON DELETE CASCADE,
    voter_agent_id TEXT NOT NULL,
    vote TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (insight_id, voter_agent_id),
    CONSTRAINT aac_insight_votes_vote_check CHECK (vote IN ('up', 'down')),
    CONSTRAINT aac_insight_votes_reason_check CHECK (length(trim(reason)) > 0)
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
        CHECK (status IN ('open', 'deleted')),
    -- Participation controls only future project episode windows.  The
    -- message-ID boundary is moved whenever the user changes this setting.
    episode_participation_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    episode_participation_after_message_id BIGINT NOT NULL DEFAULT 0
        CHECK (episode_participation_after_message_id >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_active_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT sessions_id_project_key UNIQUE (session_id, project_id)
);

CREATE INDEX IF NOT EXISTS sessions_project_idx
ON public.sessions(user_name, project_id, created_at);

ALTER TABLE public.sessions
ADD COLUMN IF NOT EXISTS episode_participation_enabled BOOLEAN NOT NULL DEFAULT TRUE,
ADD COLUMN IF NOT EXISTS episode_participation_after_message_id BIGINT NOT NULL DEFAULT 0
    CHECK (episode_participation_after_message_id >= 0);

-- Sessions are either usable or tombstoned.  Older "closed" rows had no
-- supported recovery behavior, so preserve their history as deleted sessions.
UPDATE public.sessions
SET status = 'deleted', deleted_at = COALESCE(deleted_at, now())
WHERE status = 'closed';

ALTER TABLE public.sessions
DROP CONSTRAINT IF EXISTS sessions_status_check;
ALTER TABLE public.sessions
ADD CONSTRAINT sessions_status_check
CHECK (status IN ('open', 'deleted'));

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
    search_tsvector TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('english', content)
    ) STORED,
    user_msg_id BIGINT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    acceptance_key TEXT,
    timestamp_ms BIGINT,
    lifecycle_state TEXT NOT NULL DEFAULT 'sealed'
        CHECK (lifecycle_state IN ('editable', 'sealed', 'superseded')),
    editable_until_ms BIGINT,
    sealed_at_ms BIGINT,
    selected_revision INTEGER NOT NULL DEFAULT 1,
    replaces_message_id BIGINT,
    superseded_at_ms BIGINT,
    ingestion_state TEXT NOT NULL DEFAULT 'excluded'
        CHECK (ingestion_state IN ('waiting_for_seal', 'ready', 'claimed', 'processed', 'blocked', 'excluded')),
    ingestion_not_before_ms BIGINT,
    ingestion_claim_id TEXT,
    ingestion_claimed_at_ms BIGINT,
    ingestion_attempt_count INTEGER NOT NULL DEFAULT 0
        CHECK (ingestion_attempt_count >= 0),
    ingestion_last_failure_stage TEXT,
    ingestion_last_failure_code TEXT,
    ingestion_last_failure_at_ms BIGINT,
    ingestion_last_error_summary TEXT,
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

ALTER TABLE public.messages
    ADD COLUMN IF NOT EXISTS search_tsvector TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('english', content)
    ) STORED;

ALTER TABLE public.messages
    ADD COLUMN IF NOT EXISTS acceptance_key TEXT;

ALTER TABLE public.messages
    ADD COLUMN IF NOT EXISTS ingestion_attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ingestion_last_failure_stage TEXT,
    ADD COLUMN IF NOT EXISTS ingestion_last_failure_code TEXT,
    ADD COLUMN IF NOT EXISTS ingestion_last_failure_at_ms BIGINT,
    ADD COLUMN IF NOT EXISTS ingestion_last_error_summary TEXT;

CREATE INDEX IF NOT EXISTS messages_project_idx
ON public.messages(user_name, project_id, message_id);

CREATE INDEX IF NOT EXISTS messages_search_tsvector_idx
ON public.messages USING gin (search_tsvector);

CREATE INDEX IF NOT EXISTS messages_ingestion_queue_idx
ON public.messages(user_name, session_id, message_id)
WHERE role = 'user' AND ingestion_state IN ('waiting_for_seal', 'ready', 'claimed', 'blocked');

CREATE UNIQUE INDEX IF NOT EXISTS messages_acceptance_key_idx
ON public.messages(user_name, session_id, acceptance_key)
WHERE acceptance_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.message_revisions (
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    message_id BIGINT NOT NULL,
    revision INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (user_name, session_id, message_id, revision),
    FOREIGN KEY (user_name, session_id, message_id, project_id)
        REFERENCES public.messages(user_name, session_id, message_id, project_id)
        ON DELETE CASCADE
);

ALTER TABLE public.message_revisions
ADD COLUMN IF NOT EXISTS session_id TEXT;
UPDATE public.message_revisions AS revision
SET session_id = message.session_id
FROM public.messages AS message
WHERE revision.session_id IS NULL
  AND message.user_name = revision.user_name
  AND message.project_id = revision.project_id
  AND message.message_id = revision.message_id;
ALTER TABLE public.message_revisions
ALTER COLUMN session_id SET NOT NULL;

CREATE TABLE IF NOT EXISTS public.entities (
    entity_id BIGINT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    type TEXT,
    topic TEXT NOT NULL DEFAULT 'General',
    last_mentioned_ms BIGINT,
    embedding vector(1024),
    CONSTRAINT entities_id_project_key UNIQUE (entity_id, project_id),
    CONSTRAINT entities_id_user_project_key
        UNIQUE (entity_id, user_name, project_id)
);

CREATE INDEX IF NOT EXISTS entities_project_idx
ON public.entities(user_name, project_id);

CREATE INDEX IF NOT EXISTS entities_topic_idx
ON public.entities(project_id, topic);

ALTER TABLE public.entities
    DROP COLUMN IF EXISTS session_id,
    DROP COLUMN IF EXISTS confidence,
    DROP COLUMN IF EXISTS last_updated_ms,
    DROP COLUMN IF EXISTS last_profiled_msg_id;
ALTER TABLE public.entities
    ADD COLUMN IF NOT EXISTS embedding vector(1024);

CREATE INDEX IF NOT EXISTS entities_embedding_idx
ON public.entities USING hnsw (embedding vector_cosine_ops);

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
    relationship_type TEXT NOT NULL CHECK (btrim(relationship_type) <> ''),
    "symmetric" BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT relationships_distinct_entities
        CHECK (entity_a_id <> entity_b_id),
    CONSTRAINT relationships_identity_matches_fields
        CHECK (
            relationship_id = format(
                '%s:%s:%s:%s',
                project_id,
                CASE WHEN "symmetric" THEN LEAST(entity_a_id, entity_b_id)
                     ELSE entity_a_id END,
                CASE WHEN "symmetric" THEN GREATEST(entity_a_id, entity_b_id)
                     ELSE entity_b_id END,
                lower(regexp_replace(btrim(relationship_type), '\s+', ' ', 'g'))
            )
        ),
    CONSTRAINT relationships_project_pair_type_key
        UNIQUE (project_id, entity_a_id, entity_b_id, relationship_type),
    CONSTRAINT relationships_id_project_key UNIQUE (relationship_id, project_id)
);

ALTER TABLE public.relationships
    DROP CONSTRAINT IF EXISTS relationships_domain_status_consistent,
    DROP COLUMN IF EXISTS canonical_relationship_type,
    DROP COLUMN IF EXISTS observed_relationship_label,
    DROP COLUMN IF EXISTS domain_status,
    DROP COLUMN IF EXISTS weight,
    DROP COLUMN IF EXISTS confidence,
    DROP COLUMN IF EXISTS context,
    DROP COLUMN IF EXISTS last_seen_ms;

CREATE INDEX IF NOT EXISTS relationships_pair_type_idx
ON public.relationships(project_id, entity_a_id, entity_b_id, relationship_type);

CREATE INDEX IF NOT EXISTS relationships_entity_a_idx
ON public.relationships(entity_a_id);

CREATE INDEX IF NOT EXISTS relationships_entity_b_idx
ON public.relationships(entity_b_id);

-- One row per extracted relationship phrase and message. This preserves
-- source wording and endpoint types without turning advisories into graph
-- authority. This is the only canonical relationship-evidence record.
CREATE TABLE IF NOT EXISTS public.relationship_observations (
    observation_id BIGSERIAL PRIMARY KEY,
    relationship_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id BIGINT NOT NULL,
    source_entity_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    target_entity_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    source_type TEXT,
    target_type TEXT,
    observed_relationship_label TEXT NOT NULL
        CHECK (btrim(observed_relationship_label) <> ''),
    canonical_relationship_type TEXT,
    domain_status TEXT NOT NULL DEFAULT 'unrecognized'
        CHECK (domain_status IN ('recognized', 'unrecognized')),
    domain_version INTEGER NOT NULL DEFAULT 0 CHECK (domain_version >= 0),
    "symmetric" BOOLEAN NOT NULL DEFAULT FALSE,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0
        CHECK (confidence >= 0.0 AND confidence <= 1.0),
    context TEXT,
    observed_at_ms BIGINT NOT NULL,
    CONSTRAINT relationship_observations_distinct_entities
        CHECK (source_entity_id <> target_entity_id),
    CONSTRAINT relationship_observations_relationship_fk
        FOREIGN KEY (relationship_id, project_id)
        REFERENCES public.relationships(relationship_id, project_id)
        ON DELETE CASCADE,
    CONSTRAINT relationship_observations_message_fk
        FOREIGN KEY (user_name, session_id, message_id, project_id)
        REFERENCES public.messages(user_name, session_id, message_id, project_id)
        ON DELETE CASCADE,
    CONSTRAINT relationship_observations_domain_status_consistent
        CHECK (
            (domain_status = 'recognized')
            = (canonical_relationship_type IS NOT NULL)
        ),
    CONSTRAINT relationship_observations_unique_evidence
        UNIQUE (
            project_id,
            user_name,
            session_id,
            message_id,
            source_entity_id,
            target_entity_id,
            observed_relationship_label
        )
);

CREATE INDEX IF NOT EXISTS relationship_observations_pattern_idx
ON public.relationship_observations(
    project_id,
    user_name,
    domain_status,
    observed_relationship_label,
    source_type,
    target_type
);

CREATE INDEX IF NOT EXISTS relationship_observations_relationship_idx
ON public.relationship_observations(relationship_id, project_id);

CREATE INDEX IF NOT EXISTS relationship_observations_message_idx
ON public.relationship_observations(project_id, user_name, session_id, message_id);

ALTER TABLE public.relationship_observations
    DROP CONSTRAINT IF EXISTS relationship_observations_confidence_range_check;
ALTER TABLE public.relationship_observations
    ADD CONSTRAINT relationship_observations_confidence_range_check
        CHECK (confidence >= 0.0 AND confidence <= 1.0);

-- Durable advisory disposition. Evidence remains in relationship_observations;
-- this table stores only the current user decision for each pattern.
CREATE TABLE IF NOT EXISTS public.relationship_advisories (
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    pattern_key TEXT NOT NULL,
    disposition TEXT NOT NULL DEFAULT 'pending'
        CHECK (disposition IN ('pending', 'accepted', 'dismissed', 'suppressed')),
    proposed_relationship_type TEXT,
    last_action TEXT
        CHECK (last_action IS NULL OR last_action IN (
            'accept', 'edit', 'dismiss', 'reopen', 'suppress', 'merge'
        )),
    decision_note TEXT,
    decided_by TEXT,
    decision_at TIMESTAMPTZ,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_name, project_id, pattern_key),
    CONSTRAINT relationship_advisories_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.relationship_advisory_decisions (
    decision_id BIGSERIAL PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    pattern_key TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN (
        'accept', 'edit', 'dismiss', 'reopen', 'suppress', 'merge'
    )),
    proposed_relationship_type TEXT,
    decision_note TEXT,
    decided_by TEXT,
    revision BIGINT NOT NULL CHECK (revision >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT relationship_advisory_decisions_state_fk
        FOREIGN KEY (user_name, project_id, pattern_key)
        REFERENCES public.relationship_advisories(
            user_name, project_id, pattern_key
        ) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS relationship_advisories_disposition_idx
ON public.relationship_advisories(user_name, project_id, disposition);

CREATE INDEX IF NOT EXISTS relationship_advisory_decisions_pattern_idx
ON public.relationship_advisory_decisions(
    user_name, project_id, pattern_key, created_at
);

-- A unified inbox points to workflow-owned subjects. It deliberately has no
-- resolution payload: the subject workflow owns its state and mutations.
CREATE TABLE IF NOT EXISTS public.human_reviews (
    review_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id)
        ON DELETE CASCADE,
    kind TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'resolved')),
    priority TEXT NOT NULL DEFAULT 'normal'
        CHECK (priority IN ('low', 'normal', 'high')),
    title TEXT NOT NULL,
    summary TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    UNIQUE (user_name, project_id, kind, subject_id)
);

CREATE INDEX IF NOT EXISTS human_reviews_open_inbox_idx
ON public.human_reviews(user_name, project_id, status, priority, created_at DESC);

-- Conflict groups never replace or rewrite relationship evidence. The group is
-- a user-visible interpretation workflow over immutable observation snapshots.
CREATE TABLE IF NOT EXISTS public.conflict_groups (
    conflict_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id)
        ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'resolved')),
    origin TEXT NOT NULL
        CHECK (origin IN (
            'background_discovery', 'agent_discovery', 'user_created'
        )),
    kind TEXT NOT NULL
        CHECK (kind IN (
            'possible_contradiction', 'temporal_ambiguity',
            'possible_state_change', 'identity_or_entity_ambiguity'
        )),
    rationale TEXT NOT NULL,
    confidence DOUBLE PRECISION,
    evidence_signature TEXT NOT NULL,
    resolution_kind TEXT
        CHECK (resolution_kind IS NULL OR resolution_kind IN (
            'confirmed_conflict', 'normal_temporal_change', 'not_a_conflict',
            'insufficient_evidence', 'custom'
        )),
    resolution_note TEXT,
    resolved_by TEXT,
    resolved_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_name, project_id, evidence_signature)
);

CREATE INDEX IF NOT EXISTS conflict_groups_open_idx
ON public.conflict_groups(user_name, project_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS public.conflict_evidence_refs (
    evidence_ref_id BIGSERIAL PRIMARY KEY,
    conflict_id TEXT NOT NULL REFERENCES public.conflict_groups(conflict_id)
        ON DELETE CASCADE,
    observation_id BIGINT REFERENCES public.relationship_observations(observation_id)
        ON DELETE SET NULL,
    observation_snapshot JSONB NOT NULL,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (conflict_id, observation_id)
);

CREATE INDEX IF NOT EXISTS conflict_evidence_refs_observation_idx
ON public.conflict_evidence_refs(observation_id)
WHERE observation_id IS NOT NULL;

-- The cursor is durable and determines which relationship observations have
-- already been examined.
CREATE TABLE IF NOT EXISTS public.conflict_discovery_checkpoints (
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id)
        ON DELETE CASCADE,
    last_reviewed_observation_id BIGINT NOT NULL DEFAULT 0,
    last_completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_name, project_id)
);

-- Global provider-budget state survives process restarts. Reservations protect
-- concurrent requests until provider usage is recorded or their lease expires.
CREATE TABLE IF NOT EXISTS public.llm_budget_windows (
    reset_key TEXT PRIMARY KEY,
    spent_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    reserved_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.llm_budget_reservations (
    reservation_id UUID PRIMARY KEY,
    reset_key TEXT NOT NULL REFERENCES public.llm_budget_windows(reset_key)
        ON DELETE CASCADE,
    reserved_usd DOUBLE PRECISION NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'recorded', 'expired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    recorded_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS llm_budget_reservations_expiry_idx
ON public.llm_budget_reservations(reset_key, expires_at)
WHERE status = 'active';

ALTER TABLE public.conflict_discovery_checkpoints
ADD COLUMN IF NOT EXISTS last_reviewed_observation_id BIGINT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'conflict_discovery_checkpoints'
          AND column_name = 'cursor_observation_id'
    ) THEN
        UPDATE public.conflict_discovery_checkpoints
        SET last_reviewed_observation_id = GREATEST(
            last_reviewed_observation_id,
            cursor_observation_id
        );
    END IF;
END $$;

ALTER TABLE public.conflict_discovery_checkpoints
    DROP COLUMN IF EXISTS cursor_observed_at_ms,
    DROP COLUMN IF EXISTS cursor_observation_id,
    DROP COLUMN IF EXISTS continuation,
    DROP COLUMN IF EXISTS lease_token,
    DROP COLUMN IF EXISTS lease_expires_at;

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

END $$;

ALTER TABLE public.relationship_observations
    ADD COLUMN IF NOT EXISTS domain_version INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "symmetric" BOOLEAN NOT NULL DEFAULT FALSE;

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
    user_modified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT episodes_id_project_key UNIQUE (episode_id, project_id)
);

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

CREATE INDEX IF NOT EXISTS episodes_project_updated_idx
ON public.episodes(project_id, updated_at DESC);

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
        FOREIGN KEY (episode_id, project_id)
        REFERENCES public.episodes(episode_id, project_id)
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
SET project_id = message.project_id,
    session_id = message.session_id
FROM public.messages message
WHERE message.message_id = episode_message.message_id
  AND (episode_message.project_id IS NULL OR episode_message.session_id IS NULL);

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
        WHERE conname = 'episode_messages_episode_scope_fk'
          AND conrelid = 'public.episode_messages'::regclass
    ) THEN
        ALTER TABLE public.episode_messages
        ADD CONSTRAINT episode_messages_episode_scope_fk
        FOREIGN KEY (episode_id, project_id)
        REFERENCES public.episodes(episode_id, project_id)
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

ALTER TABLE public.episodes DROP COLUMN IF EXISTS session_id;

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
        ),
    CONSTRAINT entity_merge_proposals_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE
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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT entity_merge_audits_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE
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

ALTER TABLE public.entity_merge_audits
    DROP CONSTRAINT IF EXISTS entity_merge_audits_status_check,
    DROP CONSTRAINT IF EXISTS entity_merge_audits_rollback_status_check;
ALTER TABLE public.entity_merge_audits
    ADD CONSTRAINT entity_merge_audits_status_check
        CHECK (status IN ('executing', 'executed', 'failed')),
    ADD CONSTRAINT entity_merge_audits_rollback_status_check
        CHECK (rollback_status IN ('unavailable', 'available', 'rolled_back', 'expired', 'failed'));

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

-- Durable authorization and outcome trail for every model-initiated write.
CREATE TABLE IF NOT EXISTS public.agent_tool_audits (
    audit_id UUID PRIMARY KEY,
    user_name TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    project_id TEXT,
    session_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    capability TEXT NOT NULL,
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
            'identity_write'
        )
    ),
    CONSTRAINT agent_tool_audits_status_check CHECK (
        status IN ('started', 'succeeded', 'rejected', 'failed')
    ),
    CONSTRAINT agent_tool_audits_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS agent_tool_audits_scope_idx
ON public.agent_tool_audits(
    user_name,
    project_id,
    created_at DESC
);

CREATE INDEX IF NOT EXISTS agent_tool_audits_run_idx
ON public.agent_tool_audits(run_id, created_at);

-- AAC has user-level execution ownership. Its audited local writes must not
-- pretend to belong to a project or disappear when any project is deleted.
ALTER TABLE public.agent_tool_audits
    DROP CONSTRAINT IF EXISTS agent_tool_audits_project_fk;
ALTER TABLE public.agent_tool_audits
    ALTER COLUMN project_id DROP NOT NULL;
ALTER TABLE public.agent_tool_audits
    ADD CONSTRAINT agent_tool_audits_project_fk
    FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
    ON DELETE CASCADE;

ALTER TABLE public.agent_tool_audits
    DROP CONSTRAINT IF EXISTS agent_tool_audits_capability_check;
ALTER TABLE public.agent_tool_audits
    ADD CONSTRAINT agent_tool_audits_capability_check CHECK (
        capability IN (
            'reversible_write',
            'configuration_write',
            'identity_write'
        )
    );
ALTER TABLE public.agent_tool_audits
    DROP COLUMN IF EXISTS confirmation_state;

-- 4. Message full-text search projection.
-- Since AGE nodes don't support pgvector indexes directly inside `agtype`,
-- we store the heavy vectors and tsvectors in standard relational tables
-- and join them against the graph using the integer `id` property.

DROP TABLE IF EXISTS public.entity_search;
DROP TABLE IF EXISTS public.message_search;
DROP TABLE IF EXISTS public.hierarchy_edges;
DROP TABLE IF EXISTS public.ingestion_candidate_suggestions;
DROP TABLE IF EXISTS public.parked_dlq_items;
DROP FUNCTION IF EXISTS public.enforce_hierarchy_edge_invariants();

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

END $$;

DROP TRIGGER IF EXISTS entities_search_revision_trigger ON public.entities;
DROP TRIGGER IF EXISTS messages_search_revision_trigger ON public.messages;
DROP TRIGGER IF EXISTS episodes_search_revision_trigger ON public.episodes;
DROP FUNCTION IF EXISTS public.bump_search_index_revision() CASCADE;
DROP FUNCTION IF EXISTS public.sync_entity_search_canonical_name() CASCADE;
DROP TABLE IF EXISTS public.project_search_revisions;
DROP TABLE IF EXISTS public.identity_search_revisions;

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
    indexed_at TIMESTAMPTZ,
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
        ),
    CONSTRAINT document_folder_uploads_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS document_folder_uploads_project_idx
ON public.document_folder_uploads(project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS document_folder_uploads_visibility_idx
ON public.document_folder_uploads(
    project_id,
    visibility_scope,
    session_id
);

ALTER TABLE public.document_folder_uploads
    ALTER COLUMN indexed_at DROP NOT NULL;

-- A durable identity for a synchronizable local workspace.  Folder uploads
-- remain immutable snapshots; a workspace source will later own repeated
-- manifest syncs and incremental indexing.
CREATE TABLE IF NOT EXISTS public.document_workspace_sources (
    source_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL,
    session_id TEXT,
    visibility_scope TEXT NOT NULL,
    ownership_mode TEXT NOT NULL DEFAULT 'external_sync',
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
    CONSTRAINT document_workspace_sources_ownership_mode_check
        CHECK (
            ownership_mode IN ('external_sync', 'managed_project_workspace')
            AND (
                ownership_mode = 'external_sync'
                OR (visibility_scope = 'project' AND session_id IS NULL)
            )
        ),
    CONSTRAINT document_workspace_sources_session_visibility_check
        CHECK (visibility_scope <> 'session' OR session_id IS NOT NULL),
    CONSTRAINT document_workspace_sources_manifest_counts_check
        CHECK (
            last_manifest_candidate_count >= 0
            AND last_manifest_included_count >= 0
            AND last_manifest_excluded_count >= 0
        ),
    CONSTRAINT document_workspace_sources_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE
);

ALTER TABLE public.document_workspace_sources
    ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ;
ALTER TABLE public.document_workspace_sources
    ADD COLUMN IF NOT EXISTS ownership_mode TEXT NOT NULL DEFAULT 'external_sync';
ALTER TABLE public.document_workspace_sources
    DROP CONSTRAINT IF EXISTS document_workspace_sources_ownership_mode_check;
ALTER TABLE public.document_workspace_sources
    ADD CONSTRAINT document_workspace_sources_ownership_mode_check
    CHECK (
        ownership_mode IN ('external_sync', 'managed_project_workspace')
        AND (
            ownership_mode = 'external_sync'
            OR (visibility_scope = 'project' AND session_id IS NULL)
        )
    );
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

CREATE UNIQUE INDEX IF NOT EXISTS document_workspace_sources_managed_project_unique
ON public.document_workspace_sources(project_id)
WHERE ownership_mode = 'managed_project_workspace';

CREATE TABLE IF NOT EXISTS public.project_document_scan_settings (
    project_id TEXT PRIMARY KEY REFERENCES public.projects(project_id)
        ON DELETE CASCADE,
    settings JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Project-owned source documents and their derived retrieval chunks.
CREATE TABLE IF NOT EXISTS public.project_documents (
    document_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id)
        ON DELETE CASCADE,
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
    status TEXT NOT NULL DEFAULT 'queued',
    deleted_at TIMESTAMPTZ,
    indexed_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT project_documents_visibility_scope_check
        CHECK (visibility_scope IN ('project', 'session')),
    CONSTRAINT project_documents_session_visibility_check
        CHECK (visibility_scope <> 'session' OR session_id IS NOT NULL),
    CONSTRAINT project_documents_status_check
        CHECK (status IN ('queued', 'indexing', 'indexed', 'failed', 'deleted')),
    CONSTRAINT project_documents_source_kind_check
        CHECK (source_kind IN ('manual_upload', 'folder_upload', 'workspace')),
    CONSTRAINT project_documents_folder_source_check
        CHECK (
            (
                status = 'deleted'
                AND folder_root_id IS NULL
                AND source_id IS NULL
            )
            OR
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
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE public.project_documents
    DROP CONSTRAINT IF EXISTS project_documents_status_check;
ALTER TABLE public.project_documents
    DROP CONSTRAINT IF EXISTS project_documents_source_kind_check;
ALTER TABLE public.project_documents
    DROP CONSTRAINT IF EXISTS project_documents_folder_source_check;
ALTER TABLE public.project_documents
    ALTER COLUMN status SET DEFAULT 'queued';
UPDATE public.project_documents
SET status = 'queued'
WHERE status = 'uploaded';
DROP INDEX IF EXISTS project_documents_one_replacement_per_version_idx;
DROP INDEX IF EXISTS project_documents_replacement_candidates_idx;
ALTER TABLE public.project_documents
    DROP COLUMN IF EXISTS replaces_document_id;
ALTER TABLE public.project_documents
    DROP COLUMN IF EXISTS version_number;
ALTER TABLE public.project_documents
    ADD CONSTRAINT project_documents_status_check
    CHECK (status IN ('queued', 'indexing', 'indexed', 'failed', 'deleted'));
ALTER TABLE public.project_documents
    ADD CONSTRAINT project_documents_source_kind_check
    CHECK (source_kind IN ('manual_upload', 'folder_upload', 'workspace'));
ALTER TABLE public.project_documents
    ADD CONSTRAINT project_documents_folder_source_check
    CHECK (
        (
            status = 'deleted'
            AND folder_root_id IS NULL
            AND source_id IS NULL
        )
        OR
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
ALTER TABLE public.project_documents
    DROP CONSTRAINT IF EXISTS project_documents_relative_path_size_check;
ALTER TABLE public.project_documents
    ADD CONSTRAINT project_documents_relative_path_size_check
    CHECK (octet_length(relative_path) BETWEEN 1 AND 2048);

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
    page_number INTEGER,
    start_line INTEGER,
    end_line INTEGER,
    start_row INTEGER,
    end_row INTEGER,
    section_path TEXT[],
    start_paragraph INTEGER,
    end_paragraph INTEGER,
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
        ),
    CONSTRAINT document_chunks_page_number_check
        CHECK (page_number IS NULL OR page_number >= 1),
    CONSTRAINT document_chunks_row_range_check
        CHECK (
            (start_row IS NULL AND end_row IS NULL)
            OR (start_row >= 1 AND end_row >= start_row)
    ),
    CONSTRAINT document_chunks_paragraph_range_check
        CHECK (
            (start_paragraph IS NULL AND end_paragraph IS NULL)
            OR (start_paragraph >= 1 AND end_paragraph >= start_paragraph)
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
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS page_number INTEGER;
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS start_line INTEGER;
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS end_line INTEGER;
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS start_row INTEGER;
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS end_row INTEGER;
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS section_path TEXT[];
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS start_paragraph INTEGER;
ALTER TABLE public.document_chunks ADD COLUMN IF NOT EXISTS end_paragraph INTEGER;
-- Keep this migration physically idempotent.  Repeatedly dropping and
-- recreating a PostgreSQL column leaves a dropped attribute behind in the
-- table descriptor; after enough storage-fixture resets the table reaches
-- PostgreSQL's 1600-attribute limit even though it has few live columns.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = 'public.document_chunks'::regclass
          AND attname = 'search_vector'
          AND NOT attisdropped
          AND attgenerated <> 's'
    ) THEN
        ALTER TABLE public.document_chunks DROP COLUMN search_vector;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = 'public.document_chunks'::regclass
          AND attname = 'search_vector'
          AND NOT attisdropped
    ) THEN
        ALTER TABLE public.document_chunks
            ADD COLUMN search_vector tsvector GENERATED ALWAYS AS (
                to_tsvector(
                    'simple',
                    content || ' ' || relative_path || ' '
                    || COALESCE(symbol_name, '') || ' '
                    || COALESCE(language, '')
                )
            ) STORED;
    END IF;
END $$;
ALTER TABLE public.document_chunks
    DROP CONSTRAINT IF EXISTS document_chunks_line_range_check;
ALTER TABLE public.document_chunks
    ADD CONSTRAINT document_chunks_line_range_check
    CHECK (
        (start_line IS NULL AND end_line IS NULL)
        OR (start_line >= 1 AND end_line >= start_line)
    );
ALTER TABLE public.document_chunks
    DROP CONSTRAINT IF EXISTS document_chunks_paragraph_range_check;
ALTER TABLE public.document_chunks
    ADD CONSTRAINT document_chunks_paragraph_range_check
    CHECK (
        (start_paragraph IS NULL AND end_paragraph IS NULL)
        OR (start_paragraph >= 1 AND end_paragraph >= start_paragraph)
    );
ALTER TABLE public.document_chunks
    DROP CONSTRAINT IF EXISTS document_chunks_page_number_check;
ALTER TABLE public.document_chunks
    ADD CONSTRAINT document_chunks_page_number_check
    CHECK (page_number IS NULL OR page_number >= 1);
ALTER TABLE public.document_chunks
    DROP CONSTRAINT IF EXISTS document_chunks_row_range_check;
ALTER TABLE public.document_chunks
    ADD CONSTRAINT document_chunks_row_range_check
    CHECK (
        (start_row IS NULL AND end_row IS NULL)
        OR (start_row >= 1 AND end_row >= start_row)
    );

-- Clean unreleased-system migration: indexed chunks created before locator
-- support must be rebuilt rather than served through a page-less/locator-less
-- compatibility path. New chunks satisfy the corresponding condition and are
-- therefore not requeued on subsequent schema applications.
UPDATE public.project_documents AS pd
SET
    status = 'queued',
    indexed_at = NULL,
    error_message = NULL,
    updated_at = NOW()
WHERE pd.status = 'indexed'
  AND EXISTS (
      SELECT 1
      FROM public.document_chunks AS dc
      WHERE dc.document_id = pd.document_id
        AND (
            (pd.extension = '.pdf' AND dc.page_number IS NULL)
            OR (pd.extension = '.csv' AND dc.start_row IS NULL)
            OR (pd.extension = '.docx' AND dc.start_paragraph IS NULL)
            OR (
                pd.extension NOT IN ('.pdf', '.csv', '.docx')
                AND dc.start_line IS NULL
            )
        )
  );

CREATE INDEX IF NOT EXISTS document_chunks_document_idx
ON public.document_chunks(document_id);

CREATE INDEX IF NOT EXISTS document_chunks_embedding_idx
ON public.document_chunks USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS document_chunks_search_vector_idx
ON public.document_chunks USING gin (search_vector);

-- Source context supplied to an assistant response. These rows record what a
-- completed run received; they are not claim-level citations or a general
-- source/revision ledger.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'project_documents_id_project_key'
          AND conrelid = 'public.project_documents'::regclass
    ) THEN
        ALTER TABLE public.project_documents
        ADD CONSTRAINT project_documents_id_project_key
        UNIQUE (document_id, project_id);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.message_source_refs (
    source_ref_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id BIGINT NOT NULL,
    source_kind TEXT NOT NULL,
    document_id UUID,
    source_project_id TEXT,
    canonical_url TEXT,
    source_message_id BIGINT,
    content_hash TEXT NOT NULL,
    locator JSONB NOT NULL,
    excerpt TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    encounter_kind TEXT NOT NULL,
    agent_run_id TEXT NOT NULL,
    tool_call_id TEXT,
    result_position INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT message_source_refs_message_scope_fk
        FOREIGN KEY (message_id, project_id, session_id)
        REFERENCES public.messages(message_id, project_id, session_id)
        ON DELETE CASCADE,
    CONSTRAINT message_source_refs_source_message_scope_fk
        FOREIGN KEY (source_message_id, project_id, session_id)
        REFERENCES public.messages(message_id, project_id, session_id)
        ON DELETE CASCADE,
    CONSTRAINT message_source_refs_kind_check
        CHECK (source_kind IN (
            'pdf_document',
            'text_document',
            'user_pasted_text',
            'web_search_result',
            'news_search_result'
        )),
    CONSTRAINT message_source_refs_encounter_check
        CHECK (encounter_kind IN (
            'document_search',
            'document_read',
            'user_pasted_text',
            'web_search',
            'news_search'
        )),
    CONSTRAINT message_source_refs_position_check
        CHECK (result_position >= 0),
    CONSTRAINT message_source_refs_hash_check
        CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT message_source_refs_excerpt_check
        CHECK (length(btrim(excerpt)) > 0),
    CONSTRAINT message_source_refs_source_project_shape_check
        CHECK (
            (source_kind IN ('pdf_document', 'text_document')
                AND source_project_id IS NOT NULL)
            OR (source_kind NOT IN ('pdf_document', 'text_document')
                AND source_project_id IS NULL)
        ),
    CONSTRAINT message_source_refs_source_shape_check
        CHECK (
            (
                source_kind = 'pdf_document'
                AND document_id IS NOT NULL
                AND source_project_id IS NOT NULL
                AND canonical_url IS NULL
                AND source_message_id IS NULL
                AND tool_call_id IS NOT NULL
                AND encounter_kind IN ('document_search', 'document_read')
                AND locator ->> 'kind' = 'pdf_page'
                AND jsonb_typeof(locator -> 'page') = 'number'
                AND locator ->> 'page' ~ '^[1-9][0-9]*$'
                AND COALESCE(metadata ->> 'document_name', '') <> ''
            )
            OR (
                source_kind = 'text_document'
                AND document_id IS NOT NULL
                AND source_project_id IS NOT NULL
                AND canonical_url IS NULL
                AND source_message_id IS NULL
                AND tool_call_id IS NOT NULL
                AND encounter_kind IN ('document_search', 'document_read')
                AND (
                    (
                        locator ->> 'kind' IN ('text_lines', 'code_lines')
                        AND jsonb_typeof(locator -> 'start_line') = 'number'
                        AND jsonb_typeof(locator -> 'end_line') = 'number'
                        AND locator ->> 'start_line' ~ '^[1-9][0-9]*$'
                        AND locator ->> 'end_line' ~ '^[1-9][0-9]*$'
                    )
                    OR (
                        locator ->> 'kind' = 'csv_rows'
                        AND jsonb_typeof(locator -> 'start_row') = 'number'
                        AND jsonb_typeof(locator -> 'end_row') = 'number'
                        AND locator ->> 'start_row' ~ '^[1-9][0-9]*$'
                        AND locator ->> 'end_row' ~ '^[1-9][0-9]*$'
                    )
                    OR (
                        locator ->> 'kind' = 'docx_paragraphs'
                        AND jsonb_typeof(locator -> 'start_paragraph') = 'number'
                        AND jsonb_typeof(locator -> 'end_paragraph') = 'number'
                        AND locator ->> 'start_paragraph' ~ '^[1-9][0-9]*$'
                        AND locator ->> 'end_paragraph' ~ '^[1-9][0-9]*$'
                    )
                )
                AND COALESCE(metadata ->> 'document_name', '') <> ''
            )
            OR (
                source_kind = 'user_pasted_text'
                AND document_id IS NULL
                AND source_project_id IS NULL
                AND canonical_url IS NULL
                AND source_message_id IS NOT NULL
                AND tool_call_id IS NULL
                AND encounter_kind = 'user_pasted_text'
                AND locator ->> 'kind' = 'character_span'
                AND jsonb_typeof(locator -> 'start_char') = 'number'
                AND jsonb_typeof(locator -> 'end_char') = 'number'
                AND locator ->> 'start_char' ~ '^[0-9]+$'
                AND locator ->> 'end_char' ~ '^[1-9][0-9]*$'
            )
            OR (
                source_kind IN ('web_search_result', 'news_search_result')
                AND document_id IS NULL
                AND source_project_id IS NULL
                AND source_message_id IS NULL
                AND canonical_url ~ '^https?://[^[:space:]#]+$'
                AND tool_call_id IS NOT NULL
                AND locator ->> 'kind' = 'search_result'
                AND COALESCE(locator ->> 'provider', '') <> ''
                AND COALESCE(locator ->> 'query', '') <> ''
                AND jsonb_typeof(locator -> 'rank') = 'number'
                AND locator ->> 'rank' ~ '^[1-9][0-9]*$'
                AND COALESCE(metadata ->> 'title', '') <> ''
                AND metadata -> 'discovery_snippet' = 'true'::jsonb
                AND (
                    (source_kind = 'web_search_result' AND encounter_kind = 'web_search')
                    OR (source_kind = 'news_search_result' AND encounter_kind = 'news_search')
                )
            )
        )
);

-- Existing unreleased databases may still scope document references to the
-- answer project. Retain their history while moving document identity to the
-- actual source project. Source identity is validated when written, but cannot
-- retain an FK: deleting a source project must not delete another project's
-- historical answer provenance.
ALTER TABLE public.message_source_refs
ADD COLUMN IF NOT EXISTS source_project_id TEXT;

UPDATE public.message_source_refs
SET source_project_id = project_id
WHERE document_id IS NOT NULL
  AND source_project_id IS NULL;

DO $$
BEGIN
    ALTER TABLE public.message_source_refs
    DROP CONSTRAINT IF EXISTS message_source_refs_document_project_fk;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'message_source_refs_source_project_shape_check'
          AND conrelid = 'public.message_source_refs'::regclass
    ) THEN
        ALTER TABLE public.message_source_refs
        ADD CONSTRAINT message_source_refs_source_project_shape_check
        CHECK (
            (source_kind IN ('pdf_document', 'text_document')
                AND source_project_id IS NOT NULL)
            OR (source_kind NOT IN ('pdf_document', 'text_document')
                AND source_project_id IS NULL)
        );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS message_source_refs_message_scope_idx
ON public.message_source_refs(message_id, project_id, session_id);

CREATE INDEX IF NOT EXISTS message_source_refs_episode_lookup_idx
ON public.message_source_refs(project_id, session_id, message_id, created_at);

-- ============================================================================
-- ADDITIVE INTEGRITY CONSTRAINTS
--
-- These constraints are intentionally NOT VALID for existing deployments: they
-- protect all new writes without silently rewriting legacy rows. Validate them
-- after the deployment-specific legacy-data repair has completed.
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'entities_id_positive_check'
          AND conrelid = 'public.entities'::regclass
    ) THEN
        ALTER TABLE public.entities
        ADD CONSTRAINT entities_id_positive_check
        CHECK (entity_id > 0) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'entities_canonical_name_nonblank_check'
          AND conrelid = 'public.entities'::regclass
    ) THEN
        ALTER TABLE public.entities
        ADD CONSTRAINT entities_canonical_name_nonblank_check
        CHECK (btrim(canonical_name) <> '') NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'entities_type_nonblank_check'
          AND conrelid = 'public.entities'::regclass
    ) THEN
        ALTER TABLE public.entities
        ADD CONSTRAINT entities_type_nonblank_check
        CHECK (type IS NULL OR btrim(type) <> '') NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'entities_topic_nonblank_check'
          AND conrelid = 'public.entities'::regclass
    ) THEN
        ALTER TABLE public.entities
        ADD CONSTRAINT entities_topic_nonblank_check
        CHECK (btrim(topic) <> '') NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'messages_id_positive_check'
          AND conrelid = 'public.messages'::regclass
    ) THEN
        ALTER TABLE public.messages
        ADD CONSTRAINT messages_id_positive_check
        CHECK (message_id > 0) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'message_source_refs_locator_range_check'
          AND conrelid = 'public.message_source_refs'::regclass
    ) THEN
        ALTER TABLE public.message_source_refs
        ADD CONSTRAINT message_source_refs_locator_range_check
        CHECK (
            CASE locator ->> 'kind'
                WHEN 'text_lines' THEN
                    CASE
                        WHEN locator ->> 'start_line' ~ '^[1-9][0-9]*$'
                         AND locator ->> 'end_line' ~ '^[1-9][0-9]*$'
                        THEN (locator ->> 'end_line')::BIGINT
                             >= (locator ->> 'start_line')::BIGINT
                        ELSE FALSE
                    END
                WHEN 'code_lines' THEN
                    CASE
                        WHEN locator ->> 'start_line' ~ '^[1-9][0-9]*$'
                         AND locator ->> 'end_line' ~ '^[1-9][0-9]*$'
                        THEN (locator ->> 'end_line')::BIGINT
                             >= (locator ->> 'start_line')::BIGINT
                        ELSE FALSE
                    END
                WHEN 'csv_rows' THEN
                    CASE
                        WHEN locator ->> 'start_row' ~ '^[1-9][0-9]*$'
                         AND locator ->> 'end_row' ~ '^[1-9][0-9]*$'
                        THEN (locator ->> 'end_row')::BIGINT
                             >= (locator ->> 'start_row')::BIGINT
                        ELSE FALSE
                    END
                WHEN 'docx_paragraphs' THEN
                    CASE
                        WHEN locator ->> 'start_paragraph' ~ '^[1-9][0-9]*$'
                         AND locator ->> 'end_paragraph' ~ '^[1-9][0-9]*$'
                        THEN (locator ->> 'end_paragraph')::BIGINT
                             >= (locator ->> 'start_paragraph')::BIGINT
                        ELSE FALSE
                    END
                WHEN 'character_span' THEN
                    CASE
                        WHEN locator ->> 'start_char' ~ '^[0-9]+$'
                         AND locator ->> 'end_char' ~ '^[1-9][0-9]*$'
                        THEN (locator ->> 'end_char')::BIGINT
                             > (locator ->> 'start_char')::BIGINT
                        ELSE FALSE
                    END
                ELSE TRUE
            END
        ) NOT VALID;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION public.enforce_episode_focus_entity_limit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.is_focus_entity AND (
        SELECT count(*)
        FROM public.episode_entities
        WHERE episode_id = NEW.episode_id
          AND is_focus_entity
    ) > 2 THEN
        RAISE EXCEPTION
            'episodes may contain at most two focus entities'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS episode_entities_focus_limit_trigger
    ON public.episode_entities;
CREATE TRIGGER episode_entities_focus_limit_trigger
AFTER INSERT OR UPDATE OF episode_id, is_focus_entity
ON public.episode_entities
FOR EACH ROW EXECUTE FUNCTION public.enforce_episode_focus_entity_limit();

-- Existing unreleased databases may predate the direct project foreign keys
-- above.  Make project deletion one cascade root rather than a handwritten
-- list of every descendant table.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'agent_tool_audits_project_fk'
    ) THEN
        ALTER TABLE public.agent_tool_audits
        ADD CONSTRAINT agent_tool_audits_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'entity_merge_proposals_project_fk'
    ) THEN
        ALTER TABLE public.entity_merge_proposals
        ADD CONSTRAINT entity_merge_proposals_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'entity_merge_audits_project_fk'
    ) THEN
        ALTER TABLE public.entity_merge_audits
        ADD CONSTRAINT entity_merge_audits_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'document_folder_uploads_project_fk'
    ) THEN
        ALTER TABLE public.document_folder_uploads
        ADD CONSTRAINT document_folder_uploads_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'document_workspace_sources_project_fk'
    ) THEN
        ALTER TABLE public.document_workspace_sources
        ADD CONSTRAINT document_workspace_sources_project_fk
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'project_document_scan_settings_project_id_fkey'
    ) THEN
        ALTER TABLE public.project_document_scan_settings
        ADD CONSTRAINT project_document_scan_settings_project_id_fkey
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'project_documents_project_id_fkey'
    ) THEN
        ALTER TABLE public.project_documents
        ADD CONSTRAINT project_documents_project_id_fkey
        FOREIGN KEY (project_id) REFERENCES public.projects(project_id)
        ON DELETE CASCADE;
    END IF;
END $$;

-- Preserve the session-owned episode shape while adding the current user-edit
-- marker to databases created before that field was introduced.
ALTER TABLE public.episodes
ADD COLUMN IF NOT EXISTS user_modified BOOLEAN NOT NULL DEFAULT FALSE;
