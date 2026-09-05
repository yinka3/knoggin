-- Canonical fresh-install schema for unreleased Knoggin.
-- Extensions and the AGE graph are created by the deployment/test bootstrap.
-- Historical upgrades and developer data cleanup do not belong in this file.

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;
CREATE SCHEMA IF NOT EXISTS public;
COMMENT ON SCHEMA public IS 'standard public schema';
CREATE FUNCTION public.enforce_message_entity_ref_scope() RETURNS trigger
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
              entity.entity_id = 1
              OR EXISTS (
                  SELECT 1
                  FROM public.project_entity_contexts context
                  WHERE context.entity_id = entity.entity_id
                    AND context.project_id = message_project_id
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
CREATE FUNCTION public.enforce_relationship_scope() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.entities entity
        WHERE entity.entity_id = NEW.entity_a_id
          AND entity.user_name = NEW.user_name
          AND (
              entity.entity_id = 1
              OR EXISTS (
                  SELECT 1
                  FROM public.project_entity_contexts context
                  WHERE context.entity_id = entity.entity_id
                    AND context.project_id = NEW.project_id
              )
          )
    ) OR NOT EXISTS (
        SELECT 1
        FROM public.entities entity
        WHERE entity.entity_id = NEW.entity_b_id
          AND entity.user_name = NEW.user_name
          AND (
              entity.entity_id = 1
              OR EXISTS (
                  SELECT 1
                  FROM public.project_entity_contexts context
                  WHERE context.entity_id = entity.entity_id
                    AND context.project_id = NEW.project_id
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
CREATE FUNCTION public.reject_entity_identity_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.user_name IS DISTINCT FROM OLD.user_name THEN
        RAISE EXCEPTION 'Entity user ownership is immutable';
    END IF;
    IF NEW.canonical_name IS DISTINCT FROM OLD.canonical_name THEN
        RAISE EXCEPTION 'Entity canonical_name is immutable';
    END IF;
    NEW.updated_at_ms := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
    RETURN NEW;
END;
$$;
CREATE FUNCTION public.enforce_context_block_entity_scope() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    block_user_name TEXT;
BEGIN
    SELECT project.user_name
    INTO block_user_name
    FROM public.project_context_blocks AS block
    JOIN public.projects AS project ON project.project_id = block.project_id
    WHERE block.block_id = NEW.block_id
      AND block.project_id = NEW.project_id;
    IF NOT FOUND OR NOT EXISTS (
        SELECT 1
        FROM public.entities AS entity
        WHERE entity.entity_id = NEW.entity_id
          AND entity.user_name = block_user_name
          AND (
              entity.entity_id = 1
              OR EXISTS (
                  SELECT 1
                  FROM public.project_entity_contexts AS context
                  WHERE context.project_id = NEW.project_id
                    AND context.entity_id = entity.entity_id
              )
          )
    ) THEN
        RAISE EXCEPTION
            'context block entity must be visible to its project scope'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
SET default_tablespace = '';
SET default_table_access_method = heap;
CREATE TABLE public.aac_discussions (
    discussion_id text NOT NULL,
    user_name text NOT NULL,
    topic text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    end_reason text,
    token_budget bigint NOT NULL,
    tokens_used bigint DEFAULT 0 NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    ended_at timestamp with time zone,
    CONSTRAINT aac_discussions_end_reason_check CHECK (((end_reason IS NULL) OR (end_reason = ANY (ARRAY['completed'::text, 'token_budget'::text, 'user_stopped'::text, 'no_participants'::text, 'shutdown'::text, 'failed'::text, 'startup_recovery'::text, 'interrupted'::text])))),
    CONSTRAINT aac_discussions_status_check CHECK ((status = ANY (ARRAY['active'::text, 'completed'::text, 'stopped'::text, 'interrupted'::text, 'failed'::text]))),
    CONSTRAINT aac_discussions_token_budget_check CHECK ((token_budget >= 0)),
    CONSTRAINT aac_discussions_tokens_used_check CHECK ((tokens_used >= 0))
);
CREATE TABLE public.aac_insight_votes (
    insight_id text NOT NULL,
    voter_agent_id text NOT NULL,
    vote text NOT NULL,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT aac_insight_votes_reason_check CHECK ((length(TRIM(BOTH FROM reason)) > 0)),
    CONSTRAINT aac_insight_votes_vote_check CHECK ((vote = ANY (ARRAY['up'::text, 'down'::text])))
);
CREATE TABLE public.aac_insights (
    insight_id text NOT NULL,
    user_name text NOT NULL,
    discussion_id text,
    author_agent_id text NOT NULL,
    visibility text DEFAULT 'shared'::text NOT NULL,
    content text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT aac_insights_visibility_check CHECK ((visibility = ANY (ARRAY['shared'::text, 'private'::text])))
);
CREATE TABLE public.aac_timeline (
    timeline_id text NOT NULL,
    discussion_id text NOT NULL,
    kind text NOT NULL,
    agent_id text,
    content text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT aac_timeline_kind_check CHECK ((kind = ANY (ARRAY['agent_message'::text, 'system_event'::text])))
);
CREATE TABLE public.agent_brain_snapshots (
    agent_id text NOT NULL,
    revision integer NOT NULL,
    user_name text NOT NULL,
    content text NOT NULL,
    edited_by text DEFAULT 'agent'::text NOT NULL,
    change_type text DEFAULT 'initial_seed'::text NOT NULL,
    changed_section text,
    change_summary text DEFAULT 'Initial Brain'::text NOT NULL,
    restored_from_revision integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_brain_snapshots_revision_check CHECK ((revision >= 1))
);
CREATE TABLE public.agent_tool_audits (
    audit_id uuid NOT NULL,
    user_name text NOT NULL,
    agent_id text NOT NULL,
    project_id text,
    session_id text NOT NULL,
    run_id text NOT NULL,
    tool_name text NOT NULL,
    capability text NOT NULL,
    arguments jsonb DEFAULT '{}'::jsonb NOT NULL,
    result jsonb,
    status text NOT NULL,
    error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT agent_tool_audits_capability_check CHECK ((capability = ANY (ARRAY['reversible_write'::text, 'configuration_write'::text, 'identity_write'::text]))),
    CONSTRAINT agent_tool_audits_status_check CHECK ((status = ANY (ARRAY['started'::text, 'succeeded'::text, 'rejected'::text, 'failed'::text])))
);
CREATE TABLE public.agents (
    agent_id text NOT NULL,
    user_name text NOT NULL,
    name text NOT NULL,
    persona text,
    brain text,
    model text,
    temperature double precision,
    enabled_tools jsonb,
    is_default boolean DEFAULT false NOT NULL,
    aac_enabled boolean DEFAULT false NOT NULL,
    spawned_by text,
    brain_revision integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_turn_at timestamp with time zone,
    CONSTRAINT agents_brain_revision_check CHECK ((brain_revision >= 1))
);
CREATE TABLE public.document_chunks (
    chunk_id uuid NOT NULL,
    document_id uuid NOT NULL,
    chunk_index integer NOT NULL,
    content text NOT NULL,
    relative_path text NOT NULL,
    embedding public.vector(1024) NOT NULL,
    language text,
    chunk_kind text DEFAULT 'text'::text NOT NULL,
    symbol_name text,
    page_number integer,
    start_line integer,
    end_line integer,
    start_row integer,
    end_row integer,
    section_path text[],
    start_paragraph integer,
    end_paragraph integer,
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, ((((((content || ' '::text) || relative_path) || ' '::text) || COALESCE(symbol_name, ''::text)) || ' '::text) || COALESCE(language, ''::text)))) STORED,
    CONSTRAINT document_chunks_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT document_chunks_line_range_check CHECK ((((start_line IS NULL) AND (end_line IS NULL)) OR ((start_line >= 1) AND (end_line >= start_line)))),
    CONSTRAINT document_chunks_page_number_check CHECK (((page_number IS NULL) OR (page_number >= 1))),
    CONSTRAINT document_chunks_paragraph_range_check CHECK ((((start_paragraph IS NULL) AND (end_paragraph IS NULL)) OR ((start_paragraph >= 1) AND (end_paragraph >= start_paragraph)))),
    CONSTRAINT document_chunks_row_range_check CHECK ((((start_row IS NULL) AND (end_row IS NULL)) OR ((start_row >= 1) AND (end_row >= start_row))))
);
CREATE TABLE public.document_extractions (
    document_id uuid NOT NULL,
    extracted_text text,
    extracted_content_hash text
);
CREATE TABLE public.entities (
    entity_id bigint NOT NULL,
    user_name text NOT NULL,
    canonical_name text NOT NULL,
    embedding public.vector(1024),
    status text DEFAULT 'active'::text NOT NULL,
    redirect_entity_id bigint,
    created_at_ms bigint DEFAULT (floor((EXTRACT(epoch FROM clock_timestamp()) * (1000)::numeric)))::bigint NOT NULL,
    updated_at_ms bigint DEFAULT (floor((EXTRACT(epoch FROM clock_timestamp()) * (1000)::numeric)))::bigint NOT NULL,
    CONSTRAINT entities_redirect_status_check CHECK (((status = 'redirected'::text) = (redirect_entity_id IS NOT NULL))),
    CONSTRAINT entities_status_check CHECK ((status = ANY (ARRAY['active'::text, 'redirected'::text, 'retired'::text])))
);
CREATE TABLE public.entity_aliases (
    entity_id bigint NOT NULL,
    alias text NOT NULL
);
CREATE TABLE public.entity_global_merge_audits (
    merge_id text NOT NULL,
    user_name text NOT NULL,
    survivor_entity_id bigint NOT NULL,
    retired_entity_id bigint NOT NULL,
    plan jsonb NOT NULL,
    affected_project_ids jsonb DEFAULT '[]'::jsonb NOT NULL,
    status text DEFAULT 'executed'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    failure_reason text,
    CONSTRAINT entity_global_merge_audits_affected_project_ids_check CHECK ((jsonb_typeof(affected_project_ids) = 'array'::text)),
    CONSTRAINT entity_global_merge_audits_distinct_entities CHECK ((survivor_entity_id <> retired_entity_id)),
    CONSTRAINT entity_global_merge_audits_plan_check CHECK ((jsonb_typeof(plan) = 'object'::text)),
    CONSTRAINT entity_global_merge_audits_status_check CHECK ((status = ANY (ARRAY['executing'::text, 'executed'::text, 'rolled_back'::text, 'failed'::text])))
);
CREATE TABLE public.entity_global_merge_mutations (
    mutation_id bigint NOT NULL,
    merge_id text NOT NULL,
    object_kind text NOT NULL,
    object_key text NOT NULL,
    before_value jsonb,
    after_value jsonb,
    inverse_status text DEFAULT 'safe'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT entity_global_merge_mutations_inverse_status_check CHECK ((inverse_status = ANY (ARRAY['safe'::text, 'conflict'::text, 'applied'::text])))
);
CREATE SEQUENCE public.entity_global_merge_mutations_mutation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.entity_global_merge_mutations_mutation_id_seq OWNED BY public.entity_global_merge_mutations.mutation_id;
CREATE SEQUENCE public.entity_id_seq
    START WITH 2
    INCREMENT BY 1
    MINVALUE 2
    NO MAXVALUE
    CACHE 1;
CREATE TABLE public.episode_entities (
    episode_id text NOT NULL,
    project_id text NOT NULL,
    entity_id bigint NOT NULL,
    source_message_count integer DEFAULT 0 NOT NULL,
    first_seen_at timestamp with time zone,
    last_seen_at timestamp with time zone,
    CONSTRAINT episode_entities_source_message_count_check CHECK ((source_message_count >= 0))
);
CREATE TABLE public.episode_messages (
    episode_id text NOT NULL,
    project_id text NOT NULL,
    session_id text NOT NULL,
    message_id bigint NOT NULL,
    message_position integer NOT NULL,
    attached_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT episode_messages_message_position_check CHECK ((message_position >= 0))
);
CREATE TABLE public.episode_processing_checkpoints (
    project_id text NOT NULL,
    session_id text NOT NULL,
    last_evaluated_message_id bigint DEFAULT 0 NOT NULL,
    last_evaluated_timestamp_ms bigint,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT episode_processing_checkpoints_last_evaluated_message_id_check CHECK ((last_evaluated_message_id >= 0))
);
CREATE TABLE public.episode_relationships (
    episode_id text NOT NULL,
    project_id text NOT NULL,
    relationship_id text NOT NULL,
    source_message_count integer DEFAULT 0 NOT NULL,
    CONSTRAINT episode_relationships_source_message_count_check CHECK ((source_message_count >= 0))
);
CREATE TABLE public.episodes (
    episode_id text NOT NULL,
    project_id text NOT NULL,
    summary text NOT NULL,
    new_developments jsonb DEFAULT '[]'::jsonb NOT NULL,
    updates jsonb DEFAULT '[]'::jsonb NOT NULL,
    unresolved jsonb DEFAULT '[]'::jsonb NOT NULL,
    search_tsvector tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, ((((((summary || ' '::text) || (new_developments)::text) || ' '::text) || (updates)::text) || ' '::text) || (unresolved)::text))) STORED,
    source_message_count integer DEFAULT 0 NOT NULL,
    first_message_at timestamp with time zone,
    last_message_at timestamp with time zone,
    embedding public.vector(1024),
    generator_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    user_modified boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT episodes_source_message_count_check CHECK ((source_message_count >= 0))
);
CREATE TABLE public.llm_budget_reservations (
    reservation_id uuid NOT NULL,
    reset_key text NOT NULL,
    reserved_usd double precision NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    recorded_at timestamp with time zone,
    CONSTRAINT llm_budget_reservations_status_check CHECK ((status = ANY (ARRAY['active'::text, 'recorded'::text, 'expired'::text])))
);
CREATE TABLE public.llm_budget_windows (
    reset_key text NOT NULL,
    spent_usd double precision DEFAULT 0 NOT NULL,
    reserved_usd double precision DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE public.maintenance_frontiers (
    user_name text NOT NULL,
    project_id text NOT NULL,
    frontier_message_id bigint DEFAULT 0 NOT NULL,
    frontier_timestamp_ms bigint,
    frontier_token text NOT NULL,
    captured_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT maintenance_frontiers_frontier_message_id_check CHECK ((frontier_message_id >= 0))
);
CREATE TABLE public.maintenance_reinterpretation_audits (
    audit_id uuid NOT NULL,
    user_name text NOT NULL,
    project_id text NOT NULL,
    observation_ids jsonb NOT NULL,
    changes jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT maintenance_reinterpretation_audits_changes_check CHECK ((jsonb_typeof(changes) = 'array'::text)),
    CONSTRAINT maintenance_reinterpretation_audits_observation_ids_check CHECK ((jsonb_typeof(observation_ids) = 'array'::text))
);
CREATE TABLE public.maintenance_review_checkpoints (
    user_name text NOT NULL,
    project_id text NOT NULL,
    last_reviewed_observation_id bigint DEFAULT 0 NOT NULL,
    last_completed_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT maintenance_review_checkpoin_last_reviewed_observation_id_check CHECK ((last_reviewed_observation_id >= 0))
);
CREATE TABLE public.maintenance_review_events (
    event_id bigint NOT NULL,
    review_id text NOT NULL,
    status text NOT NULL,
    actor text NOT NULL,
    reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT maintenance_review_events_status_check CHECK ((status = ANY (ARRAY['open'::text, 'applied'::text, 'dismissed'::text, 'stale'::text])))
);
CREATE SEQUENCE public.maintenance_review_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.maintenance_review_events_event_id_seq OWNED BY public.maintenance_review_events.event_id;
CREATE TABLE public.maintenance_review_evidence (
    review_id text NOT NULL,
    evidence_kind text NOT NULL,
    evidence_id text NOT NULL,
    observation_id bigint,
    snapshot jsonb DEFAULT '{}'::jsonb NOT NULL
);
CREATE TABLE public.maintenance_reviews (
    review_id text NOT NULL,
    user_name text NOT NULL,
    scope text NOT NULL,
    project_id text,
    kind text NOT NULL,
    dedupe_key text,
    evidence_refs jsonb DEFAULT '[]'::jsonb NOT NULL,
    evidence_snapshot jsonb DEFAULT '{}'::jsonb NOT NULL,
    reasoning text NOT NULL,
    proposed_plan jsonb NOT NULL,
    expected_state jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    signature text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_at timestamp with time zone,
    CONSTRAINT maintenance_reviews_evidence_refs_check CHECK ((jsonb_typeof(evidence_refs) = 'array'::text)),
    CONSTRAINT maintenance_reviews_evidence_snapshot_check CHECK ((jsonb_typeof(evidence_snapshot) = 'object'::text)),
    CONSTRAINT maintenance_reviews_expected_state_check CHECK ((jsonb_typeof(expected_state) = 'object'::text)),
    CONSTRAINT maintenance_reviews_project_scope_fk CHECK (((scope = 'user-global'::text) OR (project_id IS NOT NULL))),
    CONSTRAINT maintenance_reviews_proposed_plan_check CHECK ((jsonb_typeof(proposed_plan) = 'object'::text)),
    CONSTRAINT maintenance_reviews_reasoning_check CHECK ((btrim(reasoning) <> ''::text)),
    CONSTRAINT maintenance_reviews_scope_check CHECK ((scope = ANY (ARRAY['project'::text, 'user-global'::text]))),
    CONSTRAINT maintenance_reviews_status_check CHECK ((status = ANY (ARRAY['open'::text, 'applied'::text, 'dismissed'::text, 'stale'::text])))
);
CREATE TABLE public.message_entity_refs (
    message_id bigint NOT NULL,
    entity_id bigint NOT NULL
);
CREATE SEQUENCE public.message_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
CREATE TABLE public.message_revisions (
    user_name text NOT NULL,
    session_id text NOT NULL,
    project_id text NOT NULL,
    message_id bigint NOT NULL,
    revision integer NOT NULL,
    content text NOT NULL,
    created_at_ms bigint NOT NULL
);
CREATE TABLE public.message_source_refs (
    source_ref_id uuid NOT NULL,
    project_id text NOT NULL,
    session_id text NOT NULL,
    message_id bigint NOT NULL,
    source_kind text NOT NULL,
    document_id uuid,
    source_project_id text,
    canonical_url text,
    source_message_id bigint,
    content_hash text NOT NULL,
    locator jsonb NOT NULL,
    excerpt text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    encounter_kind text NOT NULL,
    agent_run_id text NOT NULL,
    tool_call_id text,
    result_position integer NOT NULL,
    idempotency_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT message_source_refs_encounter_check CHECK ((encounter_kind = ANY (ARRAY['document_search'::text, 'document_read'::text, 'document_selection'::text, 'user_pasted_text'::text, 'web_search'::text, 'news_search'::text, 'web_read'::text]))),
    CONSTRAINT message_source_refs_excerpt_check CHECK ((length(btrim(excerpt)) > 0)),
    CONSTRAINT message_source_refs_hash_check CHECK ((content_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT message_source_refs_kind_check CHECK ((source_kind = ANY (ARRAY['pdf_document'::text, 'text_document'::text, 'user_pasted_text'::text, 'web_search_result'::text, 'news_search_result'::text, 'web_page'::text, 'web_pdf'::text]))),
    CONSTRAINT message_source_refs_position_check CHECK ((result_position >= 0)),
    CONSTRAINT message_source_refs_source_project_shape_check CHECK ((((source_kind = ANY (ARRAY['pdf_document'::text, 'text_document'::text])) AND (source_project_id IS NOT NULL)) OR ((source_kind <> ALL (ARRAY['pdf_document'::text, 'text_document'::text])) AND (source_project_id IS NULL)))),
    CONSTRAINT message_source_refs_source_shape_check CHECK ((((source_kind = 'pdf_document'::text) AND (document_id IS NOT NULL) AND (source_project_id IS NOT NULL) AND (canonical_url IS NULL) AND (source_message_id IS NULL) AND (((tool_call_id IS NOT NULL) AND (encounter_kind = ANY (ARRAY['document_search'::text, 'document_read'::text]))) OR ((tool_call_id IS NULL) AND (encounter_kind = 'document_selection'::text))) AND ((locator ->> 'kind'::text) = 'pdf_page'::text) AND (jsonb_typeof((locator -> 'page'::text)) = 'number'::text) AND ((locator ->> 'page'::text) ~ '^[1-9][0-9]*$'::text) AND (COALESCE((metadata ->> 'document_name'::text), ''::text) <> ''::text)) OR ((source_kind = 'text_document'::text) AND (document_id IS NOT NULL) AND (source_project_id IS NOT NULL) AND (canonical_url IS NULL) AND (source_message_id IS NULL) AND (((tool_call_id IS NOT NULL) AND (encounter_kind = ANY (ARRAY['document_search'::text, 'document_read'::text]))) OR ((tool_call_id IS NULL) AND (encounter_kind = 'document_selection'::text))) AND ((((locator ->> 'kind'::text) = ANY (ARRAY['text_lines'::text, 'code_lines'::text])) AND (jsonb_typeof((locator -> 'start_line'::text)) = 'number'::text) AND (jsonb_typeof((locator -> 'end_line'::text)) = 'number'::text) AND ((locator ->> 'start_line'::text) ~ '^[1-9][0-9]*$'::text) AND ((locator ->> 'end_line'::text) ~ '^[1-9][0-9]*$'::text) AND (((locator ->> 'end_line'::text))::bigint >= ((locator ->> 'start_line'::text))::bigint)) OR (((locator ->> 'kind'::text) = 'csv_rows'::text) AND (jsonb_typeof((locator -> 'start_row'::text)) = 'number'::text) AND (jsonb_typeof((locator -> 'end_row'::text)) = 'number'::text) AND ((locator ->> 'start_row'::text) ~ '^[1-9][0-9]*$'::text) AND ((locator ->> 'end_row'::text) ~ '^[1-9][0-9]*$'::text) AND (((locator ->> 'end_row'::text))::bigint >= ((locator ->> 'start_row'::text))::bigint)) OR (((locator ->> 'kind'::text) = 'docx_paragraphs'::text) AND (jsonb_typeof((locator -> 'start_paragraph'::text)) = 'number'::text) AND (jsonb_typeof((locator -> 'end_paragraph'::text)) = 'number'::text) AND ((locator ->> 'start_paragraph'::text) ~ '^[1-9][0-9]*$'::text) AND ((locator ->> 'end_paragraph'::text) ~ '^[1-9][0-9]*$'::text) AND (((locator ->> 'end_paragraph'::text))::bigint >= ((locator ->> 'start_paragraph'::text))::bigint))) AND (COALESCE((metadata ->> 'document_name'::text), ''::text) <> ''::text)) OR ((source_kind = 'user_pasted_text'::text) AND (document_id IS NULL) AND (source_project_id IS NULL) AND (canonical_url IS NULL) AND (source_message_id IS NOT NULL) AND (tool_call_id IS NULL) AND (encounter_kind = 'user_pasted_text'::text) AND ((locator ->> 'kind'::text) = 'character_span'::text) AND (jsonb_typeof((locator -> 'start_char'::text)) = 'number'::text) AND (jsonb_typeof((locator -> 'end_char'::text)) = 'number'::text) AND ((locator ->> 'start_char'::text) ~ '^[0-9]+$'::text) AND ((locator ->> 'end_char'::text) ~ '^[1-9][0-9]*$'::text) AND (((locator ->> 'end_char'::text))::bigint > ((locator ->> 'start_char'::text))::bigint)) OR ((source_kind = ANY (ARRAY['web_search_result'::text, 'news_search_result'::text])) AND (document_id IS NULL) AND (source_project_id IS NULL) AND (source_message_id IS NULL) AND (canonical_url ~ '^https?://[^[:space:]#]+$'::text) AND (tool_call_id IS NOT NULL) AND ((locator ->> 'kind'::text) = 'search_result'::text) AND (COALESCE((locator ->> 'provider'::text), ''::text) <> ''::text) AND (COALESCE((locator ->> 'query'::text), ''::text) <> ''::text) AND (jsonb_typeof((locator -> 'rank'::text)) = 'number'::text) AND ((locator ->> 'rank'::text) ~ '^[1-9][0-9]*$'::text) AND (COALESCE((metadata ->> 'title'::text), ''::text) <> ''::text) AND ((metadata -> 'discovery_snippet'::text) = 'true'::jsonb) AND (((source_kind = 'web_search_result'::text) AND (encounter_kind = 'web_search'::text)) OR ((source_kind = 'news_search_result'::text) AND (encounter_kind = 'news_search'::text)))) OR ((source_kind = 'web_page'::text) AND (document_id IS NULL) AND (source_project_id IS NULL) AND (source_message_id IS NULL) AND (canonical_url ~ '^https?://[^[:space:]#]+$'::text) AND (tool_call_id IS NOT NULL) AND (encounter_kind = 'web_read'::text) AND ((locator ->> 'kind'::text) = 'text_lines'::text) AND (jsonb_typeof((locator -> 'start_line'::text)) = 'number'::text) AND (jsonb_typeof((locator -> 'end_line'::text)) = 'number'::text) AND ((locator ->> 'start_line'::text) ~ '^[1-9][0-9]*$'::text) AND ((locator ->> 'end_line'::text) ~ '^[1-9][0-9]*$'::text) AND (((locator ->> 'end_line'::text))::bigint >= ((locator ->> 'start_line'::text))::bigint) AND ((NOT (metadata ? 'title'::text)) OR (COALESCE((metadata ->> 'title'::text), ''::text) <> ''::text)) AND ((metadata -> 'discovery_snippet'::text) IS DISTINCT FROM 'true'::jsonb)) OR ((source_kind = 'web_pdf'::text) AND (document_id IS NULL) AND (source_project_id IS NULL) AND (source_message_id IS NULL) AND (canonical_url ~ '^https?://[^[:space:]#]+$'::text) AND (tool_call_id IS NOT NULL) AND (encounter_kind = 'web_read'::text) AND ((locator ->> 'kind'::text) = 'pdf_page'::text) AND (jsonb_typeof((locator -> 'page'::text)) = 'number'::text) AND ((locator ->> 'page'::text) ~ '^[1-9][0-9]*$'::text) AND ((NOT (metadata ? 'title'::text)) OR (COALESCE((metadata ->> 'title'::text), ''::text) <> ''::text)) AND ((metadata -> 'discovery_snippet'::text) IS DISTINCT FROM 'true'::jsonb))))
);
CREATE TABLE public.messages (
    user_name text NOT NULL,
    session_id text NOT NULL,
    message_id bigint NOT NULL,
    project_id text NOT NULL,
    role text NOT NULL,
    content text NOT NULL,
    search_tsvector tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, content)) STORED,
    user_msg_id bigint,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    acceptance_key text,
    timestamp_ms bigint,
    lifecycle_state text DEFAULT 'sealed'::text NOT NULL,
    editable_until_ms bigint,
    sealed_at_ms bigint,
    selected_revision integer DEFAULT 1 NOT NULL,
    replaces_message_id bigint,
    superseded_at_ms bigint,
    ingestion_state text DEFAULT 'excluded'::text NOT NULL,
    ingestion_not_before_ms bigint,
    ingestion_claim_id text,
    ingestion_claimed_at_ms bigint,
    ingestion_attempt_count integer DEFAULT 0 NOT NULL,
    ingestion_last_failure_stage text,
    ingestion_last_failure_code text,
    ingestion_last_failure_at_ms bigint,
    ingestion_last_error_summary text,
    exchange_state text DEFAULT 'open'::text NOT NULL,
    exchange_outcome text,
    exchange_closed_at_ms bigint,
    CONSTRAINT messages_ingestion_attempt_count_check CHECK ((ingestion_attempt_count >= 0)),
    CONSTRAINT messages_ingestion_state_check CHECK ((ingestion_state = ANY (ARRAY['waiting_for_seal'::text, 'ready'::text, 'claimed'::text, 'processed'::text, 'failed'::text, 'excluded'::text]))),
    CONSTRAINT messages_lifecycle_state_check CHECK ((lifecycle_state = ANY (ARRAY['editable'::text, 'sealed'::text, 'superseded'::text]))),
    CONSTRAINT messages_exchange_state_check CHECK ((exchange_state = ANY (ARRAY['open'::text, 'closed'::text]))),
    CONSTRAINT messages_exchange_user_shape_check CHECK (((role = 'user'::text) AND (((exchange_state = 'open'::text) AND (exchange_outcome IS NULL) AND (exchange_closed_at_ms IS NULL)) OR ((exchange_state = 'closed'::text) AND (exchange_outcome = ANY (ARRAY['assistant_final'::text, 'clarification'::text, 'failed'::text, 'cancelled'::text, 'user_only'::text])) AND (exchange_closed_at_ms IS NOT NULL) AND (exchange_closed_at_ms >= 0)))) OR ((role <> 'user'::text) AND (exchange_state = 'open'::text) AND (exchange_outcome IS NULL) AND (exchange_closed_at_ms IS NULL)))
);
CREATE TABLE public.project_artifact_revisions (
    artifact_id uuid NOT NULL,
    revision integer NOT NULL,
    schema_version integer NOT NULL,
    kind text NOT NULL,
    title text NOT NULL,
    status text NOT NULL,
    blocks jsonb NOT NULL,
    markdown text NOT NULL,
    content_hash text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT project_artifact_revisions_blocks_check CHECK (((jsonb_typeof(blocks) = 'array'::text) AND (jsonb_array_length(blocks) > 0))),
    CONSTRAINT project_artifact_revisions_hash_check CHECK ((content_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT project_artifact_revisions_kind_check CHECK ((kind = ANY (ARRAY['general'::text, 'research_brief'::text, 'research_report'::text]))),
    CONSTRAINT project_artifact_revisions_markdown_check CHECK (((length(btrim(markdown)) > 0) AND (length(markdown) <= 100000))),
    CONSTRAINT project_artifact_revisions_revision_check CHECK (((revision >= 1) AND (schema_version >= 1))),
    CONSTRAINT project_artifact_revisions_status_check CHECK ((status = ANY (ARRAY['complete'::text, 'incomplete'::text])))
);
CREATE TABLE public.project_artifacts (
    artifact_id uuid NOT NULL,
    user_name text NOT NULL,
    project_id text NOT NULL,
    session_id text NOT NULL,
    originating_message_id bigint NOT NULL,
    kind text NOT NULL,
    title text NOT NULL,
    status text DEFAULT 'complete'::text NOT NULL,
    current_revision integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT project_artifacts_kind_check CHECK ((kind = ANY (ARRAY['general'::text, 'research_brief'::text, 'research_report'::text]))),
    CONSTRAINT project_artifacts_revision_check CHECK ((current_revision >= 1)),
    CONSTRAINT project_artifacts_status_check CHECK ((status = ANY (ARRAY['complete'::text, 'incomplete'::text]))),
    CONSTRAINT project_artifacts_title_check CHECK (((length(btrim(title)) > 0) AND (length(title) <= 200)))
);
CREATE TABLE public.project_document_scan_settings (
    project_id text NOT NULL,
    settings jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE public.project_documents (
    document_id uuid NOT NULL,
    project_id text NOT NULL,
    original_name text NOT NULL,
    relative_path text NOT NULL,
    extension text DEFAULT ''::text NOT NULL,
    size_bytes bigint NOT NULL,
    content_hash text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    deleted_at timestamp with time zone,
    indexed_at timestamp with time zone,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT project_documents_relative_path_size_check CHECK (((octet_length(relative_path) >= 1) AND (octet_length(relative_path) <= 2048))),
    CONSTRAINT project_documents_size_check CHECK ((size_bytes >= 0)),
    CONSTRAINT project_documents_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'indexing'::text, 'indexed'::text, 'failed'::text, 'deleted'::text])))
);
CREATE TABLE public.project_entity_contexts (
    project_id text NOT NULL,
    entity_id bigint NOT NULL,
    user_name text NOT NULL,
    entity_type text NOT NULL,
    topic text DEFAULT 'General'::text NOT NULL,
    last_mentioned_ms bigint,
    created_at_ms bigint DEFAULT (floor((EXTRACT(epoch FROM clock_timestamp()) * (1000)::numeric)))::bigint NOT NULL,
    updated_at_ms bigint DEFAULT (floor((EXTRACT(epoch FROM clock_timestamp()) * (1000)::numeric)))::bigint NOT NULL,
    CONSTRAINT project_entity_contexts_topic_nonblank_check CHECK ((btrim(topic) <> ''::text)),
    CONSTRAINT project_entity_contexts_type_nonblank_check CHECK ((btrim(entity_type) <> ''::text))
);
CREATE TABLE public.project_read_scopes (
    user_name text NOT NULL,
    project_id text NOT NULL,
    readable_project_id text NOT NULL,
    CONSTRAINT project_read_scopes_check CHECK ((project_id <> readable_project_id))
);
CREATE TABLE public.projects (
    project_id text NOT NULL,
    user_name text NOT NULL,
    name text NOT NULL,
    description text,
    status text DEFAULT 'active'::text NOT NULL,
    domain_config jsonb NOT NULL,
    episode_window_size integer DEFAULT 24 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    archived_at timestamp with time zone,
    deleted_at timestamp with time zone,
    last_activity_at timestamp with time zone,
    CONSTRAINT projects_domain_config_check CHECK ((jsonb_typeof(domain_config) = 'object'::text)),
    CONSTRAINT projects_episode_window_size_check CHECK (((episode_window_size >= 8) AND (episode_window_size <= 72))),
    CONSTRAINT projects_status_check CHECK ((status = ANY (ARRAY['active'::text, 'archived'::text, 'deleted'::text])))
);
CREATE TABLE public.project_contexts (
    project_id text NOT NULL,
    user_name text NOT NULL,
    current_revision_id uuid,
    projection_hash text,
    projection_synced_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT project_contexts_projection_hash_check CHECK (((projection_hash IS NULL) OR (projection_hash ~ '^[0-9a-f]{64}$'::text)))
);
CREATE TABLE public.project_semantic_windows (
    window_id uuid NOT NULL,
    user_name text NOT NULL,
    project_id text NOT NULL,
    origin text DEFAULT 'conversation'::text NOT NULL,
    stage text DEFAULT 'claimed'::text NOT NULL,
    domain_version integer NOT NULL,
    policy_snapshot jsonb NOT NULL,
    source_token_count bigint DEFAULT 0 NOT NULL,
    token_estimator text NOT NULL,
    token_estimator_version text NOT NULL,
    overfill_tokens bigint DEFAULT 0 NOT NULL,
    overfill_ratio double precision DEFAULT 0 NOT NULL,
    episode_result_recorded boolean DEFAULT false NOT NULL,
    context_revision_id uuid,
    attempt_count integer DEFAULT 0 NOT NULL,
    last_failure_stage text,
    last_failure_code text,
    last_failure_at_ms bigint,
    last_error_summary text,
    next_retry_at_ms bigint,
    claimed_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT project_semantic_windows_attempt_count_check CHECK ((attempt_count >= 0)),
    CONSTRAINT project_semantic_windows_domain_version_check CHECK ((domain_version >= 0)),
    CONSTRAINT project_semantic_windows_failure_shape_check CHECK ((((last_failure_stage IS NULL) AND (last_failure_code IS NULL) AND (last_failure_at_ms IS NULL) AND (last_error_summary IS NULL)) OR ((last_failure_stage IS NOT NULL) AND (last_failure_code IS NOT NULL) AND (last_failure_at_ms IS NOT NULL) AND (last_error_summary IS NOT NULL)))),
    CONSTRAINT project_semantic_windows_origin_check CHECK ((origin = ANY (ARRAY['conversation'::text, 'human_edit'::text]))),
    CONSTRAINT project_semantic_windows_overfill_check CHECK (((overfill_tokens >= 0) AND (overfill_ratio >= (0)::double precision))),
    CONSTRAINT project_semantic_windows_policy_snapshot_check CHECK ((jsonb_typeof(policy_snapshot) = 'object'::text)),
    CONSTRAINT project_semantic_windows_source_token_count_check CHECK ((source_token_count >= 0)),
    CONSTRAINT project_semantic_windows_stage_check CHECK ((stage = ANY (ARRAY['claimed'::text, 'context_committed'::text, 'knowledge_committed'::text, 'completed'::text]))),
    CONSTRAINT project_semantic_windows_terminal_shape_check CHECK ((((stage = 'completed'::text) AND (completed_at IS NOT NULL)) OR ((stage <> 'completed'::text) AND (completed_at IS NULL)))),
    CONSTRAINT project_semantic_windows_token_estimator_check CHECK (((btrim(token_estimator) <> ''::text) AND (btrim(token_estimator_version) <> ''::text)))
);
CREATE TABLE public.project_semantic_window_messages (
    window_id uuid NOT NULL,
    project_id text NOT NULL,
    message_id bigint NOT NULL,
    session_id text NOT NULL,
    exchange_user_message_id bigint NOT NULL,
    role text NOT NULL,
    ordinal integer NOT NULL,
    CONSTRAINT project_semantic_window_messages_exchange_message_check CHECK ((exchange_user_message_id > 0)),
    CONSTRAINT project_semantic_window_messages_ordinal_check CHECK ((ordinal >= 0)),
    CONSTRAINT project_semantic_window_messages_role_check CHECK ((role = ANY (ARRAY['user'::text, 'assistant'::text])))
);
CREATE TABLE public.project_semantic_window_maintenance (
    window_id uuid NOT NULL,
    project_id text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    last_error text,
    enqueued_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT project_semantic_window_maintenance_attempt_count_check CHECK ((attempt_count >= 0)),
    CONSTRAINT project_semantic_window_maintenance_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'completed'::text]))),
    CONSTRAINT project_semantic_window_maintenance_terminal_shape_check CHECK ((((status = 'completed'::text) AND (completed_at IS NOT NULL)) OR ((status = 'pending'::text) AND (completed_at IS NULL))))
);
CREATE TABLE public.project_semantic_window_episodes (
    window_id uuid NOT NULL,
    project_id text NOT NULL,
    episode_id text NOT NULL,
    ordinal integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT project_semantic_window_episodes_ordinal_check CHECK ((ordinal >= 0))
);
CREATE TABLE public.project_context_revisions (
    revision_id uuid NOT NULL,
    project_id text NOT NULL,
    revision_number integer NOT NULL,
    parent_revision_id uuid,
    window_id uuid,
    origin text NOT NULL,
    domain_version integer NOT NULL,
    edit_summary text DEFAULT ''::text NOT NULL,
    content_hash text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT project_context_revisions_content_hash_check CHECK ((content_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT project_context_revisions_domain_version_check CHECK ((domain_version >= 0)),
    CONSTRAINT project_context_revisions_edit_summary_check CHECK ((length(edit_summary) <= 2000)),
    CONSTRAINT project_context_revisions_origin_check CHECK ((origin = ANY (ARRAY['conversation'::text, 'human_edit'::text]))),
    CONSTRAINT project_context_revisions_revision_number_check CHECK ((revision_number >= 1))
);
CREATE TABLE public.project_context_blocks (
    block_id uuid NOT NULL,
    project_id text NOT NULL,
    section_key text NOT NULL,
    markdown text NOT NULL,
    content_hash text NOT NULL,
    assertion_kind text NOT NULL,
    supersedes_block_id uuid,
    source_time_ms bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT project_context_blocks_assertion_kind_check CHECK ((assertion_kind = ANY (ARRAY['user_asserted'::text, 'source_grounded'::text, 'agent_derived'::text, 'human_asserted'::text]))),
    CONSTRAINT project_context_blocks_content_hash_check CHECK ((content_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT project_context_blocks_markdown_check CHECK (((length(btrim(markdown)) > 0) AND (length(markdown) <= 50000))),
    CONSTRAINT project_context_blocks_section_key_check CHECK ((section_key ~ '^[a-z][a-z0-9_]{0,39}$'::text)),
    CONSTRAINT project_context_blocks_source_time_check CHECK (((source_time_ms IS NULL) OR (source_time_ms >= 0)))
);
CREATE TABLE public.project_context_revision_blocks (
    revision_id uuid NOT NULL,
    project_id text NOT NULL,
    block_id uuid NOT NULL,
    ordinal integer NOT NULL,
    CONSTRAINT project_context_revision_blocks_ordinal_check CHECK ((ordinal >= 0))
);
CREATE TABLE public.project_context_block_supports (
    block_id uuid NOT NULL,
    project_id text NOT NULL,
    message_id bigint NOT NULL,
    session_id text NOT NULL,
    source_ref_id uuid,
    support_kind text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT project_context_block_supports_kind_check CHECK ((support_kind = ANY (ARRAY['user_message'::text, 'assistant_message'::text, 'assistant_source'::text]))),
    CONSTRAINT project_context_block_supports_source_shape_check CHECK ((((support_kind = 'assistant_source'::text) AND (source_ref_id IS NOT NULL)) OR ((support_kind <> 'assistant_source'::text) AND (source_ref_id IS NULL))))
);
CREATE TABLE public.project_context_revision_impact_blocks (
    revision_id uuid NOT NULL,
    project_id text NOT NULL,
    block_id uuid NOT NULL
);
CREATE TABLE public.context_block_entities (
    block_id uuid NOT NULL,
    project_id text NOT NULL,
    entity_id bigint NOT NULL,
    mention_text text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT context_block_entities_mention_text_check CHECK ((btrim(mention_text) <> ''::text))
);
CREATE TABLE public.relationship_observation_blocks (
    observation_id bigint NOT NULL,
    project_id text NOT NULL,
    block_id uuid NOT NULL
);
CREATE TABLE public.relationship_observations (
    observation_id bigint NOT NULL,
    relationship_id text,
    project_id text NOT NULL,
    user_name text NOT NULL,
    session_id text,
    message_id bigint,
    semantic_window_id uuid,
    source_entity_id bigint NOT NULL,
    target_entity_id bigint NOT NULL,
    observed_relationship_label text NOT NULL,
    interpretation_source text DEFAULT 'observed'::text NOT NULL,
    context text,
    observed_at_ms bigint NOT NULL,
    retired_at timestamp with time zone,
    retired_reason text,
    CONSTRAINT relationship_observations_distinct_entities CHECK ((source_entity_id <> target_entity_id)),
    CONSTRAINT relationship_observations_evidence_shape_check CHECK (((semantic_window_id IS NOT NULL) OR ((session_id IS NOT NULL) AND (message_id IS NOT NULL)))),
    CONSTRAINT relationship_observations_interpretation_source_check CHECK ((interpretation_source = ANY (ARRAY['observed'::text, 'domain'::text, 'review'::text]))),
    CONSTRAINT relationship_observations_observed_relationship_label_check CHECK ((btrim(observed_relationship_label) <> ''::text)),
    CONSTRAINT relationship_observations_retirement_shape_check CHECK ((((retired_at IS NULL) AND (retired_reason IS NULL)) OR ((retired_at IS NOT NULL) AND (btrim(retired_reason) <> ''::text))))
);
CREATE SEQUENCE public.relationship_observations_observation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.relationship_observations_observation_id_seq OWNED BY public.relationship_observations.observation_id;
CREATE TABLE public.relationships (
    relationship_id text NOT NULL,
    user_name text NOT NULL,
    project_id text NOT NULL,
    entity_a_id bigint NOT NULL,
    entity_b_id bigint NOT NULL,
    relationship_type text NOT NULL,
    "symmetric" boolean DEFAULT false NOT NULL,
    CONSTRAINT relationships_distinct_entities CHECK ((entity_a_id <> entity_b_id)),
    CONSTRAINT relationships_identity_matches_fields CHECK ((relationship_id = format('%s:%s:%s:%s'::text, project_id,
CASE
    WHEN "symmetric" THEN LEAST(entity_a_id, entity_b_id)
    ELSE entity_a_id
END,
CASE
    WHEN "symmetric" THEN GREATEST(entity_a_id, entity_b_id)
    ELSE entity_b_id
END, lower(regexp_replace(btrim(relationship_type), '\s+'::text, ' '::text, 'g'::text))))),
    CONSTRAINT relationships_relationship_type_check CHECK ((btrim(relationship_type) <> ''::text))
);
CREATE TABLE public.saved_web_links (
    link_id uuid NOT NULL,
    project_id text NOT NULL,
    url text NOT NULL,
    title text,
    summary text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT saved_web_links_url_check CHECK ((url ~* '^https?://[^[:space:]]+$'::text))
);
CREATE TABLE public.sessions (
    session_id text NOT NULL,
    user_name text NOT NULL,
    project_id text NOT NULL,
    model text,
    agent_id text,
    enabled_tools jsonb,
    document_focus jsonb,
    status text DEFAULT 'open'::text NOT NULL,
    episode_participation_enabled boolean DEFAULT true NOT NULL,
    episode_participation_after_message_id bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_active_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    CONSTRAINT sessions_episode_participation_after_message_id_check CHECK ((episode_participation_after_message_id >= 0)),
    CONSTRAINT sessions_status_check CHECK ((status = ANY (ARRAY['open'::text, 'deleted'::text])))
);
ALTER TABLE ONLY public.entity_global_merge_mutations ALTER COLUMN mutation_id SET DEFAULT nextval('public.entity_global_merge_mutations_mutation_id_seq'::regclass);
ALTER TABLE ONLY public.maintenance_review_events ALTER COLUMN event_id SET DEFAULT nextval('public.maintenance_review_events_event_id_seq'::regclass);
ALTER TABLE ONLY public.relationship_observations ALTER COLUMN observation_id SET DEFAULT nextval('public.relationship_observations_observation_id_seq'::regclass);
ALTER TABLE ONLY public.aac_discussions
    ADD CONSTRAINT aac_discussions_pkey PRIMARY KEY (discussion_id);
ALTER TABLE ONLY public.aac_insight_votes
    ADD CONSTRAINT aac_insight_votes_pkey PRIMARY KEY (insight_id, voter_agent_id);
ALTER TABLE ONLY public.aac_insights
    ADD CONSTRAINT aac_insights_pkey PRIMARY KEY (insight_id);
ALTER TABLE ONLY public.aac_timeline
    ADD CONSTRAINT aac_timeline_pkey PRIMARY KEY (timeline_id);
ALTER TABLE ONLY public.agent_brain_snapshots
    ADD CONSTRAINT agent_brain_snapshots_pkey PRIMARY KEY (agent_id, revision);
ALTER TABLE ONLY public.agent_tool_audits
    ADD CONSTRAINT agent_tool_audits_pkey PRIMARY KEY (audit_id);
ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_pkey PRIMARY KEY (agent_id);
ALTER TABLE ONLY public.document_chunks
    ADD CONSTRAINT document_chunks_document_index_unique UNIQUE (document_id, chunk_index);
ALTER TABLE ONLY public.document_chunks
    ADD CONSTRAINT document_chunks_pkey PRIMARY KEY (chunk_id);
ALTER TABLE ONLY public.document_extractions
    ADD CONSTRAINT document_extractions_pkey PRIMARY KEY (document_id);
ALTER TABLE public.entities
    ADD CONSTRAINT entities_canonical_name_nonblank_check CHECK ((btrim(canonical_name) <> ''::text));
ALTER TABLE public.entities
    ADD CONSTRAINT entities_id_positive_check CHECK ((entity_id > 0));
ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_id_user_key UNIQUE (entity_id, user_name);
ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (entity_id);
ALTER TABLE ONLY public.entity_aliases
    ADD CONSTRAINT entity_aliases_pkey PRIMARY KEY (entity_id, alias);
ALTER TABLE ONLY public.entity_global_merge_audits
    ADD CONSTRAINT entity_global_merge_audits_pkey PRIMARY KEY (merge_id);
ALTER TABLE ONLY public.entity_global_merge_mutations
    ADD CONSTRAINT entity_global_merge_mutations_merge_id_object_kind_object_k_key UNIQUE (merge_id, object_kind, object_key);
ALTER TABLE ONLY public.entity_global_merge_mutations
    ADD CONSTRAINT entity_global_merge_mutations_pkey PRIMARY KEY (mutation_id);
ALTER TABLE ONLY public.episode_entities
    ADD CONSTRAINT episode_entities_pkey PRIMARY KEY (episode_id, entity_id);
ALTER TABLE ONLY public.episode_messages
    ADD CONSTRAINT episode_messages_episode_id_message_position_key UNIQUE (episode_id, message_position);
ALTER TABLE ONLY public.episode_messages
    ADD CONSTRAINT episode_messages_pkey PRIMARY KEY (episode_id, message_id);
ALTER TABLE ONLY public.episode_processing_checkpoints
    ADD CONSTRAINT episode_processing_checkpoints_pkey PRIMARY KEY (project_id, session_id);
ALTER TABLE ONLY public.episode_relationships
    ADD CONSTRAINT episode_relationships_pkey PRIMARY KEY (episode_id, relationship_id);
ALTER TABLE ONLY public.episodes
    ADD CONSTRAINT episodes_id_project_key UNIQUE (episode_id, project_id);
ALTER TABLE ONLY public.episodes
    ADD CONSTRAINT episodes_pkey PRIMARY KEY (episode_id);
ALTER TABLE ONLY public.llm_budget_reservations
    ADD CONSTRAINT llm_budget_reservations_pkey PRIMARY KEY (reservation_id);
ALTER TABLE ONLY public.llm_budget_windows
    ADD CONSTRAINT llm_budget_windows_pkey PRIMARY KEY (reset_key);
ALTER TABLE ONLY public.maintenance_frontiers
    ADD CONSTRAINT maintenance_frontiers_pkey PRIMARY KEY (user_name, project_id);
ALTER TABLE ONLY public.maintenance_reinterpretation_audits
    ADD CONSTRAINT maintenance_reinterpretation_audits_pkey PRIMARY KEY (audit_id);
ALTER TABLE ONLY public.maintenance_review_checkpoints
    ADD CONSTRAINT maintenance_review_checkpoints_pkey PRIMARY KEY (user_name, project_id);
ALTER TABLE ONLY public.maintenance_review_events
    ADD CONSTRAINT maintenance_review_events_pkey PRIMARY KEY (event_id);
ALTER TABLE ONLY public.maintenance_review_evidence
    ADD CONSTRAINT maintenance_review_evidence_pkey PRIMARY KEY (review_id, evidence_kind, evidence_id);
ALTER TABLE ONLY public.maintenance_reviews
    ADD CONSTRAINT maintenance_reviews_pkey PRIMARY KEY (review_id);
ALTER TABLE ONLY public.maintenance_reviews
    ADD CONSTRAINT maintenance_reviews_signature_key UNIQUE (signature);
ALTER TABLE ONLY public.message_entity_refs
    ADD CONSTRAINT message_entity_refs_pkey PRIMARY KEY (message_id, entity_id);
ALTER TABLE ONLY public.message_revisions
    ADD CONSTRAINT message_revisions_pkey PRIMARY KEY (user_name, session_id, message_id, revision);
ALTER TABLE ONLY public.message_source_refs
    ADD CONSTRAINT message_source_refs_idempotency_key_key UNIQUE (idempotency_key);
ALTER TABLE ONLY public.message_source_refs
    ADD CONSTRAINT message_source_refs_id_message_scope_key UNIQUE (source_ref_id, message_id, project_id, session_id);
ALTER TABLE public.message_source_refs
    ADD CONSTRAINT message_source_refs_locator_range_check CHECK (
CASE (locator ->> 'kind'::text)
    WHEN 'text_lines'::text THEN
    CASE
        WHEN (((locator ->> 'start_line'::text) ~ '^[1-9][0-9]*$'::text) AND ((locator ->> 'end_line'::text) ~ '^[1-9][0-9]*$'::text)) THEN (((locator ->> 'end_line'::text))::bigint >= ((locator ->> 'start_line'::text))::bigint)
        ELSE false
    END
    WHEN 'code_lines'::text THEN
    CASE
        WHEN (((locator ->> 'start_line'::text) ~ '^[1-9][0-9]*$'::text) AND ((locator ->> 'end_line'::text) ~ '^[1-9][0-9]*$'::text)) THEN (((locator ->> 'end_line'::text))::bigint >= ((locator ->> 'start_line'::text))::bigint)
        ELSE false
    END
    WHEN 'csv_rows'::text THEN
    CASE
        WHEN (((locator ->> 'start_row'::text) ~ '^[1-9][0-9]*$'::text) AND ((locator ->> 'end_row'::text) ~ '^[1-9][0-9]*$'::text)) THEN (((locator ->> 'end_row'::text))::bigint >= ((locator ->> 'start_row'::text))::bigint)
        ELSE false
    END
    WHEN 'docx_paragraphs'::text THEN
    CASE
        WHEN (((locator ->> 'start_paragraph'::text) ~ '^[1-9][0-9]*$'::text) AND ((locator ->> 'end_paragraph'::text) ~ '^[1-9][0-9]*$'::text)) THEN (((locator ->> 'end_paragraph'::text))::bigint >= ((locator ->> 'start_paragraph'::text))::bigint)
        ELSE false
    END
    WHEN 'character_span'::text THEN
    CASE
        WHEN (((locator ->> 'start_char'::text) ~ '^[0-9]+$'::text) AND ((locator ->> 'end_char'::text) ~ '^[1-9][0-9]*$'::text)) THEN (((locator ->> 'end_char'::text))::bigint > ((locator ->> 'start_char'::text))::bigint)
        ELSE false
    END
    ELSE true
END);
ALTER TABLE ONLY public.message_source_refs
    ADD CONSTRAINT message_source_refs_pkey PRIMARY KEY (source_ref_id);
ALTER TABLE public.messages
    ADD CONSTRAINT messages_id_positive_check CHECK ((message_id > 0));
ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_id_project_session_key UNIQUE (message_id, project_id, session_id);
ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_message_id_key UNIQUE (message_id);
ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_pkey PRIMARY KEY (user_name, session_id, message_id);
ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_scope_project_key UNIQUE (user_name, session_id, message_id, project_id);
ALTER TABLE ONLY public.project_artifact_revisions
    ADD CONSTRAINT project_artifact_revisions_pkey PRIMARY KEY (artifact_id, revision);
ALTER TABLE ONLY public.project_artifacts
    ADD CONSTRAINT project_artifacts_origin_message_unique UNIQUE (originating_message_id);
ALTER TABLE ONLY public.project_artifacts
    ADD CONSTRAINT project_artifacts_pkey PRIMARY KEY (artifact_id);
ALTER TABLE ONLY public.project_document_scan_settings
    ADD CONSTRAINT project_document_scan_settings_pkey PRIMARY KEY (project_id);
ALTER TABLE ONLY public.project_documents
    ADD CONSTRAINT project_documents_id_project_key UNIQUE (document_id, project_id);
ALTER TABLE ONLY public.project_documents
    ADD CONSTRAINT project_documents_pkey PRIMARY KEY (document_id);
ALTER TABLE ONLY public.project_contexts
    ADD CONSTRAINT project_contexts_pkey PRIMARY KEY (project_id);
ALTER TABLE ONLY public.project_contexts
    ADD CONSTRAINT project_contexts_project_user_key UNIQUE (project_id, user_name);
ALTER TABLE ONLY public.project_context_revisions
    ADD CONSTRAINT project_context_revisions_pkey PRIMARY KEY (revision_id);
ALTER TABLE ONLY public.project_context_revisions
    ADD CONSTRAINT project_context_revisions_id_project_key UNIQUE (revision_id, project_id);
ALTER TABLE ONLY public.project_context_revisions
    ADD CONSTRAINT project_context_revisions_project_number_key UNIQUE (project_id, revision_number);
ALTER TABLE ONLY public.project_context_revisions
    ADD CONSTRAINT project_context_revisions_window_key UNIQUE (window_id);
ALTER TABLE ONLY public.project_context_blocks
    ADD CONSTRAINT project_context_blocks_pkey PRIMARY KEY (block_id);
ALTER TABLE ONLY public.project_context_blocks
    ADD CONSTRAINT project_context_blocks_id_project_key UNIQUE (block_id, project_id);
ALTER TABLE ONLY public.project_context_revision_blocks
    ADD CONSTRAINT project_context_revision_blocks_pkey PRIMARY KEY (revision_id, block_id);
ALTER TABLE ONLY public.project_context_revision_blocks
    ADD CONSTRAINT project_context_revision_blocks_ordinal_key UNIQUE (revision_id, ordinal);
ALTER TABLE ONLY public.project_context_revision_impact_blocks
    ADD CONSTRAINT project_context_revision_impact_blocks_pkey PRIMARY KEY (revision_id, block_id);
ALTER TABLE ONLY public.project_semantic_windows
    ADD CONSTRAINT project_semantic_windows_pkey PRIMARY KEY (window_id);
ALTER TABLE ONLY public.project_semantic_windows
    ADD CONSTRAINT project_semantic_windows_id_project_key UNIQUE (window_id, project_id);
ALTER TABLE ONLY public.project_semantic_window_messages
    ADD CONSTRAINT project_semantic_window_messages_pkey PRIMARY KEY (window_id, message_id);
ALTER TABLE ONLY public.project_semantic_window_messages
    ADD CONSTRAINT project_semantic_window_messages_message_key UNIQUE (message_id);
ALTER TABLE ONLY public.project_semantic_window_messages
    ADD CONSTRAINT project_semantic_window_messages_ordinal_key UNIQUE (window_id, ordinal);
ALTER TABLE ONLY public.project_semantic_window_maintenance
    ADD CONSTRAINT project_semantic_window_maintenance_pkey PRIMARY KEY (window_id);
ALTER TABLE ONLY public.project_semantic_window_episodes
    ADD CONSTRAINT project_semantic_window_episodes_pkey PRIMARY KEY (window_id, episode_id);
ALTER TABLE ONLY public.project_semantic_window_episodes
    ADD CONSTRAINT project_semantic_window_episodes_ordinal_key UNIQUE (window_id, ordinal);
ALTER TABLE ONLY public.context_block_entities
    ADD CONSTRAINT context_block_entities_pkey PRIMARY KEY (block_id, entity_id);
ALTER TABLE ONLY public.relationship_observation_blocks
    ADD CONSTRAINT relationship_observation_blocks_pkey PRIMARY KEY (observation_id, block_id);
ALTER TABLE ONLY public.project_entity_contexts
    ADD CONSTRAINT project_entity_contexts_pkey PRIMARY KEY (project_id, entity_id);
ALTER TABLE ONLY public.project_read_scopes
    ADD CONSTRAINT project_read_scopes_pkey PRIMARY KEY (user_name, project_id, readable_project_id);
ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (project_id);
ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_user_name_project_id_key UNIQUE (user_name, project_id);
ALTER TABLE ONLY public.relationship_observations
    ADD CONSTRAINT relationship_observations_pkey PRIMARY KEY (observation_id);
ALTER TABLE ONLY public.relationship_observations
    ADD CONSTRAINT relationship_observations_id_project_key UNIQUE (observation_id, project_id);
ALTER TABLE ONLY public.relationship_observations
    ADD CONSTRAINT relationship_observations_unique_evidence UNIQUE (project_id, user_name, session_id, message_id, source_entity_id, target_entity_id, observed_relationship_label);
ALTER TABLE ONLY public.relationship_observations
    ADD CONSTRAINT relationship_observations_unique_semantic_window_evidence UNIQUE (project_id, semantic_window_id, source_entity_id, target_entity_id, observed_relationship_label);
ALTER TABLE ONLY public.relationships
    ADD CONSTRAINT relationships_id_project_key UNIQUE (relationship_id, project_id);
ALTER TABLE ONLY public.relationships
    ADD CONSTRAINT relationships_pkey PRIMARY KEY (relationship_id);
ALTER TABLE ONLY public.relationships
    ADD CONSTRAINT relationships_project_pair_type_key UNIQUE (project_id, entity_a_id, entity_b_id, relationship_type);
ALTER TABLE ONLY public.saved_web_links
    ADD CONSTRAINT saved_web_links_pkey PRIMARY KEY (link_id);
ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_id_project_key UNIQUE (session_id, project_id);
ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (session_id);
CREATE INDEX aac_discussions_user_started_idx ON public.aac_discussions USING btree (user_name, started_at DESC);
CREATE INDEX aac_insights_user_visibility_created_idx ON public.aac_insights USING btree (user_name, visibility, created_at DESC);
CREATE INDEX aac_timeline_discussion_created_idx ON public.aac_timeline USING btree (discussion_id, created_at, timeline_id);
CREATE INDEX agent_brain_snapshots_user_idx ON public.agent_brain_snapshots USING btree (user_name, agent_id, revision DESC);
CREATE INDEX agent_tool_audits_run_idx ON public.agent_tool_audits USING btree (run_id, created_at);
CREATE INDEX agent_tool_audits_scope_idx ON public.agent_tool_audits USING btree (user_name, project_id, created_at DESC);
CREATE UNIQUE INDEX agents_one_default_per_user_idx ON public.agents USING btree (user_name) WHERE is_default;
CREATE INDEX document_chunks_document_idx ON public.document_chunks USING btree (document_id);
CREATE INDEX document_chunks_embedding_idx ON public.document_chunks USING hnsw (embedding public.vector_cosine_ops);
CREATE INDEX document_chunks_search_vector_idx ON public.document_chunks USING gin (search_vector);
CREATE INDEX context_block_entities_entity_idx ON public.context_block_entities USING btree (project_id, entity_id);
CREATE INDEX entities_embedding_idx ON public.entities USING hnsw (embedding public.vector_cosine_ops);
CREATE INDEX entities_user_name_idx ON public.entities USING btree (user_name, canonical_name);
CREATE INDEX entity_aliases_alias_idx ON public.entity_aliases USING btree (alias);
CREATE INDEX entity_global_merge_audits_user_idx ON public.entity_global_merge_audits USING btree (user_name, created_at DESC);
CREATE INDEX entity_global_merge_mutations_merge_idx ON public.entity_global_merge_mutations USING btree (merge_id, mutation_id);
CREATE INDEX episode_entities_lookup_idx ON public.episode_entities USING btree (entity_id, episode_id);
CREATE INDEX episode_messages_message_idx ON public.episode_messages USING btree (message_id, episode_id);
CREATE INDEX episode_relationships_lookup_idx ON public.episode_relationships USING btree (relationship_id, episode_id);
CREATE INDEX episodes_embedding_idx ON public.episodes USING hnsw (embedding public.vector_cosine_ops) WHERE (embedding IS NOT NULL);
CREATE INDEX episodes_project_updated_idx ON public.episodes USING btree (project_id, updated_at DESC);
CREATE INDEX episodes_search_tsvector_idx ON public.episodes USING gin (search_tsvector);
CREATE INDEX llm_budget_reservations_expiry_idx ON public.llm_budget_reservations USING btree (reset_key, expires_at) WHERE (status = 'active'::text);
CREATE INDEX maintenance_frontiers_updated_idx ON public.maintenance_frontiers USING btree (user_name, updated_at DESC);
CREATE INDEX maintenance_reinterpretation_audits_project_idx ON public.maintenance_reinterpretation_audits USING btree (project_id, created_at DESC);
CREATE INDEX maintenance_review_events_review_idx ON public.maintenance_review_events USING btree (review_id, created_at, event_id);
CREATE INDEX maintenance_review_evidence_observation_idx ON public.maintenance_review_evidence USING btree (observation_id) WHERE (observation_id IS NOT NULL);
CREATE INDEX maintenance_reviews_key_idx ON public.maintenance_reviews USING btree (user_name, project_id, kind, dedupe_key, created_at DESC);
CREATE INDEX maintenance_reviews_open_idx ON public.maintenance_reviews USING btree (user_name, project_id, status, created_at DESC);
CREATE INDEX message_entity_refs_entity_idx ON public.message_entity_refs USING btree (entity_id, message_id);
CREATE INDEX message_source_refs_episode_lookup_idx ON public.message_source_refs USING btree (project_id, session_id, message_id, created_at);
CREATE INDEX message_source_refs_message_scope_idx ON public.message_source_refs USING btree (message_id, project_id, session_id);
CREATE UNIQUE INDEX messages_acceptance_key_idx ON public.messages USING btree (user_name, session_id, acceptance_key) WHERE (acceptance_key IS NOT NULL);
CREATE INDEX messages_ingestion_queue_idx ON public.messages USING btree (user_name, session_id, message_id) WHERE ((role = 'user'::text) AND (ingestion_state = ANY (ARRAY['waiting_for_seal'::text, 'ready'::text, 'claimed'::text])));
CREATE INDEX messages_project_idx ON public.messages USING btree (user_name, project_id, message_id);
CREATE INDEX messages_search_tsvector_idx ON public.messages USING gin (search_tsvector);
CREATE INDEX project_context_blocks_project_section_idx ON public.project_context_blocks USING btree (project_id, section_key, created_at);
CREATE INDEX project_context_revision_blocks_snapshot_idx ON public.project_context_revision_blocks USING btree (revision_id, ordinal);
CREATE INDEX project_context_revision_impact_blocks_project_idx ON public.project_context_revision_impact_blocks USING btree (project_id, block_id);
CREATE UNIQUE INDEX project_context_block_supports_without_source_unique_idx ON public.project_context_block_supports USING btree (block_id, message_id, session_id, support_kind) WHERE (source_ref_id IS NULL);
CREATE UNIQUE INDEX project_context_block_supports_with_source_unique_idx ON public.project_context_block_supports USING btree (block_id, message_id, session_id, source_ref_id, support_kind) WHERE (source_ref_id IS NOT NULL);
CREATE UNIQUE INDEX messages_one_assistant_per_exchange_idx ON public.messages USING btree (user_name, project_id, session_id, user_msg_id) WHERE (role = 'assistant'::text);
CREATE INDEX project_context_revisions_project_created_idx ON public.project_context_revisions USING btree (project_id, revision_number DESC);
CREATE INDEX project_semantic_window_messages_window_ordinal_idx ON public.project_semantic_window_messages USING btree (window_id, ordinal);
CREATE INDEX project_semantic_window_maintenance_pending_idx ON public.project_semantic_window_maintenance USING btree (project_id, updated_at) WHERE (status = 'pending'::text);
CREATE INDEX project_semantic_window_episodes_window_ordinal_idx ON public.project_semantic_window_episodes USING btree (window_id, ordinal);
CREATE UNIQUE INDEX project_semantic_windows_one_active_per_project_idx ON public.project_semantic_windows USING btree (project_id) WHERE (stage <> 'completed'::text);
CREATE INDEX project_semantic_windows_retry_idx ON public.project_semantic_windows USING btree (project_id, next_retry_at_ms) WHERE (stage <> 'completed'::text);
CREATE INDEX project_artifacts_project_updated_idx ON public.project_artifacts USING btree (project_id, updated_at DESC);
CREATE INDEX project_artifacts_session_updated_idx ON public.project_artifacts USING btree (session_id, updated_at DESC);
CREATE INDEX project_documents_hash_idx ON public.project_documents USING btree (project_id, content_hash);
CREATE UNIQUE INDEX project_documents_live_path_idx ON public.project_documents USING btree (project_id, relative_path) WHERE (status <> 'deleted'::text);
CREATE INDEX project_documents_project_idx ON public.project_documents USING btree (project_id, created_at DESC);
CREATE INDEX project_entity_contexts_activity_idx ON public.project_entity_contexts USING btree (project_id, last_mentioned_ms DESC NULLS LAST);
CREATE INDEX project_entity_contexts_entity_idx ON public.project_entity_contexts USING btree (user_name, entity_id);
CREATE INDEX project_entity_contexts_topic_idx ON public.project_entity_contexts USING btree (project_id, topic);
CREATE INDEX relationship_observations_message_idx ON public.relationship_observations USING btree (project_id, user_name, session_id, message_id);
CREATE INDEX relationship_observations_pattern_idx ON public.relationship_observations USING btree (project_id, user_name, interpretation_source, observed_relationship_label);
CREATE INDEX relationship_observations_relationship_idx ON public.relationship_observations USING btree (relationship_id, project_id);
CREATE INDEX relationship_observations_active_support_idx ON public.relationship_observations USING btree (project_id, relationship_id) WHERE (retired_at IS NULL);
CREATE INDEX relationship_observation_blocks_block_idx ON public.relationship_observation_blocks USING btree (project_id, block_id, observation_id);
CREATE INDEX relationships_entity_a_idx ON public.relationships USING btree (entity_a_id);
CREATE INDEX relationships_entity_b_idx ON public.relationships USING btree (entity_b_id);
CREATE INDEX relationships_pair_type_idx ON public.relationships USING btree (project_id, entity_a_id, entity_b_id, relationship_type);
CREATE INDEX saved_web_links_project_updated_idx ON public.saved_web_links USING btree (project_id, updated_at DESC, link_id DESC);
CREATE UNIQUE INDEX saved_web_links_project_url_idx ON public.saved_web_links USING btree (project_id, url);
CREATE INDEX sessions_project_idx ON public.sessions USING btree (user_name, project_id, created_at);
CREATE TRIGGER entities_identity_immutable_trigger BEFORE UPDATE ON public.entities FOR EACH ROW EXECUTE FUNCTION public.reject_entity_identity_mutation();
CREATE TRIGGER message_entity_refs_scope_trigger BEFORE INSERT OR UPDATE OF message_id, entity_id ON public.message_entity_refs FOR EACH ROW EXECUTE FUNCTION public.enforce_message_entity_ref_scope();
CREATE TRIGGER context_block_entities_scope_trigger BEFORE INSERT OR UPDATE OF block_id, project_id, entity_id ON public.context_block_entities FOR EACH ROW EXECUTE FUNCTION public.enforce_context_block_entity_scope();
CREATE TRIGGER relationships_scope_trigger BEFORE INSERT OR UPDATE OF user_name, project_id, entity_a_id, entity_b_id ON public.relationships FOR EACH ROW EXECUTE FUNCTION public.enforce_relationship_scope();
ALTER TABLE ONLY public.aac_insight_votes
    ADD CONSTRAINT aac_insight_votes_insight_id_fkey FOREIGN KEY (insight_id) REFERENCES public.aac_insights(insight_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.aac_insights
    ADD CONSTRAINT aac_insights_discussion_id_fkey FOREIGN KEY (discussion_id) REFERENCES public.aac_discussions(discussion_id) ON DELETE SET NULL;
ALTER TABLE ONLY public.aac_timeline
    ADD CONSTRAINT aac_timeline_discussion_id_fkey FOREIGN KEY (discussion_id) REFERENCES public.aac_discussions(discussion_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.agent_brain_snapshots
    ADD CONSTRAINT agent_brain_snapshots_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(agent_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.agent_tool_audits
    ADD CONSTRAINT agent_tool_audits_project_fk FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.document_chunks
    ADD CONSTRAINT document_chunks_document_id_fkey FOREIGN KEY (document_id) REFERENCES public.project_documents(document_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.document_extractions
    ADD CONSTRAINT document_extractions_document_id_fkey FOREIGN KEY (document_id) REFERENCES public.project_documents(document_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_redirect_entity_fk FOREIGN KEY (redirect_entity_id) REFERENCES public.entities(entity_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.entity_aliases
    ADD CONSTRAINT entity_aliases_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(entity_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.entity_global_merge_mutations
    ADD CONSTRAINT entity_global_merge_mutations_merge_id_fkey FOREIGN KEY (merge_id) REFERENCES public.entity_global_merge_audits(merge_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.episode_entities
    ADD CONSTRAINT episode_entities_entity_context_fk FOREIGN KEY (project_id, entity_id) REFERENCES public.project_entity_contexts(project_id, entity_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.episode_entities
    ADD CONSTRAINT episode_entities_episode_project_fk FOREIGN KEY (episode_id, project_id) REFERENCES public.episodes(episode_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.episode_messages
    ADD CONSTRAINT episode_messages_episode_scope_fk FOREIGN KEY (episode_id, project_id) REFERENCES public.episodes(episode_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.episode_messages
    ADD CONSTRAINT episode_messages_message_scope_fk FOREIGN KEY (message_id, project_id, session_id) REFERENCES public.messages(message_id, project_id, session_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.episode_processing_checkpoints
    ADD CONSTRAINT episode_processing_checkpoints_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.episode_processing_checkpoints
    ADD CONSTRAINT episode_processing_checkpoints_session_project_fk FOREIGN KEY (session_id, project_id) REFERENCES public.sessions(session_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.episode_relationships
    ADD CONSTRAINT episode_relationships_episode_project_fk FOREIGN KEY (episode_id, project_id) REFERENCES public.episodes(episode_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.episode_relationships
    ADD CONSTRAINT episode_relationships_relationship_project_fk FOREIGN KEY (relationship_id, project_id) REFERENCES public.relationships(relationship_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.episodes
    ADD CONSTRAINT episodes_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.llm_budget_reservations
    ADD CONSTRAINT llm_budget_reservations_reset_key_fkey FOREIGN KEY (reset_key) REFERENCES public.llm_budget_windows(reset_key) ON DELETE CASCADE;
ALTER TABLE ONLY public.maintenance_frontiers
    ADD CONSTRAINT maintenance_frontiers_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.maintenance_reinterpretation_audits
    ADD CONSTRAINT maintenance_reinterpretation_audits_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.maintenance_review_checkpoints
    ADD CONSTRAINT maintenance_review_checkpoints_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.maintenance_review_events
    ADD CONSTRAINT maintenance_review_events_review_id_fkey FOREIGN KEY (review_id) REFERENCES public.maintenance_reviews(review_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.maintenance_review_evidence
    ADD CONSTRAINT maintenance_review_evidence_observation_id_fkey FOREIGN KEY (observation_id) REFERENCES public.relationship_observations(observation_id) ON DELETE SET NULL;
ALTER TABLE ONLY public.maintenance_review_evidence
    ADD CONSTRAINT maintenance_review_evidence_review_id_fkey FOREIGN KEY (review_id) REFERENCES public.maintenance_reviews(review_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.maintenance_reviews
    ADD CONSTRAINT maintenance_reviews_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.message_entity_refs
    ADD CONSTRAINT message_entity_refs_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(entity_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.message_entity_refs
    ADD CONSTRAINT message_entity_refs_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.messages(message_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.message_revisions
    ADD CONSTRAINT message_revisions_user_name_session_id_message_id_project__fkey FOREIGN KEY (user_name, session_id, message_id, project_id) REFERENCES public.messages(user_name, session_id, message_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.message_source_refs
    ADD CONSTRAINT message_source_refs_message_scope_fk FOREIGN KEY (message_id, project_id, session_id) REFERENCES public.messages(message_id, project_id, session_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.message_source_refs
    ADD CONSTRAINT message_source_refs_source_message_scope_fk FOREIGN KEY (source_message_id, project_id, session_id) REFERENCES public.messages(message_id, project_id, session_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_session_project_fk FOREIGN KEY (session_id, project_id) REFERENCES public.sessions(session_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_contexts
    ADD CONSTRAINT project_contexts_project_scope_fk FOREIGN KEY (user_name, project_id) REFERENCES public.projects(user_name, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_contexts
    ADD CONSTRAINT project_contexts_current_revision_scope_fk FOREIGN KEY (current_revision_id, project_id) REFERENCES public.project_context_revisions(revision_id, project_id) ON DELETE SET NULL (current_revision_id);
ALTER TABLE ONLY public.project_context_revisions
    ADD CONSTRAINT project_context_revisions_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_context_revisions
    ADD CONSTRAINT project_context_revisions_parent_scope_fk FOREIGN KEY (parent_revision_id, project_id) REFERENCES public.project_context_revisions(revision_id, project_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.project_context_revisions
    ADD CONSTRAINT project_context_revisions_window_scope_fk FOREIGN KEY (window_id, project_id) REFERENCES public.project_semantic_windows(window_id, project_id) ON DELETE SET NULL (window_id);
ALTER TABLE ONLY public.project_context_blocks
    ADD CONSTRAINT project_context_blocks_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_context_blocks
    ADD CONSTRAINT project_context_blocks_supersedes_scope_fk FOREIGN KEY (supersedes_block_id, project_id) REFERENCES public.project_context_blocks(block_id, project_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.project_context_revision_blocks
    ADD CONSTRAINT project_context_revision_blocks_revision_scope_fk FOREIGN KEY (revision_id, project_id) REFERENCES public.project_context_revisions(revision_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_context_revision_blocks
    ADD CONSTRAINT project_context_revision_blocks_block_scope_fk FOREIGN KEY (block_id, project_id) REFERENCES public.project_context_blocks(block_id, project_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.project_context_revision_impact_blocks
    ADD CONSTRAINT project_context_revision_impact_blocks_revision_scope_fk FOREIGN KEY (revision_id, project_id) REFERENCES public.project_context_revisions(revision_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_context_revision_impact_blocks
    ADD CONSTRAINT project_context_revision_impact_blocks_block_scope_fk FOREIGN KEY (block_id, project_id) REFERENCES public.project_context_blocks(block_id, project_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.project_context_block_supports
    ADD CONSTRAINT project_context_block_supports_block_scope_fk FOREIGN KEY (block_id, project_id) REFERENCES public.project_context_blocks(block_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_context_block_supports
    ADD CONSTRAINT project_context_block_supports_message_scope_fk FOREIGN KEY (message_id, project_id, session_id) REFERENCES public.messages(message_id, project_id, session_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_context_block_supports
    ADD CONSTRAINT project_context_block_supports_source_scope_fk FOREIGN KEY (source_ref_id, message_id, project_id, session_id) REFERENCES public.message_source_refs(source_ref_id, message_id, project_id, session_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.context_block_entities
    ADD CONSTRAINT context_block_entities_block_scope_fk FOREIGN KEY (block_id, project_id) REFERENCES public.project_context_blocks(block_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.context_block_entities
    ADD CONSTRAINT context_block_entities_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(entity_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_semantic_windows
    ADD CONSTRAINT project_semantic_windows_project_scope_fk FOREIGN KEY (user_name, project_id) REFERENCES public.projects(user_name, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_semantic_windows
    ADD CONSTRAINT project_semantic_windows_context_revision_scope_fk FOREIGN KEY (context_revision_id, project_id) REFERENCES public.project_context_revisions(revision_id, project_id) ON DELETE SET NULL (context_revision_id);
ALTER TABLE ONLY public.project_semantic_window_messages
    ADD CONSTRAINT project_semantic_window_messages_window_scope_fk FOREIGN KEY (window_id, project_id) REFERENCES public.project_semantic_windows(window_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_semantic_window_messages
    ADD CONSTRAINT project_semantic_window_messages_message_scope_fk FOREIGN KEY (message_id, project_id, session_id) REFERENCES public.messages(message_id, project_id, session_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_semantic_window_messages
    ADD CONSTRAINT project_semantic_window_messages_exchange_scope_fk FOREIGN KEY (exchange_user_message_id, project_id, session_id) REFERENCES public.messages(message_id, project_id, session_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.project_semantic_window_maintenance
    ADD CONSTRAINT project_semantic_window_maintenance_window_scope_fk FOREIGN KEY (window_id, project_id) REFERENCES public.project_semantic_windows(window_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_semantic_window_episodes
    ADD CONSTRAINT project_semantic_window_episodes_window_scope_fk FOREIGN KEY (window_id, project_id) REFERENCES public.project_semantic_windows(window_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_semantic_window_episodes
    ADD CONSTRAINT project_semantic_window_episodes_episode_scope_fk FOREIGN KEY (episode_id, project_id) REFERENCES public.episodes(episode_id, project_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.project_artifact_revisions
    ADD CONSTRAINT project_artifact_revisions_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.project_artifacts(artifact_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_artifacts
    ADD CONSTRAINT project_artifacts_message_scope_fk FOREIGN KEY (originating_message_id, project_id, session_id) REFERENCES public.messages(message_id, project_id, session_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_artifacts
    ADD CONSTRAINT project_artifacts_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_artifacts
    ADD CONSTRAINT project_artifacts_session_scope_fk FOREIGN KEY (session_id, project_id) REFERENCES public.sessions(session_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_document_scan_settings
    ADD CONSTRAINT project_document_scan_settings_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_documents
    ADD CONSTRAINT project_documents_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_entity_contexts
    ADD CONSTRAINT project_entity_contexts_entity_id_user_name_fkey FOREIGN KEY (entity_id, user_name) REFERENCES public.entities(entity_id, user_name) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_entity_contexts
    ADD CONSTRAINT project_entity_contexts_user_name_project_id_fkey FOREIGN KEY (user_name, project_id) REFERENCES public.projects(user_name, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_read_scopes
    ADD CONSTRAINT project_read_scopes_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.project_read_scopes
    ADD CONSTRAINT project_read_scopes_readable_project_id_fkey FOREIGN KEY (readable_project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.relationship_observations
    ADD CONSTRAINT relationship_observations_message_fk FOREIGN KEY (user_name, session_id, message_id, project_id) REFERENCES public.messages(user_name, session_id, message_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.relationship_observations
    ADD CONSTRAINT relationship_observations_semantic_window_scope_fk FOREIGN KEY (semantic_window_id, project_id) REFERENCES public.project_semantic_windows(window_id, project_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.relationship_observations
    ADD CONSTRAINT relationship_observations_relationship_fk FOREIGN KEY (relationship_id, project_id) REFERENCES public.relationships(relationship_id, project_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.relationship_observations
    ADD CONSTRAINT relationship_observations_source_entity_id_fkey FOREIGN KEY (source_entity_id) REFERENCES public.entities(entity_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.relationship_observations
    ADD CONSTRAINT relationship_observations_target_entity_id_fkey FOREIGN KEY (target_entity_id) REFERENCES public.entities(entity_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.relationship_observation_blocks
    ADD CONSTRAINT relationship_observation_blocks_observation_scope_fk FOREIGN KEY (observation_id, project_id) REFERENCES public.relationship_observations(observation_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.relationship_observation_blocks
    ADD CONSTRAINT relationship_observation_blocks_block_scope_fk FOREIGN KEY (block_id, project_id) REFERENCES public.project_context_blocks(block_id, project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.relationships
    ADD CONSTRAINT relationships_entity_a_id_fkey FOREIGN KEY (entity_a_id) REFERENCES public.entities(entity_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.relationships
    ADD CONSTRAINT relationships_entity_b_id_fkey FOREIGN KEY (entity_b_id) REFERENCES public.entities(entity_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.saved_web_links
    ADD CONSTRAINT saved_web_links_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(agent_id) ON DELETE SET NULL;
ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(project_id) ON DELETE CASCADE;
