-- A request and its cleanup targets survive retries until physical erasure.
CREATE TABLE public.project_forget_records (
    forget_id uuid PRIMARY KEY,
    project_id text NOT NULL REFERENCES public.projects(project_id) ON DELETE CASCADE,
    user_name text NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT now(),
    request_message_ids bigint[] NOT NULL,
    values text[] NOT NULL,
    block_ids uuid[] NOT NULL DEFAULT '{}',
    message_ids bigint[] NOT NULL DEFAULT '{}',
    episode_ids text[] NOT NULL DEFAULT '{}',
    rewritten_episode_ids text[] NOT NULL DEFAULT '{}',
    deleted_entity_ids bigint[] NOT NULL DEFAULT '{}',
    deleted_names text[] NOT NULL DEFAULT '{}',
    cleanup_completed_at timestamptz,
    UNIQUE (project_id, request_message_ids),
    CHECK (cardinality(request_message_ids) > 0)
);
CREATE INDEX project_forget_records_due_idx ON public.project_forget_records(requested_at);
