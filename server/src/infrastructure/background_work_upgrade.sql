-- One-time upgrade for existing local databases. Fresh databases use schema.sql.
-- Research jobs and folder notices are recorded so a restart does not lose them.
BEGIN;
CREATE TABLE IF NOT EXISTS public.background_work (
    work_id text NOT NULL,
    user_name text NOT NULL,
    kind text NOT NULL,
    project_id text NOT NULL,
    session_id text NOT NULL,
    state text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT background_work_kind_check CHECK ((kind = ANY (ARRAY['research_job'::text, 'folder_notice'::text])))
);
ALTER TABLE ONLY public.background_work
    ADD CONSTRAINT background_work_pkey PRIMARY KEY (work_id);
CREATE INDEX background_work_user_kind_idx ON public.background_work USING btree (user_name, kind, created_at);
COMMIT;
