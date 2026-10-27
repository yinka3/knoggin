-- One-time upgrade for existing local databases. Fresh databases use schema.sql.
-- A workspace is a named top-level folder of a project.
ALTER TABLE public.projects
    ADD COLUMN IF NOT EXISTS workspaces text[] DEFAULT '{}'::text[] NOT NULL;
