-- Workspaces become a jsonb object keyed by name, each with its settings, so
-- new settings need no new column. Starts with read_only.
ALTER TABLE public.projects ADD COLUMN workspace_settings jsonb DEFAULT '{}'::jsonb NOT NULL;
UPDATE public.projects SET workspace_settings = COALESCE(
    (SELECT jsonb_object_agg(name, jsonb_build_object('read_only', false))
     FROM unnest(workspaces) AS name),
    '{}'::jsonb
);
ALTER TABLE public.projects DROP COLUMN workspaces;
ALTER TABLE public.projects RENAME COLUMN workspace_settings TO workspaces;
ALTER TABLE public.projects
    ADD CONSTRAINT projects_workspaces_object_check CHECK (jsonb_typeof(workspaces) = 'object');
