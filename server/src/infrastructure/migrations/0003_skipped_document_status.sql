-- A document kept and tracked but not split or embedded: code, config and
-- data files inside a workspace, which agents read directly instead.
ALTER TABLE public.project_documents DROP CONSTRAINT project_documents_status_check;
ALTER TABLE public.project_documents ADD CONSTRAINT project_documents_status_check
    CHECK (status = ANY (ARRAY['queued', 'indexing', 'indexed', 'skipped', 'failed', 'deleted']));
