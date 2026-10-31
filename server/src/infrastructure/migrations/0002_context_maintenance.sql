-- Marks the Context revision where the maintainer last compressed, merged, or
-- dropped blocks. It runs again after enough new Episodes.
ALTER TABLE public.project_context_revisions
    ADD COLUMN maintained boolean DEFAULT false NOT NULL;
