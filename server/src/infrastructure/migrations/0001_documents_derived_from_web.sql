-- A project file the agent wrote after reading web content. Its passages may
-- quote untrusted pages, so the Context stage treats them like web sources.
ALTER TABLE public.project_documents
    ADD COLUMN derived_from_web boolean DEFAULT false NOT NULL;
