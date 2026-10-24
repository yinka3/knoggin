ALTER TABLE public.project_documents
    ADD COLUMN IF NOT EXISTS extraction_strategy text DEFAULT 'auto'::text NOT NULL;

ALTER TABLE public.project_documents
    DROP CONSTRAINT IF EXISTS project_documents_extraction_strategy_check;

ALTER TABLE public.project_documents
    ADD CONSTRAINT project_documents_extraction_strategy_check
    CHECK (extraction_strategy = ANY (ARRAY['auto'::text, 'full'::text, 'bounded'::text]));
