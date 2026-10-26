-- One-time upgrade for existing local databases. Fresh databases use schema.sql.
BEGIN;
ALTER TABLE public.project_notebooks
    ADD COLUMN board_state jsonb NOT NULL DEFAULT '{"questions": []}'::jsonb;
ALTER TABLE public.project_notebooks DROP CONSTRAINT project_notebooks_view_check;
ALTER TABLE public.project_notebooks
    ADD CONSTRAINT project_notebooks_view_check
    CHECK (view IN ('source_desk', 'timeline', 'evidence_board'));
ALTER TABLE public.project_notebooks
    ADD CONSTRAINT project_notebooks_board_state_check
    CHECK (jsonb_typeof(board_state) = 'object');
COMMIT;
