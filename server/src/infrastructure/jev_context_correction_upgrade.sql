-- One-time upgrade for existing local databases. Fresh databases use schema.sql.
BEGIN;
ALTER TABLE public.project_entity_contexts
    ADD COLUMN human_correction_review_id text;
COMMIT;
