-- One-time upgrade for existing local databases. Fresh databases use schema.sql.
-- The background duplicate review records each pair it checked, so an
-- unchanged pair is not sent to the merge model again.
BEGIN;
CREATE TABLE IF NOT EXISTS public.entity_duplicate_checks (
    user_name text NOT NULL,
    entity_a_id bigint NOT NULL,
    entity_b_id bigint NOT NULL,
    state_hash text NOT NULL,
    verdict text NOT NULL,
    checked_at timestamp with time zone DEFAULT now() NOT NULL
);
ALTER TABLE ONLY public.entity_duplicate_checks
    ADD CONSTRAINT entity_duplicate_checks_pkey PRIMARY KEY (user_name, entity_a_id, entity_b_id);
COMMIT;
