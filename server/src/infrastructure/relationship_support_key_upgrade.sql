-- One-time manual upgrade for relationship evidence-set identity.
-- Fresh databases already include support_key in schema.sql. Each existing
-- observation becomes one support group, identified by its sorted Context
-- block IDs; observations without block rows use the empty-set key.
-- Requires a maintenance window because both evidence tables are locked.
BEGIN;
LOCK TABLE public.relationship_observations,
           public.relationship_observation_blocks IN ACCESS EXCLUSIVE MODE;
ALTER TABLE public.relationship_observations
    ADD COLUMN support_key text;
UPDATE public.relationship_observations AS observation
SET support_key = md5(COALESCE((
    SELECT string_agg(support.block_id::text, ',' ORDER BY support.block_id::text)
    FROM public.relationship_observation_blocks AS support
    WHERE support.observation_id = observation.observation_id
      AND support.project_id = observation.project_id
), '[]'));
ALTER TABLE public.relationship_observations
    ALTER COLUMN support_key SET NOT NULL,
    ADD CONSTRAINT relationship_observations_support_key_check
        CHECK (support_key ~ '^[0-9a-f]{32}$');
ALTER TABLE public.relationship_observations
    DROP CONSTRAINT relationship_observations_unique_semantic_window_evidence;
ALTER TABLE public.relationship_observations
    ADD CONSTRAINT relationship_observations_unique_semantic_window_evidence
        UNIQUE (
            project_id,
            semantic_window_id,
            source_entity_id,
            target_entity_id,
            observed_relationship_label,
            support_key
        );
COMMIT;
