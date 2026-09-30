-- One-time manual upgrade for an existing database, before deploying sequence
-- pagination. Fresh databases already get this column from schema.sql.
-- Preserves all events; historical order follows the previous reader ordering.
-- Requires a maintenance window: the table is locked until this transaction ends.
BEGIN;
LOCK TABLE public.aac_timeline IN ACCESS EXCLUSIVE MODE;
ALTER TABLE public.aac_timeline ADD COLUMN event_sequence bigint;
WITH ordered AS (
    SELECT timeline_id,
           row_number() OVER (ORDER BY created_at, timeline_id) AS sequence
    FROM public.aac_timeline
)
UPDATE public.aac_timeline AS timeline
SET event_sequence = ordered.sequence
FROM ordered
WHERE timeline.timeline_id = ordered.timeline_id;
ALTER TABLE public.aac_timeline ALTER COLUMN event_sequence SET NOT NULL;
ALTER TABLE public.aac_timeline ALTER COLUMN event_sequence ADD GENERATED ALWAYS AS IDENTITY;
SELECT setval(
    pg_get_serial_sequence('public.aac_timeline', 'event_sequence'),
    GREATEST(COALESCE(MAX(event_sequence), 0), 1),
    COUNT(*) > 0
) FROM public.aac_timeline;
CREATE UNIQUE INDEX aac_timeline_discussion_sequence_idx
    ON public.aac_timeline (discussion_id, event_sequence);
COMMIT;
