-- One-time upgrade for existing local databases. Fresh databases use schema.sql.
-- Each chat keeps one summary of the turns that no longer fit the prompt.
BEGIN;
ALTER TABLE public.sessions ADD COLUMN IF NOT EXISTS chat_summary text;
ALTER TABLE public.sessions
    ADD COLUMN IF NOT EXISTS chat_summary_through_message_id bigint DEFAULT 0 NOT NULL;
COMMIT;
