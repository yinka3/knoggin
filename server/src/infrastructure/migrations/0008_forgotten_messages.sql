-- A message the user asked to forget stays stored but stops being memory
-- evidence. Retrieval reads only sealed messages, so it no longer surfaces.
-- An Episode linked to a message forgotten after its last update still needs
-- its narrative rewritten.
ALTER TABLE public.messages DROP CONSTRAINT messages_lifecycle_state_check;
ALTER TABLE public.messages ADD CONSTRAINT messages_lifecycle_state_check
    CHECK (lifecycle_state = ANY (ARRAY['editable', 'sealed', 'superseded', 'forgotten']));
ALTER TABLE public.messages ADD COLUMN forgotten_at timestamp with time zone;
