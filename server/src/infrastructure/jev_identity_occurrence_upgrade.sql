-- One-time upgrade for existing local databases. Fresh databases use schema.sql.
ALTER TABLE public.context_block_entities
    ADD COLUMN IF NOT EXISTS occurrence_provenance_complete boolean DEFAULT false NOT NULL;
ALTER TABLE public.message_entity_refs
    ADD COLUMN IF NOT EXISTS occurrence_provenance_complete boolean DEFAULT false NOT NULL;
CREATE TABLE IF NOT EXISTS public.context_block_entity_occurrences (
    window_id uuid NOT NULL,
    project_id text NOT NULL,
    pass_number integer NOT NULL,
    occurrence_index integer NOT NULL,
    block_id uuid NOT NULL,
    resolved_entity_id bigint NOT NULL,
    mention_text text NOT NULL,
    source_start integer,
    source_end integer,
    identity_decision_key text,
    corrected_entity_id bigint,
    human_correction_review_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT context_block_entity_occurrences_pkey PRIMARY KEY
        (window_id, pass_number, occurrence_index, block_id),
    CONSTRAINT context_block_entity_occurrences_offsets_check CHECK
        ((source_start IS NULL AND source_end IS NULL) OR
         (source_start >= 0 AND source_end > source_start)),
    CONSTRAINT context_block_entity_occurrences_mention_check CHECK (btrim(mention_text) <> ''),
    CONSTRAINT context_block_entity_occurrences_window_fk FOREIGN KEY (window_id, project_id)
        REFERENCES public.project_semantic_windows(window_id, project_id) ON DELETE CASCADE,
    CONSTRAINT context_block_entity_occurrences_block_fk FOREIGN KEY (block_id, project_id)
        REFERENCES public.project_context_blocks(block_id, project_id) ON DELETE CASCADE
);
ALTER TABLE public.context_block_entity_occurrences
    DROP CONSTRAINT IF EXISTS context_block_entity_occurrences_entity_fk,
    DROP CONSTRAINT IF EXISTS context_block_entity_occurrences_corrected_entity_fk;
ALTER TABLE public.context_block_entity_occurrences
    ADD COLUMN IF NOT EXISTS corrected_entity_id bigint,
    ADD COLUMN IF NOT EXISTS human_correction_review_id text;
ALTER TABLE public.context_block_entity_occurrences
    ADD CONSTRAINT context_block_entity_occurrences_corrected_entity_fk
    FOREIGN KEY (corrected_entity_id) REFERENCES public.entities(entity_id);
CREATE INDEX IF NOT EXISTS context_block_entity_occurrences_entity_idx
    ON public.context_block_entity_occurrences (project_id, block_id, resolved_entity_id);
CREATE TABLE IF NOT EXISTS public.message_entity_occurrences (
    window_id uuid NOT NULL,
    project_id text NOT NULL,
    pass_number integer NOT NULL,
    occurrence_index integer NOT NULL,
    block_id uuid NOT NULL,
    message_id bigint NOT NULL,
    resolved_entity_id bigint NOT NULL,
    corrected_entity_id bigint,
    human_correction_review_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT message_entity_occurrences_pkey PRIMARY KEY
        (window_id, pass_number, occurrence_index, block_id, message_id),
    CONSTRAINT message_entity_occurrences_context_fk FOREIGN KEY
        (window_id, pass_number, occurrence_index, block_id)
        REFERENCES public.context_block_entity_occurrences
        (window_id, pass_number, occurrence_index, block_id) ON DELETE CASCADE,
    CONSTRAINT message_entity_occurrences_message_fk FOREIGN KEY (message_id)
        REFERENCES public.messages(message_id) ON DELETE CASCADE,
    CONSTRAINT message_entity_occurrences_corrected_entity_fk FOREIGN KEY (corrected_entity_id)
        REFERENCES public.entities(entity_id)
);
CREATE INDEX IF NOT EXISTS message_entity_occurrences_ref_idx
    ON public.message_entity_occurrences (message_id, resolved_entity_id);
