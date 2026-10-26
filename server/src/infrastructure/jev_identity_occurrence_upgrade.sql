-- One-time upgrade for existing local databases. Fresh databases use schema.sql.
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
        REFERENCES public.project_context_blocks(block_id, project_id) ON DELETE CASCADE,
    CONSTRAINT context_block_entity_occurrences_entity_fk FOREIGN KEY (resolved_entity_id)
        REFERENCES public.entities(entity_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS context_block_entity_occurrences_entity_idx
    ON public.context_block_entity_occurrences (project_id, block_id, resolved_entity_id);
