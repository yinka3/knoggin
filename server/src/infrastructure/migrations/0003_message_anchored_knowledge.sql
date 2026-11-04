-- Knowledge is extracted from each Episode's messages, not from Context blocks,
-- so entity occurrences and relationship evidence point at messages.
DROP TABLE public.episode_entity_occurrences;
DROP TABLE public.message_entity_occurrences;
DROP TABLE public.context_block_entity_occurrences;
DROP TABLE public.context_block_entities;
DROP FUNCTION public.enforce_context_block_entity_scope();
DROP TABLE public.relationship_observation_blocks;
-- Only filled gaps the local entity model left; that model is gone.
DROP TABLE public.project_jev_extraction_decisions;

CREATE TABLE public.message_entity_occurrences (
    window_id uuid NOT NULL,
    project_id text NOT NULL,
    pass_number integer NOT NULL,
    occurrence_index integer NOT NULL,
    message_id bigint NOT NULL,
    resolved_entity_id bigint NOT NULL,
    mention_text text NOT NULL,
    identity_decision_key text,
    corrected_entity_id bigint,
    human_correction_review_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT message_entity_occurrences_pkey PRIMARY KEY
        (window_id, pass_number, occurrence_index, message_id),
    CONSTRAINT message_entity_occurrences_mention_check CHECK (btrim(mention_text) <> ''),
    CONSTRAINT message_entity_occurrences_window_fk FOREIGN KEY (window_id, project_id)
        REFERENCES public.project_semantic_windows(window_id, project_id) ON DELETE CASCADE,
    CONSTRAINT message_entity_occurrences_message_fk FOREIGN KEY (message_id)
        REFERENCES public.messages(message_id) ON DELETE CASCADE,
    CONSTRAINT message_entity_occurrences_corrected_entity_fk FOREIGN KEY (corrected_entity_id)
        REFERENCES public.entities(entity_id)
);
CREATE INDEX message_entity_occurrences_ref_idx
    ON public.message_entity_occurrences (message_id, resolved_entity_id);
CREATE INDEX message_entity_occurrences_entity_idx
    ON public.message_entity_occurrences (project_id, resolved_entity_id);

CREATE TABLE public.episode_entity_occurrences (
    episode_id text NOT NULL,
    project_id text NOT NULL,
    window_id uuid NOT NULL,
    pass_number integer NOT NULL,
    occurrence_index integer NOT NULL,
    message_id bigint NOT NULL,
    resolved_entity_id bigint NOT NULL,
    corrected_entity_id bigint,
    human_correction_review_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT episode_entity_occurrences_pkey PRIMARY KEY
        (episode_id, window_id, pass_number, occurrence_index, message_id),
    CONSTRAINT episode_entity_occurrences_message_occurrence_fk FOREIGN KEY
        (window_id, pass_number, occurrence_index, message_id)
        REFERENCES public.message_entity_occurrences
        (window_id, pass_number, occurrence_index, message_id) ON DELETE CASCADE,
    CONSTRAINT episode_entity_occurrences_episode_fk FOREIGN KEY (episode_id, project_id)
        REFERENCES public.episodes(episode_id, project_id) ON DELETE CASCADE,
    CONSTRAINT episode_entity_occurrences_corrected_entity_fk FOREIGN KEY (corrected_entity_id)
        REFERENCES public.entities(entity_id)
);
CREATE INDEX episode_entity_occurrences_link_idx
    ON public.episode_entity_occurrences (project_id, episode_id, resolved_entity_id);

CREATE TABLE public.relationship_observation_messages (
    observation_id bigint NOT NULL,
    project_id text NOT NULL,
    message_id bigint NOT NULL,
    CONSTRAINT relationship_observation_messages_pkey PRIMARY KEY (observation_id, message_id),
    CONSTRAINT relationship_observation_messages_observation_fk FOREIGN KEY
        (observation_id, project_id)
        REFERENCES public.relationship_observations(observation_id, project_id) ON DELETE CASCADE,
    CONSTRAINT relationship_observation_messages_message_fk FOREIGN KEY (message_id)
        REFERENCES public.messages(message_id) ON DELETE CASCADE
);
CREATE INDEX relationship_observation_messages_message_idx
    ON public.relationship_observation_messages (message_id, observation_id);
