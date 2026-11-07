-- Events an Episode's source messages report: what happened and when it
-- happened, as a date range when the wording is vague. One row is one
-- mention, so the same trip mentioned twice is two rows.
CREATE TABLE public.episode_events (
    episode_id text NOT NULL,
    project_id text NOT NULL,
    ordinal integer NOT NULL,
    what text NOT NULL,
    kind text NOT NULL,
    start_date date,
    end_date date,
    precision text NOT NULL,
    date_text text,
    source_message_ids bigint[] NOT NULL,
    PRIMARY KEY (episode_id, ordinal),
    CONSTRAINT episode_events_episode_scope_fk FOREIGN KEY (episode_id, project_id)
        REFERENCES public.episodes(episode_id, project_id) ON DELETE CASCADE,
    CONSTRAINT episode_events_range_check
        CHECK (start_date IS NULL OR end_date IS NULL OR end_date >= start_date),
    CONSTRAINT episode_events_sources_check CHECK (cardinality(source_message_ids) > 0)
);

CREATE INDEX episode_events_project_date_idx
    ON public.episode_events (project_id, start_date, end_date);
