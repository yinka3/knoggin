-- Episode keyword search matches word forms ("finish" finds "finished") and
-- ignores stop words. Dropping the generated column also drops its index.
ALTER TABLE public.episodes DROP COLUMN search_tsvector;

ALTER TABLE public.episodes
    ADD COLUMN search_tsvector tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'english'::regconfig,
            summary || ' ' || new_developments::text || ' ' || updates::text || ' ' || unresolved::text
        )
    ) STORED;

CREATE INDEX episodes_search_tsvector_idx ON public.episodes USING gin (search_tsvector);
