-- One-time upgrade for existing local databases. Fresh databases use schema.sql.
-- Closed exchanges are embedded by the project semantic job; existing
-- exchanges are backfilled the same way after this runs.
BEGIN;
CREATE TABLE public.exchange_embeddings (
    project_id text NOT NULL,
    session_id text NOT NULL,
    user_message_id bigint NOT NULL,
    embedding public.vector(768) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);
ALTER TABLE ONLY public.exchange_embeddings
    ADD CONSTRAINT exchange_embeddings_pkey PRIMARY KEY (project_id, user_message_id);
ALTER TABLE ONLY public.exchange_embeddings
    ADD CONSTRAINT exchange_embeddings_message_scope_fk FOREIGN KEY (user_message_id, project_id, session_id) REFERENCES public.messages(message_id, project_id, session_id) ON DELETE CASCADE;
CREATE INDEX exchange_embeddings_embedding_idx ON public.exchange_embeddings USING hnsw (embedding public.vector_cosine_ops);
COMMIT;
