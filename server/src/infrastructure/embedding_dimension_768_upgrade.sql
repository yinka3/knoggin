-- Unreleased schema transition from the retired 1,024-dimension model contract.
-- Stored vectors cannot be resized without changing their meaning, so discard
-- derived vectors and let normal durable document indexing rebuild them.

DROP INDEX IF EXISTS public.document_chunks_embedding_idx;
DROP INDEX IF EXISTS public.episodes_embedding_idx;

DELETE FROM public.document_chunks;
UPDATE public.episodes SET embedding = NULL WHERE embedding IS NOT NULL;
DELETE FROM public.embedding_configuration;

ALTER TABLE public.document_chunks
    ALTER COLUMN embedding TYPE public.vector(768)
    USING embedding::public.vector(768);
ALTER TABLE public.episodes
    ALTER COLUMN embedding TYPE public.vector(768)
    USING embedding::public.vector(768);

UPDATE public.project_documents
SET status = 'queued',
    index_attempt_count = 0,
    next_index_retry_at = NULL,
    last_index_failure_kind = NULL,
    indexed_at = NULL,
    error_message = NULL,
    updated_at = now()
WHERE status <> 'deleted';

CREATE INDEX document_chunks_embedding_idx
    ON public.document_chunks USING hnsw (embedding public.vector_cosine_ops);
CREATE INDEX episodes_embedding_idx
    ON public.episodes USING hnsw (embedding public.vector_cosine_ops)
    WHERE embedding IS NOT NULL;
