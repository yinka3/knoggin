
-- 3. File RAG / ChromaDB Replacement
CREATE TABLE IF NOT EXISTS file_chunks (
    id SERIAL PRIMARY KEY,
    file_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    chunk_index INT NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB,
    embedding vector(1024)
);

-- Index for fast vector similarity search on file chunks
CREATE INDEX IF NOT EXISTS file_chunks_embedding_idx 
ON file_chunks USING hnsw (embedding vector_cosine_ops);

-- 4. Entity and Message Vector/FTS search (Hybrid storage for the Graph)
-- Since AGE nodes don't support pgvector indexes directly inside `agtype`,
-- we store the heavy vectors and tsvectors in standard relational tables
-- and join them against the graph using the integer `id` property.

CREATE TABLE IF NOT EXISTS entity_search (
    entity_id BIGINT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    embedding vector(1024)
);

CREATE INDEX IF NOT EXISTS entity_search_embedding_idx 
ON entity_search USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS message_search (
    message_id BIGINT NOT NULL,
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    content_tsvector tsvector,
    PRIMARY KEY (user_name, session_id, message_id)
);

-- Index for Full-Text Search on messages
CREATE INDEX IF NOT EXISTS message_search_fts_idx 
ON message_search USING gin (content_tsvector);

CREATE TABLE IF NOT EXISTS fact_search (
    fact_id TEXT PRIMARY KEY,
    entity_id BIGINT NOT NULL,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    embedding vector(1024),
    invalid_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS fact_search_embedding_idx 
ON fact_search USING hnsw (embedding vector_cosine_ops);

-- 5. Multi-tenancy indexes
CREATE INDEX IF NOT EXISTS file_chunks_session_idx ON file_chunks(session_id);
CREATE INDEX IF NOT EXISTS entity_search_project_idx ON entity_search(user_name, project_id);
CREATE INDEX IF NOT EXISTS message_search_session_idx ON message_search(user_name, session_id);
CREATE INDEX IF NOT EXISTS fact_search_project_idx ON fact_search(user_name, project_id);
