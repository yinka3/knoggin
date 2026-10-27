-- One-time upgrade for existing local databases created with Apache AGE.
-- Run it while the old AGE-based image is still running, then rebuild the
-- Postgres image (docker compose build && docker compose up -d). Knoggin now
-- answers graph queries from the relationships table, so the AGE graph is a
-- copy that is no longer read or written.
--
-- The old image preloads AGE, which blocks dropping its schema. After the
-- rebuild, finish with: DROP SCHEMA IF EXISTS ag_catalog;
BEGIN;
LOAD 'age';
SELECT ag_catalog.drop_graph('knoggin_graph', true)
WHERE EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = 'knoggin_graph');
DROP EXTENSION IF EXISTS age CASCADE;
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I RESET search_path', current_database());
END
$$;
COMMIT;
