-- One-time manual upgrade for document admission origin.
-- Fresh databases already include scan_managed in schema.sql. Existing rows
-- default to direct admission because their original admission path cannot be
-- recovered reliably; this preserves user-uploaded documents across scans.
ALTER TABLE public.project_documents
    ADD COLUMN scan_managed boolean NOT NULL DEFAULT false;
