# Server public API boundary

The HTTP adapter uses the async ApplicationPort contract. Management responses
use canonical public fields, not alternate names or arbitrary object attributes.
The runtime port projects document and saved-link records onto public allowlists;
raw indexing errors and internal paths are not public document metadata.
Maintenance operation results remain domain report dictionaries inside `result`;
they are not a generic serialization path for arbitrary runtime objects.

## Existing capabilities

- Health: overall and project resource, ingestion and background snapshots.
- Projects and sessions: creation; session document-focus reads and changes.
- Runs: nonstream results and owned SSE streams using the same event contract.
- Documents: list, upload, metadata/content reads, reindex and deletion.
- Saved web links: list, update and deletion; existing source promotion.
- Folder scan settings: read, update and clear.
- Artifacts and maintenance: existing browsing, revision reads, review decisions
  and preview/application operations.

## Deferred capabilities

This cleanup does not add new endpoints. Session listing, history, updates and
deletion; AAC triggering, stopping, participation and timeline/Insight browsing;
global settings read/update/reload/application-status operations; and additional
project listing/update/archive/delete routes remain deferred. Their internal
methods stay available. Batch source admission is also deferred.

The SDK implementation and walkthrough belong to a separate pass after server
work. Future settings endpoints must keep credentials private and respect the
configuration subscriber-thread publication contract. AAC reads must preserve
the distinction between user-owned private Insights and agent-visible data.
`X-User-Name` selects a local user; it is not an authentication mechanism.

## Upload and failure contracts

Document uploads use base64 JSON, with a 50 MiB decoded-content limit. The
encoded field limit is derived from that limit. The transport body ceiling adds
64 KiB for JSON metadata; it checks actual received bytes, not just Content-Length.
Oversized requests return 413. The bounded body is buffered before JSON parsing;
this is not streaming multipart upload or a process-wide concurrent-memory cap.

Invalid request data returns 422. Invalid server responses return a safe internal
error, not a request-validation error. Public failures omit raw exception text
and unapproved details, and include request/run correlation IDs where available.
Once SSE has sent a terminal event, a later contract violation is logged without
sending a second terminal event. Stream cleanup failures do not overwrite an
already accepted outcome; no durable transport-cleanup retry queue is provided.

Completing this boundary pass does not complete every server lifecycle follow-up
listed in journal section 13.3, or verify PostgreSQL behavior locally. Database
contracts run in the configured CI PostgreSQL lane.

## Boundary audit (2026-09-27)

The API, public models/errors, session ownership, application shutdown,
configuration publication, AAC, maintenance and architecture audit passed 393
tests. Additional document/service/format/search and artifact checks passed 227
tests; two extraction tests could not reach their assertions because the local
environment lacks `docling_core`. One PostgreSQL test was deselected. Changed
API, port, public-model and boundary-test code passed Ruff.

No storage behavior changed in this boundary cleanup, so no new database
contracts were needed. `.github/workflows/server-tests.yml` provisions PostgreSQL
and runs the `requires_postgres` lane. That lane was inspected, not run locally.
Parser dependency verification and the separately tracked lifecycle follow-ups
remain outside this closeout; this is not a claim that the entire server is done.
