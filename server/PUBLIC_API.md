# Server public API boundary

The HTTP adapter uses the async ApplicationPort contract. Management responses
use canonical public fields, not alternate names or arbitrary object attributes.
The runtime port projects document and saved-link records onto public allowlists;
raw indexing errors and internal paths are not public document metadata.
Maintenance operation results remain domain report dictionaries inside `result`;
they are not a generic serialization path for arbitrary runtime objects.

## Existing capabilities

- Health: overall and project resource, ingestion and background snapshots.
- Projects and sessions: creation; session listing, history, metadata updates,
  deletion, and document-focus reads and changes.
- Runs: nonstream results and owned SSE streams using the same event contract.
- Documents: list, upload, metadata/content reads, reindex and deletion.
- Saved web links: list, update and deletion; existing source promotion.
- Folder scan settings: read, update and clear.
- Artifacts and maintenance: existing browsing, revision reads, review decisions
  and preview/application operations.

## Management endpoint follow-up

The follow-up endpoint pass is now approved. Session, project and AAC endpoints
are implemented. Global settings read/update/reload/application-status operations
remain pending. Their internal
methods stay available. Batch admission is approved for documents and web links
and remains pending implementation.

Session routes are `GET /v1/sessions`, `GET /v1/sessions/{session_id}/history`,
`PATCH /v1/sessions/{session_id}` and `DELETE /v1/sessions/{session_id}`. Listing
returns the user's open sessions. History returns the recent canonical window,
oldest first, with `limit` between 1 and 1000 (default 100). Neither read resumes
a runtime. Missing/non-open sessions return 404 for history/update/delete.
Deletion tombstones the session through its manager; it is not document deletion.
PATCH accepts only model, agent_id and enabled_tools. Omitted fields stay unchanged;
null restores inherited settings and an empty tools list disables all tools.
Listing currently uses the existing manager's complete open-session metadata read;
it is not a paginated database query.

Project routes are `GET /v1/projects`, `GET /v1/projects/{project_id}`,
`PATCH /v1/projects/{project_id}`, `POST /v1/projects/{project_id}/archive` and
`DELETE /v1/projects/{project_id}`. PATCH accepts name, description and
allowed_projects, not lifecycle status. Omitted or null fields are unchanged
(description can be emptied with an empty string); an empty allowed_projects
list clears additional readable scopes. Active leases block scope changes,
archive and deletion with 409, without forcibly unloading sessions.
Deletion uses the manager's hard-delete flow and returns file_cleanup_status
(`complete` or `pending`), including retries where only a cleanup task survives.
Missing projects return 404. Metadata responses omit domain configuration and
internal paths. Listing uses the existing full manager query, not pagination.

AAC routes are `POST /v1/aac/trigger`, `POST /v1/aac/stop`,
`PUT /v1/aac/agents/{agent_id}/participation` (body: enabled),
`GET /v1/aac/discussions`, `GET /v1/aac/discussions/{discussion_id}/timeline`,
`GET /v1/aac/insights` and `GET /v1/aac/insights/{insight_id}/votes`.
Trigger returns started/skipped with a bounded reason code. Stop returns whether
an active discussion received a stop request; it does not mean its current agent
run has already ended. Participation returns only the agent ID and persisted flag,
never the agent's Brain or configuration. Missing agents return 404.

Discussion/Insight lists accept limit 1–100 (default 20); timeline defaults to 100
and accepts after_sequence >= 0. Use the last event_sequence as the next cursor;
gaps are valid and allocation order is not concurrent transaction commit order.
Insight search accepts an optional query. These are user-owned reads: private
Insights are visible to that user, not exposed through an agent browsing interface.
Unknown or out-of-scope timeline/vote IDs return empty lists, matching the scoped
store queries. Votes use the existing complete vote list, without new pagination.

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
