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
- Episodes: project-scoped narrative/revision reads and optimistic user edits.
- Artifacts and maintenance: existing browsing, revision reads, review decisions
  and preview/application operations.

## Episode editing

`GET /v1/projects/{project_id}/episodes/{episode_id}` returns the narrative,
user_modified flag and created/updated timestamps. It reads only the exact
user-owned project, not additional readable projects, and never resumes a
session. Archived projects can be read. Vectors, generator metadata and raw
source messages/evidence links are not included in this response.

`PATCH` on the same path replaces all four narrative fields: summary,
new_developments, updates and unresolved. All are required, including explicit
empty lists to clear a field. Include the timezone-aware expected_updated_at
from GET (or the last successful PATCH). Text is trimmed; blank entries,
unknown fields and client-supplied embeddings/source/ownership changes are
rejected. Lists are bounded to 100 entries each, individual text and aggregate
narrative to 20,000 characters, with the configured
developer_settings.jobs.episode.max_narrative_chars applied before embedding.

Successful PATCH returns episode_id, project_id, user_modified=true and the new
updated_at. Narrative and its regenerated search vector persist together through
KnowledgeStore; canonical source and evidence links do not change. Existing
automation respects user_modified rather than silently replacing curated text.
Missing/out-of-scope targets return 404; archived edits return 403; stale edits
return safe 409 episode_conflict. Read again before retrying a conflict.

An edit holds an exact active-project lease, preventing local archive/delete
until its owned work settles. The writer also checks active project ownership
and the revision timestamp atomically. Caller cancellation waits for an already
started edit and lease cleanup; the edit may commit without its caller receiving
the response. GET can confirm the persisted revision after disconnection.
These are server operations only; no SDK implementation is included.

## Management endpoint follow-up

The follow-up endpoint pass is now approved. Session, project and AAC endpoints
and settings endpoints are implemented. Their internal
methods stay available. Batch admission is approved for documents and web links
and is implemented as described below.

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
Project metadata/scope updates, archive and reactivation serialize with session
admission through the manager's ownership lock. Caller cancellation waits for an
already-started mutation to settle before releasing that lock; a mutation may
therefore complete even if its caller no longer receives the response. Failed
runtime shutdown retains its owner for retry instead of publishing a replacement.
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

Settings routes are `GET /v1/settings`, `PATCH /v1/settings`,
`POST /v1/settings/reload`, `GET /v1/settings/status` and
`POST /v1/settings/retry-applies`. PATCH takes `{ "updates": { ... } }`, a
partial RootConfig-shaped update validated against current configuration.
Unknown fields and invalid candidates return safe 422 responses without writes.
Credentials may be written but never read back. GET returns safe LLM model/budget
fields and credential-configured flags, not API keys or the provider base URL
(which can embed credentials). It is a public view, not a complete config export.

PATCH/reload return `accepted` plus application status. Persistence or reload
failure returns accepted=false without activating the candidate. Accepted=true
does not imply every subscriber applied it: inspect fully_applied, failed and
pending subscription IDs. Status is value-free. Reload reads existing YAML
without rewriting it; persisted refers to the accepted on-disk candidate, not
whether this request wrote it. Retry reapplies failed subscriptions without
rewriting config. Publication runs synchronously on the subscriber thread inside
the async route, not on a worker. File I/O/callbacks can block the event loop;
this preserves the existing manager's thread-ownership contract.

Batch admission is `POST /v1/projects/{project_id}/sources/batch` with an items
array (1–20 entries). Each item has source_type `document` plus the existing
upload fields, or `web_link` plus url and optional title/summary. The batch shares
the single-upload transport/encoded ceiling and a 50 MiB total decoded-content
ceiling. Oversized batches fail with 413 before admission; malformed JSON/item
shapes fail with 422. Items run sequentially under one exact project lease.

A 200 response contains ordered results with the original zero-based index and
status accepted_document, accepted_web_link or failed. Failures carry safe public
errors and request correlation. Invalid base64/URLs and service failures do not
prevent later items from running. Documents are admitted to indexing, not promised
fully indexed; web links are bookmarks, not fetched or indexed. There is no batch
transaction/rollback or batch idempotency key. Cancellation stops further items
but does not undo accepted items. A reported failure (for example after storage
commit but before response projection) does not guarantee nothing was persisted;
inspect the existing catalog before blindly retrying. Caller cancellation can
also leave accepted items without a received response.

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

## Management endpoint closeout (2026-09-27)

All 22 approved follow-up endpoints are implemented. Final verification passed
543 API/port/model/config/AAC/project/session/shutdown/document/architecture tests
plus one OpenAPI surface test. Ruff passed. No storage schema was changed and
PostgreSQL was not run locally; its existing contracts remain in the CI lane.
The earlier local Docling dependency limitation and smaller lifecycle reviews
remain separate. No SDK implementation or walkthrough was included.
