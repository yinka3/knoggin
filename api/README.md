# Knoggin API

The root `api/` package is the local HTTP bridge between `server/` and `app/`.
It starts one `ApplicationRuntime` for the configured user and shuts it down
when the HTTP process exits. API requests do not choose a user or config store.

From the repository root, after installing dependencies and starting Postgres:

```bash
uv run --package api uvicorn app:create_app --factory --env-file .env --host 127.0.0.1
```

Set `RUNTIME_USER_NAME` and `SETTINGS_DIR` in `.env`. The API uses the running
process's user; requests cannot select a different one.

## Whole-instance backup and restore

Stop the API and any other Knoggin writers before creating a backup. PostgreSQL
can stay running. Install compatible `pg_dump` and `pg_restore` command-line
tools on the machine running the CLI. The offline CLI captures a PostgreSQL custom dump, the
managed `knoggin.yml` settings file when present, and the configured project
library. It checks file hashes and rejects symlinks. The output directory is
private to the current OS user and must not already exist. It may contain API
keys from `knoggin.yml`; store it accordingly. `.env`, database credentials,
downloaded models, and code are not included.

From the repository root, the usual command is:

```bash
./backup.sh
```

It loads `.env` and creates a timestamped directory under
`~/knoggin-backups`. Pass a directory path as its only argument to choose a
different destination. Use `./backup.sh verify <backup-directory>` to check a
saved backup later. The lower-level CLI is also available when
`DATABASE_URL` and `SETTINGS_DIR` are already set in the environment:

```bash
uv run --package api python -m instance_backup backup ~/backups/knoggin-2026-10-24
uv run --package api python -m instance_backup verify ~/backups/knoggin-2026-10-24
```

Restore into a **new, empty PostgreSQL database** and empty filesystem paths,
while the API is stopped. Set `DATABASE_URL` to the new database first. The
restore command refuses a database with user tables and refuses nonempty file
destinations. An absolute library path in the saved settings needs an explicit
new `--library-root`. Restore writes that target path into `knoggin.yml`.

```bash
uv run --package api python -m instance_backup restore ~/backups/knoggin-2026-10-24 \
  --settings-dir ~/.config/knoggin-restored
```

After restoring, point the API's `DATABASE_URL` and `SETTINGS_DIR` at those new
targets, use a compatible code version and PostgreSQL extensions, and check project files, document
reads, notebooks, and a new run. If restore fails, discard the partial **new**
database and target directories before retrying. The command never replaces a
running or populated instance. A matching PostgreSQL client version is
required for `pg_dump` and `pg_restore`.

## Global settings

- `GET /v1/settings` returns the current settings and apply status. Credentials
  are represented only by `*_configured` flags.
- `GET /v1/settings/status` returns publication failures and fields that need
  a runtime restart to take effect.
- `GET /v1/settings/spending` returns LLM spend against the configured limit and
  `prompt_cache`: per-model prompt and cached token counts with a hit rate,
  counted since the API started.
- `PATCH /v1/settings/{section}` updates one section with a partial JSON body.
  Sections are `user_aliases` (a JSON array), `llm`, `search`, `jev`, and
  `developer_settings` (JSON objects). Updates are validated and saved by the
  engine's `ConfigManager`.
- Credentials are never saved in settings. Set them in the environment (or
  the repository `.env`): `OPENROUTER_API_KEY` for the LLM, `BRAVE_API_KEY` and
  `TAVILY_API_KEY` for web search, and `JEV_API_KEY` for JEV. Restart the API
  after changing one.
- `POST /v1/settings/reload` reloads the managed YAML file, and
  `POST /v1/settings/retry-applies` retries failed subscribers.

For example, after starting the API:

```bash
curl http://127.0.0.1:8000/v1/settings
curl -X PATCH http://127.0.0.1:8000/v1/settings/llm \
  -H 'Content-Type: application/json' \
  -d '{"agent_model":"google/gemini-2.5-flash"}'
curl http://127.0.0.1:8000/v1/settings/status
```

The PATCH response says whether the update was saved. Check `fully_applied`
and `restart_required_fields` before treating a saved value as effective for
every already-running service.

## Project settings

For an existing project, replace `{project_id}` in these routes:

- `GET /v1/projects/{project_id}/domain` reads the active domain revision.
- `POST /v1/projects/{project_id}/domain/validate` accepts a complete domain
  JSON object and returns `valid`, errors, warnings, and the canonical value.
- `POST /v1/projects/{project_id}/domain/preview` accepts the same object and
  describes changes that would affect future work.
- `POST /v1/projects/{project_id}/domain/activate` accepts
  `{"expected_version":1,"config":{...}}`. A stale version returns HTTP 409
  with the current version. Archived projects can be read and previewed but
  cannot be activated.
- `GET`, `PUT`, and `DELETE /v1/projects/{project_id}/scan-settings` read,
  replace, and reset folder scan rules. `PUT` accepts a `FolderScanSettings`
  JSON object; unspecified fields take their defaults. These operations require
  an active project. Changes affect later scans and persist across API restarts.

## Projects, agents, sessions, and runs

- `GET/POST /v1/projects` lists and creates projects. `GET/PATCH/DELETE
  /v1/projects/{project_id}` reads, edits, and deletes one project.
  `POST /v1/projects/{project_id}/archive` archives it; `POST
  /v1/projects/{project_id}/reactivate` restores an archived project. Deletion reports file
  cleanup status; an active session can block archive or deletion.
- `GET/POST /v1/agents` lists and creates agents. `GET/PATCH/DELETE
  /v1/agents/{agent_id}` manages editable preferences. `POST
  /v1/agents/{agent_id}/default` selects the default agent. Brain state is
  engine owned and is not returned by these routes.
- `GET/POST /v1/sessions` lists and creates sessions. Pass `project_id` as a
  query parameter to filter the list. `GET/PATCH/DELETE /v1/sessions/{session_id}`
  manages durable preferences. `GET /v1/sessions/{session_id}/history` reads
  recent messages. `GET/PUT/DELETE /v1/sessions/{session_id}/document-focus`
  reads, pins, and clears a document or subtree focus.
- `POST /v1/sessions/{session_id}/runs` accepts `{"query":"..."}` plus optional
  model, agent, tool, research mode, document focus, notebook, and idempotency choices.
  Set `notebook_id` to a project notebook UUID to pass its current, bounded
  snapshot to this run. Missing or changed sources are marked; changed passages
  are withheld until verified. The saved user query remains unchanged.
  It returns an SSE stream with `run_started`, `message_delta`, `tool`,
  `source_added`, `usage_updated`, `steer_applied`, `run_completed`, and `run_failed` events.
  Closing the HTTP stream closes the owned engine run.
- `GET /v1/sessions/{session_id}/runs/{run_id}` reads the active or most recent
  run status while its session runtime remains loaded. `POST .../steers` accepts
  `{"instruction":"..."}` for one active run and returns `pending`; the next
  model step applies it and emits `steer_applied`. If the run finishes first,
  the status becomes `undelivered`. These live statuses do not survive a
  process restart. The owning user message records the steer and its delivery
  state; applied instructions appear in later agent context.
  `DELETE /v1/sessions/{session_id}/runs/{run_id}` stops that
  active run.
- `POST /v1/sessions/{session_id}/runs/{run_id}/follow-ups` with
  `{"query":"..."}` queues a later turn for that live session. Up to five
  pending turns run in order after the current turn ends, using the session's
  preferences when each starts. `GET /v1/sessions/{session_id}/follow-ups`
  lists their statuses; `GET` or `DELETE` on `/follow-ups/{queue_id}` reads or
  cancels a pending item. Once started, use its `run_id` to stop the run.
  Queue status includes an `assistant_message_id` when completed; read the
  answer in session history. The queue is in memory and pending items are
  cancelled on session unload or API shutdown. Stopping one run does not
  cancel its queued follow-ups.
- `POST /v1/sessions/{session_id}/research-jobs` with
  `{"question":"...","links":["https://..."],"depth":"research"}` starts
  research in the background (`depth` may be `deep_research`). The agent can
  also start one itself with its `start_research_job` tool. Each job runs as a
  research turn in its own new session with one extra page read budgeted per
  link. Research runs, foreground or background, may read and may save findings
  by creating or appending project files (include the path in the question);
  they cannot edit the agent's Brain, run maintenance actions, overwrite, move,
  or delete files, or start jobs. While researching, the agent records each
  finding with `record_finding`: a claim under one planned subquestion, its
  strength, and short quotes that must appear in pages or documents the run
  read. Findings are shown on every step, fill the answer's research coverage,
  and add a findings table to the report artifact. `save_findings` writes them,
  grouped by subquestion with quotes and sources, to a new project file and
  lists that file in the project's "Research findings" notebook
  (`developer_settings.limits.max_research_findings`, default 30). When it ends, the starting session gets an
  automatic turn with the report (it appears in `/follow-ups`), and the agent
  there summarizes it. That turn is not added to memory; the job's own session
  is. `GET` on the same path lists the session's jobs; `DELETE
  .../research-jobs/{job_id}` cancels one. Up to
  `developer_settings.research_jobs.max_running_per_project` (default 3) run at
  once per project and the rest wait; `max_links` (default 50) bounds one job.
  Jobs are in memory: after a restart a job's session remains, but its report
  is not sent.

CORS currently permits `localhost:3000` and `127.0.0.1:3000`.

## Project control

- `GET /v1/projects/{project_id}/control` combines project status, session
  participation, a bounded blocked-window list, the open review count, and a
  brief ingestion-health summary.
- `GET/PUT /v1/projects/{project_id}/semantic-participation` reads or selects
  the sessions that contribute to future semantic processing. `PUT` accepts
  `{"session_ids":["..."]}`; switching participation does not retroactively
  admit earlier messages.
- `GET /v1/projects/{project_id}/maintenance/semantic-windows/blocked` lists
  failed windows. `POST /v1/projects/{project_id}/maintenance/semantic-windows/{window_id}/retry`
  retries one eligible failed window without replacing its captured membership
  or policy. The response contains only retry status fields, not its internal
  policy snapshot.
- `POST /v1/projects/{project_id}/memory/save` turns finished conversation into
  memory now. Normally Knoggin waits until a window of conversation fills
  (128K tokens, less for small extraction models) or five minutes pass with no
  new exchange. Returns 202, or 409 when
  the project is not running.

## Project notebooks

- `GET/POST /v1/projects/{project_id}/notebooks` lists and creates project-owned
  notebooks. `GET/PUT/DELETE /v1/projects/{project_id}/notebooks/{notebook_id}`
  reads, replaces, and deletes one notebook. Replacements and deletions require
  `expected_revision`; a stale revision returns 409. Entries are ordered and
  point to sources in the same project. Archived projects remain readable but
  cannot be edited.
- `GET /v1/projects/{project_id}/notebooks/{notebook_id}/source-desk`
  resolves the ordered sources with `current`, `changed`, or `missing` status.
  A `source_ref` contains a captured passage and its locator. A saved web link
  contains only its saved URL and summary; it does not imply the page was read.
- `GET /v1/projects/{project_id}/notebooks/{notebook_id}/timeline`
  orders entries by time. Each item labels `user_event`, `source_event`,
  `recorded`, or `unknown` time and includes `recorded_at` separately.

## Source, knowledge, and maintenance routes

- `GET /v1/health/engine` and `GET /v1/projects/{project_id}/health/{kind}`
  expose safe engine and project health snapshots. `kind` is `resources`,
  `ingestion`, or `background`.
- `GET /v1/community/discussions`, `GET /v1/community/insights`, and their
  timeline/vote routes read discussion history. Trigger and stop controls are
  `POST /v1/community/discussions/trigger` and `/stop`. Agent participation is
  `PUT /v1/agents/{agent_id}/aac-participation` with `{"enabled":true}`.
- Project document routes support list, upload, metadata, content, reindex, and
  delete. `GET /v1/projects/{project_id}/documents` accepts `limit` (1–100),
  `path_prefix`, and a `cursor`; `GET /v1/projects/{project_id}/links` accepts
  `limit` (1–100) and a `cursor`. Both return `items` and `next_cursor`; pass
  the returned cursor unchanged to read the next page.
  `GET /v1/projects/{project_id}/documents/search` accepts `query`, `limit` (1–50),
  and one of `document_id`, `relative_path`, or `path_prefix` to narrow results.
  Search returns bounded indexed passages in `items`. Content reads accept
  `start_line`, `end_line`, and, for PDFs,
  `page_number`; PDF responses include `total_pages`. Unindexed content returns
  `document_not_ready` (409), and invalid page or line ranges return
  `invalid_read_range` (422). Archived projects remain readable.
  `POST /v1/documents/ocr-preflight` checks a PDF without saving it.
  `POST /v1/projects/{project_id}/sources/batch` admits up to 20 documents or
  web links and reports each result. `POST /v1/projects/{project_id}/sources/promote`
  saves a cited source as a link after applying the agent web URL safety check.
  Saved links support list, create, update, and
  delete. Links normalize the HTTP(S) host and default port; a duplicate URL
  in the same project returns `link_exists` (409). Artifact routes support
  list, metadata, and revision reads.
- `POST /v1/projects/{project_id}/folders/preview` accepts `folder_name` and
  `files` containing `relative_path` and `content_base64`. It applies the
  project's saved scan rules and returns included and excluded paths without
  saving anything. Send the same manifest to `/folders/intake` to preview
  again and submit only included files; the response reports accepted and
  failed files separately. Both routes accept optional `force_include_paths`.
  This single-request form is capped at 50 MiB of decoded files.
- For a larger folder, `POST /v1/projects/{project_id}/folders/intakes` with
  `folder_name`, `files` containing `relative_path` and `size_bytes`, and
  optional `force_include_paths`. The response assigns each file a `file_id`.
  Upload each file's raw bytes with `PUT .../intakes/{intake_id}/files/{file_id}`;
  retrying the same bytes is safe. Then `POST .../intakes/{intake_id}/preview`
  runs one scan across the complete folder, including `.gitignore` rules.
  Review its included/excluded paths before calling `POST
  .../intakes/{intake_id}/commit` with `{}`. Each commit call handles one
  included file and returns every file's status. Repeat until the intake is
  `complete`; pass `{"retry_failed":true}` to retry a failed file. `GET` reads
  progress and `DELETE` cancels and removes the intake. The API clears intakes
  idle for two hours when another intake request arrives, and clears all
  staging on shutdown. Staging is local
  to the running API process, so an API restart requires a new intake.
  Individual files retain the 50 MiB upload limit. Staging allows up to 2 GiB
  per folder and two active intakes; the project's scan settings still decide
  which files can be admitted (by default 25 MiB per file, 500 MiB total,
  and 1,000 files).
- `GET/PUT /v1/projects/{project_id}/episodes/{episode_id}` reads and edits an
  episode narrative. Edits require `expected_updated_at` and return a conflict
  when the episode changed.
- `/v1/maintenance/reviews` and project maintenance review routes expose
  review history, evidence preview, and explicit decisions. Global merge
  rollback has separate preview and apply routes.

## JEV decisions and corrections

`GET /v1/projects/{project_id}/knowledge/entities/{entity_id}/jev-decisions`
and `GET /v1/projects/{project_id}/knowledge/context-blocks/{block_id}/jev-decisions`
explain committed JEV decisions. Add `?advanced=true` for bounded provenance.
An older decision without a final entity link can still appear under its
Context block; it does not prove which entity was ultimately selected.

To correct a type or topic decision, flag it through
`POST /v1/projects/{project_id}/maintenance/jev-context-corrections`, inspect
the maintenance review and preview, then apply an explicit review decision.
To correct one reused identity, use `.../jev-identity-corrections` to flag it,
inspect its review and impact preview, then call
`.../jev-identity-corrections/{review_id}/apply` with the expected state. The
review can move the exact mention to an existing entity or a new one. Later
semantic processing honors that occurrence correction until
`.../jev-identity-corrections/{review_id}/reopen` is called with the expected
target entity ID. Reopening permits later automatic evaluation; it keeps the
applied review in history. Corrections require current, exact occurrence
provenance, so some older decisions cannot be corrected this way. A global
merge remains the path when the whole entities are duplicates.

## Local verification

Run the API with a disposable PostgreSQL database when checking routes that
write project data. A simple `GET /v1/settings` confirms the HTTP process is
up; it does not verify an agent run, document citation, or restore. For those
journeys, complete a run, compare its `source_added` card with
`GET /v1/sessions/{session_id}/messages/{message_id}/sources`, and check the
saved project state after the run. Evidence-board evaluation requires a
configured JEV provider; an unconfigured instance returns HTTP 503 and does
not record a judgment. Confirming a ruled-out conclusion is a separate user
action after evaluation.

The focused API route tests live in `api/tests`. The PostgreSQL contracts for
board expiry and JEV corrections live in `server/tests/contract/storage`. These
checks complement live HTTP testing; a passing route test alone does not prove
the full journey works.

Temporary project runtime release now waits for scheduler shutdown outside the
project maintenance lock. This allows scheduler jobs to finish and prevents
new leases from using a runtime while it is shutting down.
