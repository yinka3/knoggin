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

## Global settings

- `GET /v1/settings` returns the current settings and apply status. Credentials
  are represented only by `*_configured` flags.
- `GET /v1/settings/status` returns publication failures and fields that need
  a runtime restart to take effect.
- `PATCH /v1/settings/{section}` updates one section with a partial JSON body.
  Sections are `user_aliases` (a JSON array), `llm`, `search`, `jev`, and
  `developer_settings` (JSON objects). Updates are validated and saved by the
  engine's `ConfigManager`.
- `PUT /v1/settings/{section}/secrets/{field}` with `{"value":"..."}` sets a
  credential; `DELETE` on the same path clears it. Supported pairs are
  `llm/api_key`, `search/brave_api_key`, `search/tavily_api_key`, and
  `jev/api_key`. Credentials cannot be changed by the section PATCH route.
- `POST /v1/settings/reload` reloads the managed YAML file, and
  `POST /v1/settings/retry-applies` retries failed subscribers.

For example, after starting the API:

```bash
curl http://127.0.0.1:8000/v1/settings
curl -X PATCH http://127.0.0.1:8000/v1/settings/llm \
  -H 'Content-Type: application/json' \
  -d '{"agent_model":"google/gemini-2.5-flash-preview"}'
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
  `POST /v1/projects/{project_id}/archive` archives it. Deletion reports file
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
  model, agent, tool, research mode, document focus, and idempotency choices.
  It returns an SSE stream with `run_started`, `message_delta`, `tool`,
  `source_added`, `usage_updated`, `run_completed`, and `run_failed` events.
  Closing the HTTP stream closes the owned engine run.

CORS currently permits `localhost:3000` and `127.0.0.1:3000`.

## Other API routes in progress

- `GET /v1/health/engine` and `GET /v1/projects/{project_id}/health/{kind}`
  expose safe engine and project health snapshots. `kind` is `resources`,
  `ingestion`, or `background`.
- `GET /v1/community/discussions`, `GET /v1/community/insights`, and their
  timeline/vote routes read discussion history. Trigger and stop controls are
  `POST /v1/community/discussions/trigger` and `/stop`. Agent participation is
  `PUT /v1/agents/{agent_id}/aac-participation` with `{"enabled":true}`.
- Project document routes support list, upload, metadata, content, reindex, and
  delete. `POST /v1/documents/ocr-preflight` checks a PDF without saving it.
  `POST /v1/projects/{project_id}/sources/batch` admits up to 20 documents or
  web links and reports each result. `POST /v1/projects/{project_id}/sources/promote`
  saves a cited source as a link. Saved links support list, create, update, and
  delete. Artifact routes support list, metadata, and revision reads.
- `GET/PUT /v1/projects/{project_id}/episodes/{episode_id}` reads and edits an
  episode narrative. Edits require `expected_updated_at` and return a conflict
  when the episode changed.
- `/v1/maintenance/reviews` and project maintenance review routes expose
  review history, evidence preview, and explicit decisions. Global merge
  rollback has separate preview and apply routes.

The old server API adapter was removed in Phase 5. Live checks for newer routes
still need PostgreSQL to be running.

Temporary project runtime release now waits for scheduler shutdown outside the
project maintenance lock. This allows scheduler jobs to finish and prevents
new leases from using a runtime while it is shutting down.
