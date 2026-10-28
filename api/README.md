# Knoggin API

The `api/` package is the local HTTP bridge between `server/` and the app. One
process serves one user: it starts one `ApplicationRuntime` for
`RUNTIME_USER_NAME` and `SETTINGS_DIR` from `.env`, and requests cannot pick a
different user or settings store.

```bash
uv run --package api uvicorn app:create_app --factory --env-file .env --host 127.0.0.1
```

Or run everything, Postgres included, with `docker compose up -d` (see the root
README).

`GET /v1/docs` lists every route the API serves, read from the app itself.
This README covers the rules the route list cannot show.

## Conventions

- **Errors** are `{"error": {"code": "...", "message": "..."}}` with a matching
  HTTP status. Clients should branch on `code`.
- **Pages** return `items` and `next_cursor`; pass the cursor back unchanged.
- **Edits that can race** (domain, notebooks, episodes, project files) take an
  expected version, revision, or content hash and return 409 when it is stale.
- **Archived projects** stay readable; changes return `project_archived` (409).
- **Credentials** are only read from the environment (`OPENROUTER_API_KEY`,
  `BRAVE_API_KEY`, `TAVILY_API_KEY`, `JEV_API_KEY`) and appear in settings only
  as `*_configured` flags. Restart after changing one.
- **Settings** are edited with `PATCH /v1/settings/{section}`. Check
  `fully_applied` and `restart_required_fields` in the response. Every limit
  mentioned below lives under `developer_settings`.
- **CORS** allows `localhost:3000` and `127.0.0.1:3000`.

## Runs

- `POST /v1/sessions/{session_id}/runs` returns an SSE stream: `run_started`,
  `message_delta`, `tool`, `source_added`, `usage_updated`, `steer_applied`,
  then one of `run_completed`, `clarification_required`, `run_cancelled`, or
  `run_failed`. Every event carries `run_id` and a `sequence`.
- **Reconnecting:** a run keeps going for `run_events.unwatched_run_grace_seconds`
  (default 120) after its last viewer disconnects. Resume with
  `GET .../runs/{run_id}/events?after={last_sequence}`. If nobody reconnects in
  time, the run stops with `run_cancelled` and `"reason":"unwatched"`.
  `GET .../runs` gives the active run ID if the first event was missed. Live
  runs and their events do not survive an API restart.
- **During a run:** `POST .../steers` sends one instruction the next step
  applies, and `POST .../follow-ups` queues up to five later turns.
- **Annotations:** a run, steer, or follow-up may quote up to five spans of
  earlier answers: `{"message_id", "start", "end", "quote"}`. Offsets are
  Unicode code points (JavaScript must convert from UTF-16), and the quote must
  match the stored answer exactly.
- **Tools:** `enabled_tools` takes tool names or the groups `core`, `web`, and
  `maintenance`. A run's choice wins over the session's, then the agent's, then
  `tools.default_groups`.
- **Prompt cache:** editing project context, the project brief, or other text
  sent early in a model prompt can reduce provider cache reuse on later calls.
  Those calls may take longer or cost more until the new prefix is cached.
  A revision number alone does not imply a cache miss; provider behavior varies.

## Background work

The engine reports finished background work by adding a turn to the chat that
asked for it (shown in `/follow-ups`):

- **Research jobs** (`POST .../research-jobs`). After a restart, a job that was
  cut off is `interrupted` and waits for `POST .../research-jobs/{job_id}/resume`
  (or `research_jobs.resume_after_restart`), because rerunning it costs again.
- **Folder imports** given a `session_id` report once every file is indexed or
  has failed for good.
- **Duplicate entities** are checked every `jobs.duplicate_review.interval_hours`
  (default 5) and open `entity_merge` reviews. Nothing merges until the user
  applies one.

## Multi-step flows

- **Large folder upload:** `POST .../folders/intakes` with each file's path and
  size, `PUT` each file's bytes, `POST .../preview`, then `POST .../commit`
  until `complete`. Staging lives in the API process only.
- **Duplicate pairs that need a choice:** `GET /v1/maintenance/duplicates`, then
  `/v1/maintenance/merges/preview` with `context_choices`, then
  `POST /v1/maintenance/merges`. Or dismiss the pair for good.
- **JEV corrections:** flag through `.../jev-context-corrections` or
  `.../jev-identity-corrections`, inspect the review and preview, then apply.

## Database upgrades

The API brings the database up to date when it starts: it runs each file in
`server/src/infrastructure/migrations/` that the `schema_migrations` table has
not recorded, in order, each in its own transaction. A schema change goes in
the next numbered file; never edit one that has shipped. The API refuses a
database that has tables but no `schema_migrations` table.

## Backup and restore

Stop the API, then run `./backup.sh` from the repository root. It saves a
PostgreSQL dump, `knoggin.yml`, and the project files under `~/knoggin-backups`,
and `./backup.sh verify <dir>` checks one. It needs `pg_dump` and `pg_restore`
matching your Postgres version. The backup can contain API keys stored in
`knoggin.yml`, so keep it private.

Restore only into a new, empty database and empty folders:

```bash
uv run --package server python server/scripts/instance_backup.py restore <backup-dir> \
  --settings-dir ~/.config/knoggin-restored
```

Then point `DATABASE_URL` and `SETTINGS_DIR` at the new targets.

## Tests

`api/tests` covers the routes. Postgres-backed behavior is tested in
`server/tests/contract/storage`. Passing route tests do not prove a full
journey works; for that, run a real chat against a throwaway database.
