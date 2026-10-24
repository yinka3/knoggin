# Knoggin API

The root `api/` package is the local HTTP bridge between `server/` and `app/`.
It starts one `ApplicationRuntime` for the configured user and shuts it down
when the HTTP process exits. API requests do not choose a user or config store.

From the repository root, after installing dependencies and starting Postgres:

```bash
uv run --package api uvicorn api.app:create_app --factory --env-file .env --host 127.0.0.1
```

Set `RUNTIME_USER_NAME` and `SETTINGS_DIR` in `.env`. The only route in
this first phase is `GET /v1/health`; settings and project routes follow in
later phases.
