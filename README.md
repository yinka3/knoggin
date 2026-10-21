# Knoggin

Knoggin is a local memory engine for AI agents. It turns conversations and project files into entities, relationships, and Episodes, while keeping links to the original sources. Each project supplies its own domain configuration for entity types, topics, aliases, and matching rules.

The project is under active development and is not yet released.

## Repository layout

- `server/` contains the Python engine, runtime, storage, and tests. See [server/README.md](server/README.md).
- `app/` contains an early React frontend. Its API integration is still in progress.
- `docker/` contains the local Postgres image and initialization scripts.

The engine runs inside an application and does not start an HTTP service on its own. Docker Compose starts the database only.

## Local setup

You need Python 3.12+, [uv](https://docs.astral.sh/uv/), and Docker Compose. From the repository root:

```powershell
Copy-Item .env.example .env
docker compose up -d
uv sync --package server
```

The copied `.env` works with the local Docker defaults. For a different database password, set both `POSTGRES_PASSWORD` and the password in `DATABASE_URL` to the same value before starting Postgres.

Model files download when first needed. To download them ahead of time, run:

```powershell
uv run --package server python server/scripts/prefetch_models.py
```

Stop Postgres with `docker compose down`. This keeps the database volume; `docker compose down -v` deletes it.

## Configuration

`.env.example` lists database and model settings. The application runtime also takes a configuration directory and manages `knoggin.yml` there. See [server/README.md](server/README.md) for its runtime entry point.

## Development

```powershell
uv run --package server pytest server/tests
uv run ruff check server/src server/tests
```

## License

[AGPL-3.0](LICENSE)

## Contact

Feedback is welcome at adedewe.a@northeastern.edu.
