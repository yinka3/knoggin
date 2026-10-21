# Knoggin Server

This package contains Knoggin's Python engine: project and session management, agent tools, ingestion, retrieval, and Postgres storage. An embedding application starts the runtime and owns any user interface or HTTP service.

## Getting started

Follow the [repository setup](../README.md#local-setup) to start Postgres and install dependencies. The runtime entry point is `ApplicationRuntime.start(user_name=..., config_dir=...)` in `src/runtime/application.py`. Shut it down with `await runtime.shutdown()` when the host application exits.

The `config_dir` argument selects where Knoggin manages `knoggin.yml`. The runtime does not provide a standalone server command.

## Checks

Run these from the repository root:

```powershell
uv run --package server pytest server/tests
uv run ruff check server/src server/tests
```

The package is under active development and is not yet released.
