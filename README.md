# Knoggin

Knoggin is a local, source-grounded memory engine for AI agents and personal tools. It turns conversations and project files into global entity identities, project-specific contexts, observed relationships, Episodes, and evidence—so an agent can retrieve useful context without losing the path back to the original source.

It is designed around a domain configuration supplied per project. Rather than inferring a universal ontology, Knoggin uses the entity types, aliases, relationships, topics, and matching rules that matter in your domain.

> Knoggin is an early personal project. The engine runtime is under active development and may change.

## What it does

- Keeps memory **source-grounded**: relationships, Episodes, and retrieval results retain links to the messages or documents that produced them.
- Keeps entity identity **user-global** while relationships, Episodes, documents, and entity classification remain project-scoped.
- Extracts and conservatively resolves entities with known aliases, GLiNER, and optional LLM proposals; duplicate merging is an explicit maintenance operation.
- Uses graph-aware and hybrid retrieval to give agents related context, not just isolated chunks.
- Uses confined project files as the authoritative document bytes and supports focused retrieval over a document or subtree.
- Builds bounded Episodes from completed conversational turns and returns compact cards before source-message expansion.
- Runs project-owned ingestion/Episode work and application-owned global entity-maintenance preflight.
- Records relationship conflicts and corrections as typed, evidence-backed maintenance reviews.

## Architecture

```mermaid
flowchart LR
    Client["Embedding application"]

    subgraph Engine["Knoggin engine"]
        Session["Projects, sessions, and agent runtime"]
        Agent["Agent and tools"]
        Ingest["Ingestion and extraction"]
        Jobs["Project jobs"]
        Maintenance["Application maintenance"]
    end

    Store["Postgres + Apache AGE\nevidence and graph"]

    Client --> Session
    Session --> Agent
    Session --> Ingest
    Agent --> Store
    Ingest --> Store
    Jobs <--> Store
    Maintenance <--> Store
```

The engine lives in `server/` and intentionally does not prescribe an HTTP transport. An embedding application owns its own integration surface while Knoggin owns the durable memory, retrieval, and background processing runtime.

## Quick start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker and Docker Compose

From the repository root, create local configuration and start the storage services:

```powershell
Copy-Item .env.example .env
docker compose up -d
uv sync --package server
```

The first engine start downloads the embedding, reranking, NLI, spaCy, and GLiNER models. To download those before integrating the engine, run:

```powershell
./setup.sh --prefetch-models
```

On Windows, run `setup.sh` from Git Bash, WSL, or another Bash-compatible shell. Copying `.env.example` directly is sufficient for local Docker defaults.

Stop the storage services when finished:

```powershell
docker compose down
```

This preserves the named Postgres volume. Use `docker compose down -v` only when you intend to remove local database data.

### Clean reset after schema cuts

Knoggin is unreleased and schema changes target a clean state rather than legacy compatibility. After pulling a schema cut, reset the local database and rebuild derived knowledge from canonical project files and conversations:

```bash
docker compose down -v
docker compose up -d --build
```

The first command permanently deletes the local Postgres volume. Restart Knoggin after Postgres becomes healthy, reopen or re-import the desired project sources, and run the project rebuild operation if existing canonical sources were restored. Do not treat old graph, embedding, Episode, or merge tables as portable data across a clean reset.

## Configuration

`.env.example` documents local runtime settings, including the database URL, model choices, resource profile, and document storage directory. Docker Compose starts Postgres configured with Apache AGE, pgvector, and the project schema.

Knoggin writes application-level settings to `config/knoggin.yml`. This file is managed by the app; manual changes can be overwritten. The topic seed lives at `server/src/common/templates/topics.yaml`.

For more predictable startup performance, set `KNOGGIN_RESOURCE_PROFILE` to `conservative`, `balanced` (default), or `performance`. Set `KNOGGIN_GPU=true` when a supported accelerator and matching runtime are available.

## Development

Run the server test suite:

```powershell
uv run --package server pytest server/tests
```

Lint the engine:

```powershell
uv run ruff check server/src server/tests
```

## Repository layout

```text
server/src/common/      Shared schemas, configuration, and utilities
server/src/core/agent/  Agent orchestration, prompting, execution, and tools
server/src/core/project/  Project state, domain config, and workspace services
server/src/core/session/  Session lifecycle and runtime context
server/src/core/ingestion/  Extraction, batching, resolution, and graph commits
server/src/core/knowledge/  Entity resolution, documents, graph, and retrieval
server/src/infrastructure/  Postgres, models, queues, and scheduling
server/tests/            Unit, runtime, ingestion, storage, and integration tests
docker/                 Local Postgres image and initialization
```

## License

[AGPL-3.0](./LICENSE)

## Contact

Feedback is welcome at adedewe.a@northeastern.edu.
