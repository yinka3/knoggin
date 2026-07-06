# Knoggin

Knoggin is a self-hosted memory system for AI agents and personal tools.

It is built around a simple idea: memory is more useful when it has structure, and that structure should come from the domain you actually care about. Knoggin works best when you give it a topic config that reflects your world: the kinds of entities that matter, the labels you use, the aliases people actually say, and the relationships worth tracking.

The system can then use conversations, files, facts, entities, and relationships as a local memory library for agents without losing the trail back to the original evidence. The graph helps with retrieval, but the source messages remain the ground truth.

> Knoggin is an active personal project and still early. The core pieces are being built and tested, and the design will keep changing as I learn what holds up in real use.

## Why Knoggin Exists

I wanted a memory layer where an agent could remember more than loose chunks of text, but where I could still ask: where did this come from?

Most agent memory systems lean heavily on summaries and semantic search. That can be useful, but it also gets blurry fast. Knoggin takes a more opinionated path. You define a rough domain map through topic configuration, and the system uses that map to classify extracted entities, relationships, and facts.

That means the user is part of the memory model. If you know the high-level shape of the domain, Knoggin can do a better job deciding what to preserve, what to connect, and what deserves caution.

Think of it less as "the model figures everything out" and more as a source-grounded library index that can be tuned for a project, research area, team, or personal workflow.

## What It Is Meant To Provide

- A project-scoped memory layer for agents and local applications.
- Topic configuration for domain-specific entity labels, aliases, hierarchy rules, active topics, and thresholds.
- Conversation and file recall that can point back to source evidence.
- Entity, relationship, and fact extraction from user or project activity.
- A graph that helps the system find related context instead of treating every memory as an isolated chunk.
- Background jobs for cleanup, profile refinement, duplicate detection, and replaying failed ingestion work.
- A Python SDK for interacting with Knoggin engine/API integrations.

The main design principle is that extracted knowledge should be treated as an index over evidence, not as unquestionable truth. If Knoggin says it knows something, it should be possible to ask what message, file, or observation led there.

## Topic Config Matters

Knoggin is intentionally not trying to be a universal ontology generator. It does better when the user brings some high-level domain knowledge and uses that to shape the topic config.

For example, a useful config might tell the system:

- which entities matter in this domain
- which names or aliases should probably refer to the same thing
- which relationships are worth keeping
- which topics are currently active
- how strict or forgiving entity matching should be
- when hierarchy matters, such as project -> milestone -> task

Those constraints are part of the design. They keep the graph closer to a usable index than a pile of generated guesses. When the system is uncertain, the evidence should carry more weight than the extracted graph.

## Repository Layout

This repository currently contains:

- **`server`**: the core memory engine, backed by Postgres, Apache AGE, Redis, vector search, and background jobs.
- **`sdk`**: a lightweight Python client for working with Knoggin integrations.
- **`docker/` and `docker-compose.yml`**: local infrastructure for running the backing services.

`server` is the implementation package for the engine. It exposes the
runtime managers and services the API layer can compose, but it is not meant to
define the polished public developer interface by itself. That public workflow
surface belongs in `sdk`, where project, session, agent, message, and
file operations can be shaped around external use.

## Engine Architecture

The engine can be visually seen as just two loops that share the same project memory.

- The **conversation loop** answers the user by reading from tools, memory, files, Redis, and the graph.
- The **learning loop** runs behind the scenes and turns new messages into entities, relationships, facts, and background cleanup work.

Both loops are shaped by the project topic config. That config tells the system what kinds of things matter, how entities should be labeled, which aliases are useful, and how strict entity matching should be.

```mermaid
flowchart LR
    User["User message"]
    Session["Session context<br/>project + topic config"]

    subgraph Conversation["Conversation loop"]
        Agent["Agent orchestrator"]
        Tools["Tools<br/>search, graph, files, memory"]
        Reply["Assistant reply"]
    end

    subgraph Learning["Learning loop"]
        Queue["Redis message buffer"]
        Extract["Extract mentions<br/>known aliases, GLiNER, LLM"]
        Resolve["Resolve entities<br/>topic-aware matching"]
        Write["Write evidence graph<br/>messages, entities, facts, edges"]
    end

    subgraph Maintenance["Background maintenance"]
        Jobs["Scheduled jobs<br/>profiles, merges, topic evolution, cleanup, DLQ"]
    end

    Store["Postgres + Apache AGE<br/>source-grounded graph"]
    Redis["Redis<br/>sessions, queues, config, working memory"]

    User --> Session
    Session --> Agent
    Agent --> Tools
    Tools --> Store
    Tools --> Redis
    Tools --> Reply

    Session --> Queue
    Queue --> Extract
    Extract --> Resolve
    Resolve --> Write
    Write --> Store

    Store --> Jobs
    Redis --> Jobs
    Jobs --> Store
    Jobs --> Redis
```

The main runtime object is `ProjectState`. It holds the topic config, entity resolver, text pipeline, and scheduler for a project. Active sessions get a `Context` that points into that project state, so the agent and ingestion pipeline operate against the same view of memory.

The graph is not treated as magic truth. New messages are saved as evidence first, then the extracted entities, relationships, and facts are written as an index over that evidence. Redis handles the fast-moving parts: session metadata, queues, working memory, config, events, and job state.

## Current Status

Knoggin is under active development. The important pieces are being built around:

- source-grounded memory
- project and session boundaries
- domain-shaped topic configuration
- graph-guided retrieval
- agent tool access
- local-first, self-hosted use

It is not production hardened yet. Expect rough edges, missing tests, and design changes.

## Setup

Start the local backing services:

```bash
docker-compose up -d
```

Install Python dependencies:

```bash
uv sync
```

### Running the Full Test Suite on Windows

The real-infrastructure tests use the Postgres and Redis ports published by
Docker Desktop. Start Docker Desktop in Linux-container mode, then run:

```powershell
docker compose up -d --build
docker compose ps

$env:KNOGGIN_TEST_DATABASE_URL = "postgresql://knoggin:knoggin@localhost:5432/knoggin_db"
$env:KNOGGIN_TEST_REDIS_URL = "redis://localhost:6379/1"

Set-Location .\server
uv run pytest -q
```

Storage tests backed by fakes remain independent of Postgres; only tests marked
`requires_postgres` connect to and clean the real test database.

`server` currently ships as an engine package, not as a standalone HTTP API. The API layer for frontend and hosted access is intentionally separate and should import the engine rather than live inside it.

For SDK usage and integration notes, see [sdk/README.md](./sdk/README.md).

## Development Note

I use AI tools while building Knoggin, mostly for coding help and review passes. The project core idea/direction, tradeoffs, and final calls are mine.

## License

[AGPL-3.0](./LICENSE)

## Contact

Feedback is welcome: adedewe.a@northeastern.edu
