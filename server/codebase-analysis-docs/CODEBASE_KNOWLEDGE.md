# Knoggin Server Codebase Knowledge

Use this as a quick architecture map, not as a source of truth. Verify details
in code before changing behavior.

## Current Shape

Knoggin Server is a Python service for a personal AI system. Its durable state
lives in PostgreSQL. Redis is used for bounded runtime coordination and caches.
Apache AGE is used as a graph projection/traversal layer on top of PostgreSQL
data.

Main areas:

- `src/infrastructure/`: shared process resources, Postgres, Redis, schema, job
  scheduling, embeddings, and LLM access.
- `src/core/session/`: active session context, message intake, and
  session bootstrapping.
- `src/core/project/`: durable project loading and project-scoped
  runtime state.
- `src/core/ingestion/`: async message processing and extraction.
- `src/core/knowledge/`: SQL/AGE graph writes, retrieval helpers,
  documents, fact resolution, and entity merge policy.
- `src/core/agent/`: agent orchestration, prompt assembly, tool
  execution, and durable agent management.
- `src/core/community/`: multi-agent community discussions.

## Storage Boundary

PostgreSQL is canonical for durable data:

- users, projects, project read scopes, sessions, and messages;
- agents and periodic `agent_brain_snapshots`;
- topics via `projects.topic_config`;
- entities, aliases, facts, relationships, hierarchy edges, and evidence refs;
- documents, folder uploads, chunks, and document search metadata;
- merge proposals/audits and agent tool audits.

`src/infrastructure/schema.sql` defines the current SQL model.
`PostgresClient` exposes `fetch_all`, `fetch_one`, `execute`, and
`transaction`. Do not reintroduce old read/write alias APIs.

Redis is runtime state only:

- session conversation caches and recent-message indexes;
- ingestion buffers, dedup keys, heartbeats, leases, counters, and queues;
- pub/sub and short-lived coordination state.

Redis keys are centralized in `src/infrastructure/redis_client.py`. New Redis
state should be scoped by user/project/session where relevant and should have a
TTL, bounded collection size, or rebuild path.

## Autonomy Boundary

The model may make semantic decisions and propose actions, but Python and
PostgreSQL enforce the system's safety and ownership rules. Do not rely on
prompt text as the only guard for permissions, invariants, validation, or
destructive behavior.

Model-owned decisions include:

- judging whether retrieved context is relevant to the user request;
- proposing Brain section updates based on lessons or durable project context;
- proposing topic changes based on observed project activity;
- identifying possible duplicate entities and explaining merge evidence;
- choosing among tools that are exposed for the current run.

Python-owned enforcement includes:

- filtering which tools and capabilities are exposed to a run;
- validating tool arguments against schemas before dispatch;
- enforcing user, project, session, agent, and visible-project scopes;
- protecting immutable engine policy, protected topics, and the identity
  entity;
- applying optimistic revision checks for mutable Agent Brain edits;
- issuing and validating confirmation tokens for destructive operations;
- deciding whether a proposed merge is rejected, confirmation-required, or
  executable;
- writing durable state only through Postgres-owned paths;
- treating Redis as cache/coordination state, never as durable authority.

Concrete examples:

- `edit_brain` lets the model propose one editable Brain section update, but
  Python enforces the section allow-list, size limits, and expected revision.
- `update_topics` can apply model-proposed topic changes, but Python protects
  `General` and `Identity`, rejects dangerous bulk changes, and scopes writes
  to the current project.
- `propose_entity_merge` lets the model make the semantic duplicate claim, but
  Python verifies evidence, protected entities, scope, candidate state, and
  confirmation before a destructive merge can happen.
- Redis queues, heartbeats, locks, and caches may guide runtime coordination,
  but losing Redis must not erase durable user knowledge.

## Resource Ownership

`src/infrastructure/resources.py` owns process-wide resources.

- `KnowledgeStore` owns the single `PostgresClient`.
- `ResourceManager.postgres` points at `KnowledgeStore.postgres`.
- `AsyncRedisClient` owns the Redis connection.
- `ResourceManager` also owns embeddings, reranking, LLM access, GLiNER, spaCy,
  the worker pool, and the document storage root.

Pass dependencies explicitly. Avoid constructing parallel storage clients when
the resource already exists.

## Graph and Knowledge

SQL records are the source of truth. AGE is used for graph-shaped reads and
projection, especially path/traversal behavior.

Relevant files:

- `src/infrastructure/knowledge_store.py`
- `src/core/knowledge/db/readers/graph_reader.py`
- `src/core/knowledge/db/writers/graph_writer.py`
- `src/core/knowledge/db/writers/age_projection_writer.py`
- `src/core/knowledge/db/tool_queries.py`

Scoped reads must receive visible project IDs. Scoped writes must have a
non-empty `project_id`. Missing scope should fail before database access.

## Sessions and Projects

`ProjectManager` loads durable project metadata from PostgreSQL and creates
`ProjectState`.

`ProjectState` holds project-scoped runtime services:

- topic config;
- entity service;
- ingestion pipeline and scheduler;
- PostgreSQL and Redis access;
- readable project IDs;
- `DocumentService`.

`Context` handles active session behavior:

- accepts user and assistant turns;
- allocates canonical message IDs through PostgreSQL-backed storage;
- persists message logs;
- caches active conversation state in Redis;
- enqueues user messages for async ingestion;
- exposes project and document context to the agent layer.

## Ingestion

Message ingestion is asynchronous:

1. `Context.add()` assigns an ID and records the user turn.
2. Redis buffers the message and updates heartbeat/dedup state.
3. `BatchConsumer` drains work.
4. `BatchProcessor` and `TextProcessor` extract entities, relationships, and
   facts.
5. `KnowledgeStore` writes SQL records and AGE projections.
6. Profile refinement can update facts after ingestion.

Relevant files:

- `src/core/ingestion/services/batch_consumer.py`
- `src/core/ingestion/services/pipeline_service.py`
- `src/core/ingestion/services/processor.py`
- `src/core/ingestion/jobs/profile_job.py`
- `src/core/knowledge/services/fact_resolution.py`

Pipeline prompts live in `src/common/templates/prompts/` and are loaded through
`src/common/utils/prompt_loader.py`. The `## ` section headings are part of the
loader contract.

## Documents

`DocumentService` is the current document boundary. It replaces the old
session-only file RAG approach.

It supports project-scoped metadata, folder uploads, relative paths, folder
trees, document focus, reads, and scoped search. Metadata and chunks are stored
in PostgreSQL; file content is stored under the configured document root.

Agent document tools are implemented in
`src/core/agent/tools/search.py`.

## Agent Layer

The agent layer is centered on:

- `agent/orchestrator.py`: resolves the agent and assembles a run.
- `agent/executor.py`: bounded reasoning/tool loop.
- `agent/system_prompt.py`: prompt assembly.
- `agent/tools/registry.py`: tool dispatch, schema checks, and authorization.
- `agent/services/agent_manager.py`: durable agent CRUD.
- `common/schema/agent_contracts.py`: agent config and persona contracts.

Agents live in PostgreSQL. Their mutable brain is durable text with revision
history, not Redis state.

Tool schemas must match registry dispatch and concrete methods. Capability
classes are enforced in Python. Destructive tools require explicit confirmation
state; entity merge execution is not available as a normal agent tool.

## Agent Memory Structures

An agent has four distinct memory structures with different scopes and owners:

| Memory                                           | Lifetime                           | Owner                            |
| ------------------------------------------------ | ---------------------------------- | -------------------------------- |
| Brain (`agent_brain_revisions`)                  | Persistent, versioned Markdown     | Agent writes it via `edit_brain` |
| Persona (`agents.persona`)                       | Persistent, immutable to the agent | User/settings only               |
| Knowledge graph (entities, facts, relationships) | Persistent, project-scoped         | Ingestion pipeline only          |
| `RetrievedEvidence`                              | Single run, in-memory              | Accumulated from tool calls      |

The brain and persona are stored in the same `agents` row but are separate
columns with different write paths. The agent can only reach `instructions` via
`edit_brain`. The `persona` column has no agent-accessible write path — only
`AgentManager.update_persona()` touches it, and `AgentManager` is not exposed
as a tool.

`replace_brain_section` enforces an explicit allowlist:

```
EDITABLE_BRAIN_SECTIONS = (
    "Behavioral Directives",
    "Project Context",
    "User Preferences & Lessons Learned",
)
```

The SQL in `edit_brain` only ever sets `instructions`. The persona column is
never in that `SET` clause. Do not add persona mutation to any agent tool.

## Entity Merges

Automatic background merging has been removed. The current flow is:

1. an agent inspects graph health;
2. an agent submits `propose_entity_merge` with evidence;
3. the server stores a proposal and confirmation token;
4. an authorized boundary calls `EntityMergeService.confirm()`;
5. the service rechecks policy and writes audit records.

Do not add direct model access to destructive merge execution.

## Community

`community/community_manager.py` coordinates discussions.
`agent/tools/community_tools.py` supplies restricted community tools.

Spawned specialists are durable PostgreSQL-backed agents with birth persona,
instructions/brain content, parent-agent reference, and per-discussion spawn
limits. Redis may deliver live events, but identity and brain state are durable.

## Removed Architecture

These old boundaries should not be restored without a new design decision:

- `common/schema/memory.py`
- `knowledge/services/memory_service.py`
- `knowledge/services/file_rag.py`
- `knowledge/services/topic_manager.py`
- `knowledge/jobs/merge_job.py`
- `knowledge/jobs/topics_job.py`
- `project/lifecycle.py`
- old Jinja pipeline prompts replaced by Markdown prompt files.

## Change Checklist

Before changing storage, ingestion, documents, or agent behavior:

1. Identify the canonical PostgreSQL record.
2. Identify any AGE projection that must change with it.
3. Pass user/project visibility scope explicitly and fail closed if absent.
4. Keep Redis state bounded, temporary, or rebuildable.
5. Update tool schema, registry dispatch, implementation, authorization, and
   tests together.
6. Keep document operations project-scoped and path-safe.
7. Add contract tests at the boundary being changed.

Useful regression areas:

- `tests/storage/`
- `tests/runtime/`
- `tests/agent/`
- `tests/knowledge/`
- `tests/unit/infrastructure/test_resource_manager.py`
