# Redis Replacement Design

## Purpose

This document describes how Knoggin can remove Redis without replacing it with
another standalone cache or message broker.

The proposed target is:

- PostgreSQL owns all durable application and runtime records.
- Apache AGE remains a derived graph traversal projection.
- PostgreSQL tables provide durable queues and job coordination.
- `asyncio.Event` provides immediate wakeups inside one server process.
- PostgreSQL `LISTEN`/`NOTIFY` may later provide cross-process wakeups.
- `cachetools` may provide bounded process-local caches where measurements show
  they are useful.

The goal is not to reproduce Redis commands in PostgreSQL. The goal is to model
the actual application concepts directly and remove an unnecessary stateful
service.

## Recommendation

Replace Redis with a small application-owned persistence layer backed by the
existing `PostgresClient`.

Do not build a generic key/value store or a Redis-compatible API. Knoggin does
not need arbitrary hashes, lists, sets, sorted sets, and counters. It needs a
known set of domain operations:

- create and update projects,
- create and resume sessions,
- append and read conversation turns,
- enqueue messages for ingestion,
- claim and retry background work,
- track dirty entities and merge work,
- store agent configuration and directives,
- store session memory blocks,
- record job state and activity.

Those operations are clearer as typed repositories and SQL tables than as
encoded Redis keys.

## Why PostgreSQL Is Sufficient

Knoggin's slow and expensive work is not queue manipulation. It is:

- LLM requests,
- embedding generation,
- entity extraction and resolution,
- profile refinement,
- merge classification,
- graph and search projection writes,
- external search and tool calls.

A message may spend milliseconds in queue coordination and seconds in model or
knowledge processing. PostgreSQL can claim and update queue rows much faster
than the application can produce or process those expensive work items.

Even an automation script should hit request limits, model latency, worker
concurrency, or CPU/GPU limits before a properly indexed PostgreSQL queue
becomes the bottleneck.

Redis would become worth reconsidering only after measured evidence of sustained
queue or cache pressure that PostgreSQL cannot serve economically. That is not
the current system shape.

## Current Redis Responsibilities

Redis currently acts as several different systems at once.

### Application Metadata

- `projects`
- `project_sessions`
- `project_topic_config`
- `sessions`
- `agents`
- `agents_default`
- `agent_directives`
- `session_memory`
- `community_agent_memory`

This data is durable product state rather than disposable cache state. Losing
it changes project lifecycle, access scope, session resumability, agent
behavior, and prompt memory.

### Conversation Runtime

- `conversation`
- `recent_conversation`
- `message_content`
- message deduplication keys
- `heartbeat_counter`

Canonical messages now live in PostgreSQL, so these keys duplicate or index
message state that SQL can represent directly.

### Ingestion and Failure Queues

- `buffer`
- `checkpoint`
- `last_processed`
- `project_last_processed`
- `dlq`
- `dlq_parked`

These are durable work-management concepts. They should survive process
restarts and be queryable with the messages they refer to.

### Knowledge Maintenance Work

- `dirty_entities`
- `merge_queue`
- `merge_intent`
- `merge_intents_index`
- `merge_proposals`
- `last_profile_update`
- `project_profile_complete`
- `project_user_profile_ran`

These keys coordinate profile refinement and entity merge work. They do not
contain canonical knowledge, but they determine what maintenance work happens.

### Scheduler and Operational State

- `job_last_run` (scheduler-owned cadence anchor)
- `job_lease`
- `project_last_activity`
- `project_heartbeat_counter`
- `community_discussion_active`
- `global_stats`
- community pub/sub

Some of this should be durable SQL state. Some should remain process-local.
Pub/sub should be treated as a wakeup or telemetry mechanism, not a source of
truth.

## Target Architecture

```text
API / Session Manager / Agent
             |
             v
     Typed application stores
     - ProjectStore
     - SessionStore
     - AgentStore
     - ConversationStore
     - IngestionQueue
     - MaintenanceQueue
     - JobStateStore
             |
             v
 PostgreSQL canonical and runtime tables
             |
       +-----+------+
       |            |
       v            v
 Apache AGE     Search indexes
 projection     and vectors
```

Process-local coordination sits beside this:

```text
Database commit -> asyncio.Event.set() -> worker claims SQL rows
```

For multiple application processes:

```text
Database commit -> pg_notify(...) -> listeners wake -> workers claim SQL rows
```

The notification never carries the authoritative work item. It only tells a
worker to inspect the table. Periodic polling remains as a fallback so missed
notifications cannot lose work.

## Proposed Data Model

The following SQL is a design sketch. Exact constraints and types should follow
the conventions in `src/infrastructure/schema.sql`.

### Projects

```sql
CREATE TABLE public.projects (
    project_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    access_mode TEXT NOT NULL DEFAULT 'open',
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'archived', 'deleted')),
    topic_config JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    last_activity_at TIMESTAMPTZ,
    UNIQUE (user_name, project_id)
);

CREATE TABLE public.project_read_scopes (
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id),
    readable_project_id TEXT NOT NULL REFERENCES public.projects(project_id),
    PRIMARY KEY (user_name, project_id, readable_project_id),
    CHECK (project_id <> readable_project_id)
);
```

This replaces project hashes, project lifecycle metadata, allowed-project
lists, and project topic configuration.

Topic configuration can remain `JSONB` because the configuration is loaded and
saved as one aggregate. It does not need one relational table per nested topic
setting.

### Sessions

```sql
CREATE TABLE public.sessions (
    session_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id),
    model TEXT,
    agent_id TEXT,
    enabled_tools JSONB,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'closed', 'deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_active_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX sessions_project_idx
ON public.sessions(user_name, project_id, created_at);
```

Project membership no longer needs a separate Redis set. It is represented by
`sessions.project_id`.

The in-memory `active_sessions` dictionary remains process-local. It tracks
live Python objects, not durable state.

### Canonical Conversation Turns

Extend `public.messages`:

```sql
ALTER TABLE public.messages
    ADD COLUMN user_message_id BIGINT,
    ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN dedup_key TEXT;

CREATE INDEX messages_session_time_idx
ON public.messages(user_name, session_id, timestamp_ms, message_id);

CREATE UNIQUE INDEX messages_dedup_idx
ON public.messages(user_name, session_id, dedup_key)
WHERE dedup_key IS NOT NULL;
```

Conversation identity, content, and recent history then come from one canonical
table. The existing PostgreSQL `message_id` is the sole identifier; no separate
session-local turn sequence is needed.

Conversation queries become ordinary indexed SQL:

```sql
SELECT message_id, role, content, timestamp_ms, metadata
FROM public.messages
WHERE user_name = %(user_name)s
  AND session_id = %(session_id)s
ORDER BY timestamp_ms DESC, message_id DESC
LIMIT %(limit)s;
```

### Ingestion Queue

```sql
CREATE TABLE public.ingestion_queue (
    queue_id BIGSERIAL PRIMARY KEY,
    message_id BIGINT NOT NULL UNIQUE
        REFERENCES public.messages(message_id) ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES public.sessions(session_id),
    project_id TEXT NOT NULL REFERENCES public.projects(project_id),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending',
            'processing',
            'retry',
            'completed',
            'parked'
        )),
    stage TEXT NOT NULL DEFAULT 'processing',
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    leased_until TIMESTAMPTZ,
    worker_id TEXT,
    last_error TEXT,
    processing_context JSONB,
    result_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX ingestion_queue_claim_idx
ON public.ingestion_queue(status, available_at, queue_id)
WHERE status IN ('pending', 'retry');

CREATE INDEX ingestion_queue_expired_lease_idx
ON public.ingestion_queue(leased_until, queue_id)
WHERE status = 'processing';

CREATE INDEX ingestion_queue_session_idx
ON public.ingestion_queue(user_name, session_id, queue_id);
```

A user message should be inserted into `messages` and `ingestion_queue` in the
same transaction. This is the key simplification:

1. Allocate the canonical message ID.
2. Insert the canonical message.
3. Insert its ingestion work row.
4. Commit.
5. Wake the local consumer.

There is no period where Redis contains the only copy of an accepted user
message.

### Queue Claiming

Workers claim rows with short transactions and `FOR UPDATE SKIP LOCKED`:

```sql
WITH claimable AS (
    SELECT queue_id
    FROM public.ingestion_queue
    WHERE (
        (
            status IN ('pending', 'retry')
            AND available_at <= now()
        )
        OR (
            status = 'processing'
            AND leased_until < now()
        )
      )
      AND user_name = %(user_name)s
      AND session_id = %(session_id)s
    ORDER BY queue_id
    FOR UPDATE SKIP LOCKED
    LIMIT %(batch_size)s
)
UPDATE public.ingestion_queue AS q
SET status = 'processing',
    attempts = attempts + 1,
    leased_until = now() + interval '10 minutes',
    worker_id = %(worker_id)s,
    updated_at = now()
FROM claimable
WHERE q.queue_id = claimable.queue_id
RETURNING q.*;
```

Important properties:

- concurrent workers do not claim the same rows,
- worker crashes do not permanently lose work,
- expired leases make abandoned rows claimable again,
- retries are scheduled with `available_at`,
- queue state is inspectable using SQL,
- queue insertion can share a transaction with message insertion.

The worker should load message bodies from `messages` after claiming queue IDs.
The queue should not duplicate message content.

The current architecture should continue running one ingestion consumer per
session. `SKIP LOCKED` prevents duplicate claims, but it does not by itself
guarantee that two different workers finish batches from the same session in
order. If Knoggin later distributes session work across processes, add a
session-level worker lease before allowing a process to claim that session's
rows. Do not hold a database transaction open while the batch is processed.

### Retry, DLQ, and Parking

Redis currently uses separate lists for active work, DLQ work, and parked work.
SQL can represent these as states of the same row.

On retryable failure:

```sql
UPDATE public.ingestion_queue
SET status = 'retry',
    available_at = now() + %(backoff)s::interval,
    leased_until = NULL,
    worker_id = NULL,
    last_error = %(error)s,
    stage = %(stage)s,
    updated_at = now()
WHERE queue_id = %(queue_id)s;
```

After the retry limit:

```sql
UPDATE public.ingestion_queue
SET status = 'parked',
    leased_until = NULL,
    worker_id = NULL,
    last_error = %(error)s,
    updated_at = now()
WHERE queue_id = %(queue_id)s;
```

Replay changes `parked` rows back to `retry`. No payload needs to be copied
between lists, and failure history remains attached to the original message.

If detailed attempt history is useful, add an append-only
`ingestion_attempts` table later. Do not add it until the operational need is
clear.

### Project Entity Work

Dirty entities and merge candidates can share a typed work table:

```sql
CREATE TABLE public.project_entity_work (
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id),
    entity_id BIGINT NOT NULL REFERENCES public.entities(entity_id)
        ON DELETE CASCADE,
    work_kind TEXT NOT NULL
        CHECK (work_kind IN ('profile', 'merge_scan')),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'retry')),
    priority INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    leased_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_name, project_id, entity_id, work_kind)
);
```

`INSERT ... ON CONFLICT DO UPDATE` replaces Redis `SADD`. Repeatedly marking an
entity dirty remains idempotent and can raise its priority or reset
`available_at`.

### Merge Intents and Proposals

Merge intents have enough behavior to deserve a real table:

```sql
CREATE TABLE public.merge_intents (
    intent_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id),
    primary_entity_id BIGINT NOT NULL REFERENCES public.entities(entity_id),
    secondary_entity_id BIGINT NOT NULL REFERENCES public.entities(entity_id),
    status TEXT NOT NULL
        CHECK (status IN ('prepared', 'applying', 'completed', 'failed')),
    payload JSONB NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (
        user_name,
        project_id,
        primary_entity_id,
        secondary_entity_id
    )
);

CREATE TABLE public.merge_proposals (
    proposal_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id),
    primary_entity_id BIGINT NOT NULL,
    secondary_entity_id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);
```

Expired proposals can be deleted by a normal cleanup query.

### Job State

```sql
CREATE TABLE public.project_job_state (
    user_name TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES public.projects(project_id),
    job_name TEXT NOT NULL,
    running BOOLEAN NOT NULL DEFAULT FALSE,
    last_started_at TIMESTAMPTZ,
    last_completed_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    lease_until TIMESTAMPTZ,
    last_error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (user_name, project_id, job_name)
);
```

This replaces `job_last_run`, the Redis execution lease, profile-complete flags,
temporary profile-run flags, and similar scheduler markers.

Not every scheduler field must be persisted. `_running_tasks` and the scheduler
monitor task remain local Python state. SQL only stores state that must survive
a restart or coordinate multiple processes.

### Agents, Directives, and Session Memory

```sql
CREATE TABLE public.agents (
    agent_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    name TEXT NOT NULL,
    config JSONB NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_name, name)
);

CREATE UNIQUE INDEX agents_one_default_per_user_idx
ON public.agents(user_name)
WHERE is_default;

CREATE TABLE public.agent_directives (
    directive_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES public.agents(agent_id) ON DELETE CASCADE,
    mode TEXT NOT NULL CHECK (mode IN ('require', 'prefer', 'avoid')),
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.session_memories (
    memory_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES public.sessions(session_id)
        ON DELETE CASCADE,
    topic TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX session_memories_topic_idx
ON public.session_memories(user_name, session_id, topic, created_at);
```

The current limits of ten memories per topic and 200 characters per entry can
remain service-level validation. A transaction or advisory lock can protect the
count-and-insert operation if concurrent writes become possible.

Community-agent memory can use either a separate table or the same memory table
with an explicit owner type. Prefer separate typed tables if their lifecycle
rules differ.

## Current Key to Target Mapping

| Redis key/helper | Target |
| --- | --- |
| `projects` | `projects` |
| `project_sessions` | query `sessions.project_id` |
| `project_topic_config` | `projects.topic_config` |
| `sessions` | `sessions` |
| `session_keys` | removed; relational deletes use foreign keys and typed repositories |
| `conversation` | `messages` keyed by canonical `message_id` |
| `recent_conversation` | indexed timestamp/message-ID query |
| `message_content` | `messages.content` |
| message dedup key | unique `messages.dedup_key` |
| `buffer` | `ingestion_queue` |
| `checkpoint` | `project_job_state.metadata` or computed count |
| `last_processed` | job state or max completed queue/message ID |
| `project_last_processed` | job state or max completed message ID |
| `dlq` | queue rows with `status = 'retry'` |
| `dlq_parked` | queue rows with `status = 'parked'` |
| `dirty_entities` | `project_entity_work` with `work_kind = 'profile'` |
| `merge_queue` | `project_entity_work` with `work_kind = 'merge_scan'` |
| `merge_intent` | `merge_intents` |
| `merge_intents_index` | indexed query on `merge_intents` |
| `merge_proposals` | `merge_proposals` |
| `last_profile_update` | entity work/job timestamps |
| `project_profile_complete` | derived query or job metadata |
| `project_user_profile_ran` | job metadata with expiry timestamp |
| `job_last_run` | `project_job_state.last_completed_at` |
| `job_lease` | `project_job_state.lease_until` plus worker ownership metadata |
| `project_last_activity` | `projects` runtime activity column or max session activity |
| `project_heartbeat_counter` | local counter or job metadata |
| `heartbeat_counter` | local counter or computed pending message count |
| `last_activity` | `sessions.last_active_at` |
| `agents` | `agents` |
| `agents_default` | partial unique `agents.is_default` |
| `agent_directives` | `agent_directives` |
| `session_memory` | `session_memories` |
| `community_agent_memory` | typed community memory table |
| `community_discussion_active` | community discussion SQL state |
| `community_pubsub_channel` | local event fanout or PostgreSQL notification |
| `global_stats` | metrics system or SQL usage aggregate |

## In-Process Signaling

`BatchConsumer` already owns an `asyncio.Event`. Keep it.

After a transaction enqueues ingestion work:

```python
await conversation_store.add_user_message(...)
consumer.signal()
```

The event improves latency but is not required for correctness. The consumer
must also wake periodically and inspect SQL. If the event is lost during a
restart, the committed queue row remains.

For one Knoggin server process, no database notification mechanism is required.

## Multi-Process Signaling

If Knoggin later runs multiple API or worker processes, add
PostgreSQL `LISTEN`/`NOTIFY`.

Inside the enqueue transaction:

```sql
SELECT pg_notify(
    'knoggin_ingestion',
    json_build_object(
        'user_name', %(user_name)s,
        'session_id', %(session_id)s
    )::text
);
```

Rules:

- notifications wake workers but do not contain authoritative work,
- payloads identify a scope to inspect,
- workers still poll on an interval,
- notification listeners use dedicated long-lived connections,
- application transactions remain short,
- queue rows are committed before notifications are delivered.

This can be deferred until the deployment actually has multiple processes.

## Application Interfaces

Avoid exposing SQL details or a fake Redis API throughout the codebase.

Suggested interfaces:

```python
class ProjectStore:
    async def create_project(...): ...
    async def get_project(...): ...
    async def list_projects(...): ...
    async def update_status(...): ...
    async def get_readable_project_ids(...): ...
    async def load_topic_config(...): ...
    async def save_topic_config(...): ...


class SessionStore:
    async def create_session(...): ...
    async def get_session(...): ...
    async def list_sessions(...): ...
    async def touch_session(...): ...
    async def delete_session(...): ...


class ConversationStore:
    async def add_user_message_and_enqueue(...): ...
    async def add_assistant_message(...): ...
    async def get_recent_turns(...): ...
    async def get_surrounding_turns(...): ...


class IngestionQueue:
    async def claim_batch(...): ...
    async def complete(...): ...
    async def retry(...): ...
    async def park(...): ...
    async def replay(...): ...


class JobStateStore:
    async def mark_pending(...): ...
    async def try_acquire(...): ...
    async def complete(...): ...
    async def fail(...): ...
```

These can use focused repository classes behind `KnowledgeStore`, or a renamed
facade such as `DatabaseClient` if `KnowledgeStore` becomes misleading as it gains
more non-graph application state.

Do not create one giant `RuntimeStore` with arbitrary `get`, `set`, `hset`, and
list operations. That would preserve Redis's accidental data model and merely
move it into SQL.

## Transaction Boundaries

The replacement becomes valuable when related operations share transactions.

### User Message

One transaction:

1. validate the active session and project,
2. claim the deduplication key through a unique constraint,
3. increment the session turn counter,
4. insert the canonical message,
5. insert the ingestion queue row,
6. update session and project activity,
7. commit.

After commit, signal the consumer.

### Assistant Message

One transaction:

1. allocate the canonical message ID,
2. increment the session turn counter,
3. insert the canonical assistant message,
4. update session activity,
5. commit.

There is no staged conversation row to remove if canonical persistence fails.

### Knowledge Mutation

The existing canonical knowledge transaction remains responsible for entities,
facts, relationships, hierarchy, and AGE projection. Dirty entity work should
be inserted in the same transaction when practical.

This prevents a successful knowledge mutation from being committed without its
required maintenance work being recorded.

### Project Lifecycle

Project status changes and readable-scope changes should share one SQL
transaction. Foreign keys and status queries replace manual validation across
Redis hashes.

## Failure Semantics

### Process Dies After Queue Commit

The row remains `pending`. A restarted worker claims it.

### Process Dies While Processing

The row remains `processing` until `leased_until`. It then becomes claimable.
Processing must remain idempotent using canonical message and entity IDs.

### Notification Is Missed

Periodic polling finds the row.

### Worker Repeats a Completed Operation

Unique constraints and idempotent canonical writers prevent duplicate durable
records. Completion updates should include the claimed worker or lease identity
where useful.

### PostgreSQL Is Unavailable

The request cannot durably accept new state. Return a dependency error. This is
clearer than accepting a message into Redis while canonical PostgreSQL is down
and later reconciling two stores.

### Queue Backlog Grows

Apply:

- a maximum pending-work limit per user or project,
- API rate limiting,
- bounded worker concurrency,
- visible backlog metrics,
- oldest-item age alerts,
- retry caps and parking.

Redis would not remove the need for these controls.

## Caching Policy

Do not begin by recreating every Redis read as a process-local cache.

First use indexed PostgreSQL reads. Add caches only after measurement.

Good cache candidates:

- parsed topic configuration,
- agent configuration,
- formatted directive strings,
- project readable-scope lists.

Poor cache candidates:

- queue state,
- job leases,
- message deduplication,
- lifecycle status used for write authorization,
- any state needed for recovery after restart.

When caching is justified, use the already installed `cachetools` package with
small TTL or LRU bounds. Cache invalidation can initially happen directly in
the same process after writes. Multi-process invalidation can later use
`LISTEN`/`NOTIFY` or simply short TTLs.

## Statistics and Events

`global_stats` should not determine whether Redis remains.

Options:

- keep counters in process and export them through the metrics system,
- append usage records to SQL when billing/audit durability matters,
- periodically aggregate canonical LLM usage rows,
- use logs for approximate development-only statistics.

Community pub/sub should be reviewed separately:

- WebSocket fanout within one server can use in-process subscriber queues.
- Durable community discussion state belongs in SQL.
- Multi-process fanout can use `LISTEN`/`NOTIFY`.
- Events that must be replayed need an event table, not notification payloads.

## Migration Strategy

The application is pre-release and the current schema policy already permits
recreating development databases. Even so, migrate behavior in phases so each
change remains understandable and testable.

## Implementation Process

This section turns the design into a practical code-change sequence. The point
is to remove Redis by deleting responsibility groups one at a time, not by
landing one giant storage rewrite.

The safe working rule:

> Add the Postgres home first, route one responsibility through it, verify the
> behavior, then delete the matching Redis keys and call sites.

Do not begin by replacing `AsyncRedisClient` with a fake compatibility layer.
That would preserve the Redis-shaped model. Instead, add small typed stores and
move services to those stores as each responsibility migrates.

### Step 1: Add Runtime Storage Tables

Start with schema only.

Update `src/infrastructure/schema.sql` with the runtime tables from this
document:

- `projects`
- `project_read_scopes`
- `sessions`
- message turn/dedup columns on `messages`
- `ingestion_queue`
- `project_entity_work`
- `merge_intents`
- `merge_proposals`
- `project_job_state`
- `agents`
- `agent_directives`
- `session_memories`

Keep these tables in `public` next to the canonical knowledge tables. They are
application state, not AGE projection state.

Add storage contract tests before changing service behavior. The first tests
should use direct store methods and fake no Redis behavior.

Suggested new test files:

- `tests/storage/test_project_store_contract.py`
- `tests/storage/test_session_store_contract.py`
- `tests/storage/test_conversation_store_contract.py`
- `tests/storage/test_ingestion_queue_contract.py`
- `tests/storage/test_job_state_store_contract.py`
- `tests/storage/test_agent_store_contract.py`
- `tests/storage/test_memory_store_contract.py`

Exit check:

- schema initializes on a fresh database,
- store contract tests pass,
- no production code has changed behavior yet.

### Step 2: Add Typed Store Classes

Create focused store classes near the existing database layer.

Suggested layout:

```text
src/knoggin_server/runtime/store/
  __init__.py
  project_store.py
  session_store.py
  conversation_store.py
  ingestion_queue.py
  job_state_store.py
  agent_store.py
  memory_store.py
  entity_work_store.py
  merge_intent_store.py
```

Alternatively, if `runtime` feels too broad, place them under:

```text
src/knoggin_server/state/store/
```

The important part is that these classes are not graph readers/writers. They
own application runtime state that happens to live in Postgres.

Each store should accept `PostgresClient`:

```python
class ProjectStore:
    def __init__(self, postgres: PostgresClient):
        self.postgres = postgres
```

Expose domain methods, not Redis-style methods:

```python
await project_store.create_project(...)
await session_store.create_session(...)
await conversation_store.add_user_message_and_enqueue(...)
await ingestion_queue.claim_batch(...)
await job_state_store.mark_pending(...)
```

Avoid methods like:

```python
await store.get(key)
await store.hset(key, field, value)
await store.sadd(key, value)
```

Exit check:

- no service imports the new stores yet,
- stores have narrow method names,
- store tests describe Knoggin behavior, not Redis commands.

### Step 3: Wire Stores Into Resources

`ResourceManager` should initialize Postgres first, then attach typed stores.

Current shape:

```python
instance.knowledge_store = KnowledgeStore(dsn=dsn)
instance.redis = await AsyncRedisClient.get_instance()
```

Target transitional shape:

```python
instance.knowledge_store = KnowledgeStore(dsn=dsn)
await instance.knowledge_store.connect()
instance.project_store = ProjectStore(instance.knowledge_store.postgres)
instance.session_store = SessionStore(instance.knowledge_store.postgres)
instance.conversation_store = ConversationStore(instance.knowledge_store.postgres)
instance.ingestion_queue = IngestionQueue(instance.knowledge_store.postgres)
instance.job_state_store = JobStateStore(instance.knowledge_store.postgres)
instance.agent_store = AgentStore(instance.knowledge_store.postgres)
instance.memory_store = MemoryStore(instance.knowledge_store.postgres)
```

The exact access path can differ. The key is that services receive typed stores
instead of reaching for `resources.redis`.

During the migration, Redis can still initialize while remaining call sites are
moved. Do not remove Redis from `ResourceManager` until the final step.

Exit check:

- typed stores are reachable from `resources`,
- existing tests still pass,
- Redis still exists only for unmigrated call sites.

### Step 4: Move Project And Topic Metadata

Move these Redis responsibilities first:

- `projects`
- `project_sessions`
- `project_topic_config`

Primary files:

- `src/knoggin_server/project/project_manager.py`
- `src/knoggin_server/project/state.py`
- `src/common/conf/topics_config.py`

Change `ProjectManager` to use `ProjectStore` for:

- create project,
- list projects,
- get project,
- update project,
- archive/reactivate/delete project,
- validate readable scopes,
- add/remove/list project sessions,
- load/save topic config.

After this step, `TopicConfig.load(...)` and `TopicConfig.save(...)` should not
accept a Redis client. They should either accept a `ProjectStore` or move the
load/save behavior out of `TopicConfig` entirely.

Exit check:

- project lifecycle tests pass,
- project membership tests pass,
- topic config fanout tests pass,
- `project_manager.py` no longer imports `RedisKeys`.

### Step 5: Move Session Metadata

Move:

- `sessions`
- `last_activity`
- session deletion metadata

Primary file:

- `src/knoggin_server/session/session_manager.py`

Change `SessionManager` to use `SessionStore` for:

- create session metadata,
- resume session metadata,
- list sessions,
- close/touch session,
- update session metadata,
- delete session metadata.

Do not move conversation history in this step. Keep this focused on session
records and project membership.

Exit check:

- session lifecycle tests pass,
- session assembler tests pass,
- deleting a session removes SQL session state and file RAG data,
- `session_manager.py` no longer reads or writes `RedisKeys.sessions`.

### Step 6: Move Agents, Directives, And Memory

Move:

- `agents`
- `agents_default`
- `agent_directives`
- `session_memory`
- `community_agent_memory`

Primary files:

- `src/knoggin_server/agent/services/agent_manager.py`
- `src/knoggin_server/agent/orchestrator.py`
- `src/knoggin_server/knowledge/services/memory_service.py`
- `src/knoggin_server/agent/tools/community_tools.py`
- `src/knoggin_server/community/community_manager.py`

Use `AgentStore` for agent config and default-agent selection. Use
`MemoryStore` for directives, session memories, and community-agent memories.

Keep the current user-facing models:

- `AgentConfig`
- `Directive`
- `MemoryEntry`

Only the backing store changes.

Exit check:

- agent tests pass,
- memory service tests pass,
- community seeding tests pass,
- no agent or memory service depends on Redis.

### Step 7: Move Conversation Turns To Canonical SQL

Move:

- `conversation`
- `recent_conversation`
- `message_content`
- message deduplication keys
- session heartbeat counters if they only support conversation flushing

Primary files:

- `src/knoggin_server/session/context.py`
- `src/common/utils/core_utils.py`
- `src/knoggin_server/agent/tools/search.py`
- `src/knoggin_server/session/session_manager.py`
- `src/knoggin_server/knowledge/db/readers/graph_reader.py`

`Context.add(...)` should call:

```python
msg = await conversation_store.add_user_message_and_enqueue(...)
self.consumer.signal()
```

The store method should atomically:

1. allocate or use a canonical message ID,
2. insert the canonical message,
3. insert the ingestion queue row,
4. update session/project activity.

`Context.add_assistant_turn(...)` should also write directly to canonical SQL
and should not stage Redis conversation state before SQL persistence.

Conversation reads should use `messages`:

- recent context,
- readonly session history,
- surrounding message context,
- evidence hydration fallback.

Exit check:

- context add tests pass,
- search/evidence retrieval tests pass,
- assistant-message failure cleanup code is gone,
- `AsyncRedisClient` remains connection-lifecycle-only.

### Step 8: Move Ingestion Buffer And DLQ

Move:

- `buffer`
- `checkpoint`
- `last_processed`
- `project_last_processed`
- `dlq`
- `dlq_parked`

Primary files:

- `src/knoggin_server/ingestion/services/batch_consumer.py`
- `src/knoggin_server/ingestion/services/pipeline_service.py`
- `src/knoggin_server/ingestion/jobs/dlq_job.py`
- `src/knoggin_server/session/boot.py`

`BatchConsumer` should claim from `IngestionQueue`, not read from Redis lists.
It should still use its local `asyncio.Event` for fast wakeups.

Suggested consumer loop:

```python
items = await ingestion_queue.claim_batch(
    user_name=self.user_name,
    session_id=self.session_id,
    limit=self.batch_size,
    worker_id=self.worker_id,
)
if not items:
    return

messages = await knowledge_store.get_messages_by_ids(...)
result = await processor.run(...)

if result.success and graph_success:
    await ingestion_queue.complete([...])
else:
    await ingestion_queue.retry_or_park(...)
```

`BatchProcessor.move_to_dead_letter(...)` should stop pushing JSON blobs to
Redis. It should return enough failure data for `IngestionQueue.retry_or_park`
to update the queue row.

Exit check:

- ingestion tests pass,
- DLQ replay tests pass,
- no Redis list operations remain in ingestion code.

### Step 9: Move Maintenance Work And Job State

Move:

- `dirty_entities`
- `merge_queue`
- `merge_intent`
- `merge_intents_index`
- `merge_proposals`
- `last_profile_update`
- `project_profile_complete`
- `project_user_profile_ran`
- `job_last_run`
- `job_lease`
- `project_last_activity`
- `project_heartbeat_counter`

Primary files:

- `src/infrastructure/job/scheduler.py`
- `src/knoggin_server/knowledge/jobs/profile_job.py`
- `src/knoggin_server/knowledge/jobs/merge_job.py`
- `src/knoggin_server/knowledge/db/write_graph_db.py`
- `src/knoggin_server/ingestion/jobs/archive_job.py`
- `src/knoggin_server/ingestion/jobs/cleaner_job.py`
- `src/knoggin_server/knowledge/jobs/topics_job.py`
- `src/knoggin_server/community/community_job.py`

Use:

- `EntityWorkQueue` for dirty/profile/merge candidate work,
- `MergeIntentStore` for merge recovery state,
- `JobStateStore` for last-run, pending, activity, and profile-complete state.

Graph mutation execution should mark entity work in the same database-backed
flow that writes canonical knowledge. If a mutation succeeds, the maintenance
work needed because of that mutation should also be durably recorded.

Exit check:

- profile refinement tests pass,
- merge detection tests pass,
- job clock tests pass,
- storage graph mutation plan tests pass,
- Redis is no longer needed by background jobs.

### Step 10: Move Events And Metrics

Move:

- `community_pubsub_channel`
- `community_discussion_active`
- `global_stats`

Primary files:

- `src/common/utils/events.py`
- `src/infrastructure/llm_client.py`
- `src/knoggin_server/community/community_manager.py`

Use local event fanout for one-process runtime. Use SQL event rows only for
events that must be replayed. Use PostgreSQL `LISTEN`/`NOTIFY` later only if
multiple server processes need cross-process wakeups.

For LLM stats, prefer one of:

- log-only development counters,
- SQL usage rows if durability matters,
- a metrics backend if operational metrics matter.

Exit check:

- no production feature depends on Redis pub/sub,
- LLM client does not require a Redis client,
- community manager has SQL-backed discussion state if it needs durability.

### Step 11: Remove Redis From Resources And Dependencies

Only do this after the previous steps leave no production Redis call sites.

Remove:

- `src/infrastructure/redis_client.py`
- `redis` from `pyproject.toml`
- `redis-tools` from `pyproject.toml`
- `ResourceManager.redis`
- Redis startup and shutdown calls,
- Redis test markers that no longer apply,
- fake Redis fixtures once all tests use stores.

Search gate:

```bash
rg -n "redis|RedisKeys|AsyncRedisClient|resources\\.redis|self\\.redis" \
  src tests pyproject.toml
```

Expected result:

- no production Redis dependency remains,
- only historical documentation may mention Redis.

Final verification:

```bash
uv run pytest tests/storage
uv run pytest tests/runtime
uv run pytest tests/ingestion
uv run pytest tests/knowledge
uv run pytest tests/agent
uv run pytest tests/community
uv run ruff check src tests
git diff --check
```

### Phase 0: Introduce Storage Boundaries

Add typed stores around current behavior before moving data:

- `ProjectStore`
- `SessionStore`
- `AgentStore`
- `ConversationStore`
- `IngestionQueue`
- `JobStateStore`

Initially, some implementations may still call Redis. Call sites should stop
constructing keys directly.

Exit condition:

- business services no longer import `RedisKeys` except temporary adapters,
- tests target store contracts rather than Redis command sequences.

### Phase 1: Move Durable Metadata

Move:

- projects and lifecycle status,
- readable project scopes,
- project topic configuration,
- sessions and project membership,
- agents and default-agent selection,
- directives and session memory.

This phase removes the most concerning Redis responsibility: durable product
metadata.

Update:

- `ProjectManager`
- `SessionManager`
- `TopicConfig`
- `AgentManager`
- `MemoryManager`
- community agent-memory paths

Exit condition:

- restarting with an empty Redis instance does not lose projects, sessions,
  agents, directives, or memory.

### Phase 2: Make Messages the Only Conversation Store

Add metadata and deduplication to canonical messages.

Change `Context.add` so user message persistence and ingestion enqueue happen
atomically in PostgreSQL.

Change:

- conversation context reads,
- readonly session history,
- evidence hydration,
- surrounding-message retrieval,
- assistant turn persistence.

Remove:

- conversation hashes,
- recent-conversation sorted sets,
- message-content hashes,
- staged-message cleanup logic.

Exit condition:

- all conversation reads and writes work with Redis disabled.

### Phase 3: Move Ingestion and DLQ

Add `ingestion_queue` and queue repository methods.

Change `BatchConsumer`:

- claim rows instead of reading and trimming a Redis list,
- load canonical messages by ID,
- complete, retry, or park queue rows transactionally,
- recover expired leases,
- continue using `asyncio.Event` for local wakeups.

Change `DLQReplayJob` to query parked/retry rows rather than move serialized
payloads between lists.

Exit condition:

- accepted messages survive process termination at every point,
- no Redis buffer or DLQ keys are used.

### Phase 4: Move Maintenance and Job Coordination

Move:

- dirty entities,
- profile work,
- merge candidate work,
- merge intents and proposals,
- job pending/last-run state,
- project activity timestamps,
- profile completion state.

Use queue claims or transaction-scoped advisory locks to prevent duplicate job
execution.

Exit condition:

- profile, merge, cleanup, archival, topic, and DLQ jobs run without Redis.

### Phase 5: Replace Events, Metrics, and Optional Caches

Replace:

- community pub/sub,
- approximate global statistics,
- any remaining cache-only usage.

Use:

- local subscriber queues for one process,
- `LISTEN`/`NOTIFY` for multi-process wakeups,
- SQL event rows when replay is required,
- `cachetools` only for measured hot reads.

Exit condition:

- no production imports of `redis`,
- no `RedisKeys`,
- no Redis environment variables,
- no Redis service in deployment configuration.

### Phase 6: Remove Redis

Remove:

- `redis` and `redis-tools` dependencies,
- `infrastructure/redis_client.py`,
- Redis health checks and resource initialization,
- `requires_redis` tests and markers,
- Redis container, volume, environment variables, and documentation.

Run the full test suite against PostgreSQL-only infrastructure.

## Testing Requirements

### Store Contract Tests

Each store should test:

- create/read/update/delete behavior,
- scope isolation by user, project, and session,
- lifecycle validation,
- uniqueness and idempotency,
- malformed or missing references,
- transaction rollback.

### Queue Concurrency Tests

Test with multiple concurrent workers:

- no duplicate claims,
- ordered claims within a session,
- expired lease recovery,
- retry backoff,
- parking after maximum attempts,
- replay from parked state,
- worker cancellation,
- process restart simulation.

### Message Atomicity Tests

Verify:

- message and queue row both commit,
- neither commits on failure,
- duplicate dedup keys return the original message,
- assistant messages do not enter ingestion unless explicitly requested,
- turn ordering remains stable,
- surrounding-context queries match current behavior.

### Lifecycle Tests

Verify:

- archived projects cannot create or resume sessions,
- deleted projects remain retained but unreadable,
- allowed-project validation is transactional,
- session counts come from SQL,
- project membership cannot drift from session metadata.

### Failure Injection

Inject failures:

- before message insert,
- between message and queue insert,
- immediately after queue commit,
- after claim but before processing,
- during graph write,
- during completion update,
- during shutdown.

The expected result should always be either:

- no durable acceptance, or
- a durable row that can be retried or inspected.

## Operational Impact

### Removed Infrastructure

- Redis server/container,
- Redis persistence volume,
- Redis connection pool,
- Redis health checks,
- Redis memory and eviction configuration,
- Redis backup and restore procedures,
- Redis-specific monitoring,
- cross-store startup and synchronization concerns.

### Remaining Infrastructure

- Knoggin server,
- PostgreSQL with AGE and pgvector,
- model and external API dependencies.

### Added PostgreSQL Concerns

- queue indexes and table cleanup,
- lease monitoring,
- backlog metrics,
- connection-pool sizing,
- occasional vacuum behavior for frequently updated queue rows.

These are real concerns, but they remain inside the database Knoggin already
requires and operates.

Keep completed queue rows only as long as they are useful for diagnosis. A
cleanup job can delete old completed rows in bounded batches. Parked rows should
be retained until reviewed or explicitly discarded.

## Performance Guardrails

- Keep claim and state-transition transactions short.
- Never hold a database transaction open while calling an LLM.
- Claim work, commit, process externally, then open a new completion
  transaction.
- Index only claim and lookup paths that are actually used.
- Use partial indexes for active queue states.
- Batch claims and completions.
- Bound workers by CPU, GPU, model limits, and database pool size.
- Use per-user/project backlog limits.
- Avoid rapid updates to one global row.
- Prefer one session row counter over a global conversation counter.
- Poll at a modest fallback interval even when notifications are enabled.
- Track queue depth and oldest pending age before optimizing.

## Scope and Effort

This is a moderate refactor rather than a difficult infrastructure project.

Most effort is in changing application contracts and tests, not implementing
the SQL queue itself. Redis currently crosses session, project, ingestion,
knowledge-job, agent, memory, community, metrics, and event code.

A realistic sequence is:

- durable metadata first,
- conversation canonicalization second,
- ingestion queue third,
- maintenance jobs fourth,
- optional signaling and cache cleanup last.

Do not combine all phases into one large change. Each phase should delete a
coherent category of Redis keys and leave the system runnable.

## Final Target

The final ownership model should be:

| Concern | Owner |
| --- | --- |
| Durable application metadata | PostgreSQL |
| Canonical messages and knowledge | PostgreSQL |
| Graph traversal | Apache AGE projection |
| Vector and text search | PostgreSQL derived indexes |
| Durable work queues | PostgreSQL queue tables |
| Cross-process wakeups | PostgreSQL `LISTEN`/`NOTIFY`, if needed |
| In-process wakeups | `asyncio.Event` and local subscriber queues |
| Hot object caching | bounded `cachetools` caches, if measured |
| Live Python runtime objects | process memory |

This gives Knoggin one stateful infrastructure dependency and one inspectable
source for accepted application work. It also removes the need to reason about
whether Redis and PostgreSQL agree after partial failures.
