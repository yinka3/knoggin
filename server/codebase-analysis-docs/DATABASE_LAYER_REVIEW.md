# Database Layer Review — Current System

Review date: 2026-07-20  
Reviewed baseline: `591c5b9` (`Merge pull request #39 from yinka3/aadedewe/fact-to-episodic`), plus the working-tree remediation recorded below.  
Scope: `server` persistence code, PostgreSQL/Apache AGE/Redis integration, and
storage contracts. The product is pre-release, contains no existing application
data, intentionally has no API layer, and is single-user per database.

## Executive conclusion

The database layer has completed a genuine memory-model replacement: facts are
not stored, retrieved, indexed, audited, projected, or corrected anywhere in
the active application. Durable memory is now an **episode**: a bounded,
source-linked summary of a sequence of canonical messages, with derived entity
and relationship context. This is one part of the full persistence system, not
the whole system: projects, access scopes, agents, sessions, documents, graph
state, search, community state, auditing, AGE, and Redis remain in scope here.

This substantially improves provenance. An episode is not an unsupported claim
with one source pointer: it has a many-to-many attachment to the exact messages
that informed it, along with derived entity and relationship attachments. The
normal episode path locks a session, validates the next eligible chronological
window, writes the episode, and advances the processing checkpoint in one
PostgreSQL transaction.

The previous fact-specific findings must not be used as current risk reports.
The active concerns are instead:

1. Episode roots and all episode attachments now have physical scope integrity;
   other graph ownership links still need selective hardening.
2. The remaining risks are now lifecycle completeness, consistent multi-query
   reads, and release-evidence coverage rather than the deleted fact model.

The earlier critical durability work remains fixed in the working tree:
document transaction usage, aggregate project deletion, and durable user-message
acceptance. Project deletion now includes the episodic tables.

## Current persistence model

PostgreSQL is the durable source of truth. Apache AGE is a rebuildable traversal
projection for messages, entities, relationships, topics, and hierarchy. Redis
is used for runtime state, dispatch, deduplication, leases, and DLQ handling;
it is not the durable owner of episodic memory.

### Canonical memory flow

```text
messages + message/entity refs + relationship evidence
                  |
                  v
episode eligibility marker
                  |
                  v
EpisodeJob selects one chronological session window
                  |
                  v
EpisodeWriter validates scope and derived context
                  |
                  v
episodes + message/entity/relationship attachments + checkpoint
                  |
                  v
lexical / vector episode retrieval -> canonical source-message expansion
```

### Relational inventory

| Surface | Durable contents | Integrity currently enforced |
| --- | --- | --- |
| `projects`, `project_read_scopes` | Project identity/configuration and cross-project read permissions | Project FKs and non-self scope check; scope batch atomicity is application-managed. |
| `agents`, `agent_brain_snapshots` | Agent configuration and versioned brain content | Agent/project FK and snapshot cascade; exactly-one-default is not enforced. |
| `sessions` | Conversation configuration, selected agent/tools, document focus, lifecycle | Project and optional agent FK; `(session_id, project_id)` is a composite ownership key. Cross-project agent consistency is writer-managed. |
| `messages` | Conversation content, role, user/session/project scope, timestamp and metadata | Scoped composite ownership key supports episode source links; message deletion cascades eligibility and episode-message rows. |
| `entities`, `entity_aliases`, `message_entity_refs` | Canonical entities, aliases, and message-level observations | Entity/message attachment FKs exist; episode attachments use a composite entity/project FK. Other cross-project consistency is writer-enforced. |
| `relationships`, `relationship_evidence_refs` | Scoped entity connections and their message evidence | Endpoint FKs exist; evidence and episode attachments use composite project-scoped FKs. |
| `episodes` | Summary, developments, updates, unresolved items, importance, source time range, embedding, generator metadata | Composite session/project FK, importance and source-count checks. |
| `episode_messages` | Ordered source messages and their influence | Composite FKs require its episode and source message to share one project/session; unique message per episode and unique position. |
| `episode_entities` | Derived entities, prominence, role, focus flag and observed time range | Composite FKs require the episode and entity to share a project; non-negative checks. |
| `episode_relationships` | Derived relationships and centrality | Composite FKs require the episode and relationship to share a project; non-negative checks. |
| `episode_processing_checkpoints` | Last evaluated chronological `(timestamp_ms, message_id)` cursor per project/session | Project/session FKs; writer advances it in the same transaction as the validated window. |
| `entity_search`, `message_search`, `episodes.embedding` | Search projection/index material | Entity/message search duplicates canonical scope/name fields; episode vector is on the canonical episode row. |
| merge and operational audit tables | Merge proposals/audits, candidate suggestions, tool audit trail | Several historical IDs/snapshots deliberately have no FK; retention is not defined. |
| document tables | Project/session-visible metadata, folder uploads, raw bytes, and embedded chunks | Content/chunks cascade from document rows; some project/session ownership links are not physical FKs. |

The schema defines `vector(1024)` episode embeddings and a partial HNSW index.
Episode source attachments cascade with their episode or source row and use
composite FKs to prove project/session ownership. Derived entity and
relationship attachments likewise use project-scoped composite FKs.

### AGE, Redis, and other persistence surfaces

| Surface | What it owns | Current boundary |
| --- | --- | --- |
| Apache AGE | Rebuildable message/entity/relationship/topic/hierarchy traversal projection; AGE-only community discussions | Canonical knowledge remains in PostgreSQL. Community state is global rather than project-scoped. |
| Redis rebuildable keys | Conversation caches, message content/cache markers, project activity/last-processed values | PostgreSQL is authoritative. |
| Redis ephemeral keys | Buffers, dedup claims, job leases, DLQ/replay coordination, merge coordination, maintenance counters, community pub/sub | Must remain recoverable or explicitly disposable; it is not an episode store. |
| Document bytes/vector chunks | `document_content` and `document_chunks` in PostgreSQL | Not an external object-store dependency in the current design. |

### Cross-surface lifecycle map

| Operation | Durable transaction boundary | Non-relational follow-up / risk |
| --- | --- | --- |
| User-message acceptance | Canonical message/search/AGE write occurs before Redis staging | Redis failure is restaged rather than treated as data loss. |
| Graph ingestion | Canonical messages, entities, relationship evidence, search/AGE projection, message/entity refs, and episode eligibility | Alias writes remain a separate operation from the main graph batch. |
| Episode generation | Session/checkpoint lock, source/context validation, episode attachments, and checkpoint advance | LLM/embedding work occurs before durable write; the checkpoint matches the chronological selection order. |
| Search rebuild | Captures a repeatable-read canonical snapshot, embeds outside transactions, then publishes only if project/identity revisions still match | Concurrent canonical writes invalidate the snapshot and trigger a bounded retry; embedding never holds a DB lock. |
| Project deletion | Locks project, clears AGE projection, deletes relational roots/children, then clears known Redis state | Full real-service coverage of all episode attachments remains missing. |
| Session deletion | AGE message projection, canonical messages, and session row are removed through one PostgreSQL transaction | Redis cleanup follows commit and is logged as recoverable if unavailable. |
| Document indexing | Writer transactions cover claims/chunks/failure state | Raw content lower-layer read remains unscoped if bypassing the service. |
| Community lifecycle | AGE-only create/read/retention operations | No project scope and retry-unsafe `CREATE` paths. |

## Reader/writer boundaries and current use

| Component | Role | Current behavior |
| --- | --- | --- |
| `EpisodeWriter` | Create/retry an episode; atomically write one window and checkpoint it | Validates source messages, derives allowed entity/relationship IDs from those messages, and locks the session/checkpoint. |
| `EpisodeReader` | Episode lookup, prior-episode selection, lexical/vector retrieval, source expansion, generation context | Every public episode retrieval is scoped by user, project, and session. |
| `SearchIndexer` | Rebuild message/entity search and episode vectors | Fetches source rows before embedding, then writes a replacement projection. |
| `GraphWriter` / `GraphBuilder` | Canonical message/entity/relationship writes and AGE projection | AGE no longer contains episode or fact nodes. Entity merge transfers episode-entity memberships. |
| `ProjectDeletionWriter` | Aggregate project deletion | Deletes episodic roots/checkpoints and relies on episode attachment cascades, alongside canonical relational and AGE records. |
| `MergeAuditReader` / `MergeAuditWriter` | Merge snapshots and rollback state | Snapshots/restores episode-entity memberships and records episode evidence IDs. |
| `KnowledgeStore` | Application-facing facade | Fact methods are gone; episode methods are exposed alongside graph, document, merge, and search methods. |

`create_episode()` is facade-only at this revision. Production episode creation
flows through `EpisodeJob.write_episode_window()`.

## Findings

Severity reflects operational impact once the corresponding feature is used. A
finding is not an argument for adding a premature API or multi-user mechanism.

### High

No active High-severity findings remain after the remediation recorded in the
resolved-findings table.

### Medium

### Low / maintainability

#### DB-011 — Database-global identity is intentional for one user

This remains a future multi-user design item, not a current defect. Preserve the
one-user-per-database invariant explicitly and redesign identity partitioning
only when multi-user support is planned.

#### DB-012 — Migration support is deferred, not an initial-data blocker

The system has no existing data, so a clean first deployment can initialize from
the current schema. Introduce versioned migrations before the first deployment
that must upgrade an initialized database.

#### DB-034 — Index choices need real episodic workload evidence

Fact indexes are gone. Episode retrieval now adds HNSW and attachment lookup
indexes; assess their selectivity and query plans only once representative data
exists. Existing likely duplicates and missing session-history indexes still
warrant workload review.

## Retired or resolved legacy findings

| Prior ID | Current disposition |
| --- | --- |
| DB-001 | Fixed: document multi-statement writes use the public transaction contract. |
| DB-002 | Fixed: aggregate deletion includes episodic roots/checkpoints and attachment cascades. Add a comprehensive real-Postgres episode deletion test. |
| DB-003 | Fixed: accepted user messages are durable before Redis dispatch/cache work. |
| DB-004 | Resolved in source: `search_entity` now binds entity IDs and visible project IDs to the matching placeholders. Re-add a real-Postgres test. |
| DB-005 | Fixed: `PostgresClient` registers an `agtype` loader on every AGE-enabled connection. It decodes scalars, lists, maps, and AGE vertex/edge/path JSON before readers and tools receive rows. Unit and real-AGE tests cover booleans, lists, and maps. |
| DB-006 | Fixed: confirmation locks the proposal and entity rows, snapshots, creates its audit, applies the graph/episode merge, and records final audit/proposal state through one PostgreSQL cursor. Rollback locks its audit, restores relational state, rebuilds AGE, and records the rollback in that same transaction. The schema now enforces at most one audit per proposal. Real-Postgres failure tests prove neither path commits partial state. |
| DB-008 | Fixed: `SearchIndexer` takes a repeatable-read snapshot with project and identity revisions, computes embeddings outside transactions, then locks and compares those revisions before replacing derived rows. Message/entity/episode triggers advance the relevant revision only when search inputs change. A real-Postgres test mutates a message during embedding and proves the rebuild retries and indexes the newer content. |
| DB-010 | Fixed: community discussions and spawned agents are written with `user_name` and `project_id`; every mutate, read, history, hierarchy, insight, and retention query matches both. Community-store, manager-lifecycle, and tools contracts cover scope propagation. |
| DB-013 | Fixed: episode roots use a composite session/project FK; source messages use scoped episode/message FKs; derived entity and relationship attachments use composite episode/entity-or-relationship project FKs. Direct SQL regression tests reject every cross-scope variant. |
| DB-014 | Fixed: project creation writes its metadata and initial read scopes in one transaction; a scope replacement deletes, reinserts, and updates metadata through one transaction. Real-Postgres failure tests prove creation leaves no project behind and replacement preserves the previous scope when an insert fails. |
| DB-015 | Fixed: relationship writes now raise an actionable error when either scoped endpoint is absent, rather than silently accepting a zero-row `INSERT ... SELECT`. A real-Postgres test confirms the transaction leaves no relationship behind. |
| DB-016 | Fixed: relationship evidence now stores `project_id` and has composite FKs to both `(relationship_id, project_id)` and the scoped canonical message tuple. Entity writes, entity merges, audit restoration, and reader/projection joins preserve that identity. A real-Postgres test proves cross-project evidence insertion fails and message deletion cascades. |
| DB-017 | Fixed for the reviewed graph/tool decision boundary: `GraphReader` and `ToolQueries` raise `StorageUnavailableError` on PostgreSQL/AGE failures rather than returning ordinary empty results. Normal no-row absence remains unchanged; focused contracts cover both cases. |
| DB-018 | Fixed at the decision boundaries: search rebuilding already uses a repeatable-read snapshot plus revision-checked publication; merge snapshots now use repeatable-read when independent, and merge confirmation/rollback start at repeatable-read isolation. Entity list totals and page rows also share one read-only repeatable-read transaction. Ordinary episode hydration remains a deliberately best-effort display read rather than a stale-decision input. A real-Postgres regression proves a relationship committed after the first merge-snapshot query is excluded from that snapshot. |
| DB-019 | Fixed: candidate-suggestion batches now execute every upsert through one PostgreSQL transaction cursor. A real-Postgres trigger failure on the second insert proves the first insert is rolled back too. |
| DB-022 | Resolved in source: `AgentManager.set_default_agent()` clears and assigns the default through one PostgreSQL transaction, and `agents_one_default_per_user_idx` is a partial unique index enforcing at most one default per user. |
| DB-023 | Fixed: `GraphReader.has_direct_edge()` now requires `r.project_id` to be visible as well as both endpoints. It also decodes native and string-backed AGE boolean results safely. A real-AGE regression test proves a `project-2` edge between an identity node and a `project-1` entity is not visible to `project-1`. |
| DB-024 | Fixed: `DocumentReader` now requires project/session visibility before returning either raw content or extracted text. `DocumentWriter` now rejects unequal chunk/embedding lists in its direct workspace and single-document persistence paths before starting a transaction. Real-Postgres contracts prove cross-project and cross-session reads fail closed; writer contracts prove mismatched payloads cannot truncate through `zip(...)`. |
| DB-025 | Fixed: recent-message cursors are exclusive, and surrounding-message queries use a strict `(timestamp_ms, message_id)` order with explicit nullable-last behavior. Unit and real-Postgres tests prove tied timestamps do not repeat a message. |
| DB-026 | Fixed: entity and relationship confidence are bounded to `[0, 1]`; relationship weight is positive; message/entity refs and relationship endpoints must share their user/project scope, except for the reserved identity entity; checkpoints have a composite session/project FK; merge-audit lifecycle values are enumerated; and hierarchy writes now enforce scoped endpoints and acyclicity. The hierarchy trigger serializes per-project mutations with the same advisory-lock key used by the writer. Real-Postgres contracts cover invalid direct writes, and normal relationship, merge, hierarchy, and episode paths remain green. |
| DB-027 | Fixed: `entity_search` and `message_search` now have deferrable composite foreign keys to their canonical scope tuples. Entity-search writes also validate copied canonical names, and an entity update synchronizes its search-row name. `ON UPDATE CASCADE` carries canonical scope changes into both projections. Real-Postgres tests reject direct name/scope drift and prove canonical changes remain aligned. |
| DB-021 | Fixed: one `SessionDeletionWriter` transaction clears AGE message nodes, canonical messages, and the session row. Redis cleanup follows the commit as a recoverable cache operation. Real-Postgres tests prove both the successful cleanup and rollback when message deletion fails. |
| DB-029 | Fixed: `verify_storage_ownership.py` now seeds/counts an episode, all source and derived attachments, and its checkpoint. It uses current merge fields and document schema, seeds only current Redis policy families, and completed a local no-flush verification run. |
| DB-030 | Fixed: `ToolQueries` now validates the only caller-controlled Cypher interpolation, path depth, locally in both public and internal path helpers. It accepts only non-boolean integers from 1 through 4 before a query is composed; graph name and AGE return shapes remain fixed implementation values. Focused contracts reject malformed, string, boolean, and out-of-range depths before any database call. |
| DB-028 | Fixed: merge rollback payloads already expire through `MergeCleanupJob`. `AuditRetentionCleanupJob` now purges expired candidate suggestions, tool audits, and terminal merge audit/proposal history through one transaction, with configurable 30-day, 180-day, and 180-day defaults respectively. Active merge rollback state is never removed before its undo window closes. |
| DB-031 | Fixed: scheduler leases and message deduplication already use TTLs; DLQ completion markers are now timestamped in a project-scoped sorted set and pruned after a configurable 24-hour default. Queued, processing, and parked DLQ work is deliberately not auto-expired: it remains replayable until completion, explicit requeue, or project deletion, avoiding silent loss of recovery data. |
| DB-033 | Fixed at the entity-reader boundary: list pagination now requires integer limits from 1 through 100 and offsets from 0 through 10,000; vector, connected, notable, and recent-activity entity queries use the same bounded limit, and activity windows require 1 through 365 days. Invalid values fail before opening a read transaction or issuing a query. Document lower-layer scope and chunk-payload validation is covered by DB-024. |
| DB-035 | Fixed: the unused generic session update now accepts only ordinary configuration columns (`model`, `agent_id`, and `enabled_tools`). It rejects ownership, lifecycle, timestamp, document-focus, and unknown columns before SQL is issued; document focus continues through its dedicated validated methods. |
| DB-036 | Fixed: episode selection and persistence now share a nullable-last `(timestamp_ms, message_id)` cursor. A real-Postgres regression test proves IDs 102/103 can be processed before the later-timestamp ID 101 without skipping 101. |
| DB-032 | Fixed: `ResourceManager` now owns and connects the one general `PostgresClient`, then injects it into `KnowledgeStore`. The knowledge facade no longer exposes a raw client or owns connection lifecycle. Project, session, agent, and community managers remain explicit owners of their non-knowledge tables through the general resource boundary, rather than misleading `KnowledgeStore` bypasses. Resource lifecycle contracts cover the injection, startup failure cleanup, and shutdown path. |
| DB-037 | Fixed: episodes have a stored `search_tsvector` over the summary and structured narrative fields, with a GIN index. `EpisodeReader.search_episodes()` now calculates its `websearch_to_tsquery('simple', ...)` term once and filters/ranks against the stored vector. A real-Postgres contract proves the vector matches narrative content, the GIN index exists, and PostgreSQL can produce a bitmap index plan when sequential and ordinary index scans are disabled. This proves index compatibility, not a representative-workload latency claim. |
| DB-038 | Fixed: PostgreSQL is now the sole authority for session existence. Redis-only metadata cannot resume a runtime session; a resume verifies its durable `last_active_at` update before becoming active. Canonical messages also have a composite `(session_id, project_id)` foreign key with delete cascade, so no message can outlive or cross its owning session. Runtime and real-Postgres contracts cover Redis-only resume rejection, concurrent disappearance during resume, invalid/mismatched session writes, and cascade cleanup. |
| DB-039 | Fixed: `EntityReader.get_entities_by_names()` now produces a valid scoped query and reserves an empty list for ordinary no-match absence. Database failures surface as `StorageUnavailableError` instead of being silently treated as a missing identity by maintenance. Snapshot and real-Postgres contracts cover canonical and alias lookup, project scope, and failure semantics. |
| DB-040 | Fixed: aggregate project deletion now removes its derived `project_search_revisions` row after message, entity, and episode deletions have fired revision triggers. A real-Postgres regression proves no search-revision row remains once the project is gone. |
| DB-041 | Fixed: session deletion now removes every document root carrying that session ID—manual documents, folder batches, and workspace sources—before it deletes the session. Document content and chunks cascade from their document rows. The rule is ownership by non-null `session_id`, not visibility scope, so a project-visible row with a session ID is also removed. Real-Postgres coverage proves other-session and no-session project documents remain. |
| DB-007 | Retired: no fact ID upsert, fact ownership transfer, or Fact AGE edge exists. |
| DB-009 | Retired in its reviewed form: graph writes no longer have the post-commit Redis/event step that caused committed writes to be retried. Alias and graph writes are still separate operations. |
| DB-020 | Retired: fact audit writer and applied-fact audit flow no longer exist. |

## Verification status and gaps

The historic full-suite count (`104 passed, 5 failed`) predates this remediation
and must not be treated as current release evidence. The previously stale
ownership-verifier contract has been updated.

Current targeted evidence includes:

- real-Postgres integrity tests for scoped episode source, entity, and
  relationship attachments;
- real-Postgres success and forced-rollback tests for session deletion and AGE
  message projection cleanup;
- relationship-endpoint rollback coverage;
- graph/tool storage-unavailable contracts that preserve normal no-row absence;
- tool-query Cypher-boundary contracts that reject untrusted traversal depths
  before query composition;
- real-Postgres retention cleanup coverage for advisory records, tool audits,
  and terminal merge history, plus no-network DLQ completed-marker pruning and
  scheduler configuration fan-out coverage;
- real-Postgres candidate-suggestion success and forced-rollback coverage;
- real-Postgres domain-invariant rejection coverage for relationship and entity
  values, scope, checkpoint, audit lifecycle, and hierarchy-cycle violations;
- real-Postgres search-projection ownership/name drift and canonical-sync
  coverage, plus normal search-rebuild and graph/entity projection writes;
- real-Postgres repeatable-read merge-snapshot coverage, plus merge
  rollback/failure and search-index freshness regressions;
- real-Postgres/AGE graph-reader regressions for cross-project direct edges and
  tied-timestamp surrounding-message windows;
- real-Postgres document reader scope coverage for raw and extracted content,
  plus direct-writer chunk/embedding mismatch rejection coverage;
- entity-reader pagination and bounded-query rejection coverage, plus a
  real-Postgres allowed session-configuration update and rejection coverage for
  protected session columns;
- real-Postgres lexical episode-search coverage for its stored tsvector, GIN
  index, and index-compatible bitmap plan;
- runtime and real-Postgres session-ownership coverage proving stale Redis
  metadata cannot resume a deleted session and invalid message/session scope is
  rejected at the database boundary;
- entity-name lookup snapshot and real-Postgres coverage for canonical/alias
  matches, project scope, and surfaced storage failures;
- real-Postgres aggregate-deletion coverage proving search-revision trigger
  output is removed with its project;
- real-Postgres session-deletion coverage for manual, folder, and workspace
  document roots, their cascaded content, and preserved unrelated documents;
- a successful no-flush execution of `verify_storage_ownership.py --seed`,
  which counted its episodic roots, attachments, checkpoint, merge, and
  document state.

Important remaining coverage gaps:

- real-Postgres episode create/consolidate/checkpoint tests, including rollback;
- a comprehensive aggregate project deletion test that seeds episodes and every
  attachment table;
- concurrency tests for merge confirmation/rollback and episode checkpointing;
  DB-008 now has a canonical-write-during-embedding regression test.

## Recommended order

1. Add real-Postgres episode lifecycle coverage, including consolidation and
   rollback behavior.

### Deferred triggers, not current work

- **DB-012:** introduce migrations before the first upgrade of an initialized
  deployment.
- **DB-034:** revisit broader index choices only with representative workload
  evidence; do not add speculative indexes.
- **DB-011:** redesign identity partitioning only when multi-user support is
  planned.
