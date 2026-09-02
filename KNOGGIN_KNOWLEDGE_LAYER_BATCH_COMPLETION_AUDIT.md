# Knoggin Knowledge Layer Batch Completion Audit

## Scope

This audit compares the live worktree with all twelve batches in
`KNOGGIN_KNOWLEDGE_LAYER_IMPLEMENTATION_PLAN.md`. It traces production ownership,
schemas, call sites, tests, and obsolete surfaces. It also distinguishes a code
defect from a stale fixture or an unavailable external service.

The product assumptions used here are the agreed ones: Knoggin is unreleased,
local, and single-user. The target is therefore a clean implementation, not a
legacy compatibility layer.

## Executive verdict

After the repairs made during this audit, I found no known reachable half-built
implementation in Batches 1 through 12. The principal incomplete work was real:
the global entity maintenance design was live while the old project-scoped
snapshot merge stack was still exported, composed, configured, and tested. That
parallel stack has now been removed.

Several smaller gaps were also repaired: a stale stable-ID prompt, incomplete Run
Notebook capacity configuration, a non-atomic source replacement path, a durable
maintenance-review revision that was ignored on reads, obsolete document scope
fixtures, and stale test doubles for the final configuration contracts.

The only material verification limitation is external: PostgreSQL is not
listening on `localhost:5432`, so the real-PostgreSQL schema, transaction, global
identity, merge/reversal, and cross-project gates could not be rerun. The code and
static contracts are coherent, but those database behaviors are not claimed as
proven by this audit.

## Batch-by-batch findings

| Batch | Audit verdict | Evidence and remaining limit |
| --- | --- | --- |
| 1. Correctness holes | Complete in the reachable code | Episode adapters carry the required session identity, ordinary discovery respects lifecycle boundaries, and tool routing uses current contracts. The system prompt was corrected to tell the model to discover an entity and then call `episode_check` with `entity_id`, matching the actual tool schema. |
| 2. Ingestion lifecycle | Complete in the reachable code | The duplicate settle delay and parallel ingestion FSM are absent; terminal failure, retry, exact-claim, worker-health, and FIFO behavior have focused coverage. No old `blocked` durable outcome or `IngestionStage` runtime surface remains. |
| 3. Global identity | Code complete; real PostgreSQL gate blocked | Identity is user-global and immutable while project-specific type/topic/activity lives in `project_entity_contexts`. Readers, writers, hydration, deletion, and indexes use that split. The remaining old compatibility migration that read `entities.project_id/type/topic/last_mentioned_ms` was removed. Cross-project database behavior still needs a live PostgreSQL run. |
| 4. Ingestion on global identity | Code complete; real PostgreSQL gate blocked | Source time crosses the commit boundary, extraction is conservative, and resolver/graph writes use global identity with project context. Global identity embeddings intentionally omit project type/topic; a stale unit expectation was corrected to the context-neutral embedding contract. Transactional behavior still needs PostgreSQL. |
| 5. Canonical retrieval | Complete in the reachable code | Discovery and stable-ID follow-up are separated; graph direction, activity, paths, hot-topic hydration, and attribution flow through `KnowledgeRetrieval`. Duplicate internal graph/memory retrieval paths and retrieval-time active-topic visibility are absent. |
| 6. Completed-turn Episodes | Complete in the reachable code | Session lifecycle owns eligibility after a durable assistant outcome; Episode chronology and evidence catalogs are deterministic; removed ranking/version fields have no live readers. The implementation does not attempt the separately deferred Forget semantic. |
| 7. Bounded Episode work | Complete in the reachable code | Policy is snapshotted, source evidence is bounded and complete, consolidation falls back safely, and progressive Episode cards remain the discovery surface. Focused Episode tests pass. |
| 8. Typed maintenance reviews | Complete in code; real PostgreSQL gate blocked | `MaintenanceReview` is the durable typed envelope and observations retain immutable evidence independently of interpretation. The dead human-review wrapper was removed. The reader now honors the durable `expected_state.revision` written by the advisory writer, with a deterministic fallback only for invalid/missing state. Storage transitions and reconciliation still need PostgreSQL. |
| 9. Global maintenance and reversal | Code complete; real PostgreSQL gate blocked | Application-owned global maintenance, typed plans, durable change-oriented mutation journals, safe/conflicting inverses, and frontier checks are present. The obsolete project-scoped merge proposal/audit readers, writers, service, snapshot rollback jobs, retention jobs, settings, schema creation, facade methods, and tests were removed. Global merge/reversal transactions still need PostgreSQL. |
| 10. Admission and ownership | Complete in the reachable code | Durable-by-default admission, explicit focus policy, application-owned global maintenance, and owner-specific cancellation are in place. The unused broad `cancel_project()` coordinator API was removed. Three integration callers and one schema-reset test were updated to the current project-owned document contract instead of restoring `visibility_scope`. |
| 11. Run Notebook | Complete in the reachable code | `RunNotebook` is the canonical normalized accumulator; canonical results are accumulated before localization, templates are one-way/strict, and references remain stable. AgentRun list-shaped accessors are views over the notebook, not duplicate state. |
| 12. Capacity and migration close | Complete in reachable code; model/DB gates remain | Independent limits now exist for messages, profiles, graph, paths, Episodes, documents, web discoveries, web reads, actions, next steps, summary characters, and rendered tokens. Research profiles scale web discovery/read budgets independently. Summary and source compatibility setters now enforce notebook capacity, and two-section source replacement is atomic. The `model` marker is registered, stale compatibility surfaces are removed, and reset/reindex guidance is current. Real model and PostgreSQL gates were not completed here. |

## Confirmed defects and code smells repaired

### 1. Obsolete merge architecture was still live

The strongest audit finding was not cosmetic. The repository contained two merge
architectures:

- the intended global, change-journal-based `EntityMaintenanceService`; and
- an old project-scoped proposal/audit/snapshot rollback stack with its own
  readers, writers, service, cleanup jobs, configuration, facade methods, schema,
  and tests.

Keeping both created ambiguous ownership and preserved the exact rollback model
Batch 9 replaced. The old stack was deleted. Schema bootstrap now drops its old
tables but does not recreate them; the global merge audit and mutation journals
remain canonical.

### 2. Legacy human-review wrapper survived typed reviews

`human_reviews.py`, `HumanReviewWriter`, their tests, and facade methods duplicated
the typed maintenance-review boundary from Batch 8. They had no necessary current
caller and were removed rather than retained as compatibility code.

### 3. Advisory revision reads disagreed with writes

The advisory writer persists a revision in `MaintenanceReview.expected_state`,
but `RelationshipObservationReader` previously derived the revision from row
order/count. That could report the wrong concurrency version. Reads now use a
valid stored revision and retain a deterministic fallback for malformed or
missing state.

### 4. Notebook limits were only partially configurable

`NotebookCapacity.from_limits()` reused the message cap for documents and one
source cap for both web sections. Actions, next steps, summary characters, and
the hard render ceiling were not exposed through the run settings. Each section
now has an independent validated setting and immutable run snapshot.

The `AgentRun.evidence_summary` setter also bypassed summary capacity, and source
replacement mutated web discovery and web read sections separately. Summary
writes now use the notebook boundary; source replacement is validated on a deep
copy and adopted atomically only if the complete result fits.

### 5. Prompt and fixture drift hid current contracts

- The system prompt advertised `episode_check(entity_name=...)` although the tool
  accepts `entity_id`. It now documents discovery followed by stable-ID lookup.
- Orchestrator fake settings lacked the final notebook limit fields.
- An entity cache test still expected project type to affect a global identity
  embedding.
- Document integration/schema tests still supplied the removed
  `visibility_scope` API/column.
- Typed review tests still asserted deleted workflow tables and untyped rows.
- One static schema assertion accidentally matched
  `episode_entity.project_id` while trying to ban `entity.project_id`.

These were corrected to the production target contracts. No production
compatibility parameter, legacy table, or weakened model was introduced merely
to make a stale test pass.

### 6. The reported extraction stall was misdiagnosed

The document extraction contract is healthy: all 15 tests pass in under a
second. The actual stall occurred in the fake NLI contract, which routed fake
model construction and inference through the real thread executor. The worker
completed but its pytest event-loop callback stranded in this environment.

Those tests validate lazy model loading/reuse and result mapping, not executor
integration. Their fake model work now runs inline. The file passes 9/9, and the
full non-model knowledge/agent unit slice completes normally. Production model
execution was not changed.

### 7. Dead broad ownership APIs and mutations remained

The unused broad `BackgroundWorkCoordinator.cancel_project()` API was removed in
favor of owner-specific cancellation. The dead canonical-name mutation method on
`GraphWriter` and its store facade were also removed because normal ingestion
must not mutate global canonical identity.

## Intentional surfaces retained

- `CompiledDomain.active_topics` remains an extraction/domain-activation input.
  It is not knowledge visibility state and has no retrieval filtering role.
- AgentRun list-shaped properties remain notebook-backed views for its internal
  consumers. They write through the single canonical `RunNotebook`; they are not
  parallel accumulators.
- `DROP TABLE` and `DROP COLUMN` statements for obsolete structures remain in
  schema bootstrap so an existing unreleased development database is cleaned on
  schema application. They do not preserve or recreate a legacy runtime.
- Message/source provenance survives normal session or document tombstoning, as
  required by the plan. A destructive Forget feature remains explicitly outside
  this work.

## Documentation and reset contract

The README now describes global identity plus project context, project-filesystem
authority, completed-turn Episodes, typed maintenance, and application-owned
maintenance. Because this is an unreleased schema cut, the documented migration
path is a clean local reset and rebuild, not an in-place legacy data promise:

```bash
docker compose down -v
docker compose up -d --build
```

This deletes the local PostgreSQL volume. Desired canonical project sources must
be reopened or re-imported and rebuilt after PostgreSQL becomes healthy.

## Verification performed

- `567 passed, 2 deselected` for the full non-model Knowledge, Knowledge storage
  unit, and Agent unit slice.
- `15 passed` for the extraction contract that had been reported as stalled.
- `9 passed` for the corrected fake NLI contract.
- `114 passed, 56 deselected` for storage contracts excluding PostgreSQL, Redis,
  and model-marked tests.
- `4 passed, 1 deselected` for static schema-bootstrap contracts.
- Ruff passes across `server/src` and `server/tests`.
- Python compilation passes across `server/src`.
- Architecture import checks pass.
- Full test collection succeeds: `1169 tests collected` with no stale imports
  from the removed modules.
- `git diff --check` passes.

Not verified in this environment:

- `pg_isready -h localhost -p 5432` returns `no response`; all real-PostgreSQL and
  pgvector gates remain pending.
- Real model smoke tests were not completed as part of this audit.
- Redis-specific behavior was excluded from the storage lane.

## Final assessment

The twelve-batch implementation is coherent in the reachable Python code after
the audit repairs. I found no remaining known duplicate owner, reachable legacy
merge/review implementation, partial Notebook capacity path, or stale production
contract. The work should not be called fully integration-proven until the
real-PostgreSQL/pgvector and model lanes pass on an available environment.
