# Core Stage 1 — Canonical Knowledge and Context

Status: Chunk A complete; Chunks B and C remain planned.

Baseline inspected: `aadedewe/refactor`, `acc0cac33b74759d07880a8b6fffeeee0cdee282`, 2026-09-09. Recheck HEAD and working-tree changes before implementation. Preserve unrelated changes, including the existing edit to `PROJECT_SEMANTIC_OPERATIONS.md`.

## Objective

Make current Knowledge faithfully reflect accepted Context changes and their evidence time:

1. An unchanged conversation window must not re-extract a prior revision's impact or create new observations from unchanged support.
2. Ordinary current reads must exclude retired observations without erasing explicit historical evidence.
3. Entity activity and relationship observation times must come from supporting Context evidence, not processing time.

This is one implementation checklist integrating locked decisions with recent findings. Original review documents remain the rationale; this file tracks execution and validation.

## Finding map

| Work | Source | Stage 1 decision |
| --- | --- | --- |
| No-change window reuses old impact | Core review F1 | Fix job selection and durable commit validation together. |
| Current reads include retired observations | Locked Knowledge §9; core F7 | Fix scoped current readers; retain history APIs. |
| Entity event time | Locked Knowledge §§7,13; core F9 | Update new and already-existing project entity contexts monotonically. |
| Relationship event time | Locked Knowledge §§7,14; core F9 | Derive from exactly the cited support blocks. |
| Human-edit acceptance time | Locked Knowledge §7 | Assign once at durable Context acceptance for newly authored untimed human blocks. |

## Scope boundaries

Included: Context/window ownership, effective reconciliation impact, semantic publication, current evidence filtering, source-time propagation, regression tests, and the necessary operations documentation.

Deferred: identity resolution and linked-project classification (Stage 2); participation/FIFO/policy capture (Stage 3); document hashes, source persistence, Episode edit embeddings, and graph evidence hydration (Stage 4); merge/cleanup associations, maintenance frontiers/concurrency, conflict reviews, and recovery markers (Stage 5); Agent rendering and research enforcement (Stage 6). SQLite migration, Forget, and extractor replacement remain separate work.

Do not use this stage to remove every dead setting or redesign storage ownership. In particular, the policy-capture race qualification in the core review remains in force.

## Chunk A — Make reconciliation impact belong to the semantic window

### Current behavior

`ProjectSemanticJob` reuses the existing snapshot after a no-op Context update. The Knowledge stage reloads that revision's original impact. `SemanticCommitWriter._verify_context_input()` requires the same revision-wide impact, so a job-only change would be rejected by storage. Observation identity includes the new window ID, which permits duplicate observations across windows.

### Planned implementation

Use the existing `ContextSnapshot.window_id` / persisted revision owner to distinguish newly committed Context from reuse:

- A revision owned by this window uses its persisted revision impact, including deleted blocks and affected dependencies/neighbors.
- A later conversation window reusing an already reconciled revision has an empty effective impact.
- An initial empty Context revision remains a valid durable checkpoint and completes normally.
- Human-edit windows retain their own revision/impact; absence of conversation messages does not make them no-ops.
- Reject inconsistent ownership/checkpoint combinations instead of silently skipping unreconciled work. Verify the earlier owning window reached Knowledge publication before accepting reuse as no new work. Clarify any unowned baseline revision case from actual callers before allowing it.

Prefer a small shared effective-impact rule on an existing Context/build contract, backed by storage verification of persisted ownership. Do not add a separate checkpoint table or duplicate impact collection unless live caller verification shows the existing ownership link is insufficient.

For empty impact, construct the valid empty entity/relationship result without invoking extraction models. Still run the normal durable Knowledge-stage transition and Episode finalization. Do not replace the pipeline with an early `completed` shortcut. Preserve deletion-only reconciliation even when there are no remaining extractable blocks.

### Files

| File under `server/` | Planned change |
| --- | --- |
| `src/core/ingestion/project_semantic_job.py` | Select window-specific effective impact; avoid extraction on empty work; preserve checkpoint/finalization flow. |
| `src/core/ingestion/batch.py` | Express or consume the small effective-impact contract and valid empty build results if needed. |
| `src/common/schema/context.py` | Only if needed for a shared pure ownership rule; reuse current fields. |
| `src/core/knowledge/db/writers/semantic_commit_writer.py` | Read persisted revision ownership, validate effective impact and reuse eligibility in the commit transaction; reject nonempty writes for reused/no-op Context. |
| `src/core/knowledge/db/readers/project_context_reader.py` / `src/core/knowledge/store.py` | Conditional: expose only missing scoped ownership/checkpoint reads; avoid redundant facade methods. |

### Completed implementation — 2026-09-10

- `SemanticWindowBuild` derives an empty effective impact when a later window
  checkpoints a Context revision owned by another window. Empty effective
  impacts attach a valid empty entity/relationship result.
- `ProjectSemanticJob` skips Context evidence loading and extraction for that
  no-work build, but still commits the durable Knowledge checkpoint and runs
  normal finalization.
- `SemanticCommitWriter` rechecks persisted revision ownership inside its
  transaction. Reuse is allowed only after the owner published Knowledge; it
  rejects unowned/unpublished revisions, replayed impact, and nonempty payloads
  before transitioning the later window. Reuse performs no graph writes,
  retirement, orphan cleanup, or projection rebuild.
- Added unit restart/initial-empty coverage and a PostgreSQL contract covering
  an owner publication, forged replay rejection, valid no-op reuse, idempotence,
  and finalization.

### Gate A

- Window A changes Context and writes one observation; window B makes no change: still one observation, unchanged support/count/time, no extraction calls in B.
- Restart after B's Context checkpoint: the same no-op behavior is reconstructed from durable state.
- Retrying the same Knowledge commit remains idempotent.
- Initial empty Context progresses through Episode/Knowledge completion.
- Real replacement, deletion-only edits, impacted dependencies, and message-free human edits still reconcile.
- A crafted build attempting to replay an earlier revision's nonempty impact is rejected at the writer, not merely avoided by the job.
- A later window cannot skip genuinely uncommitted work by presenting another revision owner.

Primary tests: `tests/unit/core/ingestion/test_project_semantic_context_stage.py`, `test_project_semantic_knowledge_stage.py`, `tests/contract/storage/test_semantic_commit_contract.py`, and `tests/integration/ingestion/test_project_semantic_postgres_flow.py`.

## Chunk B — Filter retired observations from current reads

### Planned implementation

Apply active-only observation predicates to current connections/statistics, path evidence references, and observation attachments in recent activity. Keep predicates on the observation join or aggregate as appropriate so filtering a retired observation does not accidentally erase a valid message or entity row.

Preserve current relationships with surviving active support. Keep retired rows and statuses available through explicit historical evidence traversal. Do not change the meaning of historical Episode prose or remove canonical messages to make a current result look clean.

### Files

| File under `server/` | Planned change |
| --- | --- |
| `src/core/knowledge/db/readers/entity_reader.py` | Exclude retired observations from current relationship counts, evidence, context, and first/last observed aggregates; inspect sibling current queries. |
| `src/core/knowledge/db/readers/graph_reader.py` | Filter retired path observation references. Fixing their separate hydration shape is Stage 4. |
| `src/core/knowledge/db/readers/knowledge_query_reader.py` | Remove retired observation attachments from current activity while retaining independently valid message activity. |
| `src/core/knowledge/db/readers/relationship_observation_reader.py` | Verify existing active-only behavior; edit only if a current-read omission is demonstrated. |

### Gate B

- A relationship with one active and one retired observation reports only the active support and its timestamps in every touched current-read path.
- A retired-only relationship disappears through normal semantic reconciliation; direct current readers do not present retired evidence as active support.
- A message remains valid historical activity even when an attached observation retires.
- Explicit evidence traversal still returns retired observations with retired status and original support.
- An unrelated project's evidence remains excluded by existing read scope.

Primary tests: `tests/contract/storage/test_graph_reader_contract.py`, `test_entity_reader_snapshot_contract.py`, and a focused current-observation storage contract file if no existing test owns the cross-reader matrix. Reuse `tests/unit/core/knowledge/test_evidence_service.py` to protect historical presentation; use real PostgreSQL for aggregate/join assertions.

## Chunk C — Preserve source/event time through publication

### Planned implementation

**Human Context:** assign one accepted-edit timestamp within `ProjectContextWriter.commit_revision()` to newly inserted, untimed `human_asserted` blocks. Existing unchanged blocks retain their original time. Preserve explicit valid evidence time. Use the returned persisted snapshot downstream so retries and projection repairs cannot invent a later acceptance time. Keep Markdown/content hashes following their existing content contract.

**Entities:** derive each entity's candidate time from its eligible block/entity associations, not from every block in the window. Update existing project contexts as well as new ones with `max(existing last_mentioned_ms, supported source times)`. Do this after the required local context rows exist, in the same semantic transaction. Missing local classification is a Stage 2 defect; do not silently manufacture classifications here.

**Relationships:** derive each observation's time as `max(source_time_ms)` over its cited support blocks. Use this value for insertion/replay rather than `get_now_ms()`. Different relationships in the same window can have different evidence times. Storage creation/update clocks continue to measure processing separately.

**Authority:** extend the writer's persisted Context read to include source times and use those values for durable publication. Do not trust a caller-modified timestamp on a build whose IDs happen to match the snapshot.

**Missing-time policy:** use available non-null cited times. Never silently substitute extraction time for missing conversation/source evidence. For wholly untimed extractable support, fail with a clear source-time diagnostic until valid provenance is supplied. Existing untimed human blocks can use their immutable acceptance metadata only through an explicit, tested repair decision; do not backfill them with today's date. Recheck canonical message timestamp guarantees before tightening validation. No automatic historical data rewrite is part of this plan.

### Files

| File under `server/` | Planned change |
| --- | --- |
| `src/core/knowledge/db/writers/project_context_writer.py` | Assign/persist accepted time for new untimed human blocks and return authoritative values. |
| `src/core/knowledge/context/projection.py` | Confirm importer uses committed timestamps; adjust only if needed to avoid generating a different time during replay/repair. |
| `src/core/knowledge/db/writers/semantic_commit_writer.py` | Load authoritative block times, update associated project entity recency, and derive per-observation source time. |
| `src/core/ingestion/batch.py` | Conditional: small derived time helpers only if they reduce duplication; no second persisted timestamp map. |

### Gate C

- Evidence timestamp 10:00 processed at 10:04 remains 10:00 for the observation and associated entity activity.
- Older evidence never moves entity recency backward; newer evidence advances it.
- Different cited block subsets produce the correct per-relationship maximum; unrelated newer blocks do not contaminate timestamps.
- Existing resolved entities receive time updates without new identity writes.
- Human edit gets accepted time once; delayed extraction, restart, and projection repair retain it.
- An unchanged human block keeps its previous source time; replaced blocks get their own appropriate time.
- Caller-altered block time cannot override the persisted Context record.
- Missing-time behavior is explicit; `created_at`/operational timestamps remain separate.

Primary tests: `tests/contract/storage/test_project_context_window_contract.py`, `test_semantic_commit_contract.py`, current entity-reader storage checks, and human-import cases in `tests/integration/ingestion/test_project_semantic_postgres_flow.py`.

## Final Stage 1 validation

Run focused tests after each chunk, then one combined correction scenario:

1. Publish an initial claim from older evidence.
2. Accept a newer correction replacing its Context support.
3. Verify current reads expose only surviving active observations and correct event time; historical evidence still explains the replaced observation.
4. Process a no-change follow-up and retry/restart its checkpoints.
5. Verify no extra observations, no inflated activity, no lost Episode completion, and no reactivation of retired support.

Use deterministic fake-model outputs for orchestration and real PostgreSQL for transaction, join, retirement, and timestamp semantics. No live LLM is needed to establish these contracts. Preserve existing same-window replay/atomicity tests. Run touched-path Ruff/compile checks and `git diff --check`; check architecture constraints if imports change.

The old review probe files intentionally assert defects. Add desired-behavior regressions to normal test suites; mark the corresponding probes/report items resolved with test references rather than treating old passing reproductions as acceptance tests.

No timings or test results in the earlier reviews count as validation of the future implementation. Record exact commands, results, and any environment limitations when this stage runs.

## Work and documentation checkpoints

- [x] A: window-specific impact and no-op/retry regressions.
- [ ] B: active-only current reads plus historical evidence regression.
- [ ] C: authoritative event time and human accepted-time regression.
- [ ] Combined correction → no-op → restart scenario passes.
- [ ] Update this checklist with changed files, validation evidence, and commit IDs if commits are requested.
- [ ] Annotate the corresponding locked Knowledge/Ingestion and core-review items with completion references; preserve their rationale and untouched later-stage work.
- [ ] Add a narrow operations-document update covering no-op semantics, current versus historical evidence, and time semantics, preserving the user's existing edits.

Suggested implementation chunks/commits: A, then B, then C, with each chunk's tests included. Before implementing each chunk, verify the exact caller and fixture contracts; promote a conditional file to the change list only for a demonstrated need. No production changes or test executions were performed while drafting this plan.
