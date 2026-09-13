# Core Stage 5 — Maintenance Integrity and Concurrency

Status: Chunks A–G complete 2026-09-12. Stage closeout remains planned.

Baseline inspected: `aadedewe/refactor`, `bcb354e`, including the typed evidence
maintenance framework committed before Core Stages 1 and 2. Recheck the current
checkout before implementation: this plan finishes that framework and must not
recreate it.

Stage 5 depends on Stage 3's final semantic-participation names and eligibility
rules. Its association and review chunks may be developed independently, but
frontier/concurrency closeout waits for Stage 3.

## Objective

Make explicit maintenance operations agree with Context-first Knowledge under
merge, cleanup, restart, and concurrent semantic work:

1. Global merge/rollback and Project cleanup include
   `context_block_entities`.
2. Maintenance quiescence includes eligible conversation work and every active
   semantic-window origin.
3. Global identity merge and semantic Knowledge commit have a proven durable
   ordering.
4. Projection repair obligations survive interruption after canonical change.
5. Conflict discovery/reviews use stable identity, exact evidence snapshots,
   and durable structured resolution.
6. User-selected maintenance automation reduces repeated review burden without
   silently broadening mutation authority.

## Finding map

| Work | Review source | Stage 5 outcome |
| --- | --- | --- |
| Merge omits Context associations | Core F8; locked Knowledge §§10–11 | Include associations in preview/hash/mutation/rollback. |
| Cleanup omits Context associations | Core F8; locked Knowledge §§10,12,28 | Remove only the target Project's associations and downstream links. |
| Frontier ignores participation/human edits | Core F12; locked Knowledge §§20–21 | Use Stage 3 eligibility plus all active semantic windows. |
| Merge/semantic serialization | Locked Knowledge §22; core remaining gate | Prove the interleaving first; add the smallest shared transaction lock only if required. |
| Lost projection repair marker | Maintenance review MC5 | Mark repair pending inside canonical merge/rollback transaction. |
| Conflict discovery/review gaps | Maintenance review MC1–MC4 | Make discovery useful and reviews stable/auditable. |
| User review burden | Prior product discussion; maintenance scoped improvement | Add bounded action-class trust policy after identity/deduplication is correct. |

## Scope boundaries

Included:

- Existing global entity merge journal, rollback, impact preview, Project entity
  cleanup, maintenance frontier, typed review envelope, conflict discovery,
  projection repair, health status, and ProjectManager cache invalidation.
- Real PostgreSQL/AGE transaction and controlled-interleaving tests.
- Server-side policy for manual/assisted/trusted maintenance behavior.

Deferred:

- Project Forget remains a separate irreversible workflow.
- Context supersession remains deterministic ingestion, not a conflict review.
- Historical Episode prose is not rewritten by merge or relationship
  reinterpretation.
- Multi-engine/distributed locking is out of scope; one local Knoggin engine
  with concurrent jobs remains in scope.
- UI/SDK presentation waits for those layers.

## Required invariants

- `context_block_entities` is first-class identity usage in preview, state hash,
  merge, rollback, cleanup, and conflict residue.
- Merge/cleanup never remove another Project's association.
- A global identity survives while any Project classification remains.
- Maintenance cannot capture or revalidate a stable frontier while an eligible
  exchange or any non-completed semantic window exists.
- Canonical merge/rollback commits with a durable projection-repair obligation;
  successful projection rebuild clears it afterward.
- Retry rebuilds derived projections from the committed audit and never replays
  canonical mutations.
- Equivalent conflict evidence maps to one review regardless of model
  confidence/rationale drift. Dismissal remains effective until evidence state
  actually changes.
- Review resolution category, note, actor, and closure are durable and readable
  without parsing prose.

## Chunk A — Association-complete global merge and rollback

Add `context_block_entities` to the current merge snapshot and mutation journal:

- include survivor/retired associations in preview and expected-state hash;
- migrate retired ID associations to survivor ID within the canonical merge;
- deduplicate `(block_id, survivor_entity_id)` collisions;
- journal inserted/deleted/deduplicated rows for inverse application;
- include association changes in rollback conflict detection and residue review;
- keep Context blocks/revisions immutable.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/db/writers/global_entity_merge_writer.py` | Snapshot, hash, mutate, journal, and invert Context associations. |
| `src/core/knowledge/entity/maintenance_service.py` | Surface accurate preview/rollback effects. |
| `src/core/knowledge/maintenance_impact.py` | Report association counts/effects honestly. |
| `tests/unit/knowledge/test_global_entity_maintenance_contract.py` | Cover deterministic plan/hash shape. |
| `tests/unit/knowledge/test_maintenance_application_contract.py` | Cover journal and rollback behavior. |
| `tests/contract/storage/test_maintenance_application_real_postgres.py` | Prove migration, dedupe, rollback, and stale-state detection. |

Acceptance:

- Merge moves every retired block association exactly once.
- Survivor/retired collision leaves one survivor association.
- Changing an association after preview invalidates the merge plan.
- Safe rollback restores associations; concurrent changes become explicit
  residue and are not overwritten.
- Episode enrichment sees only the surviving identity after merge.

Commit boundary: global merge/rollback association completeness.

## Chunk B — Project-scoped entity cleanup completeness

Extend cleanup preview and application to remove the selected entity's
`context_block_entities` only for the target Project. Reconcile dependent
Episode entity/relationship links and aggregate/projection state through the
existing cleanup transaction.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/db/readers/entity_reader.py` | Count Context associations in cleanup preview. |
| `src/core/knowledge/db/writers/graph_writer.py` | Delete target-project associations in canonical cleanup. |
| `src/core/project/entity_cleanup.py` | Preserve explicit preview/selection workflow. |
| `src/core/project/maintenance_service.py` | Return accurate typed effects and invalidate affected cache state. |
| `tests/unit/core/knowledge/test_entity_cleanup_persistence_contract.py` | Cover write ownership and dependent cleanup. |
| `tests/unit/project/test_entity_cleanup_workflow.py` | Cover preview/apply semantics. |
| Real PostgreSQL maintenance contracts | Prove cross-project survival and no stale Episode enrichment. |

Acceptance:

- Target Project associations disappear.
- Another Project's associations/classification remain untouched.
- The global entity is deleted only when no Project classification remains.
- Later Episode enrichment cannot resurrect the cleaned entity through stale
  block associations.

Commit boundary: explicit Project cleanup completeness.

## Chunk C — Participation-aware, all-origin semantic frontier

Replace the message-only pending query with one coherent quiescence read per
affected Project:

- eligible unclaimed conversation exchanges use Stage 3 participation/status/
  frontier rules;
- any `project_semantic_windows.stage <> 'completed'` blocks maintenance,
  including `human_edit` windows with no message membership;
- completed/excluded/deleted Session work does not block;
- the frontier token includes the durable semantic-window/completion boundary,
  not only maximum message ID/timestamp.

Capture and revalidate must run the same query/normalization. Avoid a second
definition of semantic eligibility drifting from admission; use one SQL helper
or focused reader owned by semantic-window storage.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/entity/maintenance_service.py` | Consume one participation-aware frontier contract. |
| `src/core/knowledge/db/readers/semantic_window_reader.py` | Expose reusable quiescence/frontier state if it prevents SQL duplication. |
| `src/core/knowledge/store.py` | Add only the narrow facade operation required. |
| `tests/unit/knowledge/test_maintenance_application_contract.py` | Cover token generation/revalidation shape. |
| Real PostgreSQL maintenance contracts | Cover disabled/re-enabled/deleted Sessions and conversation/human-edit windows. |

Acceptance:

- Active human-edit and conversation windows block maintenance.
- A completed human-edit window no longer blocks.
- Intentionally excluded Session exchanges never block.
- New eligible work after preview invalidates the plan.

Commit boundary: semantic quiescence and frontier tokens.

## Chunk D — Prove and enforce merge/semantic-commit ordering

First add a controlled PostgreSQL interleaving test:

1. Prepare a semantic build referencing entity X.
2. Pause before its durable commit.
3. Attempt a global merge retiring X.
4. Exercise both transaction orderings.
5. Assert no committed relationship/association references a stale retired
   identity and no partial state is visible.

If existing row/FK/advisory locks already establish the invariant, retain the
regression and do not add another lock. If the test demonstrates the reviewed
gap, use one short-lived user-global identity advisory transaction lock shared
by `SemanticCommitWriter.commit()` and global merge/rollback. Acquire it only
around canonical database mutation, after model work, with one documented lock
order.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/db/writers/semantic_commit_writer.py` | Acquire shared durable identity lock only if the interleaving proves it necessary. |
| `src/core/knowledge/db/writers/global_entity_merge_writer.py` | Use the same key/order for merge and rollback. |
| `src/core/knowledge/entity/maintenance_service.py` | Remove/rename its older merge-only lock if ownership moves to writers. |
| `tests/contract/storage/test_maintenance_application_real_postgres.py` | Deterministic two-connection interleaving and timeout/deadlock guard. |
| `tests/contract/storage/test_semantic_commit_contract.py` | Assert redirect validation/rollback behavior. |

Acceptance:

- Merge first causes stale semantic commit to reject/re-resolve atomically.
- Semantic commit first is fully visible to merge migration/hash validation.
- No deadlock under the documented lock order.
- Unrelated model/extraction work is not serialized.

Commit boundary: concurrency evidence plus only the proven source repair.

## Chunk E — Durable projection-repair obligation

Set `projection_repair_pending` in the same canonical transaction that marks a
merge or rollback executed. After commit:

- rebuild affected AGE projections;
- clear the marker only when every affected projection succeeds;
- retain it across exception, cancellation, or process death;
- retry by reloading the committed audit and rebuilding projections only.

Reuse the existing bounded `failure_reason='projection_repair_pending'` marker
unless implementation evidence shows that a separate column materially
simplifies the state model. No generic workflow engine is needed.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/db/writers/global_entity_merge_writer.py` | Commit the repair obligation with merge/rollback audit state. |
| `src/core/knowledge/entity/maintenance_service.py` | Clear after success and preserve on interruption. |
| `src/core/health/service.py` | Continue exposing bounded pending state without payload leakage. |
| Maintenance unit and real PostgreSQL contracts | Inject cancellation before/during/after rebuild and prove canonical mutation runs once. |

Acceptance:

- Cancellation immediately after commit leaves a visible pending marker.
- Restart repair reloads audit state and never replays merge/rollback.
- Partial multi-Project rebuild leaves the operation pending.
- Successful retry clears health state and invalidates relevant live caches.

Commit boundary: projection recovery durability.

## Chunk F — Stable conflict discovery and review records

Repair MC1–MC4 as one review contract in four internal steps:

1. Discovery eligibility distinguishes active independent disagreement from
   deterministic supersession. Context-backed current observations are not
   excluded merely because they have block support.
2. Every candidate snapshot is built from exactly its cited observation IDs,
   using the same ordered evidence service as detail/preview. Agent/user reports
   use this same snapshot path.
3. Equivalent evidence state reuses the existing review regardless of
   confidence, rationale, or origin changes. Dismissed equivalent evidence does
   not silently reopen. Changed evidence deliberately stales/succeeds it.
4. Persist a typed resolution record containing category, note, actor, and
   closure time while retaining the immutable original proposal.

Activate scheduled discovery only after these invariants pass. If the feature
is intentionally disabled by product policy, expose that honestly instead of a
job whose normal `should_run()` is permanently false.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/conflict_discovery.py` | Select reviewable current disagreement without blanket Context exclusion. |
| `src/core/knowledge/db/readers/conflict_discovery_reader.py` | Keep `retired_at IS NULL` as deterministic supersession eligibility; evidence origin remains explanatory metadata. |
| `src/core/project/maintenance_service.py` | Build candidate-local snapshots for background and direct reports. |
| `src/core/knowledge/db/writers/conflict_writer.py` | Reuse stable review identity and persist typed resolution. |
| `src/core/knowledge/db/writers/maintenance_review_writer.py` | Support immutable proposal plus durable typed closure. |
| `src/core/knowledge/db/readers/conflict_reader.py` | Return the stored resolution without parsing event text. |
| `src/core/knowledge/jobs/conflict_discovery_job.py` | Make scheduling reflect configured behavior after correctness. |
| `src/infrastructure/schema.sql` and `src/core/knowledge/maintenance_reviews.py` | Add the smallest typed resolution storage needed. |
| Conflict discovery/writer/maintenance tests | Replace MC1–MC4 defect probes with desired contracts. |

Acceptance:

- Current Context-backed disagreement reaches discovery.
- A fresh review reports `evidence_state='current'` immediately.
- Confidence-only changes do not create or notify a second review.
- Dismissal survives equivalent rediscovery.
- Changed cited evidence creates an explicit successor/stale transition.
- Resolution kind and note survive a fresh detail read.

Commit boundary: conflict discovery/review correctness.

## Chunk G — Configurable maintenance burden after correctness

Add a small server-owned trust policy based on action class, not model
confidence thresholds:

- `manual`: discovery/checks run only when requested; every proposed change is
  reviewed.
- `assisted`: bounded scheduled discovery opens deduplicated reviews and
  notifies; mutations still require explicit application.
- `trusted`: the user may explicitly whitelist exact non-destructive
  classifications or maintenance action kinds. Identity merges, Project
  cleanup, Context changes, and other canonical destructive actions remain
  previewable and explicit unless that exact action kind is separately enabled.

Keep the review/audit record for automated dispositions. Do not let a generic
"confidence" setting become mutation authority.

Primary files:

| File | Planned change |
| --- | --- |
| `src/common/schema/settings.py` | Add a compact maintenance review mode/action allowlist. |
| `src/core/project/maintenance_service.py` | Enforce policy after stable dedupe/evidence checks. |
| `src/core/knowledge/jobs/conflict_discovery_job.py` | Respect manual/assisted/trusted scheduling. |
| Health/trace contracts | Report counts/mode without exposing evidence payloads. |
| Maintenance unit/integration tests | Prove repeated evidence does not repeatedly burden the user and only whitelisted actions auto-close/apply. |

Acceptance:

- Manual mode performs no background model review.
- Assisted mode produces one review per unchanged evidence state.
- Trusted mode cannot broaden itself or bypass action-class authorization.
- Every automated disposition remains reversible/auditable where the
  underlying operation supports reversal.

Commit boundary: operator policy after MC1–MC4 are resolved.

## Combined Stage 5 scenario

Use real PostgreSQL/AGE and deterministic review/model outputs:

1. Create two Project-visible identities with Context block associations and a
   current conflict.
2. Verify discovery creates one current review from exactly cited evidence and
   equivalent reruns do not duplicate it.
3. Hold an active human-edit window and prove merge cannot capture a stable
   frontier; complete it and retry.
4. Interleave semantic commit with merge and verify the selected durable order.
5. Merge identities, inject process-style interruption before projection
   rebuild, restart repair, and prove one canonical merge plus recovered AGE.
6. Roll back safely, then introduce concurrent association change and prove it
   becomes residue rather than being overwritten.
7. Clean the identity from one Project and prove another Project's identity and
   associations survive while stale Episode enrichment cannot reintroduce it.
8. Exercise manual/assisted/trusted review modes against unchanged evidence.

## Validation and closeout

Run focused tests per chunk, then all maintenance, merge, cleanup, conflict,
semantic-commit, evidence, health, and Stage 3 participation contracts. Run real
PostgreSQL/AGE service tests with cancellation/interleaving timeouts; do not use
parallel workers against shared reset fixtures.

Also run touched-path Ruff, compileall, architecture checks when imports move,
the configured mypy scope after its stale path is repaired separately, and
`git diff --check`. No live model or external network call is required.

Record exact commands/results, changed files, limitations, and local commit IDs.
Retire F8/F12/MC1–MC5 probes only after normal regressions exist. Append status
to the locked Knowledge review and maintenance correction review.

- [x] A: merge/rollback includes Context entity associations.
- [x] B: Project cleanup includes Context entity associations.
- [x] C: participation-aware, all-origin maintenance frontier.
- [x] D: merge/semantic-commit ordering is proven by existing row locks.
- [x] E: projection-repair obligation is durable before rebuild.
- [x] F: conflict discovery/review identity, evidence, and resolution are stable.
- [x] G: configurable maintenance review burden.
- [ ] Combined real-PostgreSQL/AGE scenario passes.
- [ ] Review/probe/operations closeout is recorded and committed locally.

Chunk A validation on 2026-09-12:

- `uv run pytest -q tests/unit/core/knowledge/test_maintenance_impact.py tests/unit/knowledge/test_global_entity_maintenance_contract.py tests/unit/knowledge/test_maintenance_application_contract.py tests/contract/storage/test_maintenance_application_real_postgres.py tests/unit/core/knowledge/test_project_episode_build_contract.py tests/contract/storage/test_semantic_commit_contract.py tests/unit/runtime/test_api_port.py tests/contract/api/test_app.py tests/unit/core/agent/test_conflict_reporting_contract.py tests/unit/core/agent/test_tool_dispatch_contract.py` → 77 passed; the environment emitted its existing Requests dependency warning.
- The real PostgreSQL regressions prove association migration, survivor/retired deduplication, safe rollback, changed-association residue, and stale-preview rejection.
- Focused Ruff, compileall, and `git diff --check` passed. MyPy remains deferred because its configured path baseline is still stale, as recorded separately in `MYPY_BASELINE.md`.

`context_block_entities` is now included in the merge snapshot and state hash,
the typed impact preview, the canonical association mutation journal, rollback
safety classification, and inverse application. A collision preserves the
existing survivor association; a later change remains explicit residue instead
of being overwritten. F8's Project-cleanup portion and its final review/probe
closeout remain in Chunk B and Stage 5 closeout respectively.

Next implementation task: Stage 5 Chunk B.

Chunk B validation on 2026-09-12:

- `uv run pytest -q tests/unit/core/knowledge/test_maintenance_impact.py tests/unit/knowledge/test_global_entity_maintenance_contract.py tests/unit/knowledge/test_maintenance_application_contract.py tests/unit/core/knowledge/test_entity_cleanup_persistence_contract.py tests/unit/project/test_entity_cleanup_workflow.py tests/contract/storage/test_maintenance_application_real_postgres.py tests/unit/core/knowledge/test_project_episode_build_contract.py tests/contract/storage/test_semantic_commit_contract.py tests/contract/storage/test_project_context_window_contract.py tests/unit/runtime/test_api_port.py tests/contract/api/test_app.py tests/unit/core/agent/test_conflict_reporting_contract.py tests/unit/core/agent/test_tool_dispatch_contract.py` → 105 passed; the environment emitted its existing Requests dependency warning.
- The focused cleanup gate passed 15 tests, including real PostgreSQL cleanup of an identity shared by two Projects and a post-cleanup Episode-enrichment retry.
- Focused Ruff, compileall, and `git diff --check` passed. MyPy remains deferred because its configured path baseline is still stale, as recorded separately in `MYPY_BASELINE.md`.

Cleanup preview now reports `context_block_association_count`. Its canonical
transaction deletes selected associations only in the target Project, clears
that Project's AGE relationship projection, removes dependent Episode links,
and then removes the Project classification. A shared global entity and the
other Project's classification/association remain intact; an enrichment retry
has no target-Project association from which to recreate the removed Episode
link. The existing Project maintenance service already invalidates the target
runtime cache with the selected IDs.

F8 now has normal merge and cleanup regressions. Its historical probe and
review closeout remain pending the Stage 5 closeout.

Chunk C validation on 2026-09-12:

- `uv run pytest -q tests/unit/knowledge/test_global_entity_maintenance_contract.py tests/contract/storage/test_maintenance_application_real_postgres.py` → 13 passed; the environment emitted its existing Requests dependency warning.
- `uv run pytest -q tests/unit/core/ingestion/test_semantic_window_admission.py tests/contract/storage/test_semantic_window_participation_admission_contract.py tests/contract/storage/test_project_context_window_contract.py tests/contract/storage/test_semantic_commit_contract.py` → 37 passed; the same existing warning appeared.
- `uv run pytest -q tests/unit/core/knowledge/test_maintenance_impact.py tests/unit/knowledge/test_global_entity_maintenance_contract.py tests/unit/knowledge/test_maintenance_application_contract.py tests/unit/core/knowledge/test_entity_cleanup_persistence_contract.py tests/unit/project/test_entity_cleanup_workflow.py tests/contract/storage/test_maintenance_application_real_postgres.py tests/unit/core/knowledge/test_project_episode_build_contract.py tests/contract/storage/test_semantic_commit_contract.py tests/contract/storage/test_project_context_window_contract.py tests/unit/runtime/test_api_port.py tests/contract/api/test_app.py tests/unit/core/agent/test_conflict_reporting_contract.py tests/unit/core/agent/test_tool_dispatch_contract.py` → 109 passed; the same existing warning appeared.
- Focused Ruff, `uv run python -m compileall`, and `git diff --check` passed. MyPy remains deferred because its configured path baseline is still stale, as recorded separately in `MYPY_BASELINE.md`.

`SemanticWindowReader` now owns one shared participation-eligible exchange CTE
for admission and maintenance. Its one-read quiescence state blocks eligible
unclaimed closed exchanges and every non-completed semantic window, while its
completed-window boundary changes the frontier token even for human edits with
no message membership. Capture and revalidation use the same query and
normalization. Disabled, deleted, and pre-frontier exchanges stay outside the
pending set; a newly eligible post-frontier exchange invalidates the merge
plan.

Next implementation task: Stage 5 Chunk D.

Chunk D validation on 2026-09-12:

- `uv run pytest -q tests/contract/storage/test_semantic_commit_contract.py -k 'merge_first_rejects_a_stale_semantic_commit_without_partial_state or semantic_commit_first_is_migrated_by_the_waiting_global_merge'` → 2 passed.
- `uv run pytest -q tests/contract/storage/test_semantic_commit_contract.py` → 13 passed.
- `uv run pytest -q tests/contract/storage/test_maintenance_application_real_postgres.py` → 7 passed; the environment emitted its existing Requests dependency warning.
- `uv run pytest -q tests/contract/storage/test_semantic_commit_contract.py tests/contract/storage/test_maintenance_application_real_postgres.py` → 20 passed; the same existing warning appeared.
- Focused Ruff, `uv run python -m compileall`, and `git diff --check` passed. MyPy remains deferred because its configured path baseline is still stale, as recorded separately in `MYPY_BASELINE.md`.

The real-PostgreSQL regression uses the fixture's two connections and bounded
timeouts to force both canonical orders. A merge first holds `entities` with
`FOR UPDATE`; the stale semantic commit waits at its reused-identity
`FOR KEY SHARE` validation, observes the redirected identity, and rolls its
whole transaction back. A semantic commit first holds that key-share lock;
the merge waits, snapshots the committed association and relationship, then
migrates them before redirecting the retired identity. Both tests assert no
remaining Context association, relationship, observation, message reference,
or Project classification names the retired identity.

No shared user-global lock was added. The existing lock order is sufficient:
semantic commit takes its window/context locks before key-sharing each reused
identity, while global merge takes its merge advisory lock and then updates
the relevant entity rows. Neither path takes those resources in reverse order,
and model/extraction work is complete before `SemanticCommitWriter.commit()`
opens its transaction. The regression releases the waiting task in each order
and completes it under its deadlock guard.

Chunk E validation on 2026-09-12:

- `uv run pytest -q tests/unit/knowledge/test_maintenance_application_contract.py` → 21 passed; the environment emitted its existing Requests dependency warning.
- `uv run pytest -q tests/contract/storage/test_maintenance_application_real_postgres.py` → 11 passed; the same existing warning appeared.
- `uv run pytest -q tests/unit/core/knowledge/test_maintenance_impact.py tests/unit/knowledge/test_global_entity_maintenance_contract.py tests/unit/knowledge/test_maintenance_application_contract.py tests/unit/health/test_runtime_health_service.py tests/contract/storage/test_maintenance_application_real_postgres.py tests/contract/storage/test_semantic_commit_contract.py` → 66 passed; the same existing warning appeared.
- Focused Ruff, `uv run python -m compileall`, and `git diff --check` passed. MyPy remains deferred because its configured path baseline is still stale, as recorded separately in `MYPY_BASELINE.md`.

The canonical merge audit now becomes `executed` with
`failure_reason='projection_repair_pending'` in the same transaction. A
rollback that applies any inverse mutation sets the same marker in its
canonical transaction, including a partial rollback; a no-op rollback leaves
an existing marker untouched. The post-commit rebuild clears the marker only
after all affected Projects succeed. Cancellation before rebuild, during a
multi-Project rebuild, or after rebuild before clearing leaves the committed
obligation intact. Restart repair reloads the audit and rebuilds only derived
AGE state; it does not replay merge or rollback. Existing health reporting
remains bounded, and ProjectManager's existing repair path invalidates affected
live entity caches after retry.

Next implementation task: Stage 5 Chunk F.

Chunk F validation on 2026-09-12:

- `uv run pytest -q tests/unit/knowledge/test_conflict_discovery.py tests/unit/knowledge/test_conflict_writer.py tests/unit/core/knowledge/test_conflict_discovery_persistence_contract.py tests/unit/core/agent/test_conflict_reporting_contract.py tests/unit/knowledge/test_maintenance_application_contract.py tests/unit/knowledge/test_maintenance_reviews.py tests/contract/storage/test_current_observation_reader_contract.py tests/contract/storage/test_conflict_review_contract.py tests/contract/storage/test_maintenance_application_real_postgres.py tests/contract/storage/test_semantic_commit_contract.py tests/contract/postgres/test_schema_bootstrap_contract.py` → 72 passed; the environment emitted its existing Requests dependency warning.
- Focused Ruff, `uv run python -m compileall -q src`, and `git diff --check` passed. MyPy remains deferred because its configured path baseline is still stale, as recorded separately in `MYPY_BASELINE.md`.

Current Context-backed observations now reach the bounded discovery packet;
deterministically superseded observations remain excluded by `retired_at`. Every
background, direct, and agent report captures the ordered evidence snapshot for
only its cited observations, so a new review reads as current immediately.
The writer rejects empty or packet-wide observation snapshots before it can
persist an incorrect review.
The conflict subject lock and stable subject key reuse equivalent evidence
regardless of model confidence, rationale, or origin; changed state stales an
open predecessor and records an immutable `supersedes_review_id` link. A
dedicated resolution table stores the validated category, note, actor, and
closure time, while the original proposal remains unchanged. Scheduled
discovery now follows the configured enabled/LLM state.

Chunk G validation on 2026-09-12:

- `uv run pytest -q tests/unit/knowledge/test_conflict_discovery.py tests/unit/knowledge/test_conflict_writer.py tests/unit/knowledge/test_maintenance_policy.py tests/unit/core/knowledge/test_conflict_discovery_persistence_contract.py tests/unit/core/agent/test_conflict_reporting_contract.py tests/unit/knowledge/test_maintenance_application_contract.py tests/unit/knowledge/test_maintenance_reviews.py tests/unit/runtime/test_project_config_fanout.py tests/unit/health/test_runtime_health_service.py tests/contract/storage/test_current_observation_reader_contract.py tests/contract/storage/test_conflict_review_contract.py tests/contract/storage/test_maintenance_application_real_postgres.py tests/contract/storage/test_semantic_commit_contract.py tests/contract/postgres/test_schema_bootstrap_contract.py` → 98 passed; the environment emitted its existing Requests dependency warning.
- `uv run ruff check` and `uv run ruff format --check` on the touched source/test paths, `uv run python -m compileall -q src`, and `git diff --check` passed. MyPy remains deferred because its configured path baseline is still stale, as recorded separately in `MYPY_BASELINE.md`.

`ConflictDiscoverySettings` now has an explicit `manual`, `assisted`, or
`trusted` mode. Its trusted allowlist accepts only four exact conflict
classification actions; entity merge, Project cleanup, Context changes, and
generic plan kinds are not representable in the setting. The shared Project
maintenance service owns the active policy and updates from the configuration
manager, so a caller cannot supply a stronger policy at disposition time.

The conflict-discovery job is registered with active Project runtimes and now
defers to scheduler cadence rather than returning a continuation trigger on
every scheduler check. Manual mode disables scheduling while retaining an
explicit execution path; assisted and trusted modes schedule bounded proposal
creation and retain stable deduplication from Chunk F. Model candidates remain
proposals in every mode. A separate trusted entry point can only close a
current review using an exact allowlisted classification, records the fixed
`maintenance-trust-policy` actor and reason, and performs no canonical
mutation. Background health now exposes only the mode, policy/action count,
interval, LLM availability, and bounded last-run counts.

Next implementation task: Stage 5 combined scenario and review/probe closeout.
