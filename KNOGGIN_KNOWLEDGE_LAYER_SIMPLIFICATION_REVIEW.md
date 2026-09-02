# Knoggin Knowledge-Layer Simplification Review

**Review date:** 2026-09-02
**Scope:** Current staged knowledge-layer worktree, with production call-site and test tracing.
**Goal:** Find incomplete behavior and make the implementation more concise without weakening current contracts.

## Verdict

The current direction is sound, but the work is not yet only a cleanup exercise. Two maintenance workflows can presently record or imply success without completing all required state changes:

1. A project maintenance review can be marked `applied` without applying its typed plan.
2. Global entity merge execution and rollback are not connected to an application confirmation boundary; rollback also omits projection and active-cache repair.

Those should be fixed before broad simplification. Afterward, there is a meaningful amount of removable compatibility and abandoned merge-candidate code. The largest safe deletion is the old duplicate-discovery/merge stack still embedded in `EntityResolver`.

This review did not change production or test code.

## Priority 0: Behavior that is incomplete

### 1. `transition_maintenance_review` changes status but does not apply the plan

**Evidence**

- `ProjectMaintenanceService.transition_maintenance_review()` delegates directly to `MaintenanceReviewWriter.transition()` (`server/src/core/project/maintenance_service.py:142`).
- The writer validates the requested status and updates only the review row/event (`server/src/core/knowledge/db/writers/maintenance_review_writer.py:292`).
- The actual relationship mutation, episode reconciliation, graph rebuild, and atomic review transition live in `RelationshipInterpretationWriter.apply_plan()` (`server/src/core/knowledge/db/writers/relationship_interpretation_writer.py:44`).
- `KnowledgeStore.apply_relationship_interpretation_plan()` exposes that behavior but currently has no production caller (`server/src/core/knowledge/store.py:939`).

**Why this matters**

A caller can set a relationship-interpretation review to `applied` while leaving every relationship observation unchanged. The durable review history would then claim a mutation occurred when it did not.

**Recommended shape**

- Replace the generic “set status to applied” path with an application-owned review dispatcher.
- On acceptance, validate the stored `MaintenancePlan` and route it to the correct deterministic service.
- Let the mutation and review transition occur in the same transaction where supported. `RelationshipInterpretationWriter` already does this.
- Keep dismissal and stale transitions as status-only operations.
- Do not create a generic JSON-patch or SQL executor; plan kinds have different invariants.

### 2. User-confirmed global merge and rollback have no reachable application path

**Evidence**

- `ProjectManager` owns one user-global `EntityMaintenanceService` (`server/src/core/project/project_manager.py:111`).
- The application scheduler calls only deterministic `preflight()` (`server/src/core/knowledge/jobs/application_maintenance_scheduler.py:44`).
- The agent can discover candidates and create a confirmation-required review, but it cannot execute the merge (`server/src/core/agent/tools/maintenance.py:110`).
- Production searches found no API, runtime, or manager caller for `EntityMaintenanceService.merge()`, `merge_entities()`, `plan_rollback()`, or `rollback()`.

**Why this matters**

The proposal half of the workflow exists, but there is no application boundary that turns explicit user confirmation into the typed merge, nor one that exposes the planned rollback flow.

**Recommended shape**

- Add one application-level confirmation entry point that loads the open review, revalidates its expected state, and dispatches `EntityMergePlan` to the manager-owned service.
- Add a separate explicit rollback-preview/rollback entry point. Rollback conflict approvals should remain typed and deliberate.
- Keep direct destructive execution out of agent tools.
- Remove the `merge_entities()` forwarding alias once the chosen public method name is established.

### 3. Global merge and rollback leave live runtime caches stale; rollback also leaves AGE stale

**Evidence**

- Merge rebuilds AGE projections for affected projects after the relational transaction (`server/src/core/knowledge/entity/maintenance_service.py:325`).
- Rollback calls `GlobalEntityMergeWriter.rollback_safe()` and returns without rebuilding affected project projections (`server/src/core/knowledge/entity/maintenance_service.py:390`).
- `rollback_safe()` does not return `affected_project_ids` (`server/src/core/knowledge/db/writers/global_entity_merge_writer.py:1007`), although the audit row stores them.
- Neither merge nor rollback updates active `ProjectRuntime.entities` indexes.
- The project cleanup workflow explicitly calls `runtime.entities.remove_entities(...)`, demonstrating that relational writes do not automatically invalidate the in-memory resolver (`server/src/core/project/maintenance_service.py:353`).

**Why this matters**

After a merge, loaded projects can continue resolving the retired entity. After rollback, both the AGE projection and loaded entity indexes can disagree with PostgreSQL.

**Recommended shape**

- Have merge and rollback return the affected project IDs and entity IDs needed for repair.
- At the `ProjectManager` boundary, refresh or invalidate affected active entity indexes after the durable mutation succeeds.
- Rebuild each affected AGE projection after rollback, just as merge does.
- Report projection failures separately from relational success, preserving the current auditability model.

## Priority 1: High-confidence simplifications

### 4. Remove the abandoned resolver-owned duplicate/merge stack

`EntityResolver` is 1,409 lines. Its old maintenance section starts at `merge_into()` and runs through the fuzzy/vector/NLI merge classifiers near the end of the file (`server/src/core/knowledge/entity/resolver.py:1037`). Production call tracing found no caller for:

- `merge_into()`
- `find_alias_collisions_targeted()`
- `resolve_entity_name()`
- `_collect_candidate_pairs()` and `_classify_pair()`
- the merge type/topic/evidence classifiers
- the associated vector/NLI thresholds

`EntityIndex.merge_into()` is consequently also test-only (`server/src/core/knowledge/entity/index.py:173`). This code belongs to the superseded project-runtime merge design; global candidate discovery now lives in `EntityMaintenanceService`.

**Recommended change:** remove the dead resolver chain and its obsolete tests, then prune only the storage facade/reader methods that become truly unreferenced. Keep `EntityResolver.remove_entities()`, which is used for live cache invalidation.

Likely follow-on removals, after a final caller check, include resolver-only facades for similar-entity search, direct-edge checks, neighbor lookup, merge-topic strength, and batch neighbor lookup. Do not remove lower-level writer methods merely because their `KnowledgeStore` forwarding method is dead; for example, `GraphWriter.update_entity_aliases()` has an internal caller.

### 5. Remove the legacy list surface from `AgentRun`

`AgentRun` currently exposes `profiles`, `messages`, `graph`, `paths`, `episodes`, `sources`, and `evidence_summary` as compatibility properties (`server/src/core/agent/run.py:336`). The list properties are used by tests, not production. Production reads only `evidence_summary`, in prompt rendering (`server/src/core/agent/prompt_context.py:218`).

The compatibility layer also requires `_SectionView`, whose own docstring identifies it as migration-friendly (`server/src/core/agent/notebook.py:123`). `AgentRun.messages` is especially costly because it recreates message formatting that `RunNotebook.model_view()` already owns.

**Recommended change:** migrate tests to `run.notebook`, change prompt rendering to read `run.notebook.summary.text`, and remove the compatibility properties. If no callers remain, remove `_SectionView` and expose intentional notebook operations instead of mutable list emulation.

Do not remove `AgentRun.has_any()` or `rollover_notebook()`; they are live aggregate/lifecycle operations.

### 6. Use `NotebookApplyResult.changed` instead of fingerprinting twice

`AgentRun.accumulate_tool_result()` fingerprints the notebook before and after `RunNotebook.apply()` (`server/src/core/agent/run.py:514`). `RunNotebook.apply()` already performs that comparison and returns it as `NotebookApplyResult.changed` (`server/src/core/agent/notebook.py:1019`).

**Recommended change:** retain the result of `apply()`, use `result.changed`, and delete `AgentRun._evidence_fingerprint()`. This shortens the method and avoids a second pair of notebook serializations without changing behavior.

### 7. Remove small, proven-dead agent methods and parameters

- `AgentRun.render_notebook()` has no caller; runtime rendering uses the notebook directly.
- `AgentRun.clear_short_uuid_references()` has no caller.
- `AgentRun.tool_limit_reached(..., config=None)` immediately discards `config`; the executor nevertheless passes `self.ctx.limits` (`server/src/core/agent/executor.py:595`). Remove the argument and update that call.
- `MaintenanceTools._merge_candidate_rank_key()` and `_format_merge_candidate()` format the superseded resolver candidate shape and have no caller (`server/src/core/agent/tools/maintenance.py:48`).
- `propose_entity_merge(..., confidence=None)` and its tool-schema field are unused. Remove them unless confidence will be validated and persisted deliberately (`server/src/core/agent/tools/maintenance.py:110`).
- `report_relationship_conflict()` returns both `review_id` and the explicitly legacy `conflict_id` alias (`server/src/core/agent/tools/maintenance.py:179`). Because Knoggin is unreleased, remove the alias after updating the current UI/test contract.
- UUID-result mappings for episode evidence under `check_graph_health` no longer match that tool's returned global-candidate shape (`server/src/core/agent/tool_references.py:26`). Prune mappings that cannot occur.

### 8. Remove dead retrieval parameters and compatibility fallbacks

In `KnowledgeRetrieval`:

- `search_entities(..., session_id)` never reads `session_id` (`server/src/core/knowledge/retrieval.py:120`).
- `_serialize_episodes(..., session_id)` never reads `session_id` (`server/src/core/knowledge/retrieval.py:593`).
- `_episode_retrieval_limit()` only returns `DEFAULT_EPISODE_RETRIEVAL_LIMIT` (`server/src/core/knowledge/retrieval.py:732`).
- `_serialize_episodes()` defensively uses `getattr()` for `get_project_episode_source_refs`, although the current `KnowledgeStore` contract supplies it (`server/src/core/knowledge/retrieval.py:603`).

**Recommended change:** remove the unused arguments, use the constant directly, and call the required store method directly. Update stale fakes to implement the current contract rather than preserving an unreleased fallback.

### 9. Consolidate repeated maintenance-service dependencies and validation

`ProjectMaintenanceService` repeatedly:

- constructs `DomainConfigStore(self.pg)`;
- checks `resources.knowledge_store` for `None`;
- validates `expected_domain_version`;
- performs slightly different project-existence checks.

**Recommended change:** construct one domain store in `__init__`, add `_require_knowledge_store()` and `_validate_expected_domain_version()`, and consistently use `_require_domain_project()`. Generalize its error text; it currently says every prohibited operation is a “domain configuration operation” (`server/src/core/project/maintenance_service.py:90`).

Keep the mutation workflows themselves explicit. Their lock, runtime-exclusion, projection, and embedding order is behaviorally important.

### 10. Centralize repeated notebook mappings and evidence admission

`RunNotebook` repeats the same section-to-capacity mapping in `_fits_capacity()`, `capacity_report()`, and `_bounded_retained_references()` (`server/src/core/agent/notebook.py:304`, `:324`, `:1175`). `_add_relationship()`, `_add_episode()`, and `_add_path()` also repeat evidence-message admission and reference deduplication (`:575`, `:599`, `:638`).

**Recommended change:** define one explicit capacity mapping and one narrow evidence-admission helper. Keep the mappings explicit rather than deriving field names through reflection; explicit names make capacity policy reviewable.

## Priority 2: Larger cleanup that needs a dedicated change

### 11. Turn `schema.sql` into canonical fresh-schema DDL

`server/src/infrastructure/schema.sql` is 2,330 lines and contains 229 combined migration markers such as `ALTER TABLE`, `ADD COLUMN IF NOT EXISTS`, `DROP COLUMN IF EXISTS`, `NOT VALID`, and migration comments. It mixes fresh table definitions with historical upgrades, legacy data cleanup, and repeated column additions.

This conflicts with the agreed unreleased clean-reset model, but it should not be edited blindly: the real-PostgreSQL fixture currently executes the entire schema before every reset (`server/tests/contract/storage/conftest.py:32`). That fixture has made schema reapplication a de facto test requirement.

**Recommended change:**

1. Produce one canonical fresh-schema DDL with final columns and constraints.
2. Move any temporary developer cleanup into a separate, disposable migration/reset script.
3. Make storage tests create a fresh database/schema once per test session, then truncate/reset canonical tables without replaying historical migration logic.
4. Replace static substring assertions such as required `ADD COLUMN IF NOT EXISTS` text with actual catalog/constraint assertions.

This is likely the largest overall line-count reduction, but it requires a live PostgreSQL/AGE validation pass.

### 12. Prune `KnowledgeStore`; do not dissolve the boundary

`KnowledgeStore` is 1,405 lines and contains many forwarding methods. Some are dead, including `write_message_source_refs()` (`server/src/core/knowledge/store.py:400`), while others are used only by the abandoned resolver chain.

The facade still has value: core services should not know the SQL/AGE reader/writer layout. The concise target is therefore a smaller facade, not direct infrastructure access throughout the core.

**Recommended change:** after removing the dead resolver stack, perform a method-by-method caller audit and delete only unreferenced facade methods. If the facade remains unwieldy, split it by lifecycle capability behind stable core-facing protocols rather than exposing every reader and writer.

### 13. Simplify `EntityMaintenanceService` construction and ownership

The constructor accepts either `postgres`, `knowledge_store`, and `user_name` or a `resources` object (`server/src/core/knowledge/entity/maintenance_service.py:33`). `self.knowledge_store` is never used. `ProjectManager` owns a long-lived service, while `MaintenanceTools` creates new service instances per call.

**Recommended change:** choose one explicit construction contract and inject the application-owned service into tools where practical. Remove the unused knowledge-store dependency and the redundant re-export module `core/knowledge/entity/maintenance.py` if no external import remains.

### 14. Improve evidence hydration without obscuring retrieval policy

`get_connections()`, `get_recent_activity()`, and path retrieval repeat the same “pop evidence refs, hydrate, attach evidence” sequence. `_hydrate_evidence()` also performs a nested item scan for every returned message (`server/src/core/knowledge/retrieval.py:468`).

**Recommended change:** add one narrow result-evidence hydration helper and index requested references by `(user_name, session_id, message_id)`. Current result caps make this an elegance/clarity improvement rather than an urgent performance fix.

Keep exact, semantic, lexical, and fallback episode retrieval branches explicit; they have different telemetry and result semantics.

## Logic that should not be condensed yet

### Prompt freshness across multi-tool batches

`_format_evidence()` combines `last_applied_references` with `references_for_result()` previews of every recent tool result (`server/src/core/agent/prompt_context.py:159`). Although the preview deepcopy is expensive, `last_applied_references` contains only the most recent individual apply, so removing the preview would lose freshness markers for earlier results in a parallel batch.

Simplify this only after `AgentRun` or `RunNotebook` records the union of references admitted by the whole tool batch.

### Notebook application branches

Do not turn `RunNotebook._apply_unchecked()` into a generic table-driven mapper solely to reduce lines. Tool result shapes and normalization rules differ materially. The atomic deepcopy/rollover behavior in `apply()` should also remain easy to inspect.

### Global merge and rollback writer

The merge writer is long because it records reversible per-object mutations and rechecks current values before inversion. This is justified complexity. Reduce only obvious repeated query/serialization helpers after real PostgreSQL tests cover merge, partial rollback, concurrent changes, and projection repair.

### Background work cancellation and health snapshots

`snapshot()` and `snapshot_for_health()` intentionally serve different trust boundaries (`server/src/infrastructure/background_work.py:240`). Cancellation and shutdown loops look similar but maintain different owner/global semantics and counters. They are not good abstraction targets unless shared behavior is first captured by focused concurrency tests.

### Knowledge facade and maintenance transaction boundaries

Do not shorten the code by letting agent tools call database writers directly, by combining all plan kinds into one generic mutation engine, or by moving project-runtime ownership into storage. Those changes would reduce lines while weakening the architecture.

## Suggested implementation order

1. Repair maintenance-review application dispatch and add confirmation/rollback application entry points.
2. Add merge/rollback projection rebuild and active-runtime cache invalidation.
3. Remove the resolver-owned merge stack and then prune its storage dependencies.
4. Remove `AgentRun`/`RunNotebook` compatibility surfaces and small dead agent logic.
5. Simplify retrieval and project-maintenance helpers.
6. Consolidate notebook capacity/evidence helpers.
7. Refactor `schema.sql` and its real-PostgreSQL reset strategy as a dedicated, database-validated batch.

## Validation expectations

Each implementation batch should run focused unit/contract tests, touched-path Ruff/compile checks, and `git diff --check`. Merge, rollback, schema reset, constraints, and projection repair require real PostgreSQL/AGE coverage; unit fakes cannot establish those behaviors. Active-runtime tests should explicitly prove that a retired entity stops resolving after merge and returns correctly after a completed rollback.
