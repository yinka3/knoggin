# Correction, maintenance, and forgetting review — 2026-09-09

Reviewed current checkout `aadedewe/refactor`, HEAD `acc0cac33b74759d07880a8b6fffeeee0cdee282`. Production scope is `server/`; no production edits. Used the code review guide, locked Knowledge/Project decisions, and Project Forget implementation plan as intent. This report supplements the core and provenance/Agent reviews without recounting their defects as new findings.

## Verdict

**The deterministic correction machinery is more complete than the automatic maintenance workflow around it.** Context supersession, typed relationship reinterpretation, explicit identity merge, and guarded rollback have meaningful implementations. The surrounding discovery, review identity, evidence snapshots, and interrupted projection recovery contain concrete gaps.

The largest product concern is the distinction between closing a review and correcting memory. The code deliberately treats conflict classification as a judgment, relationship reinterpretation as a graph attachment change, and Context reconciliation as a change to current understanding. Those are valid separate operations. A future interface must describe their actual effect, rather than presenting all three as “fix memory.”

## New findings

### MC1 — P1 / Finish: automatic conflict discovery skips the current ingestion output

The discovery reader labels an observation `context` whenever it has a `relationship_observation_blocks` row. The packet builder excludes all such seeds. If all seeds are Context-backed, the job advances its checkpoint without calling the model.

The current production observation insertion path is `SemanticCommitWriter`, which attaches support blocks to its Context relationship writes. No alternative production observation insertion path was found. Thus ordinary current ingestion does not seed automatic conflict discovery. Two independently grounded, simultaneously current Context assertions are excluded along with harmless supersession.

Evidence: a real Context revision and semantic commit produce an observation labeled `context`; the reproduction adds two further Context-backed observations, reads them through the actual discovery reader, and obtains an empty review packet advancing past all three. The existing unit test explicitly expects this exclusion: this is a mismatch between subsystem contracts, not an accidentally failing test.

Locations: [conflict_discovery_reader.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/readers/conflict_discovery_reader.py:162), [conflict_discovery.py](/home/yinka/dev/knoggin/server/src/core/knowledge/conflict_discovery.py:47), [conflict_discovery_job.py](/home/yinka/dev/knoggin/server/src/core/knowledge/jobs/conflict_discovery_job.py:68), [semantic_commit_writer.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/writers/semantic_commit_writer.py:555).

Keep deterministic retirement out of human review. Change discovery eligibility to distinguish surviving independent disagreement from already-resolved replacement; merely originating from Context is not that distinction. If automatic conflict discovery is intentionally being retired, remove or relabel its scheduling promise instead of maintaining a pass that normally advances without investigation.

### MC2 — P2 / Finish: new conflict reviews can immediately report changed evidence

Two creation paths disagree with detail reconstruction:

- Agent/user conflict reports go through `ConflictService` without gathering an evidence snapshot. `ConflictWriter` supplies an empty snapshot. Detail then loads real observation support, producing a different token even when nothing changed.
- Background completion gives every candidate a snapshot of the entire discovery packet, while its `evidence_refs` contain only that candidate's cited observations. Detail reconstructs only those references. If a packet contains three observations and a candidate cites two, its fresh snapshot does not match its own detail view.

Both cases were reproduced against real PostgreSQL, without intervening edits. This creates misleading stale-state presentation and blocks the generic preview path, which requires `evidence_state == 'current'`. It does not mean every dedicated conflict-classification operation is blocked; that path does not use the same preview gate.

Locations: [conflict_service.py](/home/yinka/dev/knoggin/server/src/core/knowledge/conflict_service.py:24), [conflict_writer.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/writers/conflict_writer.py:52), [maintenance_service.py](/home/yinka/dev/knoggin/server/src/core/project/maintenance_service.py:244), [maintenance_service.py](/home/yinka/dev/knoggin/server/src/core/project/maintenance_service.py:423).

Construct each review's snapshot from exactly its cited evidence, using the same ordering and traversal contract as detail/preview. Share this behavior across creation origins. Keep useful background neighborhood evidence in the investigation packet without silently treating all of it as the candidate's frozen support.

### MC3 — P2 / Finish: confidence changes bypass conflict deduplication and dismissal

`ConflictWriter` looks up an existing review by sorted observation IDs, but `MaintenanceReviewWriter.signature()` includes the whole plan, including model confidence and origin. The writer still calls `open()` even after finding an existing review. A changed confidence produces a new signature and a new open review for the same observations.

Real PostgreSQL reproduction: confidence 0.8 followed by 0.9 creates two distinct open review IDs. The returned second result nevertheless says `created=False` and `should_notify=False`. The dismissed variant creates a fresh open review after the original was dismissed, with no new evidence.

Locations: [conflict_writer.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/writers/conflict_writer.py:69), [conflict_writer.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/writers/conflict_writer.py:93), [maintenance_review_writer.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/writers/maintenance_review_writer.py:40).

Define conflict identity using the semantic subject/kind and actual evidence state, not incidental model output. Return/reuse the existing immutable review for equivalent evidence, including its dismissal. Open a successor only under a deliberate changed-evidence policy. Derive creation/notification results from the row actually persisted.

### MC4 — P2 / Finish: conflict resolution loses its structured category after saving

`ConflictWriter.resolve()` returns the requested `resolution_kind` and note but only transitions the review to `applied`. It does not persist `ConflictResolutionPlan.resolution` or `.note`. The event reason stores `resolution_note or resolution_kind`, so supplying a note loses the structured category entirely.

Reproduction: resolve as `normal_temporal_change` with “The move happened in June.” The immediate response contains the category; the stored plan retains null resolution/note, and the event retains only the sentence. A later detail read cannot recover the structured user judgment from the review. Classifying `not_a_conflict` versus `confirmed_conflict` should remain distinguishable without parsing prose.

Locations: [conflict_writer.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/writers/conflict_writer.py:139), [conflict_reader.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/readers/conflict_reader.py:14).

Persist category, note, actor, and closure atomically in an explicit resolution record or typed resolution fields, preserving the original proposal as needed. Do not turn classification into an implicit graph mutation. The current impact planner correctly says this operation has no direct canonical Knowledge mutation.

### MC5 — P1 / Finish: a committed merge can lose its projection-repair obligation on interruption

Entity merge commits its canonical mutations and executed audit first. Projection rebuilding runs afterward; only after it returns does the service write `projection_repair_pending` for collected errors. Cancellation or process death between those steps leaves the canonical merge committed with no pending marker. Health enumerates only marked audits.

Real PostgreSQL failure injection: cancel at entry to `_rebuild_projections()`. Entity 3 remains durably redirected, the audit is `executed` with `failure_reason=NULL`, and repair health reports zero pending operations. This tests the post-commit interruption boundary, not a full OS restart or the detailed contents of a stale AGE graph.

Locations: [maintenance_service.py](/home/yinka/dev/knoggin/server/src/core/knowledge/entity/maintenance_service.py:379), [global_entity_merge_writer.py](/home/yinka/dev/knoggin/server/src/core/knowledge/db/writers/global_entity_merge_writer.py:780), [maintenance_service.py](/home/yinka/dev/knoggin/server/src/core/knowledge/entity/maintenance_service.py:562).

Record the repair obligation in the canonical transaction; clear it only after successful derived rebuilds. Apply the same rule to rollback. A `finally` block alone cannot protect process death. Keep the existing explicit projection-only retry operation: it correctly reloads the audit and avoids replaying the merge. No general workflow engine is needed.

## What the correction flows currently do

| User/system action | Actual effect | Consequence for future answers |
| --- | --- | --- |
| New conversation corrects prior understanding | Context updater proposes immutable block replacement/deletion; semantic commit retires observations that lost current support and reconciles derived links/aggregates. | Can change current Context and graph. Whether the language update captures the intended correction remains model-dependent. |
| User edits controlled Context | Controlled import/versioning feeds the semantic pipeline. | This is the direct current-understanding correction path; it should not create a generic conflict review merely for replacing a block. |
| Classify a conflict as normal change/not a conflict | Closes a review; does not edit Context, observations, or Episode narrative. | Removes workflow ambiguity only. MC4 currently weakens durable retention of the judgment. |
| Apply relationship reinterpretation | Checks expected relationship attachment and domain compatibility, reattaches/detaches observations, reconciles Episode relationship links, rebuilds projection, and marks the review applied in the transaction. | Changes graph interpretation. It does not rewrite natural-language Context or historical Episode prose. |
| Merge identities | Validates inspected state/frontiers and explicit project type/topic choices, performs a journaled global merge, then rebuilds projections and invalidates affected live caches through ProjectManager. | Changes identity resolution and structural links. Earlier core-review gaps in Context entity associations still matter. |
| Roll back merge | Rechecks mutation state, applies safe inverse mutations, and creates a review for conflicting residue. | Preserves later edits rather than blindly restoring a full snapshot. Repair obligations need MC5 hardening. |

Keep these distinctions visible. A graph-only reinterpretation can coexist with unchanged Context prose describing the old interpretation. If a user intends to correct the underlying claim, direct them to Context correction or provide a clearly scoped follow-up action. Historical Episodes should not be silently rewritten simply because current state changed; current/historical labeling and source inspection are the appropriate bridge.

The prior reports already cover retired observations leaking into some retrieval aggregates, missing Context entity associations in merge/cleanup, message-only maintenance frontiers, and Agent evidence rendering. Those remain relevant to end-to-end correction but are not new findings here.

## Forget plan assessment

**Forget is not implemented in current server code.** The current `delete_project()` hard-deletes the relational project aggregate and clears its graph projection; it retains shared identities and removes entities with no surviving project context. It does not implement the plan's terminal-but-retained Delete semantics, persistent Forget barrier, reconciliation workflow, or whole-root filesystem purge. The plan explicitly identifies much of this as prerequisite work; it would be misleading to report those missing planned features as newly discovered regressions.

The plan's central choices fit Knoggin: whole-project source removal, impact capture before purge, surviving support rather than circular derived support, a small irreversible tombstone, resumable cleanup, and reuse of the existing review envelope. New independent evidence may legitimately establish the same information later.

Before implementation, resolve these concrete gaps:

1. **Copies owned by surviving projects.** Answer-level source references can belong to project B while retaining a document excerpt from project A through `source_project_id`. Existing deletion intentionally preserves those references and marks the source unavailable. That is useful deletion history, but it is not removal of A's semantic content. Decide how Forget scrubs/invalidates these copied references and follows their support into B's Context. “Other projects' Episodes remain untouched” needs a precise independent-evidence exception; copying A's excerpt into B is not independent support. Existing source-reference storage tests demonstrate this retention behavior.
2. **Global merge journals and reviews.** Before/after mutation JSON can contain aliases, classifications, and observations originating in the forgotten project. Project-root cascades do not by themselves define cleanup for user-global journal data. Remove/redact forgotten semantic payloads and make corresponding inverse operations ineligible to restore it. Reusing rollback machinery is sensible only after this constraint is enforced.
3. **Alias and canonical-name support.** `entity_aliases` currently records entity ID and alias, not alias-specific provenance. The plan correctly calls incomplete alias provenance a prerequisite. A generic surviving entity reference cannot prove that each of its aliases or its canonical spelling survives. Establish the minimum support contract before promising deterministic reconciliation; use an explicit unresolved/rebuild policy where support is unknowable.
4. **Quarantine timing.** The plan promises no normal retrieval of unvalidated affected identity state after the Forget barrier, but its listed phases compute impact and quarantine afterward. Either capture the needed structural set with the barrier, or block the relevant user-global reads/writes until that set is established. Otherwise an interval exists in which a terminal project still contributes globally visible identity state. This is a plan sequencing issue, not a current Forget defect.
5. **Recovery obligations must be durable before cleanup starts.** MC5 is a concrete example of why marking pending work after an attempted repair is insufficient. Apply the plan's barrier/checkpoint approach to database purge, filesystem cleanup, and identity reconciliation. Test cancellation before and after each durable boundary, including stale writers trying to commit.

Supporting code: [current deletion](/home/yinka/dev/knoggin/server/src/core/project/project_manager.py:745), [aggregate purge](/home/yinka/dev/knoggin/server/src/core/knowledge/db/writers/project_deletion_writer.py:27), [retained source status](/home/yinka/dev/knoggin/server/src/core/knowledge/db/readers/source_reference_reader.py:338), [merge journal schema](/home/yinka/dev/knoggin/server/src/infrastructure/schema.sql:296), [alias schema](/home/yinka/dev/knoggin/server/src/infrastructure/schema.sql:275).

## Scoped improvements

- **Make review burden evidence-driven.** Finish stable deduplication/dismissal before configurable automation. Otherwise “manual review” means repeatedly handling the same case, and automation may repeatedly classify it. No confidence threshold can repair incorrect review identity.
- **Retire obsolete reviews deliberately.** Context reconciliation already retires observations. Decide which open reviews should become stale or resolved-by-supersession as a deterministic consequence, rather than leaving the user to discover that on detail reads. Keep the audit separate from an unresolved disagreement.
- **Use one review evidence contract.** Candidate references, frozen snapshots, live previews, and apply-time validation should describe the same evidence subset. MC2 is caused by incompatible uses of otherwise useful shared types.
- **Keep the explicit mutation boundaries.** Existing apply/merge/rollback tests protect real behavior. Avoid combining proposal generation, user disposition, and canonical mutation into a general “maintenance action” abstraction.
- **Match impact previews to actual changes.** The interpretation impact planner currently labels Episode effects `episode_entity_link` and supplies observation IDs, while its writer changes `episode_relationships`. Prefer an honest typed effect description or derive exact affected IDs when needed; do not present estimated identifiers as exact affected rows.

Suggested order: fix review snapshot/deduplication/resolution persistence → make projection repair obligations durable → restore meaningful current-evidence discovery → finish the previously reported correction/retrieval gaps → implement Forget as its separately planned workflow. SQLite migration does not resolve the first three workflow problems; carry their invariant tests across the backend change.

## Validation and limits

- **46 existing tests passed:** 37 focused unit contracts for conflict discovery/writing, maintenance reviews/application/impact, global maintenance, and scheduling; nine existing real PostgreSQL maintenance, semantic-commit, and project-deletion contracts.
- **Seven review probe cases reproduce current defects**, including both duplicate-open and dismissed-conflict variants. They use the real local PostgreSQL fixture database. A passing probe asserts the undesired current behavior; no defect was fixed.
- Probe file: [maintenance_review_probes_2026_09_09.py](/home/yinka/dev/knoggin/server/reviews/maintenance_review_probes_2026_09_09.py). Tests create and remove a temporary fixture database; they do not use application records as test fixtures.
- No live-model benchmark, OS crash/restart test, or completed Forget execution was performed. Automatic-discovery eligibility and state transitions are deterministic source/DB findings; the frequency and quality of model-generated conflict judgments remain unmeasured.
- Ruff checks/formatting were run on the new probe file. Production code and existing tests were unchanged. Existing Requests/NVML warnings were environment warnings, not source-test failures.

Coverage concentrated on Context retirement and semantic commit, observation eligibility and discovery checkpoints, conflict/review identity and closure, evidence snapshot/detail/preview consistency, relationship apply transactions, merge/rollback projection recovery and cache callers, and the current deletion boundary versus the Forget plan. The report is not a claim of exhaustive verification of every maintenance interleaving.

## Stage 5 implementation status — 2026-09-13

MC1–MC5 are resolved; the original findings above remain as historical
rationale.

- **MC1:** current Context-backed observations reach the bounded discovery
  packet. Deterministically retired observations remain excluded.
- **MC2:** direct and background conflict creation capture the same ordered,
  cited evidence subset that detail reconstructs.
- **MC3:** conflict identity is stable across confidence, origin, and wording;
  unchanged evidence reuses its existing review and changed evidence creates a
  linked successor.
- **MC4:** a dedicated resolution record persists category, note, actor, and
  closure time while the original proposal stays immutable.
- **MC5:** merge and rollback record `projection_repair_pending` in the
  canonical transaction before derived rebuild work. A restarted service can
  repair from the audit without replaying the canonical mutation.

The serial Stage 5 real PostgreSQL/AGE gate passed **160 tests** with the
existing Requests dependency warning. The seven historical MC1–MC5 assertions
now fail at their former defect conditions and are retained as skipped
historical probes; desired-behavior contracts are the acceptance evidence.
The gate uses deterministic cancellation and a new service over the same
durable database state, not a live model or operating-system crash/restart.

The configurable maintenance policy is intentionally narrower than automatic
model resolution: manual disables scheduled discovery, assisted opens stable
reviews, and trusted permits only an exact allowlisted conflict classification
with an auditable policy actor. It cannot authorize a merge, Context change,
Project cleanup, or another canonical mutation.
