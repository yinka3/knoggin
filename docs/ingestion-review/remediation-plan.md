# Ingestion remediation plan

> Current verification (2026-10-19): see [verification status](verification-status.md) for the passing PostgreSQL runs, remaining coverage limits, and planned full Docling ingestion on Linux. Older database-unavailable statements below describe earlier runs.

Prepared 2026-10-08 from the [code scan](README.md). This is an implementation order and a set of acceptance checks, not a claim that the defects are already reproduced in Postgres.

## Progress

- Implemented the no-op Context version bridge and made the Context writer's equal-hash shortcut compare `domain_version` too. Added an activation guard that rejects removing sections with live blocks and waits for all active windows to finish before section removal. Explicit retry now reopens a legacy Context checkpoint when its revision uses the wrong domain version. Added a projection regression proving a section title change repairs after the bridge revision advances. The focused domain, projection, Context/Knowledge and build-validation suites passed **69 tests**; Ruff passed.
- Postgres contract regressions now cover the version bridge, both section-removal guards, and legacy checkpoint retry repair. They collect successfully but have not run because the local Postgres fixture is unavailable. A complete Context→Knowledge→next-window transition remains unverified against Postgres.
- Implemented relationship evidence identity across extraction and storage: one durable observation now represents one observed label plus one independently sufficient Context support set. Exact duplicate sets remain idempotent; separate supports and labels persist separately. Context retirement retires only the affected observation, reclassification ignores retired rows and refuses stale plans, and existing reviews citing retired evidence become stale in the same transaction. Global merge collision handling includes the support key. The focused unit suites pass **105 tests**; Ruff passes on all changed Python files; the updated storage contract collects. The Postgres contract could not be verified locally because its setup did not complete.
- Removed the unevaluated NLI capability from extraction, the embedding service, runtime settings, model prefetch, and the cached-model CI lane. A name found only in a supporting message no longer creates a Context entity association. Structured fallback verifies its local message reference and exact quote, then records an unresolved diagnostic; JEV does not submit source-only candidates. The diagnostic is in memory for the current build, not a durable review item.
- Fixed two merge review defects found while tracing the existing entity merge tool. Candidate SQL now orders by supporting-message count before applying its limit, with stable ID ties. `propose()` rejects unsupported evidence kinds and evidence that does not belong to either candidate; it also refuses unresolved project Context conflicts and stale preview hashes. Unit tests pass and a Postgres ranking contract collects, but it has not run without a local database.
- After explicit user approval for bounded stored-evidence text, the new `advise_entity_merge` tool calls configured `merge_model` with candidate profiles and selected message/episode excerpts. It validates cited IDs and returns a nonbinding recommendation plus the candidate state hash. `propose_entity_merge` can require that hash before opening a review; confirmed merge application remains separate. Scheduled preflight stays deterministic.
- Latest combined focused remediation run: **189 passed** across extraction, relationship persistence/reclassification, merge maintenance, domain activation, projection, Context/Knowledge stages, embedding, resource setup, and runtime configuration. Ruff passes on the NLI removal paths. The cached-model smoke test collects, but its real artifacts were not run locally. The 51 related Postgres contracts collect, but none of their durable assertions has been verified locally because Postgres setup is unavailable.
- Follow-up audit fixed the smaller JEV candidate-preparation work bound, clean projection repair after a domain title change, and missing-Docling-artifact retry classification. The focused follow-up run passed **181 tests** and Ruff; Postgres and real model behavior remain unverified. See [completion audit](extraction-knowledge-completion-audit.md).

## Rules to preserve

- Every Knowledge commit uses a Context revision with the window's frozen domain version. An older active window may finish under its own frozen version.
- A relationship remains supported while at least one independently sufficient evidence set is current. Distinct observed labels remain distinct observations.
- A model may recommend an entity merge; the existing typed plan, evidence validation, state checks, and explicit review still control the write.
- A name absent from the Context block is not linked automatically, even when a supporting message names it. A valid local message reference and exact quote can be recorded as an unresolved diagnostic.
- An unrecognized edit to `CONTEXT.md` is never overwritten by a domain transition.

## 0. Make the failures executable

Restore a local or CI Postgres fixture for `test_project_semantic_postgres_flow.py`; its seven cases failed in setup during this scan. Add narrow regression cases before changing behavior:

1. Existing Context at domain v1, activate v2, Context updater returns no operations: Knowledge completes and the next window can be claimed. The unit path is covered; run the full transition against Postgres.
2. Change a section title/order with a clean old projection; then try a separate user-edited file. The bridge-to-projection repair path now has a unit regression; retain the user-edit protection test and verify both through Postgres.
3. Attempt to remove a section that still owns blocks; activation now returns a clear conflict. Also verify activation waits for any active window and allows removal after the blocks/window are gone.
4. Same edge and observed label with independently sufficient `[b1]` and `[b2]`; remove `b1` and verify the edge and `b2` evidence survive. Repeat with two observed labels mapping to one canonical type.

Assert durable revisions, observations, graph answers, retry state, and the next window's progress. A mock call count alone is not enough. Existing mismatch validation already correctly rejects inconsistent inputs (`server/tests/unit/core/ingestion/test_semantic_build_validation.py:35-62`); the missing check is the real transition that creates one.

## 1. Fix domain transitions first (highest priority)

Define one transition protocol shared by activation, Context projection, and window admission (`server/src/core/project/domain_config_store.py:119-153`; `server/src/core/ingestion/project_semantic_processor.py:527-547`; `server/src/core/knowledge/context/projection.py:110-246`). **Implemented:** before a new-version window reaches Knowledge, create an immutable Context revision at that version even when its blocks did not change. Give that bridge revision **zero extraction impact**. Older active windows keep their frozen rules. Domain activation and Context revision commits take locks in project-then-Context order; activation rejects section removal when live blocks remain or any semantic window is active.

The writer's second guard is also **implemented**: its no-op shortcut compares both `domain_version` and content hash (`server/src/core/knowledge/db/writers/project_context_writer.py:196-205`); otherwise version-only bridges disappear in storage.

Treat section removal with live blocks as incompatible until those blocks have an explicit move/delete migration. For a title/order change, regenerate a clean projection when its bytes still match the recorded old hash. Keep the current conflict behavior for unknown file edits. **Implemented:** on explicit retry, reset a legacy mismatched Context checkpoint to the claimed stage so the frozen Context updater can write a valid bridge. A matching checkpoint keeps its stage and revision (`server/src/core/knowledge/db/writers/semantic_window_writer.py`). The Postgres regression is collected but awaits database availability.

Done when no-op, changed, human-edited, and retry/restart cases complete under the right domain version; old in-flight windows still finish; and a blocked window can be repaired without losing Context edits.

## 2. Preserve relationship evidence end to end (highest priority)

Replace the extractor's `(endpoints, canonical type)` winner-takes-one deduplication (`server/src/core/ingestion/relationship_extractor.py:309-320`). **Implemented:** the extractor keeps distinct observed labels and distinct **sufficient support sets**. Exact duplicate sets can still collapse.

The current SQL unique key also collapsed same-label supports within a window (`server/src/infrastructure/schema.sql:1234-1235`; `server/src/core/knowledge/db/writers/semantic_commit_writer.py:1469-1517`). **Implemented:** each observation row is one label plus one sufficient support set, identified by a stable hash of its sorted block IDs. The fresh schema contains the final support-set identity directly. The writer upserts exact sets idempotently. If any required block leaves Context, that observation retires; a sibling observation keeps the graph edge alive. Existing evidence readers already address observations separately; reclassification now filters retired rows, merge collision checks preserve distinct support keys, and open reviews citing retired evidence are invalidated.

The storage contract now asserts both supports, two labels, exact replay deduplication, single-support retirement with the aggregate edge retained, stale-review invalidation, and retired-plan rejection. It collects, but needs a working Postgres fixture before those storage assertions count as verified. Done when that contract runs successfully and losing all surviving support removes the edge.

## 3. Connect entity merge reasoning to `merge_model` (implemented for explicit tool use)

The deterministic shortlist is fixed (`server/src/core/knowledge/entity/maintenance_service.py`): it ranks by supporting-message count before applying the limit, then uses stable entity IDs. `EntityMaintenanceService.propose()` now accepts only candidate-owned message/episode evidence, rejects unresolved project Context conflicts, and compares an optional expected state hash before opening a review. The confirmed merge still uses the existing state-checked writer.

The user approved the stored-evidence data flow. The application-owned advisor now calls structured LLM with **`model=llm.merge_model`** for one explicitly selected active pair. It sends at most two messages and two episode summaries per entity, each capped at 800 characters, plus bounded candidate profiles. Its typed output is checked against the supplied evidence IDs and candidate pair. Positive advice must cite both entities. It opens no review and applies no merge (`server/src/core/knowledge/entity/merge_advisor.py`).

The new `advise_entity_merge` agent tool uses this advisor on request. `propose_entity_merge` can pass the returned state hash to reject a stale review proposal. Decide separately whether scheduled deterministic preflight should ever trigger the model; it currently does not (`server/src/core/knowledge/jobs/application_maintenance_scheduler.py`).

Focused fake-model tests assert the **actual** `merge_model` argument at the LLM boundary, skip the call when the selected pair has no cited evidence, reject invented citations and unsupported positive advice, and verify model failure creates no review. They also verify that tool forwarding does not itself mutate Knowledge. The real hosted model and Postgres evidence queries remain unrun locally (`server/tests/unit/knowledge/test_entity_merge_advisor.py`).

## 4. Resolve rules and bounded-work gaps

- **Entity evidence rule (conservative interim behavior):** a name absent from the Context block is never auto-linked. The structured fallback can return a local supporting-message reference and exact quote, which are checked against the source and placed in an unresolved issue for this build. JEV only evaluates block-literal names. Positive paraphrase and unrelated-name tests both assert no entity association (`server/src/core/ingestion/text_processor.py`; `server/src/common/templates/prompts/extraction.md`).
- **Unresolved evidence is not durable:** `SemanticWindowBuild.issues` does not persist as a review queue. Add a typed, durable proposal path only if real examples show that revisiting these source-only names is valuable. Until then, the original messages and Context remain stored, but the model's candidate and quote are not available as a later review item.
- **Relationship grounding:** build an adversarial fixture with valid handles/citations but prose that does not entail the edge. Measure how often VP-02 accepts it before deciding on a second verifier or a review flag; current checks verify membership, not meaning (`server/src/core/ingestion/relationship_extractor.py:241-299`).
- **JEV preparation:** candidate enumeration now respects remaining observation capacity, stops after 512 literal checks, and skips profile fetches for aliases absent from eligible blocks. Benchmark alias count × gap-block count under a large real catalog to check latency; the deterministic regression covers the bound (`server/src/core/ingestion/text_processor.py`).

## Decisions for the design discussion

1. For an incompatible section removal, should activation reject it until Context is migrated, or should activation accept an explicit block-mapping plan? I recommend rejecting it until an explicit migration exists.
2. Should `merge_model` run only when the entity merge tool is invoked, or also after scheduled duplicate preflight? Start with the explicit tool; add scheduling only after cost and review behavior are measured.
3. Should weakly grounded VP-02 edges be rejected or held for review? Decide from the adversarial evaluation, not from a passing schema test.
