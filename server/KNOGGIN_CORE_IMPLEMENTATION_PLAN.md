# Knoggin Core Implementation Plan

Status: Phases 1–2 complete. Phases 3–6 remain pending.

Based on [KNOGGIN_CORE_REVIEW.md](KNOGGIN_CORE_REVIEW.md) and the subsequent review of the current checkout. Initial code assessment: `ab817f4e8ba570c9e65b636f6604659b98d55664`. Recheck affected paths when starting each phase; the older revision named in the review is not the implementation baseline.

## Purpose and boundaries

Finish the current engine's contracts, remove unused paths, and improve document provenance without redesigning the core. Knoggin is an unreleased, single-user, single-machine application.

Preserve:

- AgentOrchestrator, AgentRun, AgentExecutor, ToolRuntime, and RunNotebook ownership.
- PostgreSQL as canonical state; Context files and graph/cache projections remain repairable derived state.
- Whole-exchange semantic admission, per-session FIFO, frozen membership and policy, durable checkpoints, Context revision CAS, and atomic Knowledge writes.
- Resolver publication after Knowledge commit, immutable document versions, and answer-level source references saved with assistant messages.
- Bounded background work, admission control, and orderly shutdown.

Do not add distributed coordination, a generic workflow framework, another event bus, claim-level citations, or mandatory local vision. Do not preserve compatibility solely for unreleased development snapshots.

The file lists implementation targets, not a requirement to edit every named file. Before each phase, trace current callers and refine the file-level change list. Related cleanup belongs with its phase; Phase 6 handles remaining residue.

## Phase overview

| Phase | Purpose | Exit result |
| --- | --- | --- |
| 1 | Conversation completion and request identity | One submission has consistent durable outcomes |
| 2 | Semantic processing and recovery | Ready evidence progresses reliably into memory |
| 3 | Retrieval, tools, and evidence retention | Retrieved evidence reaches synthesis consistently |
| 4 | Document parsing and provenance | Search, reads, and citations share one captured representation |
| 5 | Document recovery and public errors | Failures have bounded recovery and useful public meaning |
| 6 | Remaining cleanup and readiness verification | Obsolete paths are removed and connected flows are verified |

## Phase 1 — Conversation completion and request identity

### Intended behavior

Save final answers and clarification questions before exposing terminal completion. One idempotency key identifies one logical submission, rather than merely preventing duplicate message rows.

### Work

1. Generalize assistant finalization to accept an explicit outcome. Reuse one path to persist assistant content, sources, any artifact, and exchange closure atomically.
2. Route clarification events through that path. Return canonical message/source identifiers where appropriate and make the question available after reload.
3. Align final, clarification, failed, and cancelled outcomes across runtime, persistence, and semantic admission. Failed/cancelled exchanges must not become semantic evidence. Do not automatically replay partially emitted runs.
4. Update admission and storage validation together: current logic expects assistant evidence only for `assistant_final`. Persisting clarification alone is insufficient.
5. Move the successful-turn clock update after durable assistant finalization. Keep secondary clock-update failure from invalidating an already committed answer.
6. Load canonical accepted payload/state when an idempotency key already exists. Reject payload mismatches, prevent another run for an in-progress submission, and replay the canonical terminal result. Define which request options form the logical payload; do not compare text alone if other options change execution.
7. Handle restart/interruption explicitly: an existing submission without a durable terminal result must not silently start another potentially side-effecting run.

### Primary file targets

- `src/runtime/session_runtime.py`: terminal event handling, canonical duplicate acceptance, finalization ordering, and wake signaling.
- `src/core/agent/executor.py`, `orchestrator.py`, and `services/agent_manager.py`: separate execution success from durable turn completion and clarify clock ownership.
- `src/core/knowledge/store.py` and `db/writers/message_lifecycle_writer.py`: shared atomic finalization and duplicate-state lookup through persistence.
- `src/core/knowledge/db/readers/message_reader.py`: canonical persisted submission/result reads where needed.
- `src/core/ingestion/semantic_window_admission.py` and `src/core/knowledge/db/writers/semantic_window_writer.py`: outcome/evidence consistency.
- `src/common/schema/public.py`, relevant message contracts, API callers, and `../docker/postgres/init.sql`: update exposed and stored contracts only as required.

### Resolved decisions

- `user_only` remains deliberate user evidence. `failed` and `cancelled` are terminal but never semantic evidence.
- `last_turn_at` means a durable final assistant answer for the resolved agent. A clarification does not count, and a secondary clock-write failure does not invalidate the committed answer.
- A clarification enters the frozen window with its paired user message. Episode and Context processing label it as an unresolved question; Context cannot use it alone to establish a fact.
- A request key covers normalized query text, timezone, model, selected agent, normalized tools, document focus, pasted spans, and research mode. A duplicate with the same payload replays the canonical terminal result; a mismatch conflicts; a local in-progress request is rejected; an open durable request is surfaced as interrupted without rerunning it.
- Partial stream events are not replayed or made semantic evidence.

### Validation and exit gate

- Verify final and clarification output survive reload with the correct outcome and source/artifact associations.
- Inject assistant persistence failure: no false successful-turn update, no half-committed sources/artifact, and no admission of failed evidence.
- Exercise cancellation before completion and interrupted finalization; inspect durable state, not only emitted events.
- Repeat identical requests before and after completion, including concurrent attempts: no second model run or duplicate evidence. Different payload with the same key produces a conflict.
- Use runtime contract tests plus real PostgreSQL transaction/admission tests. Update stale outcome tests only after the intended contract is explicit.

### Phase 1 closeout — 2026-09-19

- Final answers and clarification questions now share atomic assistant finalization, including source/artifact persistence and exchange closure. Both reload with their canonical message and source identifiers.
- The runtime records the successful-turn clock only after the final answer commits, using the agent identity resolved for that run rather than a later session default.
- Logical idempotency now has durable payload fingerprints, conflict/in-progress/interrupted errors, and canonical terminal-result replay.
- Clarification outcomes are durable semantic evidence without being allowed to become Context facts merely because they were asked.
- Validation: 162 focused runtime, API, agent, semantic, Context, and PostgreSQL storage tests passed. The full server lanes also passed: 1,242 service-free tests and 143 PostgreSQL tests. Full Ruff, the architecture import check plus 3 architecture tests, the existing six-file mypy baseline, and `git diff --check` passed.

## Phase 2 — Semantic processing and recovery

### Intended behavior

Once a window is ready, advance Episode → Context → Knowledge → completion without a scheduler pause between successful stages. Resume from durable checkpoints after interruption.

### Work

1. Turn the semantic owner into a draining processor. Reload durable state after each successful stage and continue only while eligible work advances.
2. Wake it after exchange closure while retaining periodic recovery for missed wakes, due retries, Context projection repair, and human edits.
3. Preserve existing admission thresholds, idle/explicit flush behavior, whole-exchange overfill, FIFO, frozen policy, and claim revalidation. A wake is not an instruction to close a new window for every message.
4. Make production collaborators required. Remove optional-dependency branches that permit a processor to silently skip required stages.
5. Reset the single attempt counter when each durable stage succeeds, including the Episode checkpoint even though it is currently represented within `claimed`. Clear retry metadata atomically with the checkpoint.
6. Stop draining on a failed stage, a future retry deadline, exhausted attempts, shutdown, or no durable progress. Preserve bounded resource use and prevent overlapping project processing.
7. Recover resolver publication and Episode enrichment from committed Knowledge without rerunning the canonical mutation.
8. Retain DB-first Context persistence, revision CAS, safe human-edit handling, and independent filesystem repair.
9. Reuse scheduler wake/admission/shutdown mechanisms where they fit. Rename or relocate the semantic owner only where that clarifies ownership; leave truly periodic jobs on the scheduler.

### Primary file targets

- `src/core/ingestion/project_semantic_job.py`: stage loop, required dependencies, due checks, and recovery ownership; possible rename to a processor module.
- `src/core/ingestion/semantic_window_admission.py`: preserve admission behavior and integrate ready-work checks.
- `src/infrastructure/job/scheduler.py`, project runtime construction/lifecycle callers, and `src/runtime/session_runtime.py`: wake wiring, capacity, overlap protection, and shutdown.
- `src/core/knowledge/db/writers/episode_writer.py`, `semantic_window_writer.py`, and `semantic_commit_writer.py`: atomic checkpoint/retry resets.
- `src/core/knowledge/context/projection.py`: preserve repair and human-edit behavior.
- `src/common/schema/settings.py` and semantic-window contracts: change only controls/contracts required by the final design.

### Validation and exit gate

- A ready window completes all stages without artificial scheduler waits; a below-threshold open window stays open.
- Interrupt after each committed checkpoint and resume without duplicate Episodes, Context revisions, or Knowledge writes.
- Fail Episode twice, succeed, then fail Context: Context receives a fresh retry budget.
- Confirm future retries are respected, exhausted retries stop, and no-progress state cannot spin in a tight loop.
- Exercise missed wakes, capacity rejection, repeated wakes, shutdown, and restart recovery.
- Fail resolver publication and Context file writes after DB commit; verify recovery preserves canonical state and human edits.
- Inspect real persisted state with PostgreSQL tests, alongside focused processor/lifecycle tests.

### Phase 2 closeout — 2026-09-19

- `ProjectSemanticJob` now drains one active window through Episode, Context, Knowledge, resolver publication, Episode enrichment, and completion in one bounded run. It reloads the durable window after every successful checkpoint and stops when a stage fails, a retry is not due, an attempt budget is exhausted, the active window changes, or no durable state advances.
- Exchange closure now sends a targeted in-memory scheduler wake to the semantic job. It still uses normal admission to decide whether a new window is ready, so below-threshold windows remain open. A wake received during an active run is retained for one follow-up check; normal polling remains restart and missed-wake recovery.
- Context update, projection, entity build, relationship extraction, and resolver publication are required collaborators. Context filesystem repair remains independent of canonical Context and Knowledge checkpoints.
- Episode, Context/finalization, and Knowledge checkpoint writes atomically reset the retry counter and clear retry metadata. Resolver publication and Episode enrichment continue to resume from committed Knowledge without replaying the canonical mutation.
- Validation passed: 55 focused semantic, scheduler, and project-runtime tests; 7 real PostgreSQL semantic-flow tests; and 3 real PostgreSQL checkpoint-storage contracts. Full `ruff check src tests`, the architecture import check, and `git diff --check` passed.
- The broader suite lanes were not treated as passing: the service-free run waited in the unrelated model-stack smoke and project-workspace lifecycle tests, while the broader PostgreSQL storage lane later failed during host `psycopg` setup despite the Docker PostgreSQL service accepting direct reads. These did not block the focused real-storage validation above.

## Phase 3 — Retrieval, tool behavior, and evidence retention

### Intended behavior

Canonical reads belong behind existing persistence boundaries. Tool results retained for synthesis match their actual data shapes, and active tools determine their guidance.

### Work

1. Move visible/open-session SQL out of KnowledgeRetrieval into persistence, preferably MessageReader.
2. Move canonical message reads from GraphReader to MessageReader; update facade delegation and callers without adding another abstraction.
3. Expose Context through the project knowledge boundary instead of constructing ProjectContextReader inside agent tools.
4. Preserve legitimate Context absence while propagating canonical storage failures. Review catches at callers so failures do not become empty Context later.
5. Retain recent activity using an explicit notebook representation or existing evidence types that preserve entity, time, and evidence references. Do not pretend activity is a relationship.
6. Use `max_accumulated_graph` consistently for connection retrieval and relationship retention, wiring the limit through the existing boundaries.
7. Keep generic instructions in the base prompt and active-tool guidance in ToolRuntime. Remove duplicate tool policy without losing restrictions.
8. Remove hot-topic preload state, orchestration, briefing reason, and stale prompt text. Keep explicit topic-context retrieval and its notebook evidence.
9. Remove only the notebook's document-manifest tool-result branch; preserve the briefing's manifest use.
10. Give ToolExecutionError cause-sensitive retryability. Phase 5 carries this through the public API consistently.

### Primary file targets

- `src/core/knowledge/retrieval.py`, `store.py`, and `db/readers/{message_reader,graph_reader,project_context_reader}.py`: read ownership and failure semantics.
- `src/core/agent/tools/registry.py`, `tools/memory.py`, `tool_runtime.py`, and prompt construction/templates: Context access, limits, active-tool guidance, and error translation.
- `src/core/agent/notebook.py`, `notebook_renderer.py`, and evidence contracts: activity retention and capacity.
- `src/core/agent/run.py` and `orchestrator.py`: remove preload state and callers.
- `src/common/exceptions.py` and affected tool callers: cause-sensitive error contracts.

### Validation and exit gate

- Activity evidence survives subsequent tool steps and reaches synthesis with correct references.
- Configured graph limits govern both retrieval and retention; existing scope and provenance restrictions remain intact.
- Context absence returns an empty/absent result; a storage outage surfaces as failure.
- Enabled-tool guidance matches the actual tool set; removed preload paths have no remaining consumers.
- Exercise canonical message/session reads through persistence interfaces and relevant real DB contracts.

## Phase 4 — Document parsing and provenance

### Intended behavior

One captured document version has one stored structured parse representation used by indexing, reading, selections, and provenance.

### Work

1. Define the minimum parse snapshot needed to preserve text, structure, source regions, extraction method, and parser identity. Keep it within the existing document lifecycle.
2. Evaluate Docling on representative native PDFs, scanned PDFs, tables, and DOCX. Measure provenance fidelity, extraction quality, local resource cost, and failure behavior before selecting it as the primary parser.
3. Keep specialized code/Tree-sitter, exact text/Markdown lines, and CSV row parsing where appropriate. Do not replace them solely for uniformity.
4. Persist parser name, version, and configuration fingerprint with the snapshot. Define when explicit reindexing is required and how it affects existing source references.
5. Make chunking, embeddings, search, reads, and selections consume the same snapshot. Remove independent PDF/DOCX parsing from ordinary read paths once this is implemented.
6. Add a general layout-region locator for page, bounding box, optional text span/element type, and extraction method. Define coordinate units, origin, page numbering, and image equivalents unambiguously.
7. Preserve the distinction between native extraction, OCR extraction, and model interpretation. Source-grade OCR evidence needs a locator; model interpretation must not masquerade as extracted text.
8. Report a tombstoned captured version as historical rather than unavailable. Verify what remains retrievable after source replacement; a status label alone does not preserve content.
9. Keep old source references meaningful after reindexing. Decide whether immutable parse revisions or sufficient captured reference data are needed; do not silently reinterpret old locators using a new parser.

### Primary file targets

- `src/core/knowledge/documents/indexer.py`, `service.py`, `read_service.py`, and existing extraction/chunking modules: single parse/write/read flow.
- `src/core/knowledge/db/writers/document_writer.py`, `db/readers/document_reader.py`, and `../docker/postgres/init.sql`: stored snapshot and parser identity.
- `src/common/schema/document.py` and source locator/reference contracts: structured regions and extraction semantics.
- `src/core/agent/sources/document_selection.py`, `sources/tool_results.py`, and `src/core/knowledge/db/readers/source_reference_reader.py`: locator propagation and historical presentation.
- Dependency declarations and document fixtures: add parser dependencies only after the evaluation decision.

### Decisions before dependent work

- Which formats use Docling, based on fixture evidence?
- What is the snapshot/reindex identity contract, including old references?
- What captured content remains accessible after replacement or tombstoning?
- Is optional vision needed now? Leave general photo interpretation and backend expansion out unless a concrete requirement justifies them.

### Validation and exit gate

- Trace parse → chunk → search → read/selection → SourceReference for native PDF, scanned/image OCR, DOCX, code, and text/CSV cases.
- Compare expected text and source regions against fixture content independently of parser output.
- Verify ordinary reads do not reparse the file, and a replaced source cannot silently change an older version's evidence.
- Test parser fingerprint changes, explicit reindexing, old reference stability, and historical versus missing status.
- Document parser evaluation results and limitations; passing mocked parser tests is insufficient to choose a parser.

## Phase 5 — Document recovery and public error contracts

### Intended behavior

Interrupted document work recovers, temporary failures retry within limits, deterministic failures stop, and callers receive useful error codes and retryability.

### Work

1. Recover interrupted indexing into queued work after restart using durable state.
2. Classify failures at the owning boundary: transient provider/embedding/network failure, deterministic parse/content failure, or interrupted work.
3. Add bounded retry/backoff for transient failures, with durable attempts/deadlines where required for restart behavior. Exhausted work stays failed until an explicit retry.
4. Fail corrupt/unsupported content and parser validation errors without automatic repetition. Preserve source-change reconciliation as its own lifecycle case.
5. Keep index publication atomic and respect worker capacity; retries must not publish partial chunks or leave conflicting active claims.
6. Add explicit public handling for LLMBudgetExceededError and WorkspaceConflictError, including appropriate API status and stream error behavior.
7. Propagate Phase 3's tool error retryability to public responses without exposing sensitive internal exception details.
8. Keep retries at safe workflow boundaries. Do not add arbitrary transaction replay or automatic replay of partially emitted agent streams.

### Primary file targets

- `src/core/knowledge/documents/indexer.py`, `policy.py`, and recovery/admission callers: classification, retry scheduling, restart, and capacity.
- `src/core/knowledge/db/writers/document_writer.py`, `db/readers/document_reader.py`, document contracts, and `../docker/postgres/init.sql`: required durable retry state and transitions.
- `src/common/exceptions.py`, `src/common/schema/public.py`, `src/api/app.py`, and stream error translators: explicit codes, retryability, and consistent presentation.
- Document indexing and public API/stream contract tests: meaningful failure injection.

### Validation and exit gate

- Interrupt indexing and restart: work is recoverable without duplicate chunk publication.
- Fail an embedding/provider call transiently: retry honors backoff and succeeds or stops at the budget.
- Supply corrupt/unsupported content: fail immediately with a useful error; no automatic retry loop.
- Verify budget exhaustion, workspace conflict, stale selection, invalid arguments, and temporary tool outage across API/stream paths.
- Inspect durable retry state and rollback behavior with real storage tests.

## Phase 6 — Remaining cleanup and readiness verification

### Intended behavior

Remove obsolete contracts and verify the connected engine flows before treating SDK/UI behavior as stable.

### Work

1. Repeat a repository-wide usage search for legacy session-scoped Episode APIs, including embedding/recent/source-message helpers. Remove the unused family and tests that only protect those retired APIs.
2. Remove unused ingestion settings: `batch_size`, `batch_debounce_seconds`, `batch_timeout`, `message_lifecycle_poll_seconds`, `ingestion_max_attempts`, and `session_window`, only after confirming no real consumers. Do not confuse unrelated batch-size controls with these fields.
3. Remove the duplicate `JobSettings.document_indexing` declaration.
4. Remove legacy document-focus null-selector compatibility if it only serves development snapshots. Update authored configuration/examples when obsolete fields would otherwise fail strict validation.
5. Normalize scope values by returning trimmed canonical strings and deduplicating after normalization. Preserve required-scope rejection and project visibility behavior.
6. Keep schema ownership coherent and strict persisted configuration validation. Avoid broad annotation or schema reorganizations unrelated to these contracts.
7. Update review/operations documentation to describe implemented behavior, remaining limitations, and any deferred decisions.
8. Verify the connected conversation and document flows, and inspect ownership/failure behavior independently of test results.

### Primary file targets

- `src/core/knowledge/db/readers/episode_reader.py`, `store.py`, and remaining callers/tests: unused API removal.
- `src/common/schema/settings.py`, configuration examples/loaders, and tests: obsolete fields and duplicate declaration.
- `src/common/scoping.py` and document-focus parsing/contracts: normalization and compatibility cleanup.
- `KNOGGIN_CORE_REVIEW.md`, `PROJECT_SEMANTIC_OPERATIONS.md`, this plan, and relevant public contract documentation: final behavior and status.
- Existing architecture checks and CI configuration: verify the relevant checks cover final paths; change only demonstrated gaps.

### Validation and exit gate

- No production references remain to deleted APIs, settings, preload paths, or retired compatibility contracts.
- Fresh configuration and repository examples load under strict validation.
- Exercise conversation completion → semantic processing → retrieval, including duplicate submission and checkpoint recovery.
- Exercise document indexing → retrieval → source reference, including replacement, restart, and failure classification.
- Run relevant subsystem and full server tests, including the PostgreSQL/AGE lane, plus repository-required architecture/static checks and `git diff --check`.
- Record any blocked checks precisely. Do not call a phase complete because unrelated tests pass or a service-dependent check could not run.

## Execution and reporting rules

1. Resolve only the decisions that block the next slice; do not guess product semantics.
2. Implement small complete slices with their behavior checks. Each phase has its own validation gate; testing is not deferred to Phase 6.
3. Before changing tests, classify failures as source defects, stale fixtures, or intentionally retired contracts.
4. Preserve unrelated working-tree changes. Implementation and commits are separate from this planning document's creation.
5. At each phase closeout, record changed behavior, checks run/results, unresolved limitations, and remaining decisions here.

## Progress

- [x] Phase 1 — Conversation completion and request identity
- [x] Phase 2 — Semantic processing and recovery
- [ ] Phase 3 — Retrieval, tool behavior, and evidence retention
- [ ] Phase 4 — Document parsing and provenance
- [ ] Phase 5 — Document recovery and public error contracts
- [ ] Phase 6 — Remaining cleanup and readiness verification
