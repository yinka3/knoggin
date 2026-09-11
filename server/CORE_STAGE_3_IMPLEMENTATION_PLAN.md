# Core Stage 3 — Semantic Participation and Admission Policy

Status: In progress 2026-09-11. Chunks A–E are complete; combined validation
and review closeout remain.

Baseline inspected: `aadedewe/refactor`, `bcb354e`, after completed Core Stages
1 and 2. Recheck HEAD and the working tree before implementation. Preserve
unrelated edits and untracked review material.

## Objective

Make Session participation a real, durable boundary for Project semantic work:

1. A disabled or deleted Session cannot contribute a new semantic window.
2. Excluded exchanges cannot block later eligible exchanges through per-Session
   FIFO.
3. A participation change and a semantic claim have one deterministic durable
   ordering.
4. Every admitted operation captures its compiled domain and ingestion policy
   from one coherent runtime snapshot.
5. Remove policy fields that describe behavior the current engine does not
   perform.

This stage completes the admission contract shared by Session, Ingestion, and
later Knowledge maintenance. It does not redesign semantic-window ownership.

## Finding map

| Work | Review source | Stage 3 outcome |
| --- | --- | --- |
| Rename Episode-specific participation | Locked Session §12 | Use semantic/Project-memory terminology end to end. |
| Filter participation before FIFO | Locked Ingestion §18; locked Session §§12–13 | Excluded exchanges never enter an admission stream. |
| Revalidate participation while claiming | Locked Ingestion §19 | The durable claim transaction decides toggle-versus-claim ordering. |
| Exclude deleted Sessions | Locked Ingestion §20; locked Session §13 | Deletion disables future admission without erasing canonical evidence. |
| Coherent domain/policy capture | Locked Ingestion §8; core-review qualification | Capture one `IngestionPolicy`; use its exact compiled domain. |
| Remove inert policy fields | Locked Ingestion §§9,21; core I2 | Remove `llm_ner` and Episode target-message remnants. |

## Scope boundaries

Included:

- Session participation names, database columns, ProjectManager methods, and
  deletion behavior.
- Admission query/filtering, per-Session FIFO, and claim-time revalidation.
- Conversation and human-Context-import semantic policy capture.
- Removal of `session_closed` admission and inactive policy fields.
- Focused unit tests, PostgreSQL contracts, and one end-to-end concurrency
  scenario.

Deferred:

- Maintenance frontier consumption of participation state is Stage 5.
- Project Forget semantics remain in the separate Forget plan.
- Accepted-run shutdown/idempotency and unrelated Session lifecycle findings
  remain in the Session review sequence.
- SQLite migration and extractor replacement remain separate work.

Because Knoggin is unreleased, do not retain Episode-named aliases, duplicate
columns, or compatibility decoders solely for local development data.

## Required invariants

- Participation is evaluated before FIFO. An excluded exchange is absent, not
  a blocked Session head.
- Enabling participation records the current message frontier. Only later user
  exchanges are eligible.
- Disabling participation immediately excludes all unclaimed exchanges.
- If a toggle commits before claim validation, the stale claim fails. If the
  claim commits first, its frozen membership remains valid and retryable.
- Session deletion preserves existing messages/provenance but prevents any new
  semantic membership.
- One captured `IngestionPolicy` contains the exact `CompiledDomain` used by
  admission, Context import, VP-01/VP-02, and retry reconstruction.
- Whole-exchange selection, project-level single-active-window ownership, and
  Stage 1/2 checkpoint behavior remain unchanged.

## Chunk A — Canonical semantic-participation contract

Rename the current Episode-specific storage and ProjectManager surface:

- `episode_participation_enabled` → `semantic_participation_enabled`
- `episode_participation_after_message_id` →
  `semantic_participation_after_message_id`
- `get_episode_sources()` / `set_episode_sources()` → explicit Session
  semantic-participation methods.

Keep the current useful frontier behavior: every enabled/disabled transition
records the Session's current maximum message ID. Re-enabling therefore does
not revive the interval intentionally excluded while participation was off.

Primary files:

| File | Planned change |
| --- | --- |
| `src/infrastructure/schema.sql` | Rename columns and constraints directly. |
| `src/core/project/project_manager.py` | Rename the API and SQL fields; retain one transactional bulk update. |
| `src/core/knowledge/db/writers/session_deletion_writer.py` | Disable semantic participation as part of deletion. |
| `tests/project/test_session_semantic_participation_contract.py` | Renamed and expanded semantic-participation contract. |
| `tests/contract/storage/test_session_deletion_writer_contract.py` | Assert deletion closes the future semantic boundary. |

Acceptance:

- Enabling after a disabled interval records the latest message frontier.
- Repeating the same selection is idempotent and does not move the frontier.
- Unknown/cross-project/deleted Session IDs are rejected.
- No production symbol or schema field retains the misleading Episode name.

Commit boundary: participation vocabulary and storage contract only.

## Chunk B — Participation before per-Session FIFO

Make the durable exchange reader return only exchanges that may participate:

- Session is open.
- Semantic participation is enabled.
- User message ID is strictly greater than the participation frontier.

Then retain the existing Python admission logic for completeness checks and
FIFO among eligible exchanges. Remove `_Exchange.session_closed` and the
`session_closed` flush reason; a deleted Session is not a reason to publish
previously excluded material.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/db/readers/semantic_window_reader.py` | Apply participation/status/frontier eligibility in the canonical admission query. |
| `src/core/ingestion/semantic_window_admission.py` | Remove deleted-Session flushing and preserve FIFO only across eligible rows. |
| `src/core/knowledge/store.py` | Keep the facade narrow; update names/docs only as required. |
| `tests/unit/core/ingestion/test_semantic_window_admission.py` | Preserve FIFO for eligible open/editable exchanges. |
| `tests/contract/storage/test_semantic_window_participation_admission_contract.py` | Prove disabled intervals, re-enable frontiers, deleted Sessions, and eligible FIFO at the SQL boundary. |

Acceptance:

- A disabled early exchange does not FIFO-block a later post-enable exchange.
- Open/editable eligible exchanges still block later eligible exchanges in the
  same Session.
- Other Sessions remain independent.
- Deleted Session history remains stored but never appears in admission rows.

Commit boundary: read/select eligibility only; claim-time authority follows in
Chunk C.

## Chunk C — Atomic claim-time participation revalidation

Extend `SemanticWindowWriter.claim_window()` to lock and validate the selected
Sessions while it holds the existing Project claim lock. For every user
exchange, re-check:

- Session scope and open status.
- `semantic_participation_enabled`.
- `message_id > semantic_participation_after_message_id`.
- The existing sealed/closed exchange and assistant-link rules.

Do not mutate a selected proposal into a smaller window at claim time. A stale
proposal fails atomically and the next job pass reselects from durable state.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/knowledge/db/writers/semantic_window_writer.py` | Lock relevant Session/message rows and enforce participation with membership validation. |
| `src/core/project/project_manager.py` | Preserve transaction ordering for participation changes. |
| `tests/contract/storage/test_project_context_window_contract.py` | Add real PostgreSQL toggle-versus-claim ordering tests. |
| `tests/integration/ingestion/test_project_semantic_postgres_flow.py` | Prove stale selection retry and frozen claimed membership. |

Acceptance:

- Toggle committed first rejects the stale claim with no window/membership
  rows.
- Claim committed first preserves the admitted window even if participation is
  disabled immediately afterward.
- A mixed proposal cannot partially commit.
- Retry/restart uses the already claimed immutable membership.

Commit boundary: durable claim authority and concurrency regression.

## Chunk D — One semantic policy capture edge

Use `IngestionPolicy` as the coherent semantic configuration snapshot because
it already contains the compiled domain. Add one async
`ProjectRuntime.capture_semantic_policy()` operation that builds the policy
while holding `_domain_config_lock`.

The semantic job and Context importer should receive that one captured policy
and derive `domain = policy.domain`. Remove the semantic path's independent
`capture_domain` plus synchronous `capture_ingestion_policy` pairing. Keep a
general `capture_domain()` only for non-semantic callers that genuinely need
the domain alone.

This is primarily an invariant simplification. The old review's illustrated
conversation-admission race was not reproducible because no suspension occurs
between its two reads; the human-import path does contain intervening awaited
work. Tests should cover actual lock ordering and snapshot identity instead of
claiming the old illustration as a reproduced defect.

Primary files:

| File | Planned change |
| --- | --- |
| `src/runtime/project_runtime.py` | Capture domain plus settings under one async lock edge. |
| `src/runtime/project_factory.py` | Inject the single callback. |
| `src/core/ingestion/project_semantic_job.py` | Capture once for selection/claim and Context synchronization. |
| `src/core/knowledge/context/projection.py` | Accept the coherent policy for human-edit windows. |
| `src/core/ingestion/policy.py` | Remain the immutable replay contract. |
| `tests/unit/runtime/test_project_config_fanout.py` | Assert snapshot identity and config-update fanout. |
| Context/admission unit and PostgreSQL tests | Replace paired callbacks and exercise concurrent domain activation. |

Acceptance:

- `window.domain_version`, `compiled_domain`, and `ingestion_policy` always
  describe one version.
- Conversation and human-edit windows use the same capture rule.
- Domain activation either precedes or follows capture; no mixed snapshot is
  persisted.

Commit boundary: semantic configuration capture only.

## Chunk E — Remove inert ingestion/Episode policy fields

Remove current fields that do not select runtime behavior:

- `TextProcessorSettings.llm_ner`, `TextProcessor.llm_ner`, and
  `IngestionPolicy.llm_ner`.
- `EpisodeGenerationPolicy.target_message_count`, the hard-coded admission
  reference window size, and the corresponding snapshot/hash validation.

Keep actual limits: semantic-window token target, Episode source-message/token
caps, narrative cap, prior-candidate cap, and frozen policy versioning.

Primary files:

| File | Planned change |
| --- | --- |
| `src/common/schema/settings.py` | Delete `llm_ner`. |
| `src/core/ingestion/text_processor.py` | Delete inert state. |
| `src/core/ingestion/policy.py` | Remove the field from capture and replay shape. |
| `src/core/knowledge/episodes/policy.py` | Remove target-message policy and capture argument. |
| `src/core/ingestion/semantic_window_admission.py` | Remove the reference-window constant. |
| Affected settings, ingestion, Context, and storage tests | Rebuild fixtures around the smaller current contracts. |

Acceptance:

- No live config or durable snapshot promises LLM-NER selection.
- Episode policy describes generation constraints only; admission owns window
  selection.
- Current semantic-window replay remains deterministic under the new shape.

Commit boundary: policy cleanup after the capture contract is stable.

## Combined Stage 3 scenario

Use real PostgreSQL with deterministic model boundaries:

1. Create two Sessions in one Project and disable one.
2. Close exchanges while disabled, re-enable, then close a later exchange.
3. Verify excluded exchanges do not appear and do not FIFO-block the new one.
4. Pause between selection and claim. Commit a participation change first and
   prove the claim is rejected without partial membership.
5. Repeat with claim winning first and prove later disable/deletion does not
   invalidate the frozen window.
6. Activate a new domain around semantic-policy capture and prove the window
   contains one coherent policy/domain version.
7. Restart the semantic job and complete the claimed window through the Stage
   1/2 Context, Knowledge, publication, and no-op gates.

## Validation and closeout

Run focused tests per chunk, then:

- `tests/project/test_session_semantic_participation_contract.py`
- `tests/contract/storage/test_session_semantic_participation_storage_contract.py`
- `tests/unit/core/ingestion/test_semantic_window_admission.py`
- `tests/unit/runtime/test_project_config_fanout.py`
- `tests/contract/storage/test_project_context_window_contract.py`
- `tests/contract/storage/test_session_deletion_writer_contract.py`
- `tests/integration/ingestion/test_project_semantic_postgres_flow.py`
- affected Stage 1/2 ingestion, Context, and semantic-commit regressions
- touched-path Ruff, compileall, architecture check when imports change,
  configured mypy scope after repairing its stale path separately, and
  `git diff --check`

Record exact commands/results, changed files, limitations, and local commit IDs.
Retire only historical probes whose desired behavior has a normal regression.
Append implementation status to the locked Session/Ingestion/Knowledge reviews
without rewriting their original rationale.

- [x] A: canonical semantic-participation names and persistence.
- [x] B: eligibility before FIFO and deleted-Session exclusion.
- [x] C: atomic claim-time participation revalidation.
- [x] D: coherent semantic policy/domain capture.
- [x] E: inert policy cleanup.
- [ ] Combined real-PostgreSQL scenario passes.
- [ ] Review/probe/operations closeout is recorded and committed locally.

Next implementation task: Stage 3 combined validation and review closeout.
