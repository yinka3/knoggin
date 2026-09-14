# Core Stage 6 — Agent Evidence Delivery and Research Execution

Status: Chunks A–F complete 2026-09-13. Chunk G remains planned.

Baseline inspected: `aadedewe/refactor`, `bcb354e`, after completed Core Stages
1 and 2. Stage 6 consumes Stage 4's corrected graph/source contracts. Recheck
the checkout because Agent and provenance work may advance independently.

## Objective

Make the existing Agent architecture reliably use the Knowledge and provenance
it already retrieves:

1. One bounded notebook projection reaches every reasoning and synthesis step.
2. Evidence rejected by notebook capacity is not reported or persisted as if
   the model saw it.
3. Executor phase and protocol-tool restrictions are enforced at dispatch.
4. Research and deep-research modes have real grounded-execution guarantees.
5. Agent default lifecycle and full Brain replacement are safe under local
   concurrency.
6. Project briefing can be reduced on simple conversational turns without
   requiring a retrieval call for every response.

Keep `AgentOrchestrator`, `AgentRun`, `RunNotebook`, `AgentExecutor`, immutable
`ToolRuntime`, and atomic answer/source persistence. This is contract completion
and renderer consolidation, not an Agent rewrite.

## Finding map

| Work | Review source | Stage 6 outcome |
| --- | --- | --- |
| Episode result missing from prompt | Provenance/Agent PA1 | Render episode handle, narrative, chronology, and follow-up path. |
| Earlier evidence disappears | Provenance/Agent PA2 | Use one bounded accumulated notebook projection in every step. |
| Capacity rejection reported as success | Provenance/Agent PA5 | Propagate typed admission result and record only admitted encounters. |
| Synthesis dispatches hidden tools | Provenance/Agent PA6; locked Agent §12 | Enforce phase allowlist and terminal-tool exclusivity before dispatch. |
| Research modes lack guarantees | Provenance/Agent PA7; locked Agent §§7–9 | Require admitted grounded evidence and one deep-research gap review. |
| Default/Brain races | Locked Agent §§10–11 | Add local lifecycle lock, defensive SQL, and Brain CAS. |
| Provenance detail gaps | Provenance review improvements 2–4 | Render sources/support on demand and label Context assertion origin. |
| Repeated full briefing | Prior product discussion; provenance improvement 6 | Add bounded configurable adaptive briefing without a classifier service. |

Stage 4 owns PA3 graph hydration and PA4 document encounter persistence. Stage
6 verifies their final model-facing use but does not duplicate their storage
repairs.

## Scope boundaries

Included:

- Notebook renderer/model input, executor tool-result admission, phase
  authorization, research state, AgentManager concurrency, evidence display,
  and Project Brief/Context loading policy.
- Deterministic scripted-provider tests that inspect exact model input and
  dispatched calls.
- Focused PostgreSQL contracts for AgentManager/source finalization where
  durable behavior changes.

Deferred:

- Retrieval ranking/backend changes and SQLite migration remain in the separate
  SQLite/agent retrieval plan.
- Claim-level semantic entailment verification remains out of scope.
- AAC/community behavior remains a separate optional/fun feature.
- Public thinking-event projection belongs to API work.
- Stale write-audit repair belongs to Health/shared infrastructure.
- SDK/frontend presentation remains out of scope.

## Required invariants

- `RunNotebook` remains canonical. Model-local handles are a one-way projection
  and never become persistence identity.
- Capacity measurement and model rendering use the same bounded projection.
- Every item listed as consulted was admitted and exposed to the model during
  the run. Discovery remains distinct from read evidence.
- Normal mode may answer directly from recent conversation or supplied context.
  No tool call is required for greetings, acknowledgements, or already answered
  follow-ups.
- Research cannot finalize without admitted grounded investigation evidence.
- Deep research performs one executor-owned gap-review cycle after evidence is
  gathered; it does not require an arbitrary source count.
- Only tools visible/allowed for the current executor phase may dispatch.
- One default Agent remains after ordinary concurrent lifecycle operations.
- Full Brain replacement cannot overwrite a newer revision.

## Chunk A — One model-facing notebook projection

Make `notebook_renderer.py` the single accumulated-evidence renderer used by:

- PLAN and EXECUTE prompts;
- SYNTHESIZE prompts;
- notebook capacity/token measurement;
- evidence-summary and fallback-answer inputs.

Keep immediate tool feedback in `prompt_context.py`, but replace its competing
`_format_evidence()` path with the canonical rendered notebook. Expand the
renderer where necessary so episodes include a usable handle, summary,
chronology, unresolved items, and source/support references; messages,
relationships, paths, documents, and prior web reads retain bounded meaningful
content rather than counts alone.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/agent/notebook_renderer.py` | Become the one complete bounded model projection. |
| `src/core/agent/prompt_context.py` | Compose history/query/last result with canonical notebook render. |
| `src/core/agent/notebook.py` | Keep normalization/reference/capacity ownership; align token measurement. |
| `src/core/agent/formatters.py` | Remove superseded notebook formatters; retain only independent presentation helpers. |
| `src/core/agent/executor.py` | Use the same projection for every phase and fallback/summary path. |
| Agent notebook/prompt/executor tests | Assert exact final synthesis input, not only backend/notebook state. |

Acceptance:

- PA1 episode ID and narrative appear in the model prompt.
- Two independent earlier retrievals both remain in final synthesis within
  capacity.
- Previous read-web passages remain available, while discovery snippets remain
  labeled as discovery.
- Rendered token count is the actual capacity input.
- Local handles are stable for one run and canonical IDs do not leak where the
  current localized contract forbids them.

Commit boundary: renderer consolidation and model-input regressions.

### Chunk A completion — 2026-09-13

render_notebook() is now the one accumulated-evidence projection for planning,
execution, synthesis, evidence summarization, fallback generation, and
notebook capacity. prompt_context.py retains only immediate tool feedback,
conversation context, and hot-topic context around that render; its competing
formatter-based evidence path is gone.

Episode cards retain a model-callable compact ep_… handle, narrative,
chronology, bounded developments/updates/unresolved items, local evidence
references, and clearly labeled historical source support. The renderer does
not represent historical support as newly consulted in this run. It preserves
bounded message/document content, relationship qualifications, path endpoints,
web-read passages, and distinct discovery snippets. RunNotebook now records a
read_episode follow-up hint that uses the same compact episode handle.

The obsolete notebook-result helpers were removed from formatters.py; the
independent hot-topic, document-list, and document-focus renderers remain.
model_view() remains a test/inspection adapter, not a production prompt
source.

The normal regression gate passed:

- uv run pytest -q tests/unit/core/agent/test_run_notebook.py tests/unit/core/agent/test_run_notebook_core.py tests/unit/core/agent/test_agent_runtime_context_contract.py tests/unit/core/agent/test_executor_loop_contract.py tests/unit/core/agent/test_agent_prompt_contract.py tests/unit/core/agent/test_tool_dispatch_contract.py tests/unit/core/agent/test_episode_retrieval_contract.py tests/unit/core/agent/test_graph_retrieval_contract.py tests/unit/common/test_local_references.py → **99 passed** with the existing Requests dependency warning.
- uv run pytest -q tests/unit/core/agent tests/unit/common/test_local_references.py → **226 passed** with the same warning.
- The former PA1/PA2 reproductions first failed as expected because episode
  narrative and both earlier facts now reach the prompt. They are retained as
  skipped historical probes; the normal prompt/executor regressions are the
  acceptance evidence.
- Touched-path Ruff check/format, the architecture import check, compileall,
  and git diff --check passed. The configured MyPy baseline remains deferred
  because it still names stale paths, as recorded in MYPY_BASELINE.md.

Chunk B remains responsible for typed admission and encounter ordering.
Chunk F will add on-demand, model-callable provenance/support affordances; its
work is deliberately separate from this bounded historical presentation.

## Chunk B — Typed notebook admission and source encounter ordering

Return `NotebookApplyResult` from `AgentRun.accumulate_tool_result()` instead of
reducing it to a boolean. The executor must inspect `accepted`, `changed`, and
`reason` before declaring tool success.

For evidence-bearing tools:

1. Execute and validate the backend result.
2. Attempt notebook admission.
3. Only after admission, record source candidates as encountered by the model.
4. On capacity rejection, return an explicit bounded result instructing the
   model to narrow/read a smaller range; do not claim an item count or retain
   source candidates the model never received.

Start with atomic rejection because that matches the current notebook contract.
Add partial admission only if a concrete tool/result type can do it without
breaking reference/provenance identity.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/agent/run.py` | Preserve the typed notebook application result. |
| `src/core/agent/executor.py` | Sequence admission, source capture, success/error events, and retry guidance. |
| `src/core/agent/sources/tool_results.py` | Produce candidates without recording them prematurely. |
| `src/core/agent/notebook.py` | Keep atomic capacity decision and explicit reason. |
| Notebook accumulation, source, and executor-loop tests | Cover accepted, duplicate, empty, and capacity-rejected results. |

Acceptance:

- Capacity-rejected passages do not appear in prompt or final source list.
- Tool events say the result was rejected/truncated, not successfully exposed.
- The next step can request a smaller range without a false duplicate-call
  dead end.
- Accepted evidence retains existing encounter idempotency.

Commit boundary: notebook/source admission truth.

### Chunk B completion — 2026-09-13

`AgentRun.accumulate_tool_result()` now returns the full
`NotebookApplyResult`. The executor applies a raw backend result before either
localizing it or producing source candidates. Only an accepted, changed result
can add source candidates; duplicate/empty results remain successful but do
not create a new encounter.

An atomic capacity rejection now produces an empty, structured model result
with bounded guidance to narrow a query or read a smaller range. It exposes no
rejected title, passage, count, compact handle, or source candidate. The tool
event reports that the result was not added, and the executor immediately
returns to PLAN so the next call can retry with different arguments. The
existing duplicate-call guard therefore does not block a narrower retry.

The focused gate passed:

- `uv run pytest -q tests/unit/core/agent/test_agent_runtime_context_contract.py tests/unit/core/agent/test_executor_loop_contract.py tests/unit/core/agent/test_run_notebook_core.py tests/unit/core/agent/test_sources_contract.py` → **68 passed** with the existing Requests dependency warning.
- `uv run pytest -q tests/unit/core/agent tests/unit/common/test_local_references.py` → **229 passed** with the same warning.
- Touched-path Ruff check/format, `compileall`, architecture imports, and `git diff --check` passed.

The regressions prove accepted, duplicate, empty, and capacity-rejected
admission outcomes. The rejection scenario verifies that unadmitted wide web
results reach neither the next model prompt nor final source list, while an
accepted narrow retry retains its own call ID and result position.

## Chunk C — Executor phase authorization and protocol exclusivity

Before dispatch, validate every returned call against the current phase's
actual tool schema/allowlist. During SYNTHESIZE, ordinary investigative or write
tools are rejected even if the run-wide `ToolRuntime` authorizes them.

Treat terminal protocol tools as exclusive batches:

- valid: `[submit_answer]` or `[request_clarification]`;
- invalid: terminal plus ordinary tool, both terminal tools, or multiple
  terminal calls.

Malformed batches become one bounded formatting/replan error and dispatch
nothing from that batch.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/agent/executor.py` | Central phase/batch validation before counters, audit, or dispatch. |
| `src/core/agent/tool_runtime.py` | Keep immutable run-wide authorization; expose definitions if needed for phase validation. |
| `src/core/agent/tools/registry.py` | Preserve canonical protocol metadata. |
| `tests/unit/core/agent/test_agent_executor_step_contract.py` | Cover hidden/unknown calls by phase. |
| `tests/unit/core/agent/test_executor_loop_contract.py` | Cover mixed terminal batches and zero dispatch. |
| `tests/unit/core/agent/test_tool_dispatch_contract.py` | Preserve capability/audit authorization. |

Acceptance:

- A hidden synthesis search/write call never reaches its backend.
- Mixed terminal batches dispatch nothing and consume one bounded retry path.
- Run-wide capability checks still apply in PLAN/EXECUTE.
- Provider malformed-call behavior remains diagnosable without leaking raw
  sensitive arguments.

Commit boundary: executor-owned phase enforcement.

### Chunk C completion — 2026-09-13

`AgentExecutor` now derives a phase-specific allowlist from the exact schemas
shown to the model and validates the complete returned batch before terminal
handling, call counters, audit setup, or backend dispatch. A provider-returned
tool outside that phase, an unknown tool, or malformed JSON arguments produces
one bounded formatting/replan error with no raw argument value retained in the
parsed call state.

Terminal protocol calls are now exclusive: a batch containing a terminal call
plus an ordinary call, both protocol calls, or duplicate terminal calls is
rejected as a whole. The immutable run-wide `ToolRuntime` and registry protocol
metadata remain the authorization source for PLAN and EXECUTE dispatch; the
existing workspace write authorization/audit regression continues to cover that
boundary.

The scripted regressions prove that a mixed `submit_answer`/search batch emits
no tool events, records no calls, and takes one PLAN retry. They also prove a
run-authorized `edit_brain` call returned during SYNTHESIZE never reaches the
backend; the next PLAN prompt receives only a bounded phase rejection, without
the sensitive argument value.

Validation passed:

- `uv run pytest -q tests/unit/core/agent/test_agent_executor_step_contract.py tests/unit/core/agent/test_executor_loop_contract.py tests/unit/core/agent/test_tool_dispatch_contract.py tests/unit/core/agent/test_workspace_tools_contract.py` → **49 passed** with the existing Requests dependency warning.
- `uv run pytest -q tests/unit/core/agent tests/unit/common/test_local_references.py` → **239 passed** with the same warning.
- Touched-path Ruff check/format, `python -m compileall -q src/core/agent`, architecture imports, and `git diff --check` passed.

## Chunk D — Enforced research and deep-research state

Simplify `ResearchProfile` to the fields the executor uses:

- `mode`
- `default_artifact_kind`
- tool-call, attempt, and source-budget multipliers

Remove `minimum_source_count`, `iterative`, and `artifact_policy`. Centralize an
`AgentRun.has_grounded_investigation_evidence()` decision based on admitted
notebook evidence or validated source-bearing initial input—not tool attempts,
errors, empty results, Project Brief, or Context alone.

Execution behavior:

- normal may submit immediately;
- research rejects an ungrounded terminal answer and returns to PLAN/EXECUTE;
- deep research does the same, then performs one explicit gap-review PLAN pass
  after evidence exists and before SYNTHESIZE;
- gap review may proceed directly if no material gap exists or gather more
  evidence within the frozen run budget.

Primary files:

| File | Planned change |
| --- | --- |
| `src/common/schema/agent/research.py` | Remove unenforced fields and define the final profile. |
| `src/core/agent/run.py` | Own grounded-evidence and gap-review state. |
| `src/core/agent/executor.py` | Enforce terminal readiness and deep gap-review transition. |
| `src/core/agent/system_prompt.py` | Describe actual mode semantics without obsolete fields. |
| AgentRun, prompt, orchestrator, and executor-loop tests | Prove behavior for normal/research/deep-research. |

Acceptance:

- Normal greeting/direct follow-up can answer with zero tool calls.
- Research first-step `submit_answer` is rejected until real evidence is
  admitted.
- Selected document/pasted evidence can satisfy the requirement without an
  unnecessary extra search.
- Deep research records exactly one required gap-review cycle and may use one
  authoritative source when sufficient.
- Default artifact behavior remains as currently intended.

Commit boundary: research profile cleanup plus enforced state machine.

### Chunk D completion — 2026-09-13

`ResearchProfile` now retains only the selected mode, default artifact kind,
and immutable budget multipliers. The executor owns research semantics: normal
runs may submit directly, while research and deep-research runs reject an
ungrounded `submit_answer` before any artifact is accepted.

`AgentRun` distinguishes admitted notebook evidence and validated initial
source candidates from actions, empty results, summaries without retained
references, Project Brief, and Context. Deep research schedules exactly one
executor-owned high-reasoning PLAN review after evidence exists; that review can
retrieve a real remaining gap or submit directly into final synthesis when one
authoritative source is sufficient.

The focused contracts cover ungrounded research/deep-research rejection,
validated pasted text and selected-document input without an extra retrieval,
action-only non-evidence, one deep gap-review cycle, and unchanged default
research artifact kinds.

Validation passed:

- `uv run pytest -q tests/unit/core/agent/test_agent_run.py tests/unit/core/agent/test_executor_loop_contract.py tests/unit/core/agent/test_agent_prompt_contract.py tests/unit/core/agent/test_orchestrator.py tests/unit/common/test_artifact_contracts.py tests/unit/common/test_local_references.py tests/unit/core/agent/test_agent_executor_step_contract.py tests/unit/core/agent/test_tool_dispatch_contract.py tests/unit/core/agent/test_workspace_tools_contract.py` → **107 passed** with the existing Requests dependency warning.
- `uv run pytest -q tests/unit/core/agent tests/unit/common/test_artifact_contracts.py tests/unit/common/test_local_references.py` → **252 passed** with the same warning.
- Touched-path Ruff format/check, `python -m compileall -q src/core/agent src/common/schema/agent`, architecture imports, and `git diff --check` passed.

## Chunk E — Agent default lifecycle and Brain CAS

Add one `AgentManager` lifecycle lock around structural default/delete/ensure
operations, with defensive SQL inside the transaction:

- `delete_agent()` deletes only `WHERE is_default = false` and checks row count;
- `set_default_agent()` locks/validates the target in the same transaction that
  changes defaults;
- `ensure_default_agent()` participates in the same local lock;
- ordinary operations finish with exactly one default after startup has
  established one.

When `update_agent()` replaces Brain content, require
`expected_brain_revision` and update with a revision CAS. Persona/model-only
updates do not invent a Brain revision requirement. Active runs keep their
captured Brain snapshot; future runs see the new revision.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/agent/services/agent_manager.py` | Add lifecycle lock, transactional default SQL, and full-Brain CAS. |
| Agent configuration schemas/callers | Require expected revision only for Brain replacement. |
| `tests/unit/core/agent/test_agent_manager.py` | Add deterministic overlapping default/delete/ensure and stale Brain tests. |
| New focused PostgreSQL contract if unit fakes cannot prove SQL ordering | Exercise row-count/CAS behavior against the real schema. |

Acceptance:

- Concurrent set-default/delete cannot leave zero or multiple defaults.
- Default Agent cannot be deleted by a stale pre-check.
- Stale full Brain replacement returns a conflict and preserves newer content.
- Snapshot creation uses the committed new revision exactly once.

Commit boundary: durable Agent configuration concurrency.

### Chunk E completion — 2026-09-13

`AgentManager` now serializes `ensure_default_agent()`,
`set_default_agent()`, and `delete_agent()` with one local lifecycle lock.
Default promotion locks and validates its target in the same database
transaction that changes default state. Deletion retains a defensive
`is_default = false` predicate in the mutation and only reports success for
one affected row.

Full Brain replacement through `update_agent(brain=...)` now requires a
positive `expected_brain_revision`. Both ordinary and snapshot-producing
writes include that revision in their mutation predicate. A lost compare-and-
swap raises `AgentBrainRevisionConflictError` with the expected and current
revision, preserving the later Brain. Persona/model/tool updates remain
revision-free. There is no separate settings/API caller for `update_agent` in
the current server checkout, so this service boundary owns the requirement.

The unit contract covers locked overlapping promotion/deletion, startup-default
locking, target validation inside the transaction, required revision input,
stale writes, and snapshot selection. A real PostgreSQL contract confirms the
conditional delete after a stale read, one durable default under overlapping
operations, CAS preservation of revision 5, and exactly one committed
revision-5 snapshot.

Validation passed:

- `uv run pytest -q tests/unit/core/agent/test_agent_manager.py` → **20 passed**.
- `uv run pytest -q tests/contract/storage/test_agent_manager_lifecycle_contract.py` → **2 passed**.
- `uv run pytest -q tests/unit/core/agent tests/unit/core/community` → **273 passed**.
- Touched-path Ruff check, `python -m compileall -q src/core/agent/services tests/unit/core/agent/test_agent_manager.py tests/contract/storage/test_agent_manager_lifecycle_contract.py`, and `git diff --check` passed.

## Chunk F — Provenance affordances and qualified Context briefing

Use Stage 4's evidence shapes in the Agent without dumping the whole graph:

- Episode detail exposes consulted-source status/locator when the source is
  actually shown in the current run.
- Graph path results retain observation handles that an existing detailed read
  can expand.
- Agent-facing canonical Context rendering includes compact assertion kind
  (`user_asserted`, `source_grounded`, `agent_derived`, etc.) and an optional
  local support handle. `CONTEXT.md` remains a user-editable projection and is
  not the Agent's canonical reader.
- Registered evidence-document reads through ordinary workspace tools either
  route through the existing document-source capture contract or are rejected
  with guidance to use the provenance-aware read. `PROJECT.md` remains
  instruction context, not evidence; reserved `CONTEXT.md` remains inaccessible
  to generic tools.

Primary files:

| File | Planned change |
| --- | --- |
| `src/core/agent/notebook_renderer.py` | Render bounded source/support handles. |
| `src/core/agent/tools/registry.py`, `src/core/agent/tools/search.py`, and `src/core/agent/tools/workspace.py` | Expose existing detailed evidence reads and provenance-aware file behavior. |
| `src/core/knowledge/context/render.py` | Add a separate bounded Agent projection with assertion labels. |
| `src/core/agent/executor.py` | Load canonical Agent Context projection only. |
| Episode/graph/document/workspace/prompt contract tests | Prove sources shown now versus merely historical. |

Acceptance:

- Stored historical sources are not relabeled as newly consulted unless their
  content is exposed in this run.
- The model can inspect a path observation's support on demand.
- Context origin qualifications survive into model input.
- Ordinary file reads cannot silently bypass provenance for registered evidence
  documents.

Commit boundary: model-facing provenance affordances.

### Chunk F completion — 2026-09-13

The executor now loads the separate model-only Context projection, which keeps
assertion kinds and compact support handles out of user-editable `CONTEXT.md`.
Path results retain local observation handles and an on-demand
`read_observation_evidence` follow-up. Its traversal is bounded to one
observation, four Context blocks, eight leaf evidence nodes, and 16 edges
within the caller's readable-project scope. Historical episode support now
renders locators without treating stored sources as newly consulted.

`read_file` rejects active-project registered evidence documents with guidance
to use `read_document`; `PROJECT.md` remains readable instruction context and
controlled `CONTEXT.md` remains unavailable through generic workspace tools.

Validation passed:

- `uv run pytest -q tests/unit/core/knowledge/test_evidence_service.py tests/unit/core/agent/test_graph_retrieval_contract.py tests/unit/core/agent/test_run_notebook.py tests/unit/core/agent/test_agent_runtime_context_contract.py` → **49 passed**.
- `uv run pytest -q tests/unit/core/knowledge/test_context_render.py tests/unit/core/agent/test_agent_executor_step_contract.py tests/unit/core/agent/test_workspace_tools_contract.py tests/unit/core/agent/test_tool_dispatch_contract.py` → **41 passed**.
- `uv run pytest -q tests/contract/storage/test_current_observation_reader_contract.py -k visible_observation` → **1 passed, 3 deselected**.
- `uv run pytest -q tests/unit/core/agent` → **251 passed**.
- Touched-path Ruff check, targeted compileall, and `git diff --check` passed.

## Chunk G — Configurable adaptive Project briefing

Add a small per-run briefing policy with two supported modes:

- `always`: load Project Brief and canonical Context before the first model
  step, preserving current maximum-continuity behavior.
- `adaptive`: begin simple normal-mode conversational turns with Agent identity,
  recent messages, and explicit selections only. Load Brief/Context before the
  first step when deterministic run signals require Project knowledge, and
  allow one internal context-load transition if the executor discovers that it
  is needed later.

Do not add a separate classifier model/service. Use observable run inputs such
as research mode, document focus/selection, explicit Project-memory intent,
existing hot-topic preload, and a small tested conversational fast path. If the
gate is uncertain, include the briefing. Cache the decision/content for the run
so later steps do not repeatedly read storage.

Primary files:

| File | Planned change |
| --- | --- |
| `src/common/schema/settings.py` | Add the compact `always`/`adaptive` policy. |
| `src/core/agent/orchestrator.py` | Freeze briefing mode/signals into the run. |
| `src/core/agent/run.py` | Track whether Project briefing has been loaded. |
| `src/core/agent/executor.py` | Gate and cache Brief/Context loading without requiring a normal tool call. |
| `src/core/agent/system_prompt.py` | Render only the context actually loaded. |
| Executor/orchestrator/prompt tests | Cover greetings, acknowledgements, continuation, Project questions, research, and document-focused turns. |

Acceptance:

- “Hey”, “nice”, and “go to the next one” can respond from recent conversation
  without loading full Project briefing or making a retrieval tool call.
- A direct Project-memory/document/research request receives necessary context
  before reasoning.
- An adaptive miss can load context once and continue within the same run.
- `always` reproduces current behavior for users who prefer maximum continuity.
- Benchmarks record prompt tokens and extra-step frequency; adaptive mode is not
  declared successful solely because unit tests pass.

Commit boundary: briefing efficiency after evidence correctness.

## Combined Stage 6 scenarios

Use scripted providers and deterministic tool results:

1. Normal greeting answers directly with recent context and no tool/full
   briefing load.
2. Retrieve episode A, message B, graph path C, and document/web passage D over
   several steps; final synthesis receives every admitted item and its usable
   handles within capacity.
3. Reject an oversized result and prove it creates neither model-visible
   evidence nor final source references; a smaller retry succeeds.
4. Return an ordinary/write tool during SYNTHESIZE and a mixed protocol batch;
   prove zero forbidden dispatch.
5. Attempt immediate answers in research/deep-research, then gather one
   authoritative source; prove research proceeds and deep research performs its
   gap review before final artifact synthesis.
6. Overlap default/delete/ensure and stale full-Brain updates against real
   PostgreSQL or a synchronization-aware fake.
7. Compare `always` and `adaptive` briefing on the agreed conversational and
   substantive examples, recording prompt size and additional model steps.

## Validation and closeout

Run all Agent notebook, prompt, source, executor, research, orchestrator,
AgentManager, graph/Episode/document retrieval, and answer/source persistence
contracts. Retain Stage 4 provenance tests as model-input parity gates. Use no
live provider/network calls for correctness; run a small explicit manual-model
quality/latency sample only if configured and label it separately.

Also run touched-path Ruff, compileall, architecture checks when imports move,
the configured mypy scope after its stale path is repaired separately, and
`git diff --check`.

Record exact commands/results, changed files, prompt-budget measurements,
limitations, and local commit IDs. Retire PA1/PA2/PA5/PA6/PA7 probes only after
normal desired-behavior regressions exist. Append status to the locked Agent and
provenance/Agent reviews.

- [x] A: one canonical model-facing notebook projection.
- [x] B: typed notebook admission and source encounter ordering.
- [x] C: phase authorization and protocol exclusivity.
- [x] D: enforced research/deep-research semantics and profile cleanup.
- [x] E: default-Agent lifecycle and Brain CAS.
- [x] F: model-facing provenance and qualified Context briefing.
- [ ] G: configurable adaptive Project briefing.
- [ ] Combined scripted-provider and PostgreSQL scenarios pass.
- [ ] Review/probe/operations closeout is recorded and committed locally.

Next implementation task: Stage 6 Chunk G.
