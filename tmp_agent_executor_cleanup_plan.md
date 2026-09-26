# AgentExecutor Cleanup Plan

## Scope

Review and clean the `AgentExecutor` subsystem without redesigning the agent or
removing model-facing reasoning support. The executor should remain the owner of
PLAN / EXECUTE / SYNTHESIZE transitions, tool scheduling, notebook admission,
source capture, cancellation cleanup, and terminal execution events.

Associated areas that must be checked with it:

- `AgentRun` execution state and limits
- `RunNotebook` rollover and admission decisions
- tool runtime dispatch and error contracts
- source-reference capture
- prompt construction and project briefing loading
- `AgentOrchestrator` setup and public stream events
- executor contract tests

## Findings already recorded in the journal

- Verify PLAN / EXECUTE / SYNTHESIZE transitions and replanning behavior.
- Verify sequential versus explicitly safe parallel tool execution.
- Review duplicate-result handling and empty-result replanning.
- Review deep-research planning and evidence-gap review.
- Confirm source capture happens only for valid, admitted tool results.
- Confirm notebook admission remains atomic and model feedback stays bounded.
- Finish the earlier RunNotebook review by removing duplicate context-size
  ownership and simplifying tool-result dispatch where practical.
- Verify cancellation, timeouts, fallbacks, and terminal events.

## Additional findings from associated-code inspection

### 1. Remove the executor's hard-coded rollover threshold

`AgentExecutor._manage_context_size()` compares evidence against the module
constant `MAX_TOKEN_CHUNK_SIZE = 10000`, while `AgentRunLimits` and
`RunNotebook` already own `max_notebook_render_tokens`.

Use the captured run limit as the single policy source. Remove the constant and
update the compaction tests to configure the run limit instead of monkeypatching
executor internals.

### 2. Make sequential and parallel tool failures equivalent

The sequential path gives `WorkspaceConflictError` a stable
`workspace_conflict` code and emits local-reference-resolution diagnostics for
matching `ToolExecutionError` failures. The parallel path folds both into its
generic exception handling and therefore changes externally visible behavior
based only on batch shape.

Extract or share failure normalization so the same exception produces the same
error code, retryability, state update, and diagnostic event in either path.

### 3. Decouple source capture from notebook mutation

Both execution paths capture source candidates only when
`NotebookApplyResult.changed` is true. An accepted result may consult a source
that is already represented in the notebook, especially through a different
query or read. In that case the final `sources_consulted` list incorrectly omits
the consultation.

Capture valid candidates after accepted admission, then deduplicate candidates
by their stable source identity if the public response must not repeat them.
Do not capture candidates for capacity-rejected results.

### 4. Remove or relocate unreachable parse-error dispatch handling

The normal executor loop rejects `_parse_error` calls in
`_validate_tool_call_batch()` before dispatch. The sequential and parallel
dispatch paths still contain parse-error fallback logic that only direct unit
calls can reach.

Choose one owner for malformed provider arguments. Prefer phase/batch validation
before any call reservation, then delete the lower unreachable branch and test
the public loop contract rather than the private helper behavior.

### 5. Validate tool-call correlation IDs within a returned batch

Provider call IDs are forwarded to events and durable source candidates, but the
batch validator does not reject duplicate non-empty IDs. Duplicate IDs can make
tool events and provenance ambiguous even when tool arguments differ.

Add a small batch-level uniqueness check. Continue generating an ID when the
provider omits one.

## Proposed work order and commit units

### Unit 1: Context-size ownership

- Replace `MAX_TOKEN_CHUNK_SIZE` with the captured run/notebook limit.
- Update rollover tests.
- Verify previous-page retention and post-rollover token reporting.

### Unit 2: Shared tool failure normalization

- Normalize timeout, workspace conflict, expected tool failure, local-reference
  diagnostics, and unexpected failure once.
- Apply the same result to sequential and parallel execution.
- Preserve cancellation propagation and awaiting of all parallel children.

### Unit 3: Source consultation correctness

- Capture sources for every accepted source-bearing result.
- Add intentional source-candidate deduplication if needed.
- Test repeated evidence reached through distinct accepted calls.

### Unit 4: Batch validation cleanup

- Reject duplicate tool-call IDs.
- Keep malformed arguments at the pre-dispatch boundary.
- Remove unreachable private fallback behavior.
- Preserve terminal-protocol exclusivity and phase restrictions.

### Unit 5: Full executor flow audit

- Recheck phase transitions, research planning, gap review, fallback behavior,
  notebook rejection replanning, briefing loading, and terminal events.
- Remove only code proven unreachable or duplicated after the earlier units.
- Run the complete agent unit suite and targeted orchestration/session stream
  tests.

## Questions to settle during implementation

- Should `sources_consulted` preserve every consultation or expose a deduplicated
  source list? The durable candidate model suggests deduplication by stable source
  identity, not by notebook mutation.
- Should duplicate provider call IDs fail the whole step as a formatting error,
  or should Knoggin replace duplicates with generated IDs? Failing the step is
  safer because silently rewriting an supplied correlation ID hides provider
  protocol corruption.

## Non-goals

- Do not redesign `RunNotebook` again.
- Do not move session-level durable finalization into the executor.
- Do not broaden tool permissions or change tool names.
- Do not combine the Project Maintenance or Health reviews into this work.
- Do not change the public SDK contract unless an executor defect requires it.

## Verification

- Executor step and loop contract tests
- AgentRun and RunNotebook contract tests
- Source-reference capture tests
- Orchestrator and stream-event contract tests
- Ruff and `git diff --check`
- One commit after each completed unit of implementation
