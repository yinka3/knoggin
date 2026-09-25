# Knowledge Retrieval Cleanup — Starting Plan

Temporary working plan for the subsystem reviewed after RunNotebook and AgentRun.

## Goal

Keep `KnowledgeRetrieval` as the single project-scoped internal-memory read boundary while simplifying hydration, making evidence identity consistent, and aligning model-facing contracts with real behavior.

Preserve:

- readable-project and user scope enforcement;
- hybrid lexical and semantic message retrieval;
- reranking with graceful fallback;
- bounded surrounding-message context;
- compact episode discovery followed by explicit expansion;
- bounded observation traversal;
- thin Agent tool adapters.

## Baseline

- [x] Review current production callers and adjacent tool/notebook contracts.
- [x] Run focused retrieval, graph, episode, dispatch, and semantic-search tests.
- [x] Baseline: 52 focused tests pass.
- [x] Record unrelated untracked files and leave them untouched.

## Associated-Area Findings Missed by the Journal

### Hydration mutates store-returned result dictionaries before success

`_hydrate_result_evidence()` removes `evidence_refs` and `evidence_ids` directly from each input result before any asynchronous hydration completes.

If message or observation hydration fails, the caller's objects are left partially transformed. It also assumes every storage reader returns disposable dictionaries rather than shared/cached values.

- [ ] Build detached result dictionaries before removing internal reference fields.
- [ ] Preserve atomic behavior: either return fully hydrated detached results or raise without changing inputs.
- [ ] Add a regression test covering hydration failure and input preservation.

Classification: correctness and boundary hardening. Priority: high.

### Message evidence refs do not enforce the retrieval service's user scope

Observation refs explicitly reject a `user_name` different from `self.user_name`. `_normalize_evidence_ref()` instead accepts a `user_name` supplied by the ref and forwards it to storage.

Even in the local single-user product, the project-scoped retrieval boundary should not let stored/reflected input choose a different identity scope.

- [ ] Reject mismatched message-ref users, or always use the service-owned user scope.
- [ ] Keep readable-project/session enforcement in the durable storage call.
- [ ] Add a mismatched-user regression test.

Classification: scope correctness. Priority: high.

### Session validation is inconsistent across public retrieval methods

`search_messages()` validates `session_id`, while graph/activity, episode, and topic-context methods accept it without the same boundary check even when it is used for hydration or telemetry.

- [ ] Decide whether every public method accepting `session_id` should validate it consistently.
- [ ] Add parameterized direct-call tests if this invariant is adopted.

Classification: contract hardening. Priority: medium.

### Batched hydration still performs user/session groups sequentially

After references are grouped, `_hydrate_evidence()` reads each independent user/session group in sequence. Observation batches are likewise read one project at a time.

- [ ] After enforcing one user scope, assess whether session groups should use bounded `asyncio.gather()`.
- [ ] Avoid adding concurrency machinery unless tests or profiling show meaningful multi-session fanout.

Classification: performance follow-up. Priority: low-medium.

### Blank query behavior is not explicit

`episode_check()` strips its query, but message/entity searches accept blank strings and rely on downstream behavior.

- [ ] Decide whether blank direct/internal queries should return no results or raise `ValueError`.
- [ ] Express the same minimum-length rule in tool schemas.

Classification: contract hardening. Priority: low-medium.

## Phase 1 — Make Hydration Detached and Batched

- [ ] Refactor `_hydrate_result_evidence()` to work on detached copies.
- [ ] Collect all message refs across the result set.
- [ ] Hydrate the combined refs once through `_hydrate_evidence()`.
- [ ] Map hydrated messages back to their owning results without changing order.
- [ ] Keep observation hydration bounded and preserve the external result shape.
- [ ] Test duplicate refs, mixed message/observation refs, empty refs, and failure atomicity.

## Phase 2 — Canonicalize Durable Message Evidence

Create one small deterministic formatter used by:

- `_hydrate_evidence()`;
- `_get_surrounding_context()`;
- `read_episode()` source expansion.

The canonical record should consistently carry:

- `id` as `msg_<durable id>`;
- service-owned `user_name`;
- source `session_id`;
- `role`;
- `message`;
- one ISO timestamp representation;
- context/hit metadata only where the retrieval surface needs it.

- [ ] Confirm Notebook reference identity remains stable across retrieval surfaces.
- [ ] Add a test showing one durable message receives the same ID/session identity through search and episode expansion.
- [ ] Remove duplicate `message`/`content` and numeric/string ID disagreement where downstream contracts allow it.

## Phase 3 — Choose One Path Observation Expansion Model

Recommended direction:

```text
find_path
→ path plus compact observation handles

read_observation_evidence
→ full bounded evidence bundle on demand
```

- [ ] Verify the notebook can admit compact path observation handles without eager bundles.
- [ ] Update `find_path()` to avoid eager observation expansion if the notebook contract supports it.
- [ ] Keep message evidence hydration if path results can also contain direct message refs.
- [ ] Update graph retrieval, notebook, and tool-description tests together.

This is a behavior decision and should be a separate commit from mechanical batching.

## Phase 4 — Fix Model-Facing Retrieval Contracts

- [ ] Describe `search_messages` as bounded hybrid durable-message retrieval rather than exact-keyword-only search.
- [ ] Describe `read_recent_episodes` as readable project memory, not only the current conversation.
- [ ] Add `minimum: 1` to positive `limit` and `hours` schema fields.
- [ ] Add useful string length constraints for retrieval queries.
- [ ] Verify presentation overrides cannot weaken these canonical constraints.

## Phase 5 — Validate Direct Retrieval Inputs

- [ ] Replace falsy defaults with explicit `None` handling.
- [ ] Validate message/entity limits as positive non-boolean integers.
- [ ] Validate activity hours as a positive non-boolean integer.
- [ ] Ensure `read_recent_episodes` rejects booleans as limits.
- [ ] Apply consistent session/query validation decided above.

## Phase 6 — Clarify Search Configuration Ownership

Current construction merges typed internal search settings with external web-provider settings into an untyped dictionary.

- [ ] Pass only internal `SearchSettings` into `KnowledgeRetrieval`.
- [ ] Keep external web-provider configuration on `SearchTools`.
- [ ] Choose and document one lifecycle:
  - frozen for the project runtime lifetime; or
  - updated through a focused config subscription.
- [ ] Prefer frozen runtime snapshots unless live updates are an actual product requirement.
- [ ] Update ProjectRuntime and AAC/community composition paths together.

## Phase 7 — Parallelize Bounded Episode Source-Reference Reads

- [ ] Fetch source-reference lists for the bounded episode set concurrently.
- [ ] Preserve episode ordering and per-episode source ordering.
- [ ] Keep this as a small latency commit; do not create a new service abstraction.

## Validation Checklist

After every implementation unit:

- [ ] Focused Knowledge Retrieval tests pass.
- [ ] Graph and episode retrieval contract tests pass.
- [ ] Tool dispatch/schema tests pass.
- [ ] Notebook accumulation and renderer tests pass when evidence shape changes.
- [ ] Scope tests prove no user/project widening.
- [ ] Failed hydration leaves source results unchanged.
- [ ] Stable message references remain consistent.
- [ ] Ruff and `git diff --check` pass.
- [ ] Commit the completed unit before starting the next unit.

## Suggested First Work Session

1. Make result hydration detached and atomic.
2. Batch message hydration across the full result set.
3. Enforce service-owned user scope for message refs.
4. Canonicalize durable message evidence.
5. Fix schema descriptions and positive constraints.

Defer path-observation behavior and configuration lifecycle changes into their own explicitly reviewed commits.
