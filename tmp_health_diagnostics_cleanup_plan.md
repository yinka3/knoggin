# Health and Diagnostics Cleanup Plan

## Scope

Deep-review the read-only runtime health boundary while preserving the split:

```text
Health = bounded status of the running system
Maintenance = inspect, explain, retry, and repair durable state
```

Associated areas inspected with `RuntimeHealthService`:

- health schemas and sanitization
- engine lifecycle and dependency probes
- project runtime, scheduler, and background-work snapshots
- semantic-window and Context-projection durable metrics
- document-indexing progress
- entity projection-repair summary
- conflict-discovery status
- agent health tools and tool schemas
- application port, SDK, and UI health route
- unit, integration, and storage health contracts

## Findings already recorded in the journal

- Combine live runtime facts with durable facts without turning Health into a
  repair API.
- Keep detailed durable inspection and repair under Maintenance.
- Keep `get_semantic_window_health()`, but move its SQL from `KnowledgeStore`
  into `SemanticWindowReader` and retain only a forwarding facade method.
- Expose meaningful Health and Maintenance concepts through the SDK rather
  than exposing raw storage readers.
- Do not create a separate Debug service yet.
- Health tools may intentionally return structured degraded status instead of
  treating degraded health as a tool-execution failure.

## Additional findings from associated-code inspection

### 1. Project health is not a real public application contract

The agent can request engine, resource, ingestion, and background health, but
the SDK and UI expose only engine health. `ApplicationRuntimePort` exposes no
health methods. A caller therefore cannot inspect project ingestion or
background status without going through an agent tool or reaching into the
runtime.

Define typed public health operations for the intended SDK surface. Keep
runtime objects and storage-shaped details private.

### 2. Ingestion health accepts but does not use `session_id`

`get_ingestion_health()` requires `session_id`, but never reads or validates
it. The method is actually project-scoped while its signature suggests that it
checks a session and its ownership.

Choose one honest contract:

- remove `session_id` and make ingestion health explicitly project-scoped; or
- validate the live/durable session belongs to the requested user and project
  if session-scoped health is required.

Do not retain a security-looking parameter that has no effect.

### 3. Semantic-window health SQL still bypasses the focused reader

The journal already chose the intended ownership, but the move was not made.
`KnowledgeStore.get_semantic_window_health()` still executes SQL directly.
Move the query to `SemanticWindowReader`; keep the store method only as a thin
health-facing facade.

### 4. Resource activity is treated as degraded health

`get_resource_health()` marks any queued work as degraded, even when capacity
is available and the queue is moving normally. Queue presence should usually
produce `activity=busy`; degradation should require pressure evidence such as
exhausted capacity, waiters, a full queue, excessive age, or unavailable
metrics.

This currently makes healthy load look unhealthy and weakens the meaning of
the status field.

### 5. Ingestion delay semantics are incomplete

The code checks for `delay_state == "delayed"`, but no branch ever assigns that
state. A pending window is always `unknown` unless the scheduler reports a
stall. The durable oldest-pending age is calculated but never used to decide
whether work is delayed.

Define one threshold owner, or remove the dead delayed state until a threshold
exists. Do not invent a second timeout that disagrees with scheduler policy.

### 6. Engine failure classification contains unreachable logic

`dependency_failure_count >= 2` cannot currently be true because only the
PostgreSQL probe contributes to `failures`. The fallback also makes an idle
runtime fail solely because an optional subsystem is absent. Classify required
and optional dependencies explicitly and make status independent of whether a
user currently has a project/session loaded.

### 7. Component snapshots are not uniformly bounded

Async durable probes use `_bounded_read()`, but synchronous component snapshot
methods are called inline with no elapsed-time guard. Current implementations
are cheap in-memory reads, so this is not an immediate bug, but the contract is
implicit. Document that snapshot methods must be non-blocking and side-effect
free, and test that async work cannot accidentally enter this path.

### 8. Conflict-discovery health is displayed but not interpreted

Background health includes the conflict-discovery snapshot, but none of its
fields affect status or warnings. Decide which states are informational and
which mean assisted discovery is unavailable. Avoid showing a detailed block
that the aggregate status silently ignores.

### 9. Health-tool failures are safely redacted but invisible operationally

Agent health tools broadly convert all service exceptions into a safe degraded
snapshot. This is appropriate for their result contract, but there is no
bounded internal log or error category, so a programming error looks identical
to a temporarily unavailable probe. Preserve the safe response while recording
the internal failure without user data.

### 10. The generic `details` map weakens contract stability

`HealthSnapshot` strongly types the envelope but leaves every subsystem payload
as an arbitrary dictionary. Sanitization is strong and should remain, but the
public SDK cannot rely on stable drill-down shapes. Introduce typed detail
models only for the fields approved for the public boundary; do not model every
internal scheduler field.

## Proposed work order and commit units

### Unit 1: Ownership and contract inventory

- Classify every reported field as live status, durable status, maintenance
  pointer, or internal debug detail.
- Record which dependencies are required versus optional.
- Record current callers and decide project-scoped versus session-scoped health.
- Add boundary tests that prove Health performs no writes or repairs.

#### Unit 1 inventory result

- Engine health is application-scoped. PostgreSQL and the runtime-owned model,
  background, knowledge-store, executor, embedding, and LLM dependencies are
  required after successful startup. Projection-repair health is an optional
  diagnostic read and its absence must not imply that repair succeeded.
- Resource health is project-filtered capacity status over application-owned
  coordinators. Queue activity is live status, not durable maintenance state.
- Ingestion health is project-scoped: semantic windows and Context projection
  are both project aggregates. The unused `session_id` parameter was removed
  instead of preserving a false session-ownership check.
- Background health combines a loaded ProjectRuntime snapshot with a bounded
  durable document count. It reports status only; document recovery remains
  owned by the document subsystem and durable repair remains Maintenance work.
- Agent tools are presentation adapters. The SDK/application port are the
  correct public owners; storage and runtime objects are not public contracts.
- Existing drill-down integration coverage proves the health path performs no
  writes. Later units will retain that invariant while tightening status rules.

### Unit 2: Durable health read ownership

- Move semantic-window health SQL into `SemanticWindowReader`.
- Keep a narrow `KnowledgeStore` forwarding method for the health service.
- Verify bounded aggregate queries expose no identifiers or content.
- Keep blockage inspection and repair in Maintenance.

#### Unit 2 result

- Semantic-window health aggregation now belongs to `SemanticWindowReader`.
- `KnowledgeStore` retains a narrow forwarding method because Health consumes
  the persistence facade rather than a raw reader.
- The aggregate remains project-scoped and returns counts/timestamps only; it
  exposes no window, session, message, or content identifiers.
- Failed-window details and orphaned-exchange inspection remain Maintenance
  operations and were not added to Health.

### Unit 3: Status versus activity semantics

- Treat ordinary moving work as busy rather than degraded.
- Define genuine resource pressure and unavailable-capacity conditions.
- Resolve the dead ingestion `delayed` state using existing scheduler policy or
  remove it until a single threshold exists.
- Make engine required/optional dependency classification explicit.

#### Unit 3 result

- Ordinary queued or active work is now `busy` rather than degraded.
- Resource health degrades only for missing capacity metrics, database waiters,
  model work queued at active capacity, or a full background queue.
- Engine startup dependencies are explicit: if any runtime-owned core
  dependency is unavailable after startup, engine status is failed regardless
  of whether a project or session happens to be loaded.
- Projection repair obligations and runtime closing remain degraded states.
- The unreachable ingestion `delayed` branch was removed. Scheduler stall
  detection remains the single current owner of delayed semantic-work status.

### Unit 4: Project scope and live/durable correlation

- Settle and enforce the unused `session_id` contract.
- Verify project runtime absence, shutdown, and archived/inactive cases return
  honest bounded status.
- Interpret conflict-discovery availability consistently with its configured
  mode.
- Document synchronous snapshot methods as cheap, read-only operations.

#### Unit 4 result

- Unloaded project runtimes now report an explicit degraded status instead of
  claiming a missing semantic job is a failed engine component.
- Ingestion details state whether the project runtime is loaded while durable
  project aggregates remain readable and bounded.
- Assisted conflict discovery affects background status only when it is both
  configured and expected to run. Manual or intentionally disabled discovery
  remains informational.
- Component `snapshot_for_health` methods are required to be synchronous,
  non-blocking, side-effect-free memory views. Accidentally async snapshots are
  rejected as unavailable rather than awaited inside Health.

### Unit 5: Tool failure and redaction contract

- Keep degraded health as successful structured tool data.
- Log unexpected service/tool adapter failures safely and without payloads.
- Verify health sanitization removes IDs, URLs, credentials, tracebacks, and
  unbounded component data.
- Confirm malformed component snapshots cannot escape the public envelope.

#### Unit 5 result

- Agent health tools still return a valid degraded `HealthSnapshot` when a
  health read fails; degraded health remains data rather than a tool error.
- Unexpected adapter failures now record the operation name and exception type
  only. Exception messages, tracebacks, arguments, and user/project/session
  scope are not logged by this boundary.
- Invalid dictionary snapshots follow the same safe categorization path before
  returning the public fallback envelope.
- Existing schema sanitization continues to bound detail depth, item counts,
  strings, warnings, and sensitive key/value patterns.

### Unit 6: Application and SDK health surface

- Add approved typed engine/project health operations to the application port.
- Expose resource, ingestion, and background health through the SDK if they are
  product-facing.
- Return typed user concepts rather than raw runtime/service objects.
- Align the UI health route with actual engine status instead of always wrapping
  it in top-level `status: "ok"`.

#### Unit 6 result

- The application port now owns typed engine, resource, ingestion, and
  background health operations.
- Project health operations validate user ownership and project existence
  before reading runtime status.
- The direct SDK exposes all four health concepts as stable JSON snapshots; it
  does not expose the service or runtime objects themselves.
- The UI health route now mirrors the engine snapshot status instead of always
  claiming top-level success.
- The dependency-injected HTTP API exposes typed engine and project health
  routes backed by the same application port rather than reaching into runtime
  internals.

### Unit 7: Final Health/Maintenance boundary audit

- Confirm Health remains read-only and bounded.
- Confirm every repair link points to Maintenance rather than performing work.
- Remove dead branches and wrappers only after caller verification.
- Run health, maintenance, runtime lifecycle, SDK/API, and relevant PostgreSQL
  contract suites.

#### Unit 7 result

- Health remains read-only: it probes, aggregates, sanitizes, and classifies;
  it does not retry, wake, reconcile, or mutate a subsystem.
- Durable failed-window and orphaned-exchange details remain under Maintenance,
  including every explicit repair operation.
- Live runtime snapshots are synchronous bounded views; durable reads use
  timeout-bounded async probes.
- Agent tools, HTTP routes, the application port, SDK, and UI all consume the
  same `HealthSnapshot` envelope and expose no runtime or storage objects.
- The final cross-subsystem audit passed 82 Health, Maintenance, runtime
  lifecycle, integration, and API tests.

## Decisions to settle during implementation

- Is ingestion health project-scoped or session-scoped?
- Which runtime dependencies are required for engine health?
- What existing scheduler signal, if any, owns delayed semantic-work status?
- Is assisted conflict discovery part of project health or informational only?
- Which drill-down shapes belong in the first typed SDK contract?

## Non-goals

- Do not add repair methods to Health.
- Do not expose raw scheduler, runtime, reader, or storage objects.
- Do not create a separate Debug service.
- Do not report ordinary queue activity as a failure merely because work exists.
- Do not expose durable identifiers or user content in health payloads.

## Verification

- Runtime health-service unit tests
- Health schema and sanitization tests
- Agent health-tool tests
- Health drill-down integration tests
- Application-port, API, and SDK contract tests
- Semantic-window storage contracts
- Project/session lifecycle and shutdown tests
- Maintenance boundary tests
- Ruff and `git diff --check`
- One commit after each completed unit
