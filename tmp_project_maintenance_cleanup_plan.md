# Project Maintenance Cleanup Plan

## Scope

Deep-review and clean the application-owned maintenance boundary without moving
read-only status reporting into it. Preserve the intended split:

```text
Health = bounded status and drill-down
Maintenance = explicit inspection, preview, retry, repair, and reviewed mutation
```

Associated areas that must be inspected with `ProjectMaintenanceService`:

- `ProjectManager` lifecycle operations and its shared maintenance lock
- user-global `EntityMaintenanceService`
- semantic-window inspection and manual retry
- session/exchange blockage inspection and recovery
- maintenance reviews and impact previews
- entity cleanup, reclassification, relationship normalization, and rebuilds
- conflict discovery, reports, advisories, and resolutions
- projection/embedding repair markers
- agent maintenance tools
- application port and public maintenance contracts
- health projections that report repairable conditions

## Findings already recorded in the journal

- Review semantic-window inspection and retry.
- Add or verify inspection and recovery for orphaned session work.
- Review durable maintenance proposals and confirmation workflows.
- Review entity cleanup and user-global entity merge/rollback.
- Review historical entity reclassification and relationship normalization.
- Review conflict discovery, conflict groups, and relationship advisories.
- Review projection and embedding rebuilds.
- Keep durable diagnostics behind Health or Maintenance instead of exposing raw
  storage methods.
- Verify lifecycle locking and runtime/cache invalidation.
- Do not delete diagnostic storage capabilities before their application owner
  is settled.

## Additional findings from associated-code inspection

### 1. Session-blockage maintenance is still missing

The journal expanded Maintenance to include orphaned open exchanges and stalled
session work, but the current maintenance boundary only retries semantic windows.
There is no project maintenance operation that inspects or repairs FIFO-blocking
open exchanges.

First define a bounded diagnostic result and conservative repair policy. The
repair must distinguish genuinely active work from abandoned durable state and
must coordinate with `SessionManager` ownership before closing anything.

### 2. Agent conflict reporting bypasses the maintenance boundary

`MaintenanceTools.report_relationship_conflict()` constructs
`ConflictWriter`/`ConflictService` directly. This bypasses
`ProjectMaintenanceService.record_conflict_detection()`, its project validation,
and any lifecycle/locking policy added there later.

Inject and call the application-owned maintenance service from the agent tool.
Keep the agent tool as a thin presentation adapter.

### 3. Relationship advisory “read” has hidden writes

`get_relationship_advisories()` reads evidence but also materializes pending
advisory reviews. Its name and placement make a mutating refresh look like a
plain query, and it currently does not hold the maintenance lock.

Separate deterministic advisory discovery from explicit materialization, or
rename the operation as a refresh and apply the correct lock/idempotency policy.

### 4. Conflict discovery completion needs a clearer ownership contract

`complete_conflict_discovery()` accepts a package containing its own user/project
scope, performs durable writes, and advances the cursor without revalidating the
project through the service boundary or using the maintenance lock. Notifications
happen after commit, so a notification failure can make a committed discovery run
look failed and be retried.

Review package freshness, scope validation, concurrency with lifecycle changes,
cursor advancement, and post-commit notification failure semantics. Preserve the
existing atomic conflict-write/cursor transaction.

### 5. Lock scope is broader and less explicit than the service name suggests

One `asyncio.Lock` serializes project-scoped maintenance, user-global entity
maintenance, and ProjectManager lifecycle changes. Some read-only previews take
the lock, while several durable conflict/advisory mutations do not.
`_require_no_active_runtimes()` also checks leases for every project rather than
only the target project.

Document which invariants are application-global and which are project-local,
then make locking match those invariants. Do not narrow the lock until tests prove
that shared entity identity, domain activation, and live resolver caches remain
safe.

### 6. Most repair operations are backend-only

The application port exposes maintenance-review decisions and global merge
rollback, but not semantic-window retry, blockage inspection/repair, entity
cleanup, historical reclassification, relationship normalization, embedding
rebuild, projection repair, advisory decisions, or conflict resolution.

Apply the journal's SDK rule: preserve valid backend capabilities, but expose
typed user concepts through the application port instead of leaking services or
storage methods. Decide which operations are product features before deleting
anything.

### 7. Agent maintenance errors bypass the canonical tool failure contract

`MaintenanceTools` broadly catches exceptions and returns their raw strings as
successful tool results. This can expose internal details and prevents the tool
runtime from assigning the correct retryability/error category.

Expected user-facing conflicts should become typed safe errors. Unexpected
failures should propagate to the canonical tool error boundary.

### 8. “Graph health” is actually maintenance discovery

`check_graph_health()` searches for possible duplicate entities and proposes
repair candidates. That is a maintenance inspection, not engine health status,
and its current name blurs the Health/Maintenance boundary.

Rename it when this subsystem's public/tool contract is settled; no compatibility
alias is needed under the current naming policy.

## Proposed work order and commit units

### Unit 1: Maintenance ownership and inventory

- Classify every service method as status, inspection, preview, reviewed mutation,
  direct repair, or internal job operation.
- Record project-local versus user-global invariants.
- Identify production callers and public gaps before deleting wrappers.
- Add focused ownership/lock tests for the intended boundary.

### Unit 2: Route agent maintenance through the canonical boundary

- Inject project maintenance into agent tools.
- Route conflict reporting through `record_conflict_detection()`.
- Replace broad raw-error returns with the canonical tool failure contract.
- Rename `check_graph_health()` to an explicit maintenance-inspection name.

### Unit 3: Conflict discovery and advisory consistency

- Validate conflict package scope/freshness at completion.
- Define lock behavior for discovery completion and direct conflict mutations.
- Keep conflict writes and cursor advancement atomic.
- Treat notification as a post-commit side effect with explicit retry/reporting.
- Split advisory discovery from materialization or rename it as a refresh.

### Unit 4: Semantic and session blockage diagnostics

- Add typed bounded inspection for failed/exhausted semantic windows.
- Keep manual semantic retry durable and idempotent.
- Design orphaned-exchange inspection using durable timestamps/state plus live
  session ownership.
- Add a conservative reviewed or explicit repair operation for abandoned work.
- Ensure Health reports the condition without performing the repair.

### Unit 5: Lock and runtime/cache invariants

- Test maintenance versus project activation/archive/delete/domain transitions.
- Decide whether inactive-runtime requirements are target-project or global.
- Verify entity cleanup, merge, rollback, reclassification, and normalization
  invalidate every affected live resolver/projection.
- Avoid holding a global lock across long read-only work unless it protects a
  documented snapshot invariant.

### Unit 6: Application/public maintenance contract

- Add typed application-port requests/responses for approved repair workflows.
- Expose user concepts, not `KnowledgeStore` or runtime objects.
- Keep previews separate from confirmed mutations.
- Preserve expected-state tokens and idempotency/conflict behavior.
- Leave unfinished or intentionally internal operations clearly documented.

### Unit 7: Final maintenance/health audit

- Confirm Health is read-only and bounded.
- Confirm Maintenance owns every repair and reviewed mutation.
- Remove only wrappers proven superseded after the public contract is chosen.
- Run maintenance, project lifecycle, health drill-down, API, and relevant real
  PostgreSQL contract suites.

## Decisions to settle before implementation

- What durable age/state makes an open exchange eligible for orphan inspection,
  and what live-owner evidence prevents repair?
- Should conflict discovery share the global lifecycle lock, use a project-local
  lock, or rely on transactional cursor/evidence freshness checks?
- Are historical reclassification and embedding rebuild safe when unrelated
  project runtimes are active?
- Which backend repair operations are intended for the first SDK/application-port
  surface?
- Should advisory materialization be automatic background work or an explicit
  maintenance command?

## Non-goals

- Do not turn Health into a repair API.
- Do not let agents directly apply destructive maintenance.
- Do not expose storage readers/writers as the SDK contract.
- Do not redesign entity identity, semantic windows, or session execution inside
  this review.
- Do not delete backend capabilities merely because the SDK is unfinished.

## Verification

- Project maintenance application-contract tests
- Global entity maintenance and rollback tests
- Conflict discovery persistence and job tests
- Semantic-window admission/retry tests
- Session lifecycle/orphan-recovery tests
- Project lifecycle and lock-interleaving tests
- Health drill-down and runtime-health tests
- Application-port/public-contract tests
- Real PostgreSQL maintenance contracts where available
- Ruff and `git diff --check`
- One commit after each completed implementation unit
