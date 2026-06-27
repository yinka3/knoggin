# Agent Autonomy Architecture Checklist

This checklist tracks the architectural issues identified while reviewing the
shift from Jinja templates, structured rules/preferences, and background jobs
to Markdown prompts, an editable agent identity, agent-managed topics, and
more autonomous tools.

## Guiding Boundary

- [ ] Document the core rule: the model may make semantic decisions, while
  Python enforces permissions, invariants, validation, and destructive-action
  safety.
- [x] Ensure every durable value has one authoritative source.
- [x] Ensure Redis can be flushed without losing durable user knowledge.

## 1. Agent Identity and Brain

- [x] Choose one runtime location and storage model for mutable agent identity.
  Packaged templates should seed identities, not serve as mutable production
  state. Postgres `agents.instructions` is the durable Brain.
- [x] Fix the read/write path mismatch:
  - Read currently targets `templates/{agent_id}.md`.
  - Write currently targets `templates/agents/{agent_id}.md`.
- [x] Pass the loaded Markdown identity body into the actual system prompt.
- [x] Ensure `persona`, `instructions`, and Markdown identity fields do not
  conflict or silently override one another. `persona` is the stable,
  user-editable cognitive differentiator; the Markdown Brain owns evolving
  self-conception, directives, context, and lessons.
- [x] Keep persona editable through agent settings while excluding it from
  autonomous Brain edits.
- [x] Expose persona to settings as five structured fields: attention bias,
  reasoning style, social temperament, communication signature, and productive
  flaw.
- [x] Make the five-field `PersonaProfile` the public `AgentConfig` and agent
  creation contract; render Markdown only at the Postgres/prompt boundary.
- [x] Allow AAC agents to define a persona only while spawning a new
  specialist; do not expose an autonomous tool for editing existing personas.
- [x] Resolve custom agents from their authoritative Postgres records rather
  than silently falling back to `AGENT_IDENTITY.md`.
- [x] Add schemas for `read_brain` and `edit_brain`.
- [x] Remove or intentionally retain the obsolete `save_memory` and
  `forget_memory` schemas.
- [x] Enforce read-before-write using a revision/version token rather than only
  a prompt instruction.
- [x] Restrict edits to recognized Markdown sections.
- [x] Preserve immutable engine policy outside the editable identity.
- [x] Add size limits for the full identity and individual sections.
- [x] Use atomic writes or transactional database updates.
- [ ] Record identity revisions and allow rollback. Revisions are now recorded;
  a rollback operation remains to be added.
- [x] Decide which sections the agent may edit autonomously and which require
  user confirmation.

### Identity Acceptance Criteria

- [x] An agent edit is visible on its next run.
- [x] Editing one agent cannot modify another agent's identity.
- [x] A malformed edit cannot remove system safety policy.
- [ ] Every edit has an actor, timestamp, previous revision, and new revision.

## 2. Prompt Architecture

- [x] Keep pipeline prompts as readable Markdown with stable named sections.
- [x] Validate required prompt sections at startup rather than failing only
  during ingestion.
- [x] Detect unresolved `{placeholder}` values after prompt rendering.
- [x] Clarify which content belongs in:
  - Engine system policy.
  - Agent identity.
  - Per-run directives.
  - Retrieved context.
- [x] Remove parameters that are accepted but never injected into the prompt.
- [x] Review prompt files for encoding corruption and normalize them to UTF-8.
  Prompt loading now rejects common corruption markers.
- [x] Decide whether prompt caching needs an invalidation mechanism during
  development. Cached files are reparsed when their modification time changes.

## 3. Topics and Configuration

- [x] Establish one source of truth for project topics.
- [x] Reconcile `topics.yaml` with the Postgres-backed `projects.topic_config`.
- [x] Treat `topics.yaml` as either a seed/default file or remove it from the
  runtime update path.
- [x] Make `update_topics` operate on the current user and project, not a shared
  repository file.
- [x] Apply updates through `TopicConfig` so its derived caches are invalidated.
- [x] Restore deterministic topic guards around model proposals:
  - Protect `General` and `Identity`.
  - Prevent accidental removal of existing topics.
  - Reject bulk deactivation.
  - Cap topic creation per evaluation.
  - Normalize and validate names, labels, and aliases.
  - Preserve or explicitly validate hierarchy changes.
- [x] Reset the Redis heartbeat only after the durable Postgres update commits.
- [x] Prevent repeated mandatory topic updates during every reasoning step of
  the same run.

### Topic Acceptance Criteria

- [x] Topic changes are scoped to one project.
- [x] Restarting the application preserves topic changes.
- [x] Flushing Redis does not remove topic configuration.
- [x] A hostile or malformed model proposal cannot deactivate protected topics
  or replace the entire configuration.

## 4. Tool Registry and Capability Model

- [x] Make tool schemas, dispatch entries, and concrete implementations agree.
- [x] Register `MaintenanceTools` with the main `Tools` implementation.
- [x] Add dispatch entries for `check_graph_health` and `propose_entity_merge`, or
  remove their schemas until implemented.
- [x] Ensure every schema offered to the model is executable.
- [x] Ensure every intended executable tool has a schema.
- [x] Replace broad `write` tags with explicit capability classes:
  - `read`
  - `reversible_write`
  - `configuration_write`
  - `identity_write`
  - `destructive_write`
- [x] Enforce capability classes in Python; do not treat tags as descriptive
  metadata only.
- [x] Define safe default capabilities when `enabled_tools` is absent.
- [x] Do not expose destructive tools by default.
- [x] Validate tool arguments against their schemas before dispatch.
- [x] Add authorization context to tool execution: user, agent, project,
  session, run, and confirmation state.
- [x] Record an audit event for every write tool call and its result.

Implementation notes:

- Tool schemas now carry one authoritative `capability` value. Domain `tags`
  remain descriptive and are not used as permission grants.
- Missing `enabled_tools` uses all non-destructive capabilities. A destructive
  tool must be explicitly named and still cannot execute unless runtime
  confirmation state is `confirmed`.
- The registry validates schema, dispatch, parameter, capability, and concrete
  method consistency during import.
- Runtime dispatch rechecks the exact schemas exposed for that run, validates
  arguments, and rejects model calls outside the allow-list.
- Every write starts a durable `agent_tool_audits` record before execution and
  records its sanitized result or failure afterward.

## 5. Entity Merge Autonomy

- [x] Let the model inspect candidates and make the semantic merge proposal.
- [x] Replace direct model access to `merge_entities` with a proposal-oriented
  interface such as `propose_entity_merge`.
- [x] Require evidence identifiers in a merge proposal, not only free-text
  reasoning and model confidence.
- [x] Treat model confidence as advisory, never as authorization.
- [ ] Add deterministic pre-merge checks:
  - Both entities still exist.
  - Both entities are visible in the authorized project scope.
  - Neither entity is the protected identity entity.
  - Entity types are compatible.
  - Stable identifiers do not conflict.
  - Important facts and timelines do not conflict.
  - The candidate has not changed since it was reviewed.
- [x] Return an explicit policy result:
  - `executed`
  - `confirmation_required`
  - `rejected`
- [x] Require explicit user confirmation for ambiguous or destructive merges.
- [x] Use a server-issued confirmation token tied to the exact entity IDs and
  revisions.
- [ ] Make merges reversible where practical.
- [x] Preserve aliases, source facts, relationships, and provenance.
- [x] Store a complete merge audit record, including before/after state.
- [ ] Provide an administrative rollback path.
- [x] Start with conservative automatic-merge rules and widen them only from
  observed evidence.

### Merge Acceptance Criteria

- [x] The model cannot merge the identity entity.
- [x] The model cannot bypass confirmation by claiming high confidence.
- [x] A stale proposal cannot merge entities that changed after review.
- [x] Every completed merge can be explained and traced to evidence.
- [ ] A mistaken merge has a documented recovery procedure.

## 6. Postgres and Redis Ownership

### Postgres: Durable Authority

- [x] Store projects and project membership in Postgres.
- [x] Store agents and durable agent identity revisions in Postgres or another
  explicitly durable user store.
- [x] Store project topic configuration in Postgres.
- [x] Store documents, knowledge records, permissions, and merge history in
  Postgres.
- [x] Keep foreign keys, uniqueness constraints, and protected-entity
  invariants in the database where possible.

### Redis: Ephemeral Coordination

- [x] Limit Redis to caches, queues, locks, counters, heartbeats, active-run
  state, and short-lived deduplication data.
- [x] Apply TTLs to temporary keys.
- [x] Namespace keys consistently by user, project, and session.
- [x] Define which Redis structures can be rebuilt from Postgres.
- [ ] Make queue processing idempotent where Redis retries are possible.
- [x] Configure an explicit Docker memory limit.
- [x] Choose and document an eviction policy.
- [x] Decide whether Redis persistence is disabled or best-effort.
- [x] Ensure Redis loss causes degraded performance or delayed work, not loss
  of durable user knowledge.

Implementation notes:

- Postgres is the durable owner for projects, project membership/read scopes,
  sessions, agents, agent brain revisions, topic configuration, messages,
  documents/chunks, graph records, permissions, merge proposals/audits, and
  tool-write audits.
- Redis is now documented in `RedisKeys` as cache/coordination only:
  rebuildable caches, ephemeral queues/locks/counters, and legacy
  non-authoritative key families.
- Session document focus and project search-index repair now read from
  Postgres instead of Redis.
- Session conversation caches, message-content caches, buffers, checkpoints,
  active discussions, dedup keys, job leases, and activity keys have bounded
  TTLs or lease-style expiry.
- Docker Redis is memory-bounded (`256mb`, container `512m`) with `allkeys-lru`
  eviction and persistence explicitly disabled.
- Follow-up risk: ingestion, DLQ, profile-refinement, and other Redis-backed
  work queues need a closer idempotency/rebuildability pass. Before relying on
  non-persistent Redis in production, confirm each queue either can be rebuilt
  from Postgres, only contains disposable work, or should move to a durable
  Postgres-backed job table.
- Add a small structured coordination inspection log, not a full WAL. Start
  minimal and only track event families that support a matching manual recovery
  operation.
  - Format: append-only JSONL with schema version, timestamp, event type,
    user, project, session/run/job IDs, durable Postgres IDs, Redis key names,
    short human-readable summary, and a bounded event snapshot.
  - Rule: the log may include actual event details for inspection, but durable
    data must still live in Postgres. Log snapshots are evidence/debug context,
    not authoritative state.
  - Initial event families:
    - DLQ item created/replayed/parked.
    - Dirty entities marked/cleared for profile refinement.
    - Optional next: merge proposal queued/removed and maintenance deferred.
  - Initial manual recovery operations:
    - Requeue a lost DLQ item when the log references durable message IDs or
      another durable Postgres input.
    - Re-mark dirty entities from logged entity IDs.
    - Manually replay a specific operation only when its durable inputs still
      exist in Postgres and the operation is idempotent or explicitly reviewed.
  - Non-goal for now: automatic replay, rollback guarantees, ordering
    guarantees, corruption handling, or treating the log as a source of truth.
- Reuse the existing emitted event stream as the producer-side vocabulary, but
  persist only selected recovery-grade events. See
  `codebase-analysis-docs/EVENT_PERSISTENCE_AUDIT.md` for the event-by-event
  classification.

### Storage Acceptance Criteria

- [ ] Flush Redis in a development environment and confirm durable state
  remains intact.
- [ ] Restart Redis and confirm sessions recover or fail cleanly.
- [ ] Restart the full stack and confirm agents, topics, documents, and merge
  history remain available.
- [ ] Verify no durable field has competing writable copies in both Postgres
  and Redis.

## 7. Heartbeats and Autonomous Maintenance

- [ ] Keep heartbeat counters in Redis.
- [ ] Make maintenance eligibility a Python decision.
- [ ] Let the model decide how to handle presented maintenance candidates.
- [ ] Ensure a heartbeat request cannot force a tool that is disabled or
  unavailable.
- [ ] Mark maintenance as handled only after successful completion.
- [ ] Prevent normal user responses from being blocked by failed maintenance.
- [ ] Separate topic evaluation cadence from graph merge-maintenance cadence.
- [ ] Add bounded retries and cooldowns for failed maintenance.

## 8. Verification and Cleanup

Testing is intentionally deferred while the architecture is being reconciled,
but these contracts should be restored before the redesign is considered
complete.

- [ ] Update prompt contract tests to reflect the Markdown identity design.
- [ ] Add a contract test asserting the identity body reaches the system prompt.
- [ ] Add read/edit/read brain round-trip coverage.
- [ ] Add schema/dispatch/implementation consistency coverage.
- [ ] Add topic guard and per-project isolation coverage.
- [ ] Add destructive capability and confirmation-token coverage.
- [ ] Add merge rejection tests for identity, type conflicts, stale revisions,
  and contradictory identifiers.
- [ ] Add Redis flush/recovery integration coverage.
- [ ] Remove stale imports, parameters, schemas, and tests from the previous
  memory architecture.
- [ ] Update architecture documentation after the final ownership boundaries
  are implemented.

## Suggested Order

- [ ] Phase 1: Fix broken wiring between identity, prompts, schemas, and tools.
- [ ] Phase 2: Establish single sources of truth for identities and topics.
- [ ] Phase 3: Implement capability enforcement and destructive confirmation.
- [ ] Phase 4: Introduce proposal-based, reversible entity merging.
- [ ] Phase 5: Harden Redis/Postgres recovery boundaries.
- [ ] Phase 6: Reconcile tests and remove obsolete architecture.
