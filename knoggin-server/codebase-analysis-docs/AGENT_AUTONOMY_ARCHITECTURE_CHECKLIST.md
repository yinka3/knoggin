# Agent Autonomy Architecture Checklist

This checklist tracks the architectural issues identified while reviewing the
shift from Jinja templates, structured rules/preferences, and background jobs
to Markdown prompts, an editable agent identity, agent-managed topics, and
more autonomous tools.

## Guiding Boundary

- [x] Document the core rule: the model may make semantic decisions, while
  Python enforces permissions, invariants, validation, and destructive-action
  safety.
- [x] Ensure every durable value has one authoritative source.
- [x] Ensure Redis can be flushed without losing durable user knowledge.

## 1. Agent Identity and Brain

- [x] Choose one runtime location and storage model for mutable agent identity.
  Packaged templates should seed identities, not serve as mutable production
  state. Postgres `agents.brain` is the durable Brain.
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
- [x] Keep a Brain revision counter and periodic restore snapshots. Snapshots
  are stored at revision 1, every 5 revisions, and every section restore.
- [x] Decide which sections the agent may edit autonomously and which require
  user confirmation.

### Identity Acceptance Criteria

- [x] An agent edit is visible on its next run.
- [x] Editing one agent cannot modify another agent's identity.
- [x] A malformed edit cannot remove system safety policy.
- [x] Brain restore points record actor, timestamp, revision, change type,
  changed section, deterministic summary, and restored-from revision when
  applicable. Ordinary edits increment the live revision counter but are not
  complete history.

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
- [x] Add deterministic pre-merge checks:
  - Both entities still exist.
  - Both entities are visible in the authorized project scope.
  - Neither entity is the protected identity entity.
  - Entity types are compatible.
  - Stable identifiers do not conflict.
  - Important facts and timelines require confirmation when nuanced.
  - The candidate has not changed since it was reviewed.
- [x] Return an explicit policy result:
  - `executed`
  - `confirmation_required`
  - `rejected`
- [x] Require explicit user confirmation for ambiguous or destructive merges.
- [x] Use a server-issued confirmation token tied to the exact entity IDs and
  revisions.
- [x] Make merges reversible where practical.
- [x] Preserve aliases, source facts, relationships, and provenance.
- [x] Store a complete merge audit record, including before/after state.
- [x] Provide an administrative rollback path.
- [x] Start with conservative automatic-merge rules and widen them only from
  observed evidence.

### Merge Acceptance Criteria

- [x] The model cannot merge the identity entity.
- [x] The model cannot bypass confirmation by claiming high confidence.
- [x] A stale proposal cannot merge entities that changed after review.
- [x] Every completed merge can be explained and traced to evidence.
- [x] A mistaken merge has a documented recovery procedure.

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
- [x] Make queue processing idempotent where Redis retries are possible.
- [x] Configure an explicit Docker memory limit.
- [x] Choose and document an eviction policy.
- [x] Decide whether Redis persistence is disabled or best-effort.
- [x] Ensure Redis loss causes degraded performance or delayed work, not loss
  of durable user knowledge.

Implementation notes:

- Postgres is the durable owner for projects, project membership/read scopes,
  sessions, agents, agent Brain snapshots, topic configuration, messages,
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
  minimal and only track event families that support inspection of Redis-backed
  work transitions.
  - Format: append-only logfmt/key-value lines with timestamp, event type,
    label, retention class, user, project, session/job IDs, durable Postgres
    IDs, Redis key names, reason codes, and bounded error metadata.
  - Rule: the log may include actual event details for inspection, but durable
    data must still live in Postgres. Log snapshots are evidence/debug context,
    not authoritative state.
  - Initial event families:
    - DLQ item created/replayed/parked.
    - Dirty entities marked/cleared for profile refinement.
    - Optional next: merge proposal queued/removed and maintenance deferred.
  - Event-log UI goal: read-only list/filter/search plus inspect-one-entry
    details. Recovery helpers belong to later Redis idempotency/recovery work.
  - Non-goal for now: automatic replay, rollback guarantees, ordering
    guarantees, corruption handling, dashboards, analytics, or treating the log
    as a source of truth.
- Reuse the existing emitted event stream as the producer-side vocabulary, but
  persist only selected inspection-grade events. See
  `codebase-analysis-docs/COORDINATION_EVENT_LOG_AUDIT.md` for the event-by-event
  classification.

### Storage Acceptance Criteria

- Verification helper: run `scripts/verify_storage_ownership.py --seed` for a
  non-destructive proof setup/report, then rerun with `--flush-redis` and the
  explicit confirmation phrase against a disposable local Redis DB to prove the
  Redis flush criterion.
- [x] Flush Redis in a development environment and confirm durable state
  remains intact.
- [x] Restart Redis and confirm sessions recover or fail cleanly.
- [x] Restart the full stack and confirm agents, topics, documents, and merge
  history remain available.
- [x] Verify no durable field has competing writable copies in both Postgres
  and Redis.

## 7. Heartbeats and Autonomous Maintenance

- [x] Keep heartbeat counters in Redis.
- [x] Make maintenance eligibility a Python decision.
- [x] Let the model decide how to handle presented maintenance candidates.
- [x] Ensure a heartbeat request cannot force a tool that is disabled or
  unavailable.
- [x] Mark maintenance as handled only after successful completion.
- [x] Prevent normal user responses from being blocked by failed maintenance.
- [x] Separate topic evaluation cadence from graph merge-maintenance cadence.
- [x] Add bounded retries and cooldowns for failed maintenance.

Implementation notes:

- Heartbeat counters remain Redis-owned runtime state. User turns increment
  session and project heartbeat counters; maintenance lifecycle uses additional
  Redis-only attempt and cooldown keys.
- Python now builds `MaintenanceCandidate` entries before an agent run. Topic
  evaluation and graph merge scans are separate candidates with separate
  reasons, priorities, tools, attempt counters, and cooldown state.
- Candidate eligibility checks tool availability before presenting work to the
  model. A heartbeat can create an eligible candidate only when `update_topics`
  is actually available for that run; graph merge scans require
  `check_graph_health`.
- The executor now presents maintenance as optional context, not a mandatory
  command. It no longer auto-adds `update_topics` or any other maintenance tool.
- Maintenance is marked handled only after a successful relevant tool result.
  Failed maintenance records an attempt and cooldown, removes the candidate
  from the current run, and does not by itself terminally fail the user response.

Manual verification commands:

Status: passing in local manual verification.

```bash
cd knoggin-server
uv run pytest -q \
  tests/agent/test_maintenance_candidates.py \
  tests/agent/test_agent_executor_step_contract.py \
  tests/agent/test_agent_executor_tools_contract.py \
  tests/agent/test_agent_executor_loop_contract.py \
  tests/agent/test_orchestrator.py \
  tests/unit/infrastructure/test_redis_keys.py
```

```bash
cd knoggin-server
uv run ruff check \
  src/knoggin_server/agent/maintenance.py \
  src/knoggin_server/agent/orchestrator.py \
  src/knoggin_server/agent/executor.py \
  src/knoggin_server/agent/types.py \
  src/infrastructure/redis_client.py \
  tests/agent/test_maintenance_candidates.py \
  tests/agent/test_agent_executor_step_contract.py \
  tests/agent/test_agent_executor_tools_contract.py \
  tests/agent/test_agent_executor_loop_contract.py \
  tests/agent/test_orchestrator.py \
  tests/unit/infrastructure/test_redis_keys.py
```

## 8. Verification and Cleanup

Testing is intentionally deferred while the architecture is being reconciled,
but these contracts should be restored before the redesign is considered
complete.

- [x] Update prompt contract tests to reflect the Markdown identity design.
- [x] Add a contract test asserting the identity body reaches the system prompt.
- [x] Add read/edit/read brain round-trip coverage.
- [x] Add schema/dispatch/implementation consistency coverage.
- [x] Add topic guard and per-project isolation coverage.
- [x] Add destructive capability and confirmation-token coverage.
- [x] Add merge rejection tests for identity, type conflicts, stale revisions,
  and contradictory identifiers.
- [x] Add Redis flush/recovery integration coverage.
- [x] Remove stale imports, parameters, schemas, and tests from the previous
  memory architecture.
- [x] Update architecture documentation after the final ownership boundaries
  are implemented.

Implementation notes pending full manual test verification:

- Markdown Brain prompt coverage lives in
  `tests/agent/test_agent_prompt_contract.py`; it asserts the current Brain is
  rendered inside `<agent_brain>` and does not reintroduce the old nested
  `<instructions>` wrapper.
- Brain tool coverage lives in `tests/knowledge/test_memory_service.py`; it now
  includes an explicit read/edit/read round trip against the durable agent row,
  plus stale revision, section allow-list, snapshot metadata, and section
  restore checks.
- Schema, dispatch, implementation, old memory-tool cleanup, direct merge
  exposure, and destructive confirmation coverage live in
  `tests/agent/test_tool_dispatch_contract.py` and
  `tests/agent/test_autonomy_architecture_cleanup_contract.py`.
- Topic scope and guard coverage lives in
  `tests/runtime/test_topic_config_job_scope_contract.py` and
  `tests/unit/common/test_topics_config.py`.
- Merge proposal, confirmation token, stale-state, protected identity,
  visibility, type, and stable identifier coverage lives in
  `tests/knowledge/test_entity_merge_classification_contract.py`,
  `tests/knowledge/test_merge_detection_job_contract.py`, and
  `tests/storage/test_graph_writer_contract.py`.
- Redis/Postgres ownership coverage lives in
  `tests/storage/test_storage_ownership_verifier.py` and the manual
  `scripts/verify_storage_ownership.py` flow documented in Section 6.
- Stale `save_memory` / `forget_memory` tool exposure is now guarded against;
  Brain tools are the supported agent identity interface.

Manual verification commands:

```bash
cd knoggin-server
uv run pytest -q \
  tests/agent/test_agent_prompt_contract.py \
  tests/knowledge/test_memory_service.py \
  tests/agent/test_tool_dispatch_contract.py \
  tests/agent/test_autonomy_architecture_cleanup_contract.py \
  tests/runtime/test_topic_config_job_scope_contract.py \
  tests/unit/common/test_topics_config.py \
  tests/knowledge/test_entity_merge_classification_contract.py \
  tests/knowledge/test_merge_detection_job_contract.py \
  tests/storage/test_graph_writer_contract.py \
  tests/storage/test_storage_ownership_verifier.py
```

```bash
cd knoggin-server
uv run ruff check \
  tests/agent/test_agent_internals_contract.py \
  tests/agent/test_autonomy_architecture_cleanup_contract.py \
  tests/knowledge/test_memory_service.py
```

## Suggested Order

- [x] Phase 1: Fix broken wiring between identity, prompts, schemas, and tools.
- [x] Phase 2: Establish single sources of truth for identities and topics.
- [x] Phase 3: Implement capability enforcement and destructive confirmation.
- [x] Phase 4: Introduce proposal-based, reversible entity merging.
- [x] Phase 5: Harden Redis/Postgres recovery boundaries.
- [x] Phase 6: Reconcile tests and remove obsolete architecture.
