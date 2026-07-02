# Agent Autonomy Sections 1-6 Remaining Work

This file tracks the unfinished work from sections 1-6 of
`AGENT_AUTONOMY_ARCHITECTURE_CHECKLIST.md`. Finish these items before moving on
to section 7, Heartbeats and Autonomous Maintenance, and section 8,
Verification and Cleanup.

Working assumption: this software has not been released yet. Prefer direct
schema and contract changes over compatibility shims or legacy migration paths
unless the existing local development data needs an explicit one-time cleanup.

## Scope

Included:

- Guiding boundary documentation.
- Agent Brain revision counter, periodic snapshots, and section restore.
- Entity merge deterministic checks and recovery.
- Redis queue idempotency and recovery logging.
- Storage ownership acceptance checks.

Excluded for now:

- Section 7 heartbeat and autonomous maintenance work.
- Section 8 final verification and cleanup work.

## Current Open Checklist Items

### Guiding Boundary

- [x] Document the core rule: the model may make semantic decisions, while
  Python enforces permissions, invariants, validation, and destructive-action
  safety.

### Agent Identity and Brain

- [x] Add Brain section restore support from periodic snapshots.
- [x] Keep `agents.brain_revision` as the live edit counter while storing full
  Brain snapshots only at revision 1, every 5 revisions, and every restore.

### Entity Merge Autonomy

- [ ] Finish deterministic pre-merge checks:
  - Both entities still exist.
  - Both entities are visible in the authorized project scope.
  - Neither entity is the protected identity entity.
  - Entity types are compatible.
  - Stable identifiers do not conflict.
  - Important facts and timelines do not conflict, or else force explicit
    review.
  - The candidate has not changed since it was reviewed.
- [ ] Make merges reversible where practical.
- [ ] Provide an administrative rollback path.
- [ ] Document a recovery procedure for mistaken merges.

### Postgres and Redis Ownership

- [ ] Make queue processing idempotent where Redis retries are possible.
- [ ] Flush Redis in a development environment and confirm durable state remains
  intact.
- [ ] Restart Redis and confirm sessions recover or fail cleanly.
- [ ] Restart the full stack and confirm agents, topics, documents, and merge
  history remain available.
- [ ] Verify no durable field has competing writable copies in both Postgres and
  Redis.

## Phase 1: Document the Autonomy Boundary

Goal: make the system rule explicit before adding more autonomous behavior.

Tasks:

- [x] Add a short architecture section to the main checklist or
  `CODEBASE_KNOWLEDGE.md`.
- [x] State that the model may propose semantic actions, but Python owns:
  - tool authorization,
  - JSON/schema validation,
  - project/user scoping,
  - protected entity invariants,
  - confirmation tokens,
  - destructive-action execution,
  - durable persistence boundaries.
- [x] Reference concrete examples:
  - `edit_brain` may propose a section update, but revision checks and section
    validation are enforced in Python.
  - `update_topics` may apply model proposals, but topic guards are
    deterministic.
  - `propose_entity_merge` may identify semantic duplicates, but confirmation
    and merge execution are server-controlled.
  - Redis may coordinate work, but Postgres owns durable user knowledge.

Acceptance criteria:

- [x] A new contributor can read the architecture docs and understand which
  decisions belong to the model and which must stay in Python/Postgres.
- [x] The main checklist's guiding-boundary item can be checked off.

## Phase 2: Complete Brain Revision Counter and Periodic Snapshots

Goal: make the mutable Agent Brain recoverable through lightweight restore
points without storing complete edit-by-edit history.

Current code touchpoints:

- `src/infrastructure/schema.sql`
- `src/knoggin_server/agent/tools/memory.py`
- `src/knoggin_server/agent/services/agent_manager.py`
- `src/knoggin_server/agent/tools/community_tools.py`
- `tests/knowledge/test_memory_service.py`
- `tests/agent/test_community_tools_contract.py`

Tasks:

- [x] Replace `public.agent_brain_revisions` with
  `public.agent_brain_snapshots`.
- [x] Keep `agents.brain_revision` as the monotonic live edit counter.
- [x] Store full snapshot content at revision 1, every 5 revisions, and every
  restore operation.
- [x] Store snapshot metadata:
  - actor category,
  - timestamp,
  - revision,
  - change type,
  - changed section,
  - deterministic summary,
  - restored-from revision when applicable.
- [x] Generate summaries mechanically without extra LLM calls.
- [x] Add optional capped `change_note` metadata to `edit_brain`.
- [x] Add agent-facing tools:
  - `list_brain_snapshots`,
  - `read_brain_snapshot`,
  - `restore_brain_section`.
- [x] Restrict restores to the owning user, active agent, and editable Brain
  sections.
- [x] Require expected current revision for restores.
- [x] Add tests for:
  - initial snapshot creation,
  - specialist spawn snapshot creation,
  - non-boundary edits not creating snapshots,
  - boundary edits creating snapshots,
  - snapshot listing and reading,
  - section restore from snapshot,
  - stale restore rejection,
  - non-editable section restore rejection.

Acceptance criteria:

- [x] Brain snapshots provide bounded restore points without claiming complete
  edit history.
- [x] Agents can autonomously inspect snapshots and restore one editable section
  from a snapshot.
- [x] Persona, engine policy, tools, permissions, topics, graph data, and
  documents remain outside Brain restore authority.

## Phase 3: Finish Entity Merge Preconditions

Goal: keep the model in the semantic proposal role while Python enforces merge
safety before confirmation and execution.

Current code touchpoints:

- `src/knoggin_server/knowledge/services/entity_merge_service.py`
- `src/knoggin_server/knowledge/db/writers/graph_writer.py`
- `src/infrastructure/schema.sql`
- `tests/knowledge/test_entity_merge_classification_contract.py`
- `tests/knowledge/test_merge_detection_job_contract.py`
- `tests/storage/test_graph_writer_contract.py`

Tasks:

- [x] Make project/user visibility explicit in merge snapshots. A proposal
  should verify both entities are visible to the authorized user/project scope,
  not only that they share a project ID.
- [x] Keep identity entity protection in both the service and canonical writer.
- [x] Keep type compatibility checks in the service.
- [x] Keep stable identifier conflict checks in the service.
- [x] Make important fact and timeline compatibility policy explicit:
  nuanced fact/timeline compatibility is marked as `confirmation_required`.
- [x] Ensure reviewed candidate state includes enough revision data to reject
  stale proposals after entity, fact, relationship, or hierarchy changes.
- [x] Add tests for:
  - missing entity rejection,
  - unauthorized project/user scope rejection,
  - identity entity rejection,
  - type conflict rejection,
  - stable identifier conflict rejection,
  - stale state rejection,
  - fact/timeline ambiguity requiring confirmation.

Acceptance criteria:

- [x] The deterministic pre-merge checklist can be checked off.
- [x] The model cannot bypass any merge guardrail with reasoning text or high
  confidence.

## Phase 4: Add Entity Merge Rollback and Recovery Procedure

Goal: provide a conservative administrative path to recover from mistaken
merges where the stored audit state is still sufficient and no later conflict
has occurred.

Current code touchpoints:

- `src/knoggin_server/knowledge/services/entity_merge_service.py`
- `src/knoggin_server/knowledge/db/writers/graph_writer.py`
- `src/infrastructure/schema.sql`
- `tests/knowledge/test_merge_detection_job_contract.py`
- `tests/storage/test_graph_writer_contract.py`

Tasks:

- [x] Extend `public.entity_merge_audits` with rollback metadata:
  - `rolled_back_at TIMESTAMPTZ`
  - `rolled_back_by TEXT`
  - `rollback_status TEXT`
  - `rollback_expires_at TIMESTAMPTZ`
  - `rollback_failure_reason TEXT`
- [x] Add an admin/service-only method such as
  `EntityMergeService.rollback(audit_id, actor)`.
- [x] Rollback should read `before_state` and `after_state` from the audit.
- [x] Reject rollback if:
  - the audit was not executed,
  - the audit was already rolled back,
  - the rollback window expired or rollback state was cleaned up,
  - the primary entity changed after the audited merge,
  - the duplicate entity ID has been reused,
  - facts, relationships, or hierarchy state no longer matches the expected
    post-merge state.
- [x] Restore the duplicate entity, aliases, facts, relationships, hierarchy
  edges, and projections from `before_state`.
- [x] Mark rollback result in the audit row.
- [x] Add a `merge_rollback_cleanup` job that expires rollback state after the
  configured retention window. The default window is 5 hours.
- [x] Document the manual recovery procedure in this file or a dedicated merge
  recovery doc:
  - identify the audit,
  - inspect before/after state,
  - verify no later conflicting writes,
  - run the rollback service/admin command,
  - rebuild projections/search indexes if needed.
- [x] Manual recovery procedure:
  - Locate the `entity_merge_audits` row by `audit_id`, `proposal_id`, or the
    merged entity IDs.
  - Confirm `rollback_status = 'available'` and `rollback_expires_at` has not
    passed.
  - Inspect `before_state` and `after_state` to verify the mistaken merge.
  - Run the service/admin rollback method. Do not expose rollback as an agent
    tool.
  - If rollback is rejected because state changed, repair manually from the
    audit evidence and rebuild project projection/search indexes.
  - If rollback state expired, use the retained audit metadata and external
    evidence for manual repair; automatic rollback is no longer available.
- [x] Add tests for:
  - successful rollback from an executed audit,
  - rollback rejection after conflicting later changes,
  - rollback rejection when duplicate ID has been reused,
  - rollback rejection for non-executed audits,
  - rollback idempotency when called twice.

Acceptance criteria:

- [x] A mistaken merge has a documented recovery procedure.
- [x] A practical admin rollback path exists for cleanly reversible merge cases.
- [x] Merge rollback never silently overwrites later user knowledge.

## Phase 5: Implement Coordination Inspection Logging

Goal: preserve enough bounded evidence to recover or inspect Redis-backed work
without making Redis durable authority.

Current code touchpoints:

- `src/common/utils/events.py`
- `codebase-analysis-docs/COORDINATION_EVENT_LOG_AUDIT.md`
- `src/knoggin_server/ingestion/services/pipeline_service.py`
- `src/knoggin_server/ingestion/jobs/dlq_job.py`
- `src/knoggin_server/ingestion/jobs/profile_job.py`
- `src/knoggin_server/knowledge/db/write_graph_db.py`
- `src/knoggin_server/knowledge/services/entity_merge_service.py`

Tasks:

- [x] Add `src/common/utils/coordination_log.py`.
- [x] Add `src/common/utils/event_persistence_policy.py`.
- [x] Wire the policy behind the existing event emitter instead of creating a
  second producer API.
- [x] Persist only scrubbed, bounded, recovery-grade events to a structured
  searchable coordination log.
- [x] Start with the events identified in `COORDINATION_EVENT_LOG_AUDIT.md`:
  - `pipeline.dlq_enqueued`
  - `job.dlq_parked`
  - `job.dlq_retry_success`
  - `job.dlq_retry_failed`
  - `job.dlq_reprocess_success`
  - `job.dlq_graph_write_success`
- [x] Add missing event emissions for:
  - `job.dirty_entities_marked`
  - `job.dirty_entities_cleared`
  - `job.merge_queue_marked`
  - `job.merge_queue_removed`
- [x] Keep raw prompts, conversation text, generated answers, and large payloads
  out of the coordination log.
- [x] Add tests for event filtering, logfmt shape, redaction, and
  disabled/error behavior.

Inspection examples:

- `rg "label=RECOVERY" logs/coordination.log`
- `rg "event=job.merge_queue_marked" logs/coordination.log`
- `rg "entity_ids=.*42" logs/coordination.log`

Acceptance criteria:

- [x] Redis-backed DLQ, dirty entity, and merge queue transitions have bounded
  inspection records.
- [x] Coordination logs are evidence for manual recovery, not source-of-truth
  state.
- [x] The implementation follows the `COORDINATION_EVENT_LOG_AUDIT.md` policy.

## Phase 6: Make Redis Queue Processing Idempotent

Goal: make Redis retry behavior safe enough that duplicate or interrupted work
does not corrupt durable state.

Current code touchpoints:

- `src/knoggin_server/ingestion/services/pipeline_service.py`
- `src/knoggin_server/ingestion/jobs/dlq_job.py`
- `src/knoggin_server/ingestion/jobs/profile_job.py`
- `src/knoggin_server/knowledge/services/entity_merge_service.py`
- `src/infrastructure/redis_client.py`

Tasks:

- [x] Give DLQ entries stable IDs or deterministic hashes based on durable input
  identifiers.
- [x] Replace raw `lpop` processing with a claim/processing pattern, such as:
  - move item from retry queue to processing queue,
  - process with a lease,
  - remove from processing only after success or parking,
  - requeue expired claims.
- [x] Make DLQ retry/park transitions idempotent by DLQ item ID.
- [x] Ensure reprocessing from durable message IDs is safe if a DLQ item is
  retried more than once.
- [x] Confirm set-based queues remain duplicate-tolerant:
  - `dirty_entities`
  - `merge_queue`
- [x] Add manual recovery helpers after the coordination log exists:
  - requeue DLQ item from logged durable message IDs,
  - move parked DLQ item back to retry after review,
  - re-mark dirty entities from logged IDs,
  - re-mark merge candidates from logged IDs.
- [x] Add tests for:
  - duplicate DLQ entry does not duplicate durable facts/messages,
  - worker crash between claim and completion can be recovered,
  - parking is idempotent,
  - dirty entity and merge queue re-marking is duplicate-safe.

Implementation notes:

- Active DLQ entries still use `dlq:{user}:{project}`.
- Claimed entries move through `dlq:processing:{user}:{project}`.
- Per-item status is tracked in `dlq:state:{user}:{project}`.
- Claim metadata is tracked in `dlq:claims:{user}:{project}`.
- DLQ event-log records include `dlq_id` where available.
- Dirty entity and merge candidate recovery helpers use duplicate-safe `SADD`.

Acceptance criteria:

- [x] Redis retries may duplicate scheduling, but not durable writes.
- [x] Interrupted DLQ processing can be inspected and manually recovered.
- [x] The Redis idempotency checklist item can be checked off.

## Phase 7: Run Storage Ownership Acceptance Checks

Goal: prove the documented Postgres/Redis boundary behaves correctly in a
development environment.

Tasks:

- [x] Add a dev verification harness:
  - `scripts/verify_storage_ownership.py` seeds representative Postgres-owned
    state, seeds Redis-only runtime/coordination state, optionally flushes the
    selected Redis DB behind an explicit confirmation phrase, and verifies
    durable Postgres row counts do not regress.
  - `tests/storage/test_storage_ownership_verifier.py` covers report
    formatting, durable-count regression detection, Redis key-family
    classification, and the static Redis write-family policy check.
- [x] Seed a dev environment with:
  - one project,
  - one custom agent with Brain revisions,
  - topic configuration,
  - session/messages,
  - documents/chunks,
  - graph records,
  - merge proposal/audit.
- [x] Flush Redis in development.
- [x] Confirm all durable state remains available from Postgres.
- [x] Restart Redis.
- [x] Confirm active sessions either recover expected cached state or fail
  cleanly with rebuildable/ephemeral state missing.
- [x] Restart the full stack.
- [x] Confirm agents, topics, documents, graph records, and merge history remain
  available.
- [x] Audit Redis writable key families and verify none are authoritative copies
  of durable Postgres fields.
- [x] Capture the commands/results in this doc or a linked runbook.

Dev runbook:

```bash
cd knoggin-server
uv run python scripts/verify_storage_ownership.py --seed
```

The command above is non-destructive. It creates or updates the representative
Phase 7 proof rows and prints the durable Postgres counts plus Redis runtime
state summary.

```bash
cd knoggin-server
uv run python scripts/verify_storage_ownership.py \
  --seed \
  --flush-redis \
  --confirm "flush redis for storage ownership verification"
```

The command above is destructive for the configured Redis database only. Run it
only against a disposable local Redis DB. It verifies that the seeded durable
Postgres rows remain after Redis is flushed and that the Redis coordination keys
for the proof project are gone.

For stricter restart checks, seed once before restart, restart Redis or the
full stack, then run the verifier without reseeding:

```bash
cd knoggin-server
uv run python scripts/verify_storage_ownership.py --allow-missing-redis
```

That command verifies the existing Phase 7 Postgres proof rows are still
present and treats missing Redis coordination keys as acceptable restart/loss
behavior.

The verifier loads `.env` like the runtime does. Use `DATABASE_URL` or
`KNOGGIN_TEST_DATABASE_URL` for Postgres and optional `REDIS_URL` for Redis, or
pass `--dsn` and `--redis-url` explicitly.

Recorded Redis flush proof:

```text
Seeded durable state present             PASS
Redis runtime state present              PASS  6 matching keys before flush
Durable Postgres rows after Redis flush  PASS  no count regressions
Expected Redis coordination loss         PASS  0 matching keys after flush
Redis write family policy                PASS  11 Redis write files reviewed by policy; missing key families=[]
```

Recorded full-stack restart proof without reseeding:

```text
Seeded durable state present  PASS
Redis runtime state absent    PASS  0 matching keys; acceptable for restart/loss verification
Redis flush                   PASS  skipped; pass --flush-redis with explicit confirmation
Redis write family policy     PASS  11 Redis write files reviewed by policy; missing key families=[]
```

Acceptance criteria:

- [x] Redis flush does not remove durable user knowledge.
- [x] Redis restart does not corrupt durable state.
- [x] Full-stack restart preserves Postgres-owned state.
- [x] No durable field has competing writable copies in both Postgres and Redis.

## Suggested Execution Order

1. Phase 1: document the model/Python autonomy boundary.
2. Phase 2: complete Brain revision counter, periodic snapshots, and section
   restore.
3. Phase 3: finish deterministic merge preconditions.
4. Phase 4: add merge rollback and recovery docs.
5. Phase 5: implement coordination inspection logging.
6. Phase 6: make Redis queue processing idempotent.
7. Phase 7: run storage ownership acceptance checks.

Only after these are complete should work move to section 7 and section 8 of
the main agent autonomy checklist.
