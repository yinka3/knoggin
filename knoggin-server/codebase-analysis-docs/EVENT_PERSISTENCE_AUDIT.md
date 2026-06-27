# Event Persistence Audit

This audit decides which existing emitted events should be reused for the
structured coordination inspection log.

The short version: reuse the current event vocabulary, but do not persist the
whole event stream. The current emitter is useful for live UI/debugging. The
persistent log should be a filtered sink for events that help inspection or a
manual recovery operation.

## Decision Rule

Persist an event only when it satisfies at least one of these:

- It records a Redis-backed work item crossing an important state boundary.
- It records a failure where Redis loss or retry behavior could hide useful
  recovery context.
- It records a manual recovery action or its result.
- It records bounded identifiers that can be checked against durable Postgres
  state.

Do not persist by default when:

- The event is just live progress telemetry.
- The event contains prompts, reasoning text, conversation text, or generated
  message content.
- The event can be recomputed cheaply from Postgres.
- The event is high-volume and has no direct recovery operation.

`verbose_only=True` events should default to non-persistent unless they are
converted into a scrubbed, bounded event first.

## Reuse Design

Keep the existing `emit(...)`, `emit_sync(...)`, and `emit_community(...)`
calls as the main producer-side API.

Add a second sink behind the existing debug emitter:

1. Event is emitted normally for live subscribers.
2. A persistence policy checks `(component, event)` and payload shape.
3. Approved events are normalized into a stable log schema.
4. The normalized event is appended to JSONL for inspection.

This avoids inventing another event path while still keeping the persistent log
small and intentional.

Suggested normalized shape:

```json
{
  "schema_version": 1,
  "ts": "2026-06-26T12:00:00Z",
  "component": "pipeline",
  "event": "dlq_enqueued",
  "user_name": "user",
  "project_id": "project",
  "session_id": "session",
  "run_id": null,
  "job_id": null,
  "durable_refs": {
    "message_ids": [123, 124],
    "entity_ids": []
  },
  "redis_refs": {
    "keys": ["dlq:user:project"]
  },
  "summary": "DLQ item created for graph_write",
  "snapshot": {
    "stage": "graph_write",
    "attempt": 1,
    "error": "bounded error text"
  }
}
```

The JSONL log is evidence, not authority. Recovery commands must still verify
that referenced Postgres rows exist and that the operation is safe/idempotent.

## Classification Matrix

### Strong Persist

These are the best first targets.

| Event | Why persist | Recovery value | Payload changes recommended |
| --- | --- | --- | --- |
| `pipeline.dlq_enqueued` | Captures when a Redis DLQ item is created. | Requeue/reconstruct a DLQ item if durable message IDs still exist. | Include DLQ key, project/user, bounded error, stage, attempt, message IDs, and a small input summary. |
| `job.dlq_parked` | Captures when an item leaves the retry path. | Inspect parked failures and manually requeue after review. | Include message IDs when available, DLQ parked key, original stage, attempt, reason, bounded error. |
| `job.dlq_retry_success` | Captures successful manual/automatic recovery. | Audit that a DLQ item was actually recovered. | Include message IDs or replay unit ID, stage, previous attempt. |
| `job.dlq_retry_failed` | Captures retry loop progress and requeue. | Helps distinguish still-retryable work from stuck work. | Include message IDs or replay unit ID, stage, attempt, max attempts. |
| `job.dlq_reprocess_success` | Captures full reprocess success. | Confirms replay from durable inputs worked. | Include message IDs and entity IDs/counts. |
| `job.dlq_graph_write_success` | Captures graph-write retry success. | Confirms partial pipeline recovery worked. | Include replay unit ID, entity IDs/counts, and original stage. |
| `pipeline.dlq_write_failed` | Redis DLQ write failed, so the normal failure queue may not contain the item. | High-value inspection breadcrumb for possible lost work. | Include message IDs, stage, source buffer key, bounded error if available. |
| `pipeline.graph_write_failed` | Graph write failed before or during DLQ routing. | Useful if the following DLQ write also fails or Redis is evicted. | Include message IDs, entity IDs/counts, stage, bounded error. |
| `job.invalidation_failures` | Fact invalidation failed after fact resolution. | Manual retry of invalidating specific durable facts. | Already includes failed fact IDs; add project ID in normalized metadata. |
| `job.facts_write_failed` | Fact creation failed before invalidations. | Inspect failed writes without accidentally invalidating old facts. | Include entity ID, fact count, source message IDs if available, bounded error. |
| `entities.entity_merged` | Entity merge is destructive and worth an audit trail. | Cross-check merge audit records and inspect unexpected direct merge paths. | Prefer durable `entity_merge_audits`; log only IDs and audit/proposal IDs when available. |

### Persist After Adding or Improving Events

These are important, but the current event stream is not quite the right shape.

| Needed event | Current gap | Why it matters |
| --- | --- | --- |
| `job.dirty_entities_marked` | Dirty entity Redis writes happen without a dedicated event in the places that mark profile work. | Lets us re-mark dirty entities after Redis loss. |
| `job.dirty_entities_cleared` | Profile job clears dirty IDs after processing, but the clear is not separately emitted. | Lets us inspect whether work disappeared because it was handled or because Redis was lost. |
| `job.merge_queue_marked` | Profile refinement adds updated IDs to `merge_queue` without an event. | Lets us recreate merge-maintenance candidates after Redis loss. |
| `job.merge_queue_removed` | Merge proposal/confirmation removes IDs from `merge_queue` without a dedicated event. | Distinguishes completed review from accidental queue loss. |
| `job.maintenance_deferred` | Eligibility/cooldown decisions are not persisted. | Useful later for understanding autonomous maintenance behavior without treating Redis heartbeats as durable. |

### Maybe Persist Later

These are useful for operations, but they are not first-wave recovery events.

| Event | Why maybe | Recommendation |
| --- | --- | --- |
| `pipeline.buffer_invalid_entries` | Indicates malformed/corrupt Redis buffer entries. | Persist only if it includes key name and bounded bad-entry metadata, not raw message content. |
| `pipeline.drain_complete` | Batch-level summary includes message IDs and DLQ count. | Persist only when `dlq_count > 0`, `partial_flush=True`, or other abnormal state occurs. |
| `job.profile_refinement_failed` | Profile maintenance failed and dirty IDs may remain. | Persist after adding entity IDs and dirty key. |
| `job.profiles_refined` | Confirms profile maintenance changed durable entity state. | Persist only as a bounded summary with entity IDs, not just names. |
| `job.user_profile_refined` | Durable identity-scope profile changed. | Maybe keep in normal audit/analytics, not coordination recovery. |
| `job.facts_archived` | Durable cleanup occurred. | Maybe audit, but no immediate recovery operation if archive/delete is intentional. |
| `job.entities_cleaned` | Durable orphan cleanup occurred. | Maybe audit, but no recovery operation unless before/after IDs are retained elsewhere. |
| `job.failed` / `job.timeout` | Scheduler failures can explain missing maintenance. | Persist only for maintenance jobs, not every scheduler heartbeat. |
| `pipeline.connections_failed` | Important pipeline failure, but DLQ already captures durable recovery path. | Persist only if no DLQ item is created. |

### Do Not Persist to Coordination Log

These are live UI/debug/progress events, or they carry content that does not
belong in a small recovery log.

| Event family | Reason |
| --- | --- |
| `pipeline.consumer_started`, `pipeline.consumer_stopped` | Lifecycle telemetry; not a recovery input. |
| `pipeline.buffer_empty`, `pipeline.buffer_draining` | High-volume queue progress. |
| `pipeline.checkpoint_reached` | Counter progress; recoverability should come from durable message IDs or DLQ events. |
| `pipeline.batch_start`, `pipeline.batch_complete` | Normal progress; too high-volume for the coordination log. |
| `pipeline.mentions_extracted`, `pipeline.resolution_complete`, `pipeline.connections_extracted` | Pipeline progress/debug; can be noisy and content-adjacent. |
| `pipeline.known_matched`, `pipeline.gliner_complete`, `pipeline.ner_complete` | Extraction telemetry. |
| Any `*.llm_call` carrying `prompt` | Prompt/conversation content risk; keep out of the coordination log. |
| Any `*.llm_fallback` | Diagnostic fallback telemetry; not a recovery operation. |
| `job.scheduler_started`, `job.scheduler_stopped`, `job.started`, `job.completed` | Scheduler lifecycle/progress telemetry. |
| `job.profile_trigger_volume`, `job.profile_trigger_idle`, `job.profile_skipped` | Maintenance eligibility telemetry; Redis counters/heartbeats can remain ephemeral. |
| `job.facts_skipped` | Verbose profile/fact merge diagnostic; not a recovery boundary. |
| `job.facts_changed` | Currently verbose and summary-only. Durable facts are already in Postgres. |
| `job.contradictions_detected` | Contains `new_fact`; content-heavy and verbose-only. |
| `job.dlq_processing`, `job.dlq_complete` | Operational summary; individual DLQ state transitions are more useful. |
| `job.dlq_work_unit_finished` | Verbose internal replay unit. Persist a scrubbed subset through specific DLQ success/failure events instead. |
| `system.session_shutdown` | Session lifecycle telemetry. |
| `agent.llm_call` | Reasoning trace/debug, not coordination recovery. |
| `community.discussion_started`, `community.discussion_ended`, `community.message_added`, `community.agent_reasoning`, `community.seeding_started`, `community.discussion_seeded`, `community.agent_spawned` | Community records already belong in their durable community tables when needed; some events include message/reasoning content. |
| `entities.entities_removed` | In-memory/cache removal signal, not durable recovery data. Persist the durable cleanup event instead if needed. |

## Initial Implementation Targets

Start with these because each maps to a concrete manual recovery operation:

1. Persist `pipeline.dlq_enqueued`.
2. Persist scrubbed DLQ outcomes:
   - `job.dlq_parked`
   - `job.dlq_retry_success`
   - `job.dlq_retry_failed`
   - `job.dlq_reprocess_success`
   - `job.dlq_graph_write_success`
3. Add and persist dirty entity queue transitions:
   - `job.dirty_entities_marked`
   - `job.dirty_entities_cleared`
4. Add and persist merge queue transitions:
   - `job.merge_queue_marked`
   - `job.merge_queue_removed`

Then add manual recovery commands:

- Requeue a DLQ item from logged durable message IDs.
- Move a parked DLQ item back to the retry queue after review.
- Re-mark dirty entities from logged entity IDs.
- Re-mark merge candidates from logged entity IDs.

## Placement

The cleanest place to attach the persistent sink is inside
`src/common/utils/events.py`, after the existing event object is created and
before/after it is delivered to live subscribers.

Keep policy separate from the emitter implementation, for example:

- `src/common/utils/event_persistence_policy.py`
- `src/common/utils/coordination_log.py`

That keeps `events.py` from becoming the place where every product decision
goes to live forever like a haunted junk drawer.
