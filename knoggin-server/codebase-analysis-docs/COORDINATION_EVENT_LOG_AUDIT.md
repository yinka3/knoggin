# Coordination Event Log Audit

This audit defines which emitted events belong in the read-only coordination
event log. The event log is for inspection: list entries, filter/search them,
and inspect one entry's safe metadata. It is not a replay system, dashboard,
analytics stream, WAL, or source of truth.

The short version: reuse the current event vocabulary, but do not log the whole
event stream. The live emitter remains for UI/debug streaming. The coordination
log is a filtered sink for Redis-backed work transitions that help a human
understand what happened.

## Decision Rule

Log an event only when it satisfies at least one of these:

- It records a Redis-backed work item crossing an important state boundary.
- It records a failure where Redis loss or retry behavior could hide useful
  inspection context.
- It records bounded identifiers that can be checked against durable Postgres
  state.
- It records a reviewed maintenance/recovery action result without exposing raw
  user content.

Do not log by default when:

- The event is just live progress telemetry.
- The event contains prompts, reasoning text, conversation text, generated
  message content, document chunks, or large entity/fact payloads.
- The event can be recomputed cheaply from Postgres.
- The event is high-volume and has no direct inspection value.

`verbose_only=True` events default to non-loggable unless they are converted
into a scrubbed, bounded event first.

## Log Shape

Keep the existing `emit(...)`, `emit_sync(...)`, and `emit_community(...)`
calls as the producer-side API.

The sink flow is:

1. Event is emitted normally for live subscribers.
2. A policy checks `(component, event)` and payload shape.
3. Approved events are normalized into stable key/value fields.
4. The normalized event is appended to `logs/coordination.log`.

Example line:

```text
ts=2026-06-26T12:00:00Z label=RECOVERY retention=recovery component=pipeline event=pipeline.dlq_enqueued user=user project_id=project session_id=session dlq_key=dlq:user:project message_ids=123,124 stage=graph_write attempt=1 error="bounded error text"
```

Useful inspection examples:

```bash
rg "label=RECOVERY" logs/coordination.log
rg "event=job.merge_queue_marked" logs/coordination.log
rg "entity_ids=.*42" logs/coordination.log
```

The coordination log is evidence, not authority. Any future recovery command
must verify referenced Postgres rows and operation safety before acting.

## Logged In Phase 5

| Event | Why log | Required safe metadata |
| --- | --- | --- |
| `pipeline.dlq_enqueued` | Captures when a Redis DLQ item is created. | DLQ key, DLQ ID, user, project, session, bounded error, stage, attempt, message IDs. |
| `job.dlq_parked` | Captures when an item leaves the retry path. | Parked key, DLQ ID, project, stage, attempt, reason, bounded error. |
| `job.dlq_retry_success` | Captures successful retry. | DLQ key, DLQ ID, project, stage, attempt. |
| `job.dlq_retry_failed` | Captures retry loop progress and requeue. | DLQ key, DLQ ID, project, stage, attempt, max attempts. |
| `job.dlq_reprocess_success` | Captures full reprocess success. | DLQ key, DLQ ID, project, message IDs, entity IDs/counts, stage, attempt. |
| `job.dlq_graph_write_success` | Captures graph-write retry success. | DLQ key, DLQ ID, project, entity IDs/counts, stage, attempt. |
| `job.dirty_entities_marked` | Shows profile work was queued or re-queued. | Dirty key, project, entity IDs, count, reason. |
| `job.dirty_entities_cleared` | Shows profile work was handled and removed. | Dirty key, project, entity IDs, count, reason. |
| `job.merge_queue_marked` | Shows merge-maintenance candidates were queued. | Merge key, project, entity IDs, count, reason. |
| `job.merge_queue_removed` | Shows merge candidates were removed after review/execution. | Merge key, project, entity IDs, count, reason, proposal ID when available. |

## Candidate Events For Later

These can be added when the event payloads are scrubbed and the event-log UI has
a reason to show them.

| Event | Why maybe | Recommendation |
| --- | --- | --- |
| `pipeline.dlq_write_failed` | Redis DLQ write failed, so the normal failure queue may not contain the item. | Include message IDs, stage, source buffer key, and bounded error only. |
| `pipeline.graph_write_failed` | Graph write failed before or during DLQ routing. | Include message IDs, entity IDs/counts, stage, and bounded error. |
| `job.invalidation_failures` | Fact invalidation failed after fact resolution. | Include failed fact IDs and project ID, not fact content. |
| `job.facts_write_failed` | Fact creation failed before invalidations. | Include entity ID, fact count, source message IDs, and bounded error. |
| `entities.entity_merged` | Destructive graph action worth cross-checking. | Prefer durable merge audit rows; log only entity/proposal/audit IDs. |
| `job.maintenance_deferred` | Explains why autonomous maintenance did not run. | Log later if the event-log screen needs cooldown/eligibility inspection. |
| `pipeline.buffer_invalid_entries` | Indicates malformed Redis buffer entries. | Log only key name and bounded bad-entry metadata, not raw messages. |
| `pipeline.drain_complete` | Batch-level summary can explain abnormal drains. | Log only when `dlq_count > 0`, `partial_flush=True`, or abnormal state exists. |
| `job.profile_refinement_failed` | Dirty IDs may remain after profile job failure. | Include dirty key, entity IDs/counts, and bounded error. |
| `job.profiles_refined` | Durable entity profiles changed. | Log only IDs/counts if the inspection screen needs it. |
| `job.user_profile_refined` | Durable identity profile changed. | Usually better as a normal audit/product event, not coordination inspection. |
| `job.failed` / `job.timeout` | Scheduler failures can explain missing maintenance. | Persist only for maintenance jobs, not every scheduler heartbeat. |

## Do Not Log

These are live UI/debug/progress events, high-volume events, or content-adjacent
events that do not belong in the coordination event log.

| Event family | Reason |
| --- | --- |
| `pipeline.consumer_started`, `pipeline.consumer_stopped` | Lifecycle telemetry; not an inspection entry. |
| `pipeline.buffer_empty`, `pipeline.buffer_draining` | High-volume queue progress. |
| `pipeline.checkpoint_reached` | Counter progress; durable checks should use message IDs or DLQ events. |
| `pipeline.batch_start`, `pipeline.batch_complete` | Normal progress; too high-volume. |
| `pipeline.mentions_extracted`, `pipeline.resolution_complete`, `pipeline.connections_extracted` | Pipeline progress/debug; noisy and content-adjacent. |
| `pipeline.known_matched`, `pipeline.gliner_complete`, `pipeline.ner_complete` | Extraction telemetry. |
| Any `*.llm_call` carrying `prompt` | Prompt/conversation content risk. |
| Any `*.llm_fallback` | Diagnostic fallback telemetry, not coordination inspection. |
| `job.scheduler_started`, `job.scheduler_stopped`, `job.started`, `job.completed` | Scheduler lifecycle/progress telemetry. |
| `job.profile_trigger_volume`, `job.profile_trigger_idle`, `job.profile_skipped` | Maintenance eligibility telemetry; Redis counters/heartbeats remain ephemeral. |
| `job.facts_skipped`, `job.facts_changed`, `job.contradictions_detected` | Fact diagnostics can include content or vague summaries; durable facts are in Postgres. |
| `job.dlq_processing`, `job.dlq_complete`, `job.dlq_work_unit_finished` | Operational summaries/internal replay details; specific DLQ outcomes are more useful. |
| `system.session_shutdown` | Session lifecycle telemetry. |
| `agent.llm_call` | Reasoning trace/debug, not coordination inspection. |
| Community reasoning/message events | Community records belong in durable community tables when needed, and some include content. |
| `entities.entities_removed` | In-memory/cache removal signal, not durable recovery data. |

## Placement

Keep policy separate from the emitter implementation:

- `src/common/utils/event_persistence_policy.py`
- `src/common/utils/coordination_log.py`

`events.py` should stay focused on event delivery. The policy owns filtering,
field normalization, and redaction decisions.
