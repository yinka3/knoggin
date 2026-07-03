# Coordination Event Log Policy And Search Plan

This document is now a policy/reference note for the implemented coordination
event log, plus the remaining plan for a read-only search interface.

The coordination log is for inspection: list entries, filter/search them, and
inspect one entry's safe metadata. It is not a replay system, analytics stream,
WAL, dashboard, or source of truth.

## Current State

Implemented:

- `src/common/utils/event_persistence_policy.py`
- `src/common/utils/coordination_log.py`
- `src/common/utils/events.py` coordination-log sink integration
- `CoordinationLogSettings`
- `tests/unit/common/test_coordination_log.py`

The live event emitter remains the producer-side API. Events are emitted for
live subscribers first, then a policy checks whether the event should be
persisted into `logs/coordination.log`.

The log remains evidence, not authority. Any future recovery command must verify
referenced Postgres rows and operation safety before acting.

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

`verbose_only=True` events are intentionally non-loggable unless converted into
a scrubbed, bounded event first.

## Log Shape

Current sink flow:

1. Event is emitted normally for live subscribers.
2. `event_persistence_policy.normalize_coordination_event(...)` checks
   `(component, event)` and payload shape.
3. Approved events are normalized into stable key/value fields.
4. `coordination_log.write_coordination_event(...)` appends logfmt to
   `logs/coordination.log`.

Example line:

```text
ts=2026-06-26T12:00:00Z label=RECOVERY retention=recovery component=pipeline event=pipeline.dlq_enqueued user=user project_id=project session_id=session dlq_key=dlq:user:project message_ids=123,124 stage=graph_write attempt=1 error="bounded error text"
```

Useful shell inspection:

```bash
rg "label=RECOVERY" logs/coordination.log
rg "event=job.merge_queue_marked" logs/coordination.log
rg "entity_ids=.*42" logs/coordination.log
```

## Approved Events

These events are approved by policy. Unknown fields and content-adjacent fields
are still dropped by the safe-field allowlist.

| Event | Why log |
| --- | --- |
| `pipeline.dlq_enqueued` | Captures when a Redis DLQ item is created. |
| `pipeline.dlq_write_failed` | Captures when Redis DLQ write failed and normal retry evidence may be missing. |
| `pipeline.graph_write_failed` | Captures graph write failure before or during DLQ routing. |
| `pipeline.buffer_invalid_entries` | Captures malformed Redis buffer entries with bounded metadata only. |
| `pipeline.drain_complete` | Captures abnormal batch drain summaries when emitted. |
| `job.dlq_parked` | Captures when an item leaves the retry path. |
| `job.dlq_retry_success` | Captures successful retry. |
| `job.dlq_retry_failed` | Captures retry loop progress and requeue. |
| `job.dlq_reprocess_success` | Captures full reprocess success. |
| `job.dlq_graph_write_success` | Captures graph-write retry success. |
| `job.dirty_entities_marked` | Shows profile work was queued or re-queued. |
| `job.dirty_entities_cleared` | Shows profile work was handled and removed. |
| `job.merge_queue_marked` | Shows merge-maintenance candidates were queued. |
| `job.merge_queue_removed` | Shows merge candidates were removed after review/execution. |
| `job.invalidation_failures` | Captures failed fact invalidations without fact content. |
| `job.facts_write_failed` | Captures failed fact creation before invalidation. |
| `job.maintenance_deferred` | Explains why autonomous maintenance did not run when emitted. |
| `job.profile_refinement_failed` | Captures profile job failure where dirty IDs may remain. |
| `job.profiles_refined` | Captures durable profile update counts; names/content are dropped. |
| `job.user_profile_refined` | Captures identity profile update counts. |
| `job.failed` | Captures scheduler job failure metadata. |
| `job.timeout` | Captures scheduler timeout metadata. |
| `entities.entity_merged` | Captures destructive merge identifiers when emitted. |

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

## Remaining Work: Search Interface

The remaining useful implementation is a read-only coordination-log search
interface. Keep it separate from recovery actions.

Recommended shape:

- Add a small parser for logfmt coordination entries.
- Add a read-only service, likely `common/utils/coordination_log_reader.py`.
- Support filters:
  - `event`
  - `component`
  - `project_id`
  - `user`
  - `session_id`
  - `entity_id`
  - `entity_ids contains`
  - `message_id` / `message_ids contains`
  - `dlq_id`
  - `job`
  - `reason`
  - `since` / `until`
  - free-text substring search across the raw line
- Return newest-first results with `limit` and a cursor or offset.
- Include the raw log line plus parsed safe fields.
- Reject or cap expensive unbounded scans.
- Treat malformed lines as skippable, not fatal.

Suggested API boundary:

```python
class CoordinationLogReader:
    def __init__(self, path: str): ...

    def search(
        self,
        *,
        event: str | None = None,
        component: str | None = None,
        project_id: str | None = None,
        user: str | None = None,
        entity_id: str | None = None,
        message_id: str | None = None,
        dlq_id: str | None = None,
        job: str | None = None,
        reason: str | None = None,
        text: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]: ...
```

Tests to add:

- Parses quoted logfmt fields.
- Filters by exact fields.
- Filters by contained `entity_ids` and `message_ids`.
- Returns newest-first entries.
- Applies `limit` and `offset`.
- Skips malformed lines.
- Does not expose content fields because they should never be present in the log.

## Open Follow-Up

The policy is still a general safe-field allowlist, not a per-event schema
validator. That is acceptable for now because the log is inspection-only, but a
future hardening pass could require specific fields for each approved event.
