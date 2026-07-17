import pytest

from common.schema.settings import CoordinationLogSettings
from common.utils.coordination_log import CoordinationLog, format_logfmt
from common.utils.event_persistence_policy import normalize_coordination_event
from common.utils.events import EventEmitter


def test_policy_normalizes_approved_event_and_drops_raw_content():
    record = normalize_coordination_event(
        ts="2026-06-29T12:00:00Z",
        scope_id="project-1",
        component="pipeline",
        event="dlq_enqueued",
        data={
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "dlq_key": "dlq:ada:project-1",
            "msg_ids": [1, 2],
            "stage": "graph_write",
            "error": "x" * 250,
            "prompt": "raw prompt must not be logged",
            "messages": [{"content": "raw message must not be logged"}],
        },
    )

    assert record is not None
    assert record.fields["label"] == "RECOVERY"
    assert record.fields["event"] == "pipeline.dlq_enqueued"
    assert record.fields["user"] == "ada"
    assert record.fields["message_ids"] == "1,2"
    assert record.fields["error"].endswith("...")
    assert "prompt" not in record.fields
    assert "messages" not in record.fields


def test_policy_rejects_disallowed_and_verbose_events():
    assert (
        normalize_coordination_event(
            ts="2026-06-29T12:00:00Z",
            scope_id="project-1",
            component="job",
            event="started",
            data={},
        )
        is None
    )
    assert (
        normalize_coordination_event(
            ts="2026-06-29T12:00:00Z",
            scope_id="project-1",
            component="job",
            event="dlq_retry_failed",
            data={},
            verbose_only=True,
        )
        is None
    )


def test_policy_allows_candidate_failure_events_with_safe_fields_only():
    record = normalize_coordination_event(
        ts="2026-06-29T12:00:00Z",
        scope_id="project-1",
        component="job",
        event="episodes_write_failed",
        data={
            "entity_id": 42,
            "episode_count": 3,
            "failed_episode_ids": ["episode-1", "episode-2"],
            "error": "write failed",
            "content": "raw episode text must not be logged",
        },
    )

    assert record is not None
    assert record.fields["event"] == "job.episodes_write_failed"
    assert record.fields["entity_id"] == 42
    assert record.fields["episode_count"] == 3
    assert record.fields["failed_episode_ids"] == "episode-1,episode-2"
    assert record.fields["error"] == "write failed"
    assert "content" not in record.fields


def test_policy_persists_episode_metrics_without_episode_content():
    record = normalize_coordination_event(
        ts="2026-06-29T12:00:00Z",
        scope_id="project-1",
        component="agent",
        event="episode_retrieval_completed",
        data={
            "project_id": "project-1",
            "session_id": "session-1",
            "strategy": "semantic",
            "episode_count": 3,
            "focus_episode_count": 1,
            "retrieval_latency_ms": 12.5,
            "used_raw_message_fallback": False,
            "summary": "episode summary must not be logged",
            "content": "raw evidence must not be logged",
        },
    )

    assert record is not None
    assert record.fields["event"] == "agent.episode_retrieval_completed"
    assert record.fields["strategy"] == "semantic"
    assert record.fields["episode_count"] == 3
    assert record.fields["retrieval_latency_ms"] == 12.5
    assert record.fields["used_raw_message_fallback"] is False
    assert "summary" not in record.fields
    assert "content" not in record.fields


def test_policy_persists_local_reference_failure_metrics_without_ids():
    record = normalize_coordination_event(
        ts="2026-06-29T12:00:00Z",
        scope_id="project-1",
        component="agent",
        event="local_reference_resolution_failed",
        data={
            "pipeline": "agent_tool_loop",
            "reference_type": "episode",
            "reason": "unknown_or_wrong_type",
            "episode_id": "must-not-be-persisted",
            "content": "raw model context must not be logged",
        },
    )

    assert record is not None
    assert record.fields["event"] == "agent.local_reference_resolution_failed"
    assert record.fields["pipeline"] == "agent_tool_loop"
    assert record.fields["reference_type"] == "episode"
    assert record.fields["reason"] == "unknown_or_wrong_type"
    assert "episode_id" not in record.fields
    assert "content" not in record.fields


def test_policy_normalizes_scheduler_failure_name_to_job_field():
    record = normalize_coordination_event(
        ts="2026-06-29T12:00:00Z",
        scope_id="project-1",
        component="job",
        event="timeout",
        data={"name": "profile_refinement", "summary": "ignored"},
    )

    assert record is not None
    assert record.fields["event"] == "job.timeout"
    assert record.fields["job"] == "profile_refinement"
    assert "summary" not in record.fields


def test_logfmt_quotes_values_without_breaking_searchable_keys():
    line = format_logfmt(
        {
            "event": "job.merge_queue_marked",
            "label": "RECOVERY",
            "reason": "profile refined",
            "path": 'value "quoted"',
        }
    )

    assert "event=job.merge_queue_marked" in line
    assert "label=RECOVERY" in line
    assert 'reason="profile refined"' in line
    assert 'path="value \\"quoted\\""' in line


def test_coordination_log_writes_when_enabled_and_skips_when_disabled(tmp_path):
    path = tmp_path / "coordination.log"
    log = CoordinationLog()
    log.configure(
        CoordinationLogSettings(
            enabled=True,
            path=str(path),
            retention_days=14,
            rotation_mb=10,
        )
    )

    log.write({"event": "job.merge_queue_marked", "label": "RECOVERY"})
    assert "event=job.merge_queue_marked label=RECOVERY" in path.read_text()

    disabled_path = tmp_path / "disabled.log"
    log.configure(
        CoordinationLogSettings(
            enabled=False,
            path=str(disabled_path),
            retention_days=14,
            rotation_mb=10,
        )
    )
    log.write({"event": "job.merge_queue_marked", "label": "RECOVERY"})
    assert not disabled_path.exists()


def test_coordination_log_write_failure_does_not_raise(monkeypatch):
    log = CoordinationLog()
    monkeypatch.setattr(log, "_sink_id", 1)

    class BrokenLogger:
        def info(self, *_args, **_kwargs):
            raise OSError("disk full")

    monkeypatch.setattr(
        "common.utils.coordination_log.logger.bind",
        lambda **_kwargs: BrokenLogger(),
    )

    log.write({"event": "job.merge_queue_marked", "label": "RECOVERY"})


@pytest.mark.no_network
async def test_debug_emitter_persists_approved_events_and_delivers_to_subscribers(
    monkeypatch,
):
    persisted = []
    monkeypatch.setattr(
        "common.utils.events.write_coordination_event",
        lambda fields: persisted.append(fields),
    )
    emitter = EventEmitter()
    queue = await emitter.subscribe("project-1")

    await emitter.emit(
        "project-1",
        "job",
        "merge_queue_marked",
        {
            "user_name": "ada",
            "project_id": "project-1",
            "merge_key": "merge_queue:ada:project-1",
            "entity_ids": [2, 4],
            "reason": "profile_refined",
        },
    )

    delivered = await queue.get()
    assert delivered.event == "merge_queue_marked"
    assert persisted == [
        {
            "ts": delivered.ts,
            "label": "RECOVERY",
            "retention": "recovery",
            "component": "job",
            "event": "job.merge_queue_marked",
            "user": "ada",
            "project_id": "project-1",
            "merge_key": "merge_queue:ada:project-1",
            "entity_ids": "2,4",
            "reason": "profile_refined",
        }
    ]
