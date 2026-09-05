import pytest

from common.schema.events import InternalEvent
from common.schema.settings import CoordinationLogSettings
from common.utils.coordination_log import CoordinationLog, format_logfmt
from common.utils.event_persistence_policy import normalize_coordination_event
from common.utils.events import EventEmitter


def make_event(**overrides) -> InternalEvent:
    data = {
        "ts": "2026-06-29T12:00:00Z",
        "scope_id": "project-1",
        "component": "job",
        "event": "failed",
        "data": {},
    }
    data.update(overrides)
    return InternalEvent(**data)


def test_internal_event_adapts_to_coordination_log_record():
    event = InternalEvent(
        ts="2026-01-01T00:00:00Z",
        scope_id="project-1",
        component="job",
        event="timeout",
        data={"job": "episode"},
    )

    record = normalize_coordination_event(event)

    assert record is not None
    assert record["event"] == "job.timeout"
    assert record["project_id"] == "project-1"


def test_policy_normalizes_approved_event_and_drops_raw_content():
    record = normalize_coordination_event(make_event(
        data={
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "msg_ids": [1, 2],
            "stage": "semantic_knowledge",
            "error": "x" * 250,
            "prompt": "raw prompt must not be logged",
            "messages": [{"content": "raw message must not be logged"}],
        },
    ))

    assert record is not None
    assert record["label"] == "RECOVERY"
    assert record["event"] == "job.failed"
    assert record["user"] == "ada"
    assert record["message_ids"] == "1,2"
    assert record["error"].endswith("...")
    assert "prompt" not in record
    assert "messages" not in record


def test_policy_rejects_disallowed_and_verbose_events():
    assert (
        normalize_coordination_event(make_event(
            component="job",
            event="started",
            data={},
        ))
        is None
    )
    assert (
        normalize_coordination_event(make_event(
            component="job",
            event="retry_failed",
            data={},
            verbose_only=True,
        ))
        is None
    )


def test_policy_allows_job_failure_events_with_safe_fields_only():
    record = normalize_coordination_event(make_event(
        component="job",
        event="failed",
        data={
            "entity_id": 42,
            "episode_count": 3,
            "failed_episode_ids": ["episode-1", "episode-2"],
            "error": "write failed",
            "content": "raw episode text must not be logged",
        },
    ))

    assert record is not None
    assert record["event"] == "job.failed"
    assert record["entity_id"] == 42
    assert record["episode_count"] == 3
    assert record["failed_episode_ids"] == "episode-1,episode-2"
    assert record["error"] == "write failed"
    assert "content" not in record


def test_policy_persists_episode_metrics_without_episode_content():
    record = normalize_coordination_event(make_event(
        component="agent",
        event="episode_retrieval_completed",
        data={
            "project_id": "project-1",
            "session_id": "session-1",
            "strategy": "semantic",
            "episode_count": 3,
            "matched_entity_episode_count": 1,
            "source_message_expansion_skipped_count": 3,
            "retrieval_latency_ms": 12.5,
            "used_raw_message_fallback": False,
            "summary": "episode summary must not be logged",
            "content": "raw evidence must not be logged",
        },
    ))

    assert record is not None
    assert record["event"] == "agent.episode_retrieval_completed"
    assert record["strategy"] == "semantic"
    assert record["episode_count"] == 3
    assert record["source_message_expansion_skipped_count"] == 3
    assert record["retrieval_latency_ms"] == 12.5
    assert record["used_raw_message_fallback"] is False
    assert "summary" not in record
    assert "content" not in record


def test_policy_persists_local_reference_failure_metrics_without_ids():
    record = normalize_coordination_event(make_event(
        component="agent",
        event="local_reference_resolution_failed",
        data={
            "pipeline": "agent_tool_loop",
            "reference_type": "episode",
            "reason": "unknown_or_wrong_type",
            "episode_id": "must-not-be-persisted",
            "content": "raw model context must not be logged",
        },
    ))

    assert record is not None
    assert record["event"] == "agent.local_reference_resolution_failed"
    assert record["pipeline"] == "agent_tool_loop"
    assert record["reference_type"] == "episode"
    assert record["reason"] == "unknown_or_wrong_type"
    assert "episode_id" not in record
    assert "content" not in record


def test_policy_normalizes_scheduler_failure_name_to_job_field():
    record = normalize_coordination_event(make_event(
        component="job",
        event="timeout",
        data={"name": "profile_refinement", "summary": "ignored"},
    ))

    assert record is not None
    assert record["event"] == "job.timeout"
    assert record["job"] == "profile_refinement"
    assert "summary" not in record


def test_logfmt_quotes_values_without_breaking_searchable_keys():
    line = format_logfmt(
        {
            "event": "job.failed",
            "label": "RECOVERY",
            "reason": "profile refined",
            "path": 'value "quoted"',
        }
    )

    assert "event=job.failed" in line
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

    log.write({"event": "job.failed", "label": "RECOVERY"})
    assert "event=job.failed label=RECOVERY" in path.read_text()

    disabled_path = tmp_path / "disabled.log"
    log.configure(
        CoordinationLogSettings(
            enabled=False,
            path=str(disabled_path),
            retention_days=14,
            rotation_mb=10,
        )
    )
    log.write({"event": "job.failed", "label": "RECOVERY"})
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

    log.write({"event": "job.failed", "label": "RECOVERY"})


@pytest.mark.no_network
async def test_engine_emitter_persists_approved_events(
    monkeypatch,
):
    persisted = []
    monkeypatch.setattr(
        "common.utils.events.write_coordination_event",
        lambda fields: persisted.append(fields),
    )
    emitter = EventEmitter()
    monkeypatch.setattr("common.utils.events.get_now_iso", lambda: "2026-08-14T00:00:00Z")

    await emitter.emit(
        "project-1",
        "job",
        "failed",
        {
            "user_name": "ada",
            "project_id": "project-1",
            "source_message_count": 2,
            "episode_count": 1,
        },
    )

    assert persisted == [
        {
            "ts": "2026-08-14T00:00:00Z",
            "label": "RECOVERY",
            "retention": "recovery",
            "component": "job",
            "event": "job.failed",
            "user": "ada",
            "project_id": "project-1",
            "source_message_count": 2,
            "episode_count": 1,
        }
    ]
