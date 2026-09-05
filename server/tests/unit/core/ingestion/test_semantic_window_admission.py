from types import SimpleNamespace

import pytest

from common.schema.settings import (
    EntityResolutionSettings,
    EpisodeSettings,
    IngestionSettings,
    TextProcessorSettings,
)
from core.ingestion.policy import IngestionPolicy
from core.ingestion.project_semantic_job import ProjectSemanticJob
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from infrastructure.job.base import JobContext
from tests.fixtures.factories import make_domain_config


class RecordingStore:
    def __init__(self, rows):
        self.rows = rows
        self.claims = []

    async def get_unclaimed_project_semantic_exchange_rows(self, **_kwargs):
        return self.rows

    async def claim_project_semantic_window(self, window, messages):
        self.claims.append((window, messages))
        return (window, messages)


def _row(
    user_id,
    *,
    session_id="session-1",
    timestamp_ms=None,
    closed_at_ms=1_000,
    outcome="assistant_final",
    user_content="x",
    assistant_content="",
    assistant_id=None,
    user_state="closed",
    lifecycle="sealed",
    status="open",
    claimed=False,
):
    if timestamp_ms is None:
        timestamp_ms = user_id
    if outcome == "assistant_final" and assistant_id is None:
        assistant_id = user_id + 10_000
    return {
        "user_message_id": user_id,
        "session_id": session_id,
        "user_content": user_content,
        "user_timestamp_ms": timestamp_ms,
        "user_lifecycle_state": lifecycle,
        "user_exchange_state": user_state,
        "user_exchange_outcome": outcome if user_state == "closed" else None,
        "exchange_closed_at_ms": closed_at_ms if user_state == "closed" else None,
        "assistant_message_id": assistant_id,
        "assistant_content": assistant_content,
        "assistant_timestamp_ms": timestamp_ms + 1,
        "assistant_lifecycle_state": "sealed" if assistant_id else None,
        "session_status": status,
        "already_claimed": claimed,
    }


def _admission(rows, *, target=128_000, now_ms=1_000):
    return SemanticWindowAdmission(
        RecordingStore(rows),
        IngestionSettings(semantic_window_tokens=target),
        token_counter=lambda text: text.count("x"),
        now_ms=lambda: now_ms,
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_target_crossing_keeps_the_complete_exchange_and_stops_after_it():
    rows = [
        _row(1, user_content="x" * 5, assistant_content="x" * 5),
        _row(2, user_content="x" * 5, assistant_content="x" * 5),
        _row(3, user_content="x" * 5, assistant_content="x" * 5),
    ]
    admission = _admission(rows, target=15)

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=make_domain_config().compile(),
    )

    assert selected is not None
    assert selected.close_reason == "target_crossed"
    assert [member.message_id for member in selected.messages] == [1, 10_001, 2, 10_002]
    assert selected.window.source_token_count == 20
    assert selected.window.overfill_tokens == 5
    assert selected.window.overfill_ratio == pytest.approx(5 / 15)
    assert selected.window.policy_snapshot["admission_policy"]["semantic_window_tokens"] == 15
    assert selected.window.policy_snapshot["compiled_domain"]["version"] == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_admission_persists_the_exact_context_entity_policy():
    compiled_domain = make_domain_config().compile()
    frozen_policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(gliner_threshold=0.42, llm_ner=False),
        entity_resolution=EntityResolutionSettings(resolution_threshold=0.71),
        compiled_domain=compiled_domain,
    )
    admission = _admission([_row(1, user_content="x", assistant_content="x")], target=1)

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=compiled_domain,
        ingestion_policy=frozen_policy,
    )

    assert selected is not None
    assert IngestionPolicy.from_semantic_window_snapshot(
        selected.window.policy_snapshot["ingestion_policy"]
    ) == frozen_policy


@pytest.mark.unit
@pytest.mark.no_network
async def test_open_or_editable_turn_blocks_only_its_own_session_fifo_stream():
    rows = [
        _row(1, session_id="blocked", user_state="open", lifecycle="editable"),
        _row(2, session_id="blocked", user_content="x" * 30, assistant_content="x"),
        _row(3, session_id="ready", user_content="x" * 4, assistant_content="x" * 4),
    ]
    admission = _admission(rows, target=100)

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=make_domain_config().compile(),
        force_flush=True,
    )

    assert selected is not None
    assert [member.message_id for member in selected.messages] == [3, 10_003]
    assert selected.close_reason == "explicit_flush"


@pytest.mark.unit
@pytest.mark.no_network
async def test_idle_flush_and_claim_preserve_the_frozen_policy_and_membership():
    rows = [_row(1, user_content="x" * 4, assistant_content="x" * 4, closed_at_ms=1_000)]
    store = RecordingStore(rows)
    admission = SemanticWindowAdmission(
        store,
        IngestionSettings(semantic_window_tokens=100),
        token_counter=lambda text: text.count("x"),
        now_ms=lambda: 301_000,
    )

    claimed = await admission.claim_next(
        user_name="ada",
        project_id="project-1",
        domain=make_domain_config().compile(),
    )

    assert claimed is not None
    window, members = claimed
    assert window.policy_snapshot["admission_policy"]["close_reason"] == "idle_flush"
    assert [member.ordinal for member in members] == [0, 1]
    assert store.claims == [(window, members)]


@pytest.mark.unit
@pytest.mark.no_network
async def test_single_exchange_larger_than_target_is_isolated_with_good_overfill():
    admission = _admission(
        [_row(1, user_content="x" * 12, assistant_content="x" * 12)],
        target=20,
    )

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=make_domain_config().compile(),
    )

    assert selected is not None
    assert selected.close_reason == "oversized_exchange"
    assert selected.window.source_token_count == 24
    assert selected.window.overfill_tokens == 4
    assert [member.exchange_user_message_id for member in selected.messages] == [1, 1]


@pytest.mark.unit
@pytest.mark.no_network
class _SemanticEpisodeStore:
    def __init__(self, window):
        self.window = window
        self.result = None
        self.evidence_reads = []
        self.writes = []
        self.failures = []

    async def get_active_project_semantic_window(self, **_kwargs):
        return self.window

    async def get_project_semantic_window_episode_result(self, _window_id, **_kwargs):
        return self.result

    async def get_project_semantic_window_evidence_messages(self, _window_id, **kwargs):
        self.evidence_reads.append(kwargs)
        return [
            {
                "message_id": 1,
                "session_id": "session-1",
                "role": "user",
                "content": "x",
                "timestamp_ms": 1,
            },
            {
                "message_id": 10_001,
                "session_id": "session-1",
                "role": "assistant",
                "content": "x",
                "timestamp_ms": 2,
                "user_msg_id": 1,
            },
        ]

    async def write_project_semantic_window_episodes(self, **kwargs):
        self.writes.append(kwargs)
        self.result = list(kwargs["episodes"])
        self.window = self.window.model_validate(
            self.window.model_dump() | {"episode_result_recorded": True}
        )
        return True

    async def record_project_semantic_window_failure(self, **kwargs):
        self.failures.append(kwargs)
        self.window = self.window.model_validate(
            self.window.model_dump()
            | {
                "attempt_count": self.window.attempt_count + 1,
                "last_failure_stage": kwargs["failure_stage"],
                "last_failure_code": kwargs["failure_code"],
                "last_failure_at_ms": kwargs["failed_at_ms"],
                "last_error_summary": kwargs["error_summary"],
                "next_retry_at_ms": kwargs["next_retry_at_ms"],
            }
        )
        return self.window

    async def retry_project_semantic_window(self, **kwargs):
        assert str(self.window.window_id) == kwargs["window_id"]
        self.window = self.window.model_validate(
            self.window.model_dump()
            | {
                "attempt_count": 0,
                "last_failure_stage": None,
                "last_failure_code": None,
                "last_failure_at_ms": None,
                "last_error_summary": None,
                "next_retry_at_ms": None,
            }
        )
        return self.window


class _ZeroEpisodeGenerator:
    def __init__(self):
        self.calls = []

    async def generate(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(final_episodes=[])


class _FailThenZeroEpisodeGenerator(_ZeroEpisodeGenerator):
    async def generate(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            raise ConnectionError("temporary episode provider outage")
        return SimpleNamespace(final_episodes=[])


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_semantic_job_records_zero_result_for_one_claimed_window():
    store = RecordingStore([_row(1, user_content="x", assistant_content="x")])
    admission = SemanticWindowAdmission(
        store,
        IngestionSettings(semantic_window_tokens=1),
        token_counter=lambda text: text.count("x"),
    )
    domain = make_domain_config().compile()

    async def capture_domain():
        return domain

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=domain,
    )
    assert selected is not None
    store = _SemanticEpisodeStore(selected.window)
    generator = _ZeroEpisodeGenerator()
    job = ProjectSemanticJob(
        admission,
        store,
        generator,
        settings=IngestionSettings(semantic_window_tokens=1),
        capture_domain=capture_domain,
    )
    job.enabled = True
    context = JobContext(user_name="ada", project_id="project-1")

    assert await job.should_run(context) is True
    result = await job.execute(context)

    assert result.success is True
    assert "recorded 0 episodes" in result.summary
    assert len(generator.calls) == 1
    assert len(store.writes) == 1
    assert store.window.stage.value == "claimed"
    assert store.window.episode_result_recorded is True
    assert await job.should_run(context) is False


@pytest.mark.unit
@pytest.mark.no_network
async def test_project_semantic_episode_failure_retries_the_same_claimed_window():
    admission_store = RecordingStore([_row(1, user_content="x", assistant_content="x")])
    settings = IngestionSettings(semantic_window_tokens=1)
    admission = SemanticWindowAdmission(
        admission_store,
        settings,
        token_counter=lambda text: text.count("x"),
        episode_settings=EpisodeSettings(),
    )
    domain = make_domain_config().compile()
    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=domain,
    )
    assert selected is not None
    store = _SemanticEpisodeStore(selected.window)
    generator = _FailThenZeroEpisodeGenerator()
    now = [1_000]

    async def capture_domain():
        return domain

    job = ProjectSemanticJob(
        admission,
        store,
        generator,
        settings=settings,
        capture_domain=capture_domain,
        now_ms=lambda: now[0],
    )
    job.enabled = True
    context = JobContext(user_name="ada", project_id="project-1")

    first = await job.execute(context)

    assert first.success is False
    assert len(store.failures) == 1
    assert store.failures[0]["failure_stage"] == "episode_generation"
    assert store.window.stage.value == "claimed"
    assert store.window.next_retry_at_ms == 31_000
    assert await job.should_run(context) is False

    now[0] = 31_000
    retry = await job.execute(context)

    assert retry.success is True
    assert len(generator.calls) == 2
    assert len(store.writes) == 1
    assert store.writes[0]["window_id"] == str(selected.window.window_id)


@pytest.mark.unit
@pytest.mark.no_network
async def test_explicit_retry_reuses_the_exhausted_window_without_reselection():
    admission_store = RecordingStore([_row(1, user_content="x", assistant_content="x")])
    settings = IngestionSettings.model_validate(
        {
            "semantic_window_tokens": 1,
            "semantic_window_retry": {
                "max_attempts": 1,
                "initial_backoff_seconds": 30,
                "max_backoff_seconds": 30,
            },
        }
    )
    admission = SemanticWindowAdmission(
        admission_store,
        settings,
        token_counter=lambda text: text.count("x"),
    )
    domain = make_domain_config().compile()
    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=domain,
    )
    assert selected is not None
    store = _SemanticEpisodeStore(selected.window)
    generator = _FailThenZeroEpisodeGenerator()

    async def capture_domain():
        return domain

    job = ProjectSemanticJob(
        admission,
        store,
        generator,
        settings=settings,
        capture_domain=capture_domain,
    )
    context = JobContext(user_name="ada", project_id="project-1")

    failed = await job.execute(context)

    assert failed.success is False
    assert store.window.attempt_count == 1
    assert store.window.next_retry_at_ms is None
    assert await job.should_run(context) is False

    retried = await store.retry_project_semantic_window(
        window_id=str(selected.window.window_id),
        user_name="ada",
        project_id="project-1",
    )

    assert retried.window_id == selected.window.window_id
    assert retried.stage.value == "claimed"
    assert retried.attempt_count == 0
    assert await job.should_run(context) is True
    completed = await job.execute(context)
    assert completed.success is True
    assert admission_store.claims == []
    assert store.writes[0]["window_id"] == str(selected.window.window_id)
