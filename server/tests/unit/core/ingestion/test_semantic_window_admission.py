from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.schema.context import ContextRevisionOrigin, ContextSnapshot
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
        "already_claimed": claimed,
    }


def _admission(rows, *, target=128_000, now_ms=1_000):
    return SemanticWindowAdmission(
        RecordingStore(rows),
        IngestionSettings(semantic_window_tokens=target),
        token_counter=lambda text: text.count("x"),
        now_ms=lambda: now_ms,
    )


def _policy(domain):
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
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
async def test_clarification_keeps_its_durable_assistant_question_as_evidence():
    rows = [
        _row(
            1,
            outcome="clarification",
            assistant_id=10_001,
            user_content="x",
            assistant_content="x",
        )
    ]
    admission = _admission(rows, target=2)

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=make_domain_config().compile(),
    )

    assert selected is not None
    assert [member.message_id for member in selected.messages] == [1, 10_001]


@pytest.mark.unit
@pytest.mark.no_network
async def test_failed_and_cancelled_exchanges_do_not_enter_semantic_evidence():
    rows = [
        _row(1, outcome="failed", user_content="x"),
        _row(2, outcome="cancelled", user_content="x"),
        _row(3, outcome="assistant_final", user_content="x", assistant_content="x"),
    ]
    admission = _admission(rows, target=2)

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=make_domain_config().compile(),
    )

    assert selected is not None
    assert [member.message_id for member in selected.messages] == [3, 10_003]


@pytest.mark.unit
@pytest.mark.no_network
async def test_user_only_exchange_remains_deliberate_user_evidence():
    admission = _admission([_row(1, outcome="user_only", user_content="x")], target=1)

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=make_domain_config().compile(),
    )

    assert selected is not None
    assert [member.message_id for member in selected.messages] == [1]


@pytest.mark.unit
@pytest.mark.no_network
async def test_selection_reuses_the_last_exact_prefix_token_count():
    counted_values: list[int] = []

    def count_tokens(text: str) -> int:
        # Separators contribute non-additively, so independent exchange counts
        # cannot replace the exact rendered-prefix count.
        count = len(text) + (7 * text.count("\n\n"))
        counted_values.append(count)
        return count

    admission = SemanticWindowAdmission(
        RecordingStore(
            [
                _row(1, user_content="x", assistant_content="x"),
                _row(2, user_content="x", assistant_content="x"),
            ]
        ),
        IngestionSettings(semantic_window_tokens=50),
        token_counter=count_tokens,
    )

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=make_domain_config().compile(),
    )

    assert selected is not None
    assert selected.close_reason == "target_crossed"
    assert len(counted_values) == 2
    assert selected.window.source_token_count == counted_values[-1]
    assert selected.window.overfill_tokens == selected.window.source_token_count - 50


@pytest.mark.unit
@pytest.mark.no_network
async def test_admission_persists_the_exact_context_entity_policy():
    compiled_domain = make_domain_config().compile()
    frozen_policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(gliner_threshold=0.42),
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
    ingestion_snapshot = selected.window.policy_snapshot["ingestion_policy"]
    assert IngestionPolicy.from_semantic_window_snapshot(ingestion_snapshot) == frozen_policy
    assert set(ingestion_snapshot) == {
        "gliner_threshold",
        "candidate_fuzzy_threshold",
            "resolution_threshold",
            "resolution_margin",
        "common_word_frequency_threshold",
        "sparse_context_verbs",
        "compiled_domain",
    }
    assert set(selected.window.policy_snapshot["episode_generation_policy"]) == {
        "version",
        "enabled",
        "max_episode_source_messages",
        "max_episode_source_tokens",
        "max_narrative_chars",
    }
    assert "episode_window_size" not in selected.window.policy_snapshot

    with pytest.raises(ValueError, match="shape"):
        IngestionPolicy.from_semantic_window_snapshot(
            ingestion_snapshot | {"llm_ner": False}
        )


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
        self.context_snapshot = None

    async def get_active_project_semantic_window(self, **_kwargs):
        return (
            None
            if self.window.stage.value == "completed"
            else self.window
        )

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
            self.window.model_dump()
            | {
                "episode_result_recorded": True,
                "attempt_count": 0,
                "last_failure_stage": None,
                "last_failure_code": None,
                "last_failure_at_ms": None,
                "last_error_summary": None,
                "next_retry_at_ms": None,
            }
        )
        return True

    async def get_project_semantic_window_context_snapshot(self, _window_id, **_kwargs):
        return self.context_snapshot

    async def get_current_project_context_revision(self, **_kwargs):
        return None

    async def get_project_semantic_window_assistant_source_refs(self, _window_id, **_kwargs):
        return []

    async def commit_project_context_revision(self, **kwargs):
        self.context_snapshot = ContextSnapshot(
            revision_id=uuid4(),
            project_id="project-1",
            revision_number=1,
            window_id=uuid4() if kwargs["window_id"] is None else kwargs["window_id"],
            origin=ContextRevisionOrigin.CONVERSATION,
            domain_version=self.window.domain_version,
            content_hash=kwargs["materialization"].content_hash,
            blocks=list(kwargs["materialization"].blocks),
        )
        return self.context_snapshot

    async def advance_project_semantic_window_stage(self, **kwargs):
        if kwargs["expected_stage"].value == "claimed":
            stage = "context_committed"
            context_revision_id = kwargs["context_revision_id"]
        else:
            assert kwargs["expected_stage"].value == "knowledge_committed"
            assert kwargs["next_stage"].value == "completed"
            stage = "completed"
            context_revision_id = self.window.context_revision_id
        self.window = self.window.model_validate(
            self.window.model_dump()
            | {
                "stage": stage,
                "context_revision_id": context_revision_id,
                "attempt_count": 0,
                "last_failure_stage": None,
                "last_failure_code": None,
                "last_failure_at_ms": None,
                "last_error_summary": None,
                "next_retry_at_ms": None,
            }
        )
        return True

    async def get_project_context_snapshot(self, _revision_id, **_kwargs):
        return self.context_snapshot

    async def get_project_context_revision_impact_block_ids(self, _revision_id, **_kwargs):
        return frozenset()

    async def get_project_context_block_supports(self, _block_ids, **_kwargs):
        return {}

    async def commit_project_semantic_knowledge(self, _build):
        self.window = self.window.model_validate(
            self.window.model_dump()
            | {
                "stage": "knowledge_committed",
                "attempt_count": 0,
                "last_failure_stage": None,
                "last_failure_code": None,
                "last_failure_at_ms": None,
                "last_error_summary": None,
                "next_retry_at_ms": None,
            }
        )
        return SimpleNamespace(resumed=False, relationships_written=0)

    async def get_project_semantic_window_committed_entity_ids(self, _window_id, **_kwargs):
        return ()

    async def enrich_project_semantic_window_episodes(self, **_kwargs):
        return {"entities": 0, "relationships": 0}

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


class _FailTwiceThenZeroEpisodeGenerator(_ZeroEpisodeGenerator):
    async def generate(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) <= 2:
            raise ConnectionError("temporary episode provider outage")
        return SimpleNamespace(final_episodes=[])


class _NoopContextUpdater:
    async def update(self, **_kwargs):
        return SimpleNamespace(materialization=None, edit_summary="No Context change")


class _FailingContextUpdater:
    def __init__(self):
        self.calls = 0

    async def update(self, **_kwargs):
        self.calls += 1
        raise ConnectionError("temporary Context provider outage")


class _NoopProjection:
    async def synchronize(self, **_kwargs):
        return None


class _UnexpectedBuilder:
    async def build(self, _build):
        raise AssertionError("empty Context impact must not build entities")


class _UnexpectedRelationships:
    async def extract(self, _build):
        raise AssertionError("empty Context impact must not extract relationships")


async def _publish_nothing(_entity_ids):
    return None


def _job(
    admission,
    store,
    generator,
    *,
    settings,
    capture_semantic_policy,
    context_updater=None,
    now_ms=None,
):
    return ProjectSemanticJob(
        admission,
        store,
        generator,
        settings=settings,
        capture_semantic_policy=capture_semantic_policy,
        context_updater=context_updater or _NoopContextUpdater(),
        context_projection=_NoopProjection(),
        context_entity_builder=_UnexpectedBuilder(),
        context_relationship_extractor=_UnexpectedRelationships(),
        publish_committed_entity_ids=_publish_nothing,
        now_ms=now_ms,
    )


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

    async def capture_semantic_policy():
        return _policy(domain)

    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=domain,
        ingestion_policy=_policy(domain),
    )
    assert selected is not None
    store = _SemanticEpisodeStore(selected.window)
    generator = _ZeroEpisodeGenerator()
    job = _job(
        admission,
        store,
        generator,
        settings=IngestionSettings(semantic_window_tokens=1),
        capture_semantic_policy=capture_semantic_policy,
    )
    job.enabled = True
    context = JobContext(user_name="ada", project_id="project-1")

    assert await job.should_run(context) is True
    result = await job.execute(context)

    assert result.success is True
    assert result.summary == (
        "Semantic processor completed durable stages: "
        "Episode -> Context -> Knowledge -> finalization"
    )
    assert len(generator.calls) == 1
    assert len(store.writes) == 1
    assert store.window.stage.value == "completed"
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
        ingestion_policy=_policy(domain),
    )
    assert selected is not None
    store = _SemanticEpisodeStore(selected.window)
    generator = _FailThenZeroEpisodeGenerator()
    now = [1_000]

    async def capture_semantic_policy():
        return _policy(domain)

    job = _job(
        admission,
        store,
        generator,
        settings=settings,
        capture_semantic_policy=capture_semantic_policy,
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

    cadence_attempt = await job.execute(context)

    assert cadence_attempt.success is True
    assert cadence_attempt.summary == "No semantic window stage is due"
    assert len(generator.calls) == 1
    assert admission_store.claims == []

    now[0] = 31_000
    retry = await job.execute(context)

    assert retry.success is True
    assert len(generator.calls) == 2
    assert len(store.writes) == 1
    assert store.writes[0]["window_id"] == str(selected.window.window_id)


@pytest.mark.unit
@pytest.mark.no_network
async def test_successful_episode_checkpoint_resets_the_context_retry_budget():
    admission_store = RecordingStore([_row(1, user_content="x", assistant_content="x")])
    settings = IngestionSettings.model_validate(
        {
            "semantic_window_tokens": 1,
            "semantic_window_retry": {
                "max_attempts": 3,
                "initial_backoff_seconds": 30,
                "max_backoff_seconds": 300,
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
        ingestion_policy=_policy(domain),
    )
    assert selected is not None
    store = _SemanticEpisodeStore(selected.window)
    generator = _FailTwiceThenZeroEpisodeGenerator()
    context_updater = _FailingContextUpdater()
    now = [1_000]

    async def capture_semantic_policy():
        return _policy(domain)

    job = _job(
        admission,
        store,
        generator,
        settings=settings,
        capture_semantic_policy=capture_semantic_policy,
        context_updater=context_updater,
        now_ms=lambda: now[0],
    )
    context = JobContext(user_name="ada", project_id="project-1")

    assert (await job.execute(context)).success is False
    now[0] = 31_000
    assert (await job.execute(context)).success is False
    now[0] = 91_000
    context_failure = await job.execute(context)

    assert context_failure.success is False
    assert len(generator.calls) == 3
    assert context_updater.calls == 1
    assert [failure["failure_stage"] for failure in store.failures] == [
        "episode_generation",
        "episode_generation",
        "context_update",
    ]
    assert store.window.attempt_count == 1
    assert store.window.last_failure_stage == "context_update"
    assert store.window.next_retry_at_ms == 121_000


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
        ingestion_policy=_policy(domain),
    )
    assert selected is not None
    store = _SemanticEpisodeStore(selected.window)
    generator = _FailThenZeroEpisodeGenerator()

    async def capture_semantic_policy():
        return _policy(domain)

    job = _job(
        admission,
        store,
        generator,
        settings=settings,
        capture_semantic_policy=capture_semantic_policy,
    )
    context = JobContext(user_name="ada", project_id="project-1")

    failed = await job.execute(context)

    assert failed.success is False
    assert store.window.attempt_count == 1
    assert store.window.next_retry_at_ms is None
    assert await job.should_run(context) is False

    cadence_attempt = await job.execute(context)

    assert cadence_attempt.success is True
    assert cadence_attempt.summary == "No semantic window stage is due"
    assert len(generator.calls) == 1
    assert admission_store.claims == []

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
