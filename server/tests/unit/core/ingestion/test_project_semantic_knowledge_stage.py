"""Semantic Knowledge/finalization stage contracts."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextBlockSupportRecord,
    ContextRevisionOrigin,
    ContextSnapshot,
)
from common.schema.ingestion.contracts import EntityResult
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import (
    EntityResolutionSettings,
    ExtractionSettings,
    IngestionSettings,
)
from core.ingestion.policy import IngestionPolicy
from core.ingestion.project_semantic_processor import ProjectSemanticProcessor
from infrastructure.job.base import JobContext, JobResult


def _domain():
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {"Concept": {"topic": "Work", "labels": ["concept"]}},
        }
    ).compile()


def _policy():
    return IngestionPolicy.capture(
        extraction=ExtractionSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=_domain(),
    )


async def _capture_semantic_policy():
    return _policy()


class _Admission:
    def update_settings(self, _settings):
        pass


class _Builder:
    async def build(self, build):
        build.set_entity_result(
            EntityResult(
                entity_ids=(),
                new_entity_ids=frozenset(),
                alias_updated_ids=frozenset(),
                alias_updates={},
                pending_entity_writes={},
                project_classifications={},
            )
        )
        return build.entity_result


class _Relationships:
    async def extract(self, build):
        build.set_relationship_writes(())
        return ()


class _UnexpectedBuilder:
    def __init__(self):
        self.calls = 0

    async def build(self, _build):
        self.calls += 1
        raise AssertionError("a window without Episodes must not extract entities")


class _UnexpectedRelationships:
    def __init__(self):
        self.calls = 0

    async def extract(self, _build):
        self.calls += 1
        raise AssertionError("a window without Episodes must not extract relationships")


class _Store:
    def __init__(self):
        domain = _domain()
        window_id = uuid4()
        self.block = ContextBlockRecord(
            block_id=uuid4(),
            project_id="project-1",
            section_key="current_state",
            markdown="The active Context is grounded.",
            content_hash="a" * 64,
            assertion_kind=AssertionKind.SOURCE_GROUNDED,
        )
        self.snapshot = ContextSnapshot(
            revision_id=uuid4(),
            project_id="project-1",
            revision_number=1,
            window_id=window_id,
            origin=ContextRevisionOrigin.CONVERSATION,
            domain_version=domain.version,
            content_hash="b" * 64,
            blocks=[self.block],
        )
        self.window = SemanticWindowRecord(
            window_id=window_id,
            user_name="ada",
            project_id="project-1",
            origin=SemanticWindowOrigin.CONVERSATION,
            stage=SemanticWindowStage.CONTEXT_COMMITTED,
            domain_version=domain.version,
            policy_snapshot={"ingestion_policy": _policy().semantic_window_snapshot()},
            source_token_count=1,
            token_estimator="test",
            token_estimator_version="1",
            episode_result_recorded=True,
            context_revision_id=self.snapshot.revision_id,
        )
        self.commit_calls = []
        self.support_reads = 0
        self.evidence_message_reads = 0
        self.episodes = [
            SimpleNamespace(
                episode_id="ep-1",
                summary="Ada set up the grounded Context.",
                messages=[
                    SimpleNamespace(message_id=102, message_position=1),
                    SimpleNamespace(message_id=101, message_position=0),
                ],
            )
        ]
        self.recent = [
            SimpleNamespace(episode_id="ep-1", summary="This window."),
            SimpleNamespace(episode_id="ep-0", summary="Newest earlier."),
            SimpleNamespace(episode_id="ep--1", summary="Older earlier."),
        ]
        self.committed_entity_ids = (101, 202)
        self.committed_entity_id_reads = 0
        self.enrich_calls = 0
        self.fail_commit = False
        self.commit_without_checkpoint = False
        self.fail_enrichment = False
        self.failures = []
        self.events = []

    async def get_active_project_semantic_window(self, **_kwargs):
        return None if self.window.stage is SemanticWindowStage.COMPLETED else self.window

    async def get_project_context_snapshot(self, _revision_id, **_kwargs):
        return self.snapshot

    async def get_project_semantic_window_episode_result(self, _window_id, **_kwargs):
        return self.episodes

    async def get_recent_project_episodes(self, *, limit, **_kwargs):
        return self.recent[:limit]

    async def get_project_context_block_supports(self, block_ids, **_kwargs):
        self.support_reads += 1
        if str(self.block.block_id) not in set(block_ids):
            return {}
        return {
            self.block.block_id: (
                ContextBlockSupportRecord(
                    block_id=self.block.block_id,
                    project_id="project-1",
                    message_id=101,
                    session_id="session-1",
                    support_kind="user_message",
                ),
            )
        }

    async def get_project_semantic_window_evidence_messages(self, _window_id, **_kwargs):
        self.evidence_message_reads += 1
        return [
            {"message_id": 101, "role": "user", "content": "The active Context is grounded.",
             "timestamp_ms": 5},
            {"message_id": 102, "role": "assistant", "content": "Noted.", "timestamp_ms": 6},
        ]

    async def commit_project_semantic_knowledge(self, build):
        assert build.entity_result is not None
        if self.fail_commit:
            raise OSError("Knowledge commit unavailable")
        self.commit_calls.append(build)
        self.events.append("commit")
        if not self.commit_without_checkpoint:
            self.window = self.window.model_validate(
                self.window.model_dump()
                | {"stage": SemanticWindowStage.KNOWLEDGE_COMMITTED}
            )
        return SimpleNamespace(resumed=False, relationships_written=0)

    async def get_project_semantic_window_committed_entity_ids(self, _window_id, **_kwargs):
        self.committed_entity_id_reads += 1
        self.events.append("committed_entity_ids")
        if self.snapshot.window_id != self.window.window_id:
            return ()
        return self.committed_entity_ids

    async def enrich_project_semantic_window_episodes(self, **_kwargs):
        if self.fail_enrichment:
            raise OSError("Episode enrichment unavailable")
        self.enrich_calls += 1
        self.events.append("enrich")
        return {"entities": 0, "relationships": 0}

    async def advance_project_semantic_window_stage(self, **kwargs):
        assert kwargs["expected_stage"] is SemanticWindowStage.KNOWLEDGE_COMMITTED
        assert kwargs["next_stage"] is SemanticWindowStage.COMPLETED
        self.events.append("complete")
        self.window = self.window.model_validate(
            self.window.model_dump() | {"stage": SemanticWindowStage.COMPLETED}
        )
        return True

    async def record_project_semantic_window_failure(self, **kwargs):
        self.failures.append(kwargs)
        self.window = self.window.model_validate(
            self.window.model_dump()
            | {
                "attempt_count": self.window.attempt_count
                + (1 if kwargs.get("consume_attempt", True) else 0),
                "last_failure_stage": kwargs["failure_stage"],
                "last_failure_code": kwargs["failure_code"],
                "last_failure_at_ms": kwargs["failed_at_ms"],
                "last_error_summary": kwargs["error_summary"],
                "next_retry_at_ms": kwargs["next_retry_at_ms"],
            }
        )
        return self.window


class _FailingBuilder:
    async def build(self, _build):
        raise OSError("Entity extraction unavailable")


class _FailingRelationships:
    async def extract(self, _build):
        raise OSError("Relationship extraction unavailable")


class _Publisher:
    def __init__(self, events, *, fail=False):
        self.events = events
        self.fail = fail
        self.calls = []

    async def __call__(self, entity_ids):
        self.events.append("publish")
        self.calls.append(tuple(entity_ids))
        if self.fail:
            raise OSError("Resolver publication unavailable")


class _UnusedContextUpdater:
    async def update(self, **_kwargs):
        raise AssertionError("Knowledge-stage tests must not rebuild Context")


class _NoopProjection:
    async def synchronize(self, **_kwargs):
        return None


async def _publish_nothing(_entity_ids):
    return None


def _job(
    store,
    *,
    builder,
    relationships,
    publisher=_publish_nothing,
    signal_advisories=None,
    now_ms=None,
):
    return ProjectSemanticProcessor(
        _Admission(),
        store,
        object(),
        settings=IngestionSettings(semantic_window_tokens=1),
        capture_semantic_policy=_capture_semantic_policy,
        context_updater=_UnusedContextUpdater(),
        context_projection=_NoopProjection(),
        entity_builder=builder,
        relationship_extractor=relationships,
        publish_committed_entity_ids=publisher,
        signal_relationship_advisories=signal_advisories,
        now_ms=now_ms,
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_knowledge_commit_precedes_episode_enrichment_and_completes_terminally():
    store = _Store()
    publisher = _Publisher(store.events)

    job = _job(
        store,
        builder=_Builder(),
        relationships=_Relationships(),
        publisher=publisher,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")

    completed = await job.execute(ctx)

    assert completed.success
    assert len(store.commit_calls) == 1
    assert publisher.calls == [(101, 202)]
    assert store.enrich_calls == 1
    assert store.events == [
        "commit",
        "committed_entity_ids",
        "publish",
        "enrich",
        "complete",
    ]
    assert store.window.stage is SemanticWindowStage.COMPLETED


@pytest.mark.unit
@pytest.mark.no_network
async def test_advisory_refresh_is_woken_after_knowledge_commit():
    store = _Store()
    job = _job(
        store,
        builder=_Builder(),
        relationships=_Relationships(),
        signal_advisories=lambda: store.events.append("advisory_wake") or True,
    )

    result = await job.execute(JobContext(user_name="ada", project_id="project-1"))

    assert result.success
    assert store.events.index("commit") < store.events.index("advisory_wake")


@pytest.mark.unit
@pytest.mark.no_network
async def test_the_build_holds_the_windows_episodes_hints_and_earlier_summaries():
    store = _Store()

    completed = await _job(store, builder=_Builder(), relationships=_Relationships()).execute(
        JobContext(user_name="ada", project_id="project-1")
    )

    assert completed.success
    build = store.commit_calls[0]
    [episode] = build.episodes
    assert [(m.message_id, m.role, m.content) for m in episode.messages] == [
        (101, "user", "The active Context is grounded."),
        (102, "assistant", "Noted."),
    ]
    assert build.context_hints == {101: ("The active Context is grounded.",)}
    # The larger lookback (2), oldest first, never this window's own Episode.
    assert build.earlier_summaries == ("Older earlier.", "Newest earlier.")


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("builder", "relationships", "fail_commit", "failure_stage"),
    [
        (_FailingBuilder(), _Relationships(), False, "knowledge_reconciliation"),
        (_Builder(), _FailingRelationships(), False, "knowledge_reconciliation"),
        (_Builder(), _Relationships(), True, "knowledge_reconciliation"),
    ],
)
async def test_knowledge_failures_keep_the_context_checkpoint_for_restart(
    builder, relationships, fail_commit, failure_stage
):
    store = _Store()
    store.fail_commit = fail_commit

    job = _job(
        store,
        builder=builder,
        relationships=relationships,
        now_ms=lambda: 1_000,
    )

    result = await job.execute(JobContext(user_name="ada", project_id="project-1"))

    assert result.success is False
    assert store.window.stage is SemanticWindowStage.CONTEXT_COMMITTED
    assert store.window.context_revision_id == store.snapshot.revision_id
    assert store.failures[0]["failure_stage"] == failure_stage
    assert store.enrich_calls == 0


@pytest.mark.unit
@pytest.mark.no_network
async def test_success_without_a_durable_checkpoint_stops_the_drain():
    store = _Store()
    store.commit_without_checkpoint = True
    job = _job(
        store,
        builder=_Builder(),
        relationships=_Relationships(),
    )

    result = await job.execute(JobContext(user_name="ada", project_id="project-1"))

    assert result.success
    assert result.summary == "Semantic processor stopped without durable progress"
    assert len(store.commit_calls) == 1
    assert store.window.stage is SemanticWindowStage.CONTEXT_COMMITTED
    assert store.enrich_calls == 0


@pytest.mark.unit
@pytest.mark.no_network
async def test_semantic_drain_yields_when_another_window_replaces_active_work():
    class ReplacingStore(_Store):
        async def commit_project_semantic_knowledge(self, build):
            result = await super().commit_project_semantic_knowledge(build)
            self.window = self.window.model_validate(
                self.window.model_dump() | {"window_id": uuid4()}
            )
            return result

    store = ReplacingStore()
    job = _job(store, builder=_Builder(), relationships=_Relationships())

    result = await job.execute(JobContext(user_name="ada", project_id="project-1"))

    assert result.success
    assert result.summary == "Semantic processor yielded after the active window changed"
    assert len(store.commit_calls) == 1
    assert store.enrich_calls == 0


@pytest.mark.unit
@pytest.mark.no_network
async def test_semantic_drain_stops_after_progress_when_no_next_stage_is_due(
    monkeypatch,
):
    store = _Store()
    job = _job(store, builder=_Builder(), relationships=_Relationships())

    async def checkpoint(window, _ctx):
        store.window = window.model_validate(
            window.model_dump() | {"attempt_count": window.attempt_count + 1}
        )
        return JobResult(success=True, summary="checkpointed")

    def next_stage(window):
        if window.attempt_count == 0:
            return "checkpoint", checkpoint
        return None

    monkeypatch.setattr(job, "_next_due_stage", next_stage)

    result = await job._drain_window(
        store.window,
        JobContext(user_name="ada", project_id="project-1"),
    )

    assert result.success
    assert result.summary == "Semantic processor stopped after durable stages: checkpoint"


@pytest.mark.unit
@pytest.mark.no_network
async def test_episode_enrichment_failure_keeps_the_knowledge_checkpoint_for_restart():
    store = _Store()
    store.window = store.window.model_validate(
        store.window.model_dump() | {"stage": SemanticWindowStage.KNOWLEDGE_COMMITTED}
    )
    store.fail_enrichment = True
    publisher = _Publisher(store.events)

    job = _job(
        store,
        builder=_Builder(),
        relationships=_Relationships(),
        publisher=publisher,
        now_ms=lambda: 1_000,
    )

    result = await job.execute(JobContext(user_name="ada", project_id="project-1"))

    assert result.success is False
    assert store.window.stage is SemanticWindowStage.KNOWLEDGE_COMMITTED
    assert store.failures[0]["failure_stage"] == "episode_enrichment"
    assert publisher.calls == [(101, 202)]


@pytest.mark.unit
@pytest.mark.no_network
async def test_publication_failure_retries_only_durable_entity_ids_after_restart():
    store = _Store()
    store.window = store.window.model_validate(
        store.window.model_dump() | {"stage": SemanticWindowStage.KNOWLEDGE_COMMITTED}
    )
    now = [1_000]
    failing_publisher = _Publisher(store.events, fail=True)

    failed_job = _job(
        store,
        builder=_UnexpectedBuilder(),
        relationships=_UnexpectedRelationships(),
        publisher=failing_publisher,
        now_ms=lambda: now[0],
    )
    context = JobContext(user_name="ada", project_id="project-1")

    failed = await failed_job.execute(context)

    assert failed.success is False
    assert store.window.stage is SemanticWindowStage.KNOWLEDGE_COMMITTED
    assert store.failures[-1]["failure_stage"] == "resolver_publication"
    assert failing_publisher.calls == [(101, 202)]
    assert store.enrich_calls == 0
    assert store.commit_calls == []

    now[0] = 31_001
    recovered_publisher = _Publisher(store.events)
    restarted = _job(
        store,
        builder=_UnexpectedBuilder(),
        relationships=_UnexpectedRelationships(),
        publisher=recovered_publisher,
        now_ms=lambda: now[0],
    )

    recovered = await restarted.execute(context)

    assert recovered.success
    assert recovered_publisher.calls == [(101, 202)]
    assert store.committed_entity_id_reads == 2
    assert store.enrich_calls == 1
    assert store.commit_calls == []
    assert store.window.stage is SemanticWindowStage.COMPLETED


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("episodes", [None, []])
async def test_a_window_without_episodes_completes_without_extraction(episodes):
    store = _Store()
    store.episodes = episodes
    builder = _UnexpectedBuilder()
    relationships = _UnexpectedRelationships()

    completed = await _job(store, builder=builder, relationships=relationships).execute(
        JobContext(user_name="ada", project_id="project-1")
    )

    assert completed.success
    assert store.support_reads == 0
    assert store.evidence_message_reads == 0
    [build] = store.commit_calls
    assert build.episodes == ()
    assert build.entity_result.entity_ids == ()
    assert build.relationship_writes == ()
    assert builder.calls == 0 and relationships.calls == 0
    assert store.window.stage is SemanticWindowStage.COMPLETED
