"""Batch 7 semantic Knowledge/finalization stage contracts."""

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
from common.schema.ingestion.contracts import ContextEntityResult
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import (
    EntityResolutionSettings,
    IngestionSettings,
    TextProcessorSettings,
)
from core.ingestion.policy import IngestionPolicy
from core.ingestion.project_semantic_job import ProjectSemanticJob
from infrastructure.job.base import JobContext


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
        text_processor=TextProcessorSettings(),
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
            ContextEntityResult(
                entity_ids=(),
                new_entity_ids=frozenset(),
                alias_updated_ids=frozenset(),
                alias_updates={},
                pending_entity_writes={},
                project_classifications={},
                block_entity_associations=(),
                message_entity_refs=(),
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
        raise AssertionError("no-op Context reuse must not invoke entity extraction")


class _UnexpectedRelationships:
    def __init__(self):
        self.calls = 0

    async def extract(self, _build):
        self.calls += 1
        raise AssertionError("no-op Context reuse must not invoke relationship extraction")


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
        self.impact_reads = 0
        self.support_reads = 0
        self.evidence_message_reads = 0
        self.impact_block_ids = frozenset({self.block.block_id})
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

    async def get_project_context_revision_impact_block_ids(self, _revision_id, **_kwargs):
        self.impact_reads += 1
        return self.impact_block_ids

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
        return [{"message_id": 101, "content": "The active Context is grounded."}]

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
                "attempt_count": self.window.attempt_count + 1,
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
    now_ms=None,
):
    return ProjectSemanticJob(
        _Admission(),
        store,
        object(),
        settings=IngestionSettings(semantic_window_tokens=1),
        capture_semantic_policy=_capture_semantic_policy,
        context_updater=_UnusedContextUpdater(),
        context_projection=_NoopProjection(),
        context_entity_builder=builder,
        context_relationship_extractor=relationships,
        publish_committed_entity_ids=publisher,
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
async def test_reused_context_checkpoint_skips_extraction_after_knowledge_restart():
    store = _Store()
    owner_window_id = store.window.window_id
    store.window = store.window.model_copy(
        update={"window_id": uuid4()}
    )
    store.fail_commit = True
    now = [1_000]
    builder = _UnexpectedBuilder()
    relationships = _UnexpectedRelationships()
    publisher = _Publisher(store.events)

    job = _job(
        store,
        builder=builder,
        relationships=relationships,
        publisher=publisher,
        now_ms=lambda: now[0],
    )
    context = JobContext(user_name="ada", project_id="project-1")

    failed = await job.execute(context)

    assert failed.success is False
    assert store.window.stage is SemanticWindowStage.CONTEXT_COMMITTED
    assert store.snapshot.window_id == owner_window_id
    assert store.snapshot.window_id != store.window.window_id
    assert store.impact_reads == 0
    assert store.support_reads == 0
    assert store.evidence_message_reads == 0
    assert builder.calls == 0
    assert relationships.calls == 0

    now[0] = 31_001
    store.fail_commit = False
    restarted = _job(
        store,
        builder=builder,
        relationships=relationships,
        publisher=publisher,
        now_ms=lambda: now[0],
    )
    completed = await restarted.execute(context)

    assert completed.success
    assert len(store.commit_calls) == 1
    build = store.commit_calls[0]
    assert build.impact_block_ids == frozenset()
    assert build.entity_result is not None
    assert build.entity_result.entity_ids == ()
    assert build.relationship_writes == ()
    assert store.impact_reads == 0
    assert store.support_reads == 0
    assert store.evidence_message_reads == 0
    assert builder.calls == 0
    assert relationships.calls == 0
    assert publisher.calls == []
    assert store.committed_entity_id_reads == 1
    assert store.window.stage is SemanticWindowStage.COMPLETED


@pytest.mark.unit
@pytest.mark.no_network
async def test_owned_empty_context_checkpoint_completes_without_extraction():
    store = _Store()
    store.snapshot = store.snapshot.model_copy(update={"blocks": []})
    store.impact_block_ids = frozenset()
    builder = _UnexpectedBuilder()
    relationships = _UnexpectedRelationships()

    job = _job(
        store,
        builder=builder,
        relationships=relationships,
    )
    context = JobContext(user_name="ada", project_id="project-1")

    completed = await job.execute(context)

    assert completed.success
    assert store.impact_reads == 1
    assert store.support_reads == 0
    assert store.evidence_message_reads == 0
    assert len(store.commit_calls) == 1
    assert store.commit_calls[0].impact_block_ids == frozenset()
    assert builder.calls == 0
    assert relationships.calls == 0
    assert store.window.stage is SemanticWindowStage.COMPLETED
