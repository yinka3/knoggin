import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextAdd,
    ContextBlockRecord,
    ContextRevisionOrigin,
    ContextSnapshot,
)
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import IngestionSettings
from core.ingestion.project_semantic_job import ProjectSemanticJob
from core.knowledge.context.models import ContextRevisionConflictError
from core.knowledge.context.projection import ContextProjectionResult
from core.knowledge.context.render import apply_context_edits, context_block_hash
from core.knowledge.context.updater import ContextUpdateResult
from infrastructure.job.base import JobContext
from infrastructure.job.scheduler import Scheduler


def _domain():
    return DomainConfig(version=1, topics=(), entity_types=()).compile()


def _window():
    domain = _domain()
    return SemanticWindowRecord(
        window_id=uuid4(),
        user_name="ada",
        project_id="project-1",
        origin=SemanticWindowOrigin.CONVERSATION,
        stage=SemanticWindowStage.CLAIMED,
        domain_version=domain.version,
        policy_snapshot={"compiled_domain": domain.to_dict()},
        source_token_count=1,
        token_estimator="test",
        token_estimator_version="1",
        episode_result_recorded=True,
    )


def _block(markdown: str) -> ContextBlockRecord:
    return ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key="current_state",
        markdown=markdown,
        content_hash=context_block_hash(markdown),
        assertion_kind=AssertionKind.AGENT_DERIVED,
    )


def _snapshot(*blocks: ContextBlockRecord) -> ContextSnapshot:
    return ContextSnapshot(
        revision_id=uuid4(),
        project_id="project-1",
        revision_number=1,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        content_hash="a" * 64,
        blocks=list(blocks),
    )


class _Admission:
    def update_settings(self, _settings):
        pass


class _ContextStore:
    def __init__(self, window, *, current_snapshot=None, committed_snapshot=None):
        self.window = window
        self.current_snapshot = current_snapshot
        self.committed_snapshot = committed_snapshot
        self.commit_calls = []
        self.advance_calls = []
        self.failures = []
        self.fail_commit_once = False
        self.external_snapshot_on_conflict = None

    async def get_active_project_semantic_window(self, **_kwargs):
        return self.window

    async def get_project_semantic_window_context_snapshot(self, _window_id, **_kwargs):
        return self.committed_snapshot

    async def get_current_project_context_revision(self, **_kwargs):
        if self.current_snapshot is None:
            return None
        return SimpleNamespace(revision_id=self.current_snapshot.revision_id)

    async def get_project_context_snapshot(self, _revision_id, **_kwargs):
        return self.current_snapshot

    async def get_project_semantic_window_episode_result(self, _window_id, **_kwargs):
        return []

    async def get_project_semantic_window_evidence_messages(self, _window_id, **_kwargs):
        return [
            {
                "message_id": 1,
                "session_id": "session-1",
                "role": "user",
                "content": "Use the new Context stage.",
                "timestamp_ms": 10,
            }
        ]

    async def get_project_semantic_window_assistant_source_refs(self, _window_id, **_kwargs):
        return []

    async def commit_project_context_revision(self, **kwargs):
        self.commit_calls.append(kwargs)
        if self.fail_commit_once:
            self.fail_commit_once = False
            self.current_snapshot = self.external_snapshot_on_conflict
            raise ContextRevisionConflictError("Context changed before this revision could be committed")
        materialization = kwargs["materialization"]
        parent = self.current_snapshot
        self.current_snapshot = ContextSnapshot(
            revision_id=uuid4(),
            project_id="project-1",
            revision_number=1 if parent is None else parent.revision_number + 1,
            parent_revision_id=None if parent is None else parent.revision_id,
            window_id=uuid4(),
            origin=ContextRevisionOrigin.CONVERSATION,
            domain_version=1,
            edit_summary=kwargs["edit_summary"],
            content_hash=materialization.content_hash,
            blocks=list(materialization.blocks),
        )
        self.committed_snapshot = self.current_snapshot
        return self.current_snapshot

    async def advance_project_semantic_window_stage(self, **kwargs):
        self.advance_calls.append(kwargs)
        self.window = self.window.model_validate(
            self.window.model_dump()
            | {
                "stage": SemanticWindowStage.CONTEXT_COMMITTED,
                "context_revision_id": kwargs["context_revision_id"],
                "last_failure_stage": None,
                "last_failure_code": None,
                "last_failure_at_ms": None,
                "last_error_summary": None,
                "next_retry_at_ms": None,
            }
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


class _Updater:
    def __init__(self):
        self.calls = []

    async def update(self, *, snapshot, domain, **kwargs):
        self.calls.append({"snapshot": snapshot, **kwargs})
        materialization = apply_context_edits(
            snapshot,
            [
                ContextAdd(
                    section_key="current_state",
                    markdown="The Context stage is active.",
                    evidence=[{"handle": "M1"}],
                )
            ],
            domain,
            project_id="project-1",
        )
        return ContextUpdateResult(
            materialization=materialization,
            edit_summary="Activated Context stage",
            operation_count=1,
        )


class _NoopUpdater(_Updater):
    async def update(self, *, snapshot, **kwargs):
        self.calls.append({"snapshot": snapshot, **kwargs})
        return ContextUpdateResult(
            materialization=None,
            edit_summary="No durable Context change",
            operation_count=0,
        )


class _FailingProjection:
    def __init__(self):
        self.calls = 0
        self.failures = []

    async def synchronize(self, **_kwargs):
        self.calls += 1
        raise OSError("local projection temporarily unavailable")

    async def record_sync_failure(self, **kwargs):
        self.failures.append(kwargs)


class _IdleAdmission:
    def update_settings(self, _settings):
        pass

    async def select(self, **_kwargs):
        return None

    async def claim_next(self, **_kwargs):
        return None


class _IdleStore:
    async def get_active_project_semantic_window(self, **_kwargs):
        return None


class _RecordingProjection:
    def __init__(self):
        self.called = asyncio.Event()
        self.allow_user_edit = None

    async def synchronize(self, **kwargs):
        self.allow_user_edit = kwargs["allow_user_edit"]
        self.called.set()
        return ContextProjectionResult(snapshot=None, changed=False)

    async def record_sync_failure(self, **_kwargs):
        raise AssertionError("the idle synchronization should not fail")


def _job(store, updater, *, now_ms=lambda: 1_000, projection=None):
    async def capture_domain():
        return _domain()

    return ProjectSemanticJob(
        _Admission(),
        store,
        object(),
        settings=IngestionSettings(semantic_window_tokens=1),
        capture_domain=capture_domain,
        context_updater=updater,
        context_projection=projection,
        now_ms=now_ms,
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_stage_commits_then_checkpoints_even_if_file_projection_needs_repair():
    store = _ContextStore(_window())
    updater = _Updater()
    projection = _FailingProjection()
    job = _job(store, updater, projection=projection)
    context = JobContext(user_name="ada", project_id="project-1")

    assert await job.should_run(context)
    result = await job.execute(context)

    assert result.success
    assert len(updater.calls) == 1
    assert len(store.commit_calls) == 1
    assert store.window.stage is SemanticWindowStage.CONTEXT_COMMITTED
    assert store.window.context_revision_id == store.current_snapshot.revision_id
    assert projection.calls == 1
    assert projection.failures[0]["exc"].args == ("local projection temporarily unavailable",)


@pytest.mark.unit
@pytest.mark.no_network
async def test_scheduler_cadence_runs_context_sync_without_semantic_work():
    projection = _RecordingProjection()

    async def capture_domain():
        return _domain()

    job = ProjectSemanticJob(
        _IdleAdmission(),
        _IdleStore(),
        object(),
        settings=IngestionSettings(semantic_window_tokens=1),
        capture_domain=capture_domain,
        context_projection=projection,
    )
    scheduler = Scheduler("ada", "project-1")
    scheduler.register(job)

    await scheduler.start()
    try:
        await asyncio.wait_for(projection.called.wait(), timeout=1)
    finally:
        await scheduler.stop()

    assert projection.allow_user_edit is True


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_stage_resumes_a_durable_revision_without_recalling_the_llm():
    committed = _snapshot(_block("Already durable."))
    store = _ContextStore(_window(), current_snapshot=committed, committed_snapshot=committed)
    updater = _Updater()
    job = _job(store, updater)

    result = await job.execute(JobContext(user_name="ada", project_id="project-1"))

    assert result.success
    assert "resumed" in result.summary
    assert updater.calls == []
    assert store.commit_calls == []
    assert store.advance_calls[0]["context_revision_id"] == str(committed.revision_id)


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_noop_records_the_current_revision_without_creating_a_child():
    current = _snapshot(_block("Current Context is already sufficient."))
    store = _ContextStore(_window(), current_snapshot=current)
    updater = _NoopUpdater()
    job = _job(store, updater)

    result = await job.execute(JobContext(user_name="ada", project_id="project-1"))

    assert result.success
    assert len(updater.calls) == 1
    assert store.commit_calls == []
    assert store.advance_calls[0]["context_revision_id"] == str(current.revision_id)
    assert store.window.stage is SemanticWindowStage.CONTEXT_COMMITTED


@pytest.mark.unit
@pytest.mark.no_network
async def test_first_context_noop_commits_an_explicit_empty_revision_for_the_checkpoint():
    store = _ContextStore(_window())
    updater = _NoopUpdater()
    job = _job(store, updater)

    result = await job.execute(JobContext(user_name="ada", project_id="project-1"))

    assert result.success
    assert len(store.commit_calls) == 1
    assert store.commit_calls[0]["materialization"].blocks == ()
    assert store.window.context_revision_id == store.current_snapshot.revision_id


@pytest.mark.unit
@pytest.mark.no_network
async def test_stale_parent_conflict_retries_with_the_same_window_evidence_and_reloaded_context():
    now = [1_000]
    store = _ContextStore(_window())
    store.fail_commit_once = True
    store.external_snapshot_on_conflict = _snapshot(_block("Concurrent Context revision."))
    updater = _Updater()
    job = _job(store, updater, now_ms=lambda: now[0])
    context = JobContext(user_name="ada", project_id="project-1")

    first = await job.execute(context)

    assert not first.success
    assert store.failures[0]["failure_stage"] == "context_update"
    assert store.window.stage is SemanticWindowStage.CLAIMED
    assert updater.calls[0]["messages"][0]["message_id"] == 1

    now[0] = 31_000
    second = await job.execute(context)

    assert second.success
    assert len(updater.calls) == 2
    assert updater.calls[1]["snapshot"].blocks[0].markdown == "Concurrent Context revision."
    assert store.window.stage is SemanticWindowStage.CONTEXT_COMMITTED
