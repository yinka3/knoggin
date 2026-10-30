"""Waiting for a project's memory to finish building after a flush."""

from types import SimpleNamespace

import pytest

from common.schema.semantic_window import SemanticWindowStage
from tests.fixtures.factories import make_project_state


class _SteppingProcessor:
    """Each wake runs one job pass: drain a window, or consume the flush."""

    name = "project_semantic"

    def __init__(self, store, *, windows):
        self.knowledge_store = store
        self.windows = windows
        self.flush_pending = False

    def request_flush(self):
        self.flush_pending = True

    def run_once(self):
        if self.windows:
            self.windows -= 1
            self.knowledge_store.active = object() if self.windows else None
        elif self.flush_pending:
            self.flush_pending = False


class _Store:
    def __init__(self, *, failed=()):
        self.active = object()
        self.failed = list(failed)
        self.revision = object()

    async def list_failed_project_semantic_windows(self, **_scope):
        return self.failed

    async def get_active_project_semantic_window(self, **_scope):
        return self.active

    async def get_current_project_context_revision(self, **_scope):
        return self.revision


class _Scheduler:
    def __init__(self, *, running=True):
        self.running = running
        self.processor = None
        self.wakes = 0

    def wake_job(self, _name):
        self.wakes += 1
        if self.running and self.processor is not None:
            self.processor.run_once()
        return self.running


def _runtime(store, *, windows, running=True):
    scheduler = _Scheduler(running=running)
    runtime = make_project_state(scheduler=scheduler)
    runtime.project_semantic_processor = _SteppingProcessor(store, windows=windows)
    scheduler.processor = runtime.project_semantic_processor
    return runtime, scheduler


@pytest.mark.unit
@pytest.mark.no_network
async def test_waits_through_every_window_until_the_flush_is_spent():
    store = _Store()
    runtime, scheduler = _runtime(store, windows=3)

    revision = await runtime.wait_for_semantic_idle(timeout_s=5, poll_interval_s=0)

    assert revision is store.revision
    assert not runtime.project_semantic_processor.flush_pending
    # One wake per window, plus the pass that finds nothing left.
    assert scheduler.wakes == 4


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_failed_window_raises_with_its_details():
    failed = SimpleNamespace(
        window_id="w-1",
        stage=SemanticWindowStage.CLAIMED,
        last_failure_stage="episode",
        last_failure_code="provider_error",
    )
    runtime, _ = _runtime(_Store(failed=[failed]), windows=1)

    with pytest.raises(RuntimeError, match="w-1 .*episode/provider_error"):
        await runtime.wait_for_semantic_idle(timeout_s=5, poll_interval_s=0)


@pytest.mark.unit
@pytest.mark.no_network
async def test_times_out_when_memory_never_settles():
    runtime, _ = _runtime(_Store(), windows=10_000)

    with pytest.raises(TimeoutError):
        await runtime.wait_for_semantic_idle(timeout_s=0, poll_interval_s=0)


@pytest.mark.unit
@pytest.mark.no_network
async def test_refuses_when_the_scheduler_is_not_running():
    runtime, _ = _runtime(_Store(), windows=0, running=False)

    with pytest.raises(RuntimeError, match="not running"):
        await runtime.wait_for_semantic_idle(timeout_s=5, poll_interval_s=0)
