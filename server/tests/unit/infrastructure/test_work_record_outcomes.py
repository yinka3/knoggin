import pytest

from common.schema.contracts import ExecutionScope
from infrastructure.work_record import WorkRecord, WorkStatus


def _work() -> WorkRecord:
    return WorkRecord.for_ingestion(
        ExecutionScope(
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
        ),
        [7],
    )


@pytest.mark.parametrize(
    ("finish", "expected"),
    [
        (lambda work: work.mark_succeeded(), WorkStatus.SUCCEEDED),
        (lambda work: work.mark_failed("unavailable"), WorkStatus.FAILED),
        (lambda work: work.defer("retry later"), WorkStatus.DEFERRED),
        (lambda work: work.mark_skipped("no work"), WorkStatus.SKIPPED),
        (lambda work: work.mark_cancelled("shutdown"), WorkStatus.CANCELLED),
    ],
)
def test_require_terminal_status_classifies_each_terminal_work_outcome(
    finish,
    expected,
):
    work = _work()
    work.mark_running()
    finish(work)

    assert work.require_terminal_status() is expected


@pytest.mark.parametrize(
    ("start", "message"),
    [
        (False, "pending"),
        (True, "running"),
    ],
)
def test_require_terminal_status_rejects_active_work(start, message):
    work = _work()
    if start:
        work.mark_running()

    with pytest.raises(RuntimeError, match=message):
        work.require_terminal_status()


def test_model_work_summary_requires_a_terminal_child_record():
    parent = _work()
    child = WorkRecord.for_model_operation("embedding", parent.scope)

    with pytest.raises(RuntimeError, match="pending"):
        parent.add_model_work_summary(child)

    child.mark_running()
    child.mark_succeeded("Encoded one mention")
    parent.add_model_work_summary(child)

    assert parent.metadata["model_work"] == [
        {
            "id": child.id,
            "kind": "embedding",
            "status": WorkStatus.SUCCEEDED,
            "priority": child.priority,
            "stage": child.stage,
            "queue_wait_ms": child.queue_wait_ms,
            "duration_ms": child.duration_ms,
            "summary": "Encoded one mention",
        }
    ]
