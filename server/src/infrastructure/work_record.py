"""Workflow-owned runtime telemetry for observable background operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Optional, assert_never
from uuid import uuid4

from common.schema.ingestion.contracts import (
    ExecutionScope,
    ValidationIssue,
)
from common.utils.time_utils import get_now


class WorkStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEFERRED = "deferred"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class WorkRecord:
    """One mutable telemetry record with validated lifecycle transitions."""

    id: str
    kind: str
    user_name: str
    project_id: Optional[str]
    session_id: Optional[str]
    status: WorkStatus = WorkStatus.PENDING
    parent_id: Optional[str] = None
    priority: int = 100
    cpu_weight: int = 1
    memory_weight: int = 1
    expected_llm_calls: int = 0
    expected_embedding_calls: int = 0
    graph_write_expected: bool = False
    attempt: int = 1
    stage: Optional[str] = None
    created_at: datetime = field(default_factory=get_now)
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    queue_wait_ms: Optional[int] = None
    duration_ms: Optional[int] = None
    summary: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    issues: list[ValidationIssue] = field(default_factory=list)

    @classmethod
    def for_ingestion(
        cls, scope: ExecutionScope, message_ids: list[int], priority: int = 100
    ) -> "WorkRecord":
        return cls(
            id=uuid4().hex,
            kind="message_batch",
            user_name=scope.user_name,
            project_id=scope.project_id,
            session_id=scope.session_id,
            priority=priority,
            cpu_weight=2,
            memory_weight=2,
            expected_llm_calls=2,
            expected_embedding_calls=1,
            graph_write_expected=True,
            stage="message_batch",
            metadata={"message_ids": list(message_ids), "batch_size": len(message_ids)},
        )

    @classmethod
    def for_graph_write(
        cls,
        scope: ExecutionScope,
        *,
        batch_id: Optional[str] = None,
        priority: int = 90,
    ) -> "WorkRecord":
        metadata = {"batch_work_unit_id": batch_id} if batch_id else {}
        return cls(
            id=uuid4().hex,
            kind="graph_write",
            user_name=scope.user_name,
            project_id=scope.project_id,
            session_id=scope.session_id,
            parent_id=batch_id,
            priority=priority,
            graph_write_expected=True,
            stage="graph_write",
            metadata=metadata,
        )

    @classmethod
    def for_model_operation(
        cls,
        kind: str,
        scope: ExecutionScope,
        *,
        parent_id: Optional[str] = None,
        priority: int = 100,
        stage: Optional[str] = None,
    ) -> "WorkRecord":
        return cls(
            id=uuid4().hex,
            kind=kind,
            user_name=scope.user_name,
            project_id=scope.project_id,
            session_id=scope.session_id,
            parent_id=parent_id,
            priority=priority,
            cpu_weight=2 if kind in {"spacy", "gliner", "document_index"} else 1,
            memory_weight=2 if kind in {"embedding", "rerank", "nli", "gliner"} else 1,
            expected_embedding_calls=1 if kind == "embedding" else 0,
            stage=stage or kind,
        )

    @property
    def scope(self) -> ExecutionScope:
        return ExecutionScope(
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=self.session_id,
        )

    @scope.setter
    def scope(self, value: ExecutionScope) -> None:
        if not isinstance(value, ExecutionScope):
            raise TypeError("WorkRecord.scope must be an ExecutionScope")
        self.user_name = value.user_name
        self.project_id = value.project_id
        self.session_id = value.session_id

    def queue(self) -> None:
        self._require_transition({WorkStatus.PENDING}, WorkStatus.PENDING)
        if self.started_at is not None or self.finished_at is not None:
            raise ValueError("Work cannot be queued after it has started")
        self.queued_at = get_now()

    def start(self) -> None:
        self._require_transition({WorkStatus.PENDING}, WorkStatus.RUNNING)
        self.status = WorkStatus.RUNNING
        self.started_at = get_now()
        queue_reference = self.queued_at or self.created_at
        self.queue_wait_ms = int(
            (self.started_at - queue_reference).total_seconds() * 1000
        )
        if self.queue_wait_ms < 0:
            raise ValueError("Work start time precedes its queue time")

    def succeed(self, summary: Optional[str] = None) -> None:
        self._require_transition({WorkStatus.RUNNING}, WorkStatus.SUCCEEDED)
        self.status = WorkStatus.SUCCEEDED
        self.summary = summary
        self._finish()

    def fail(self, error: str) -> None:
        self._require_transition({WorkStatus.RUNNING}, WorkStatus.FAILED)
        if not isinstance(error, str) or not error.strip():
            raise ValueError("Failed work requires a non-blank error")
        self.status = WorkStatus.FAILED
        self.summary = error.strip()
        self._finish()

    def skip(self, summary: Optional[str] = None) -> None:
        self._require_transition({WorkStatus.RUNNING}, WorkStatus.SKIPPED)
        self.status = WorkStatus.SKIPPED
        self.summary = summary
        self._finish()

    def defer(self, summary: Optional[str] = None) -> None:
        self._require_transition(
            {WorkStatus.PENDING, WorkStatus.RUNNING}, WorkStatus.DEFERRED
        )
        self.status = WorkStatus.DEFERRED
        self.summary = summary
        self._finish()

    def cancel(self, summary: Optional[str] = None) -> None:
        self._require_transition(
            {WorkStatus.PENDING, WorkStatus.RUNNING}, WorkStatus.CANCELLED
        )
        self.status = WorkStatus.CANCELLED
        self.summary = summary
        self._finish()

    # Scheduler-oriented aliases preserve concise call sites.
    mark_queued = queue
    mark_running = start
    mark_succeeded = succeed
    mark_failed = fail
    mark_skipped = skip
    mark_cancelled = cancel

    def require_terminal_status(self) -> WorkStatus:
        """Return the terminal result or reject work that is still in progress.

        Keeping every enum member explicit makes new statuses visible to static
        analysis and prevents callers from treating an active record as a result.
        """

        match self.status:
            case WorkStatus.SUCCEEDED:
                return WorkStatus.SUCCEEDED
            case WorkStatus.FAILED:
                return WorkStatus.FAILED
            case WorkStatus.DEFERRED:
                return WorkStatus.DEFERRED
            case WorkStatus.SKIPPED:
                return WorkStatus.SKIPPED
            case WorkStatus.CANCELLED:
                return WorkStatus.CANCELLED
            case WorkStatus.PENDING:
                raise RuntimeError("Work is pending and has no terminal outcome")
            case WorkStatus.RUNNING:
                raise RuntimeError("Work is running and has no terminal outcome")
            case unexpected:
                assert_never(unexpected)

    def add_model_work_summary(self, child: "WorkRecord") -> None:
        """Record a child operation produced by the live workflow."""

        if not isinstance(child, WorkRecord):
            raise TypeError("child must be a WorkRecord")
        child.require_terminal_status()
        summaries = self.metadata.setdefault("model_work", [])
        summaries.append(
            {
                "id": child.id,
                "kind": child.kind,
                "status": child.status,
                "priority": child.priority,
                "stage": child.stage,
                "queue_wait_ms": child.queue_wait_ms,
                "duration_ms": child.duration_ms,
                "summary": child.summary,
            }
        )

    def snapshot(self) -> dict[str, Any]:
        """Return the JSON-safe telemetry boundary representation."""

        return {
            "id": self.id,
            "kind": self.kind,
            "user_name": self.user_name,
            "project_id": self.project_id,
            "session_id": self.session_id,
            "status": self.status.value,
            "parent_id": self.parent_id,
            "priority": self.priority,
            "cpu_weight": self.cpu_weight,
            "memory_weight": self.memory_weight,
            "expected_llm_calls": self.expected_llm_calls,
            "expected_embedding_calls": self.expected_embedding_calls,
            "graph_write_expected": self.graph_write_expected,
            "attempt": self.attempt,
            "stage": self.stage,
            "created_at": self.created_at.isoformat(),
            "queued_at": self.queued_at.isoformat() if self.queued_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "queue_wait_ms": self.queue_wait_ms,
            "duration_ms": self.duration_ms,
            "summary": self.summary,
            "metadata": dict(self.metadata),
            "issues": [issue.model_dump(mode="json") for issue in self.issues],
        }

    @classmethod
    def from_snapshot(cls, payload: dict[str, Any]) -> "WorkRecord":
        """Hydrate runtime telemetry from a validated snapshot."""

        if not isinstance(payload, dict):
            raise TypeError("WorkRecord snapshot must be a dictionary")

        def parse_time(name: str) -> Optional[datetime]:
            value = payload.get(name)
            return datetime.fromisoformat(value) if value else None

        return cls(
            id=str(payload["id"]),
            kind=str(payload["kind"]),
            user_name=str(payload["user_name"]),
            project_id=payload.get("project_id"),
            session_id=payload.get("session_id"),
            status=WorkStatus(payload.get("status", WorkStatus.PENDING)),
            parent_id=payload.get("parent_id"),
            priority=int(payload.get("priority", 100)),
            cpu_weight=int(payload.get("cpu_weight", 1)),
            memory_weight=int(payload.get("memory_weight", 1)),
            expected_llm_calls=int(payload.get("expected_llm_calls", 0)),
            expected_embedding_calls=int(payload.get("expected_embedding_calls", 0)),
            graph_write_expected=bool(payload.get("graph_write_expected", False)),
            attempt=int(payload.get("attempt", 1)),
            stage=payload.get("stage"),
            created_at=parse_time("created_at") or get_now(),
            queued_at=parse_time("queued_at"),
            started_at=parse_time("started_at"),
            finished_at=parse_time("finished_at"),
            queue_wait_ms=payload.get("queue_wait_ms"),
            duration_ms=payload.get("duration_ms"),
            summary=payload.get("summary"),
            metadata=dict(payload.get("metadata", {})),
            issues=[
                ValidationIssue.model_validate(issue)
                for issue in payload.get("issues", [])
            ],
        )

    def _require_transition(
        self, allowed: set[WorkStatus], next_status: WorkStatus
    ) -> None:
        if self.status not in allowed:
            raise ValueError(
                f"Invalid work transition: {self.status.value} -> {next_status.value}"
            )

    def _finish(self) -> None:
        self.finished_at = get_now()
        start_reference = self.started_at or self.created_at
        self.duration_ms = int(
            (self.finished_at - start_reference).total_seconds() * 1000
        )
        if self.duration_ms < 0:
            raise ValueError("Work finish time precedes its start time")
