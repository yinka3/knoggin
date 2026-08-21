"""Bounded, read-only health aggregation for the live application runtime."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime
from inspect import isawaitable
from itertools import islice
from time import monotonic
from typing import Any, Callable

from common.schema.health import HealthActivity, HealthSnapshot, HealthStatus
from common.utils.time_utils import get_now, parse_iso_time

_ProbeResult = dict[str, bool | float | str]


class RuntimeHealthService:
    """Aggregate safe runtime health without owning or mutating subsystems."""

    def __init__(
        self,
        *,
        resources: Any,
        projects: Any,
        sessions: Any,
        started_at: datetime | None = None,
        probe_timeout_seconds: float = 0.75,
    ) -> None:
        if probe_timeout_seconds <= 0 or probe_timeout_seconds > 5.0:
            raise ValueError("probe_timeout_seconds must be between 0 and 5 seconds")
        self.resources = resources
        self.projects = projects
        self.sessions = sessions
        self.started_at = started_at or get_now()
        self.probe_timeout_seconds = probe_timeout_seconds
        self._closing = False

    @property
    def closing(self) -> bool:
        return self._closing

    def mark_closing(self) -> None:
        """Mark the runtime as closing without touching any owned resource."""

        self._closing = True

    async def get_engine_health(self) -> HealthSnapshot:
        """Return dependency and lifecycle health for the running engine."""

        postgres, redis = await asyncio.gather(
            self._probe_postgres(),
            self._probe_redis(),
        )
        active_project_count = self._active_project_count()
        active_session_count = self._active_session_count()
        failures = [
            name
            for name, result in (("PostgreSQL", postgres), ("Redis", redis))
            if not result["available"]
        ]
        subsystems = {
            "postgres": bool(postgres["available"]),
            "redis": bool(redis["available"]),
            "model_work": getattr(self.resources, "model_work", None) is not None,
            "background_work": (
                getattr(self.resources, "background_work", None) is not None
            ),
            "knowledge_store": (
                getattr(self.resources, "knowledge_store", None) is not None
            ),
            "executor": getattr(self.resources, "executor", None) is not None,
            "embedding": getattr(self.resources, "embedding", None) is not None,
            "llm": getattr(self.resources, "llm_service", None) is not None,
        }
        unavailable_subsystems = [
            name for name, available in subsystems.items() if not available
        ]
        warnings = [f"{name} probe failed" for name in failures]
        warnings.extend(
            f"{name.replace('_', ' ')} is unavailable"
            for name in unavailable_subsystems
            if name not in {"postgres", "redis"}
        )
        if self._closing:
            warnings.append("runtime is closing")

        dependency_failure_count = len(failures)
        if dependency_failure_count >= 2 or (
            not active_project_count
            and not active_session_count
            and unavailable_subsystems
        ):
            status = HealthStatus.FAILED
            summary = "Core runtime dependencies are unavailable"
        elif failures or unavailable_subsystems or self._closing:
            status = HealthStatus.DEGRADED
            summary = "Runtime is operating with degraded dependencies"
        else:
            status = HealthStatus.HEALTHY
            summary = "Runtime is healthy"

        activity = HealthActivity.IDLE
        if self._closing or any(
            result.get("reason") == "timeout" for result in (postgres, redis)
        ):
            activity = HealthActivity.DELAYED
        elif active_session_count:
            activity = HealthActivity.BUSY

        return HealthSnapshot(
            status=status,
            activity=activity,
            summary=summary,
            details={
                "runtime": {
                    "initialized": True,
                    "closing": self._closing,
                    "uptime_seconds": self._uptime_seconds(),
                },
                "postgres": postgres,
                "redis": redis,
                "loaded_project_count": active_project_count,
                "active_runtime_session_count": active_session_count,
                "subsystems": subsystems,
            },
            warnings=warnings,
        )

    async def get_resource_health(
        self,
        *,
        project_id: str,
    ) -> HealthSnapshot:
        """Return bounded capacity and queue pressure for one project scope."""

        warnings: list[str] = []
        model_snapshot = self._component_snapshot(
            getattr(self.resources, "model_work", None),
            "snapshot_for_health",
            warnings,
        )
        background_snapshot = self._component_snapshot(
            getattr(self.resources, "background_work", None),
            "snapshot_for_health",
            warnings,
            project_id=project_id,
        )
        postgres_snapshot = self._component_snapshot(
            getattr(self.resources, "postgres", None),
            "pool_snapshot",
            warnings,
        )

        model_details = self._model_capacity(model_snapshot)
        background_details = self._background_capacity(background_snapshot)
        database_details = self._database_capacity(postgres_snapshot)
        queue_pressure = (
            model_details["foreground"]["queued"]
            + model_details["background"]["queued"]
            + background_details["queued_for_project"]
            + database_details["requests_waiting"]
        )
        capacity_missing = (
            not model_snapshot
            or not background_snapshot
            or not database_details["connected"]
            or not database_details["stats_available"]
        )
        if capacity_missing:
            status = HealthStatus.DEGRADED
            summary = "Runtime resource coordinators are partially unavailable"
        elif queue_pressure:
            status = HealthStatus.DEGRADED
            summary = "Runtime resources are under queue pressure"
        else:
            status = HealthStatus.HEALTHY
            summary = "Runtime resources have available capacity"

        if database_details["requests_waiting"]:
            warnings.append("database pool has waiting requests")
        if not database_details["connected"]:
            warnings.append("database pool is unavailable")
        if background_details["queued_for_project"]:
            warnings.append("project background work is queued")
        if (
            model_details["foreground"]["queued"]
            or model_details["background"]["queued"]
        ):
            warnings.append("model work is queued")

        activity = HealthActivity.BUSY if queue_pressure else HealthActivity.IDLE
        if database_details["requests_waiting"] or capacity_missing:
            activity = HealthActivity.DELAYED

        return HealthSnapshot(
            status=status,
            activity=activity,
            summary=summary,
            details={
                "model_work": model_details,
                "background_work": background_details,
                "database_pool": database_details,
            },
            warnings=warnings,
        )

    async def get_ingestion_health(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> HealthSnapshot:
        """Return bounded worker and PostgreSQL ingestion-queue health.

        This reads only the durable queue aggregate. It never claims work,
        wakes a worker, or returns message identifiers or payloads.
        """

        warnings: list[str] = []
        worker = self._session_worker(project_id, session_id)
        worker_snapshot = self._component_snapshot(
            worker,
            "health_snapshot",
            warnings,
        )
        queue_details = await self._read_ingestion_queue_health(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        warnings.extend(queue_details.pop("warnings", []))

        queue_available = queue_details.get("queue_available") is True
        pending_count = self._nonnegative_int(queue_details.get("pending_count"))
        claimed_count = self._nonnegative_int(queue_details.get("claimed_count"))
        blocked_count = self._nonnegative_int(queue_details.get("blocked_count"))
        consecutive_failures = self._nonnegative_int(
            worker_snapshot.get("consecutive_failures")
        )

        oldest_age = queue_details.get("oldest_pending_age_seconds")
        timeout = worker_snapshot.get("batch_timeout_seconds")
        timeout_seconds = (
            float(timeout)
            if isinstance(timeout, (int, float))
            and not isinstance(timeout, bool)
            and timeout > 0
            else None
        )
        current_batch_age = self._age_seconds(
            worker_snapshot.get("current_batch_started_at")
        )
        worker_state = worker_snapshot.get("state")
        if worker_state == "failed":
            delay_state = "stalled"
        elif (
            self._nonnegative_int(worker_snapshot.get("current_batch_size"))
            and current_batch_age is not None
            and timeout_seconds is not None
            and current_batch_age > timeout_seconds
        ):
            delay_state = "stalled"
        elif (
            isinstance(oldest_age, (int, float))
            and timeout_seconds is not None
            and oldest_age > timeout_seconds
        ):
            delay_state = "delayed"
        elif pending_count:
            delay_state = "unknown"
        else:
            delay_state = "on_time"

        if delay_state == "stalled":
            warnings.append("pending ingestion work is stalled")
        elif delay_state == "delayed":
            warnings.append("pending ingestion work is delayed")
        if blocked_count:
            warnings.append("ingestion queue has blocked work")
        if consecutive_failures:
            warnings.append("ingestion worker has consecutive failures")
        if worker_state in {"not_started", "stopped"}:
            warnings.append("ingestion worker is not running")
        elif worker_state == "failed":
            warnings.append("ingestion worker has failed")

        if worker_state == "failed":
            status = HealthStatus.FAILED
            summary = "Ingestion worker has failed"
        elif (
            not queue_available
            or worker_state != "running"
            or delay_state in {"delayed", "stalled"}
            or blocked_count
            or consecutive_failures
        ):
            status = HealthStatus.DEGRADED
            summary = "Ingestion is operating with degraded health"
        else:
            status = HealthStatus.HEALTHY
            summary = "Ingestion is healthy"

        if delay_state in {"delayed", "stalled"} or not queue_available:
            activity = HealthActivity.DELAYED
        elif (
            pending_count
            or claimed_count
            or self._nonnegative_int(worker_snapshot.get("current_batch_size"))
            or worker_state == "draining"
        ):
            activity = HealthActivity.BUSY
        else:
            activity = HealthActivity.IDLE

        if blocked_count:
            message_state = "blocked"
        elif claimed_count:
            message_state = "claimed"
        elif pending_count:
            message_state = "pending"
        elif queue_details.get("last_processed_available"):
            message_state = "processed"
        else:
            message_state = "unknown"

        return HealthSnapshot(
            status=status,
            activity=activity,
            summary=summary,
            details={
                "worker": worker_snapshot,
                "queue": {
                    "available": queue_available,
                    "pending_count": pending_count,
                    "claimed_count": claimed_count,
                    "blocked_count": blocked_count,
                    "oldest_pending_available": (
                        queue_details.get("oldest_pending_available") is True
                    ),
                    "oldest_pending_age_seconds": oldest_age,
                    "delay_state": delay_state,
                },
                "progress": {
                    "last_processed_available": (
                        queue_details.get("last_processed_available") is True
                    ),
                    "message_state": message_state,
                },
            },
            warnings=warnings,
        )

    async def get_background_health(
        self,
        *,
        project_id: str,
    ) -> HealthSnapshot:
        """Return bounded scheduler, background-queue, and indexing health."""

        warnings: list[str] = []
        project = self._project_state(project_id)
        scheduler_snapshot = self._component_snapshot(
            getattr(project, "scheduler", None),
            "snapshot_for_health",
            warnings,
        )
        background_snapshot = self._component_snapshot(
            getattr(self.resources, "background_work", None),
            "snapshot_for_health",
            warnings,
            project_id=project_id,
        )
        document_service = getattr(project, "document_service", None)
        indexing_snapshot = self._component_snapshot(
            document_service,
            "indexing_snapshot_for_health",
            warnings,
        )
        pending_count, pending_error = await self._read_pending_index_count(
            document_service
        )
        if pending_error is not None:
            warnings.append("durable document-index count is unavailable")

        scheduler_state = scheduler_snapshot.get("state")
        queued_jobs = self._nonnegative_int(scheduler_snapshot.get("queued_jobs"))
        running_jobs = self._nonnegative_int(scheduler_snapshot.get("running_jobs"))
        stalled_jobs = self._nonnegative_int(scheduler_snapshot.get("stalled_jobs"))
        recent_failed_jobs = self._nonnegative_int(
            scheduler_snapshot.get("recent_failed_jobs")
        )
        queued_background = self._nonnegative_int(
            background_snapshot.get("queued_for_project")
        )
        active_background = int(background_snapshot.get("active_for_project") is True)
        local_indexing_tasks = self._nonnegative_int(
            indexing_snapshot.get("local_submission_tasks")
        )
        pending_documents = pending_count if pending_count is not None else 0
        work_present = bool(
            queued_jobs
            or running_jobs
            or stalled_jobs
            or queued_background
            or active_background
            or local_indexing_tasks
            or pending_documents
        )

        if not scheduler_snapshot and not indexing_snapshot:
            status = HealthStatus.DEGRADED
            summary = "Background runtime health is unavailable"
        elif stalled_jobs or recent_failed_jobs:
            status = HealthStatus.DEGRADED
            summary = "Background work has stalled or failed"
        elif scheduler_state != "running":
            status = HealthStatus.DEGRADED
            summary = "Background scheduler is not running"
        elif pending_error is not None:
            status = HealthStatus.DEGRADED
            summary = "Document-indexing health is unavailable"
        elif (
            pending_documents
            and not running_jobs
            and not queued_jobs
            and not queued_background
            and not active_background
        ):
            status = HealthStatus.DEGRADED
            summary = "Durable document indexing is not progressing"
        else:
            status = HealthStatus.HEALTHY
            summary = "Background work is healthy"

        if (
            stalled_jobs
            or (scheduler_state != "running" and work_present)
            or (pending_documents and not running_jobs and not active_background)
        ):
            activity = HealthActivity.DELAYED
        elif work_present:
            activity = HealthActivity.BUSY
        else:
            activity = HealthActivity.IDLE

        if scheduler_state == "stopped":
            warnings.append("background scheduler is stopped")
        if stalled_jobs:
            warnings.append("background jobs exceed their captured timeout")
        if recent_failed_jobs:
            warnings.append("recent background jobs failed or timed out")
        if queued_background:
            warnings.append("project background work is queued")
        if pending_documents:
            warnings.append("durable document indexing is pending")

        return HealthSnapshot(
            status=status,
            activity=activity,
            summary=summary,
            details={
                "scheduler": scheduler_snapshot,
                "background_work": background_snapshot,
                "document_indexing": {
                    **indexing_snapshot,
                    "pending_document_count": pending_count,
                },
            },
            warnings=warnings,
        )

    def _session_worker(self, project_id: str, session_id: str) -> Any:
        """Find the current session's worker without traversing other sessions."""

        try:
            reader = getattr(self.sessions, "get_runtime_session", None)
            context = reader(session_id) if callable(reader) else None
        except (AttributeError, TypeError):
            return None
        if context is None or getattr(context, "project_id", None) != project_id:
            return None
        return getattr(context, "consumer", None)

    def _project_state(self, project_id: str) -> Any:
        projects = getattr(self.projects, "active_projects", None)
        try:
            return projects.get(project_id) if projects is not None else None
        except (AttributeError, TypeError):
            return None

    async def _read_pending_index_count(
        self,
        document_service: Any,
    ) -> tuple[int | None, str | None]:
        method = (
            getattr(document_service, "pending_index_count", None)
            if document_service is not None
            else None
        )
        if not callable(method):
            return None, "unavailable"
        value, error = await self._bounded_read(method)
        if error is not None:
            return None, error
        return self._nonnegative_int_or_none(value), None

    async def _read_ingestion_queue_health(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> dict[str, Any]:
        """Read the canonical PostgreSQL queue aggregate without message payloads."""

        store = getattr(self.resources, "knowledge_store", None)
        read_queue_health = getattr(store, "get_ingestion_queue_health", None)
        if not callable(read_queue_health):
            return {
                "queue_available": False,
                "warnings": ["PostgreSQL ingestion metrics are unavailable"],
                "pending_count": 0,
                "claimed_count": 0,
                "blocked_count": 0,
                "oldest_pending_available": False,
                "oldest_pending_age_seconds": None,
                "last_processed_available": False,
            }

        queue, error = await self._bounded_read(
            lambda: read_queue_health(
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
            )
        )
        if error is not None or not isinstance(queue, Mapping):
            return {
                "queue_available": False,
                "warnings": ["PostgreSQL ingestion metrics are unavailable"],
                "pending_count": 0,
                "claimed_count": 0,
                "blocked_count": 0,
                "oldest_pending_available": False,
                "oldest_pending_age_seconds": None,
                "last_processed_available": False,
            }

        oldest_ms = self._nonnegative_int_or_none(queue.get("oldest_pending_ms"))
        oldest_age = (
            max(get_now().timestamp() - oldest_ms / 1000, 0.0)
            if oldest_ms is not None
            else None
        )
        return {
            "queue_available": True,
            "warnings": [],
            "pending_count": self._nonnegative_int(queue.get("pending_count")),
            "claimed_count": self._nonnegative_int(queue.get("claimed_count")),
            "blocked_count": self._nonnegative_int(queue.get("blocked_count")),
            "oldest_pending_available": oldest_ms is not None,
            "oldest_pending_age_seconds": oldest_age,
            "last_processed_available": queue.get("last_processed_ms") is not None,
        }

    async def _bounded_read(
        self, operation: Callable[[], Any]
    ) -> tuple[Any, str | None]:
        try:
            result = operation()
            if isawaitable(result):
                result = await asyncio.wait_for(
                    result, timeout=self.probe_timeout_seconds
                )
            return result, None
        except asyncio.TimeoutError:
            return None, "timeout"
        except Exception:
            return None, "read_failed"

    @staticmethod
    def _age_seconds(value: Any) -> float | None:
        if not isinstance(value, str):
            return None
        timestamp = parse_iso_time(value)
        if timestamp is None:
            return None
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=get_now().tzinfo)
        return max((get_now() - timestamp).total_seconds(), 0.0)

    async def _probe_postgres(self) -> _ProbeResult:
        postgres = getattr(self.resources, "postgres", None)
        if postgres is None:
            return self._unavailable_probe("unavailable")
        pool_snapshot = getattr(postgres, "pool_snapshot", None)
        if callable(pool_snapshot):
            try:
                pool = pool_snapshot()
            except Exception:
                pool = {}
            if isinstance(pool, Mapping) and pool.get("connected") is False:
                return self._unavailable_probe("unavailable")

        fetch_one = getattr(postgres, "fetch_one", None)
        if not callable(fetch_one):
            return self._unavailable_probe("unavailable")
        return await self._run_probe(
            lambda: fetch_one("SELECT 1 AS ok"),
        )

    async def _probe_redis(self) -> _ProbeResult:
        redis = getattr(self.resources, "redis", None)
        if redis is None:
            manager = getattr(self.resources, "redis_manager", None)
            try:
                client = getattr(manager, "client", None)
            except Exception:
                client = None
            if client is not None:
                redis = client
        ping = getattr(redis, "ping", None) if redis is not None else None
        if not callable(ping) and redis is not None:
            try:
                client = getattr(redis, "client", None)
            except Exception:
                client = None
            redis = client if client is not None else redis
            ping = getattr(redis, "ping", None)
        if not callable(ping):
            return self._unavailable_probe("unavailable")
        return await self._run_probe(ping)

    async def _run_probe(
        self,
        operation: Callable[[], Any],
    ) -> _ProbeResult:
        started = monotonic()
        try:
            result = operation()
            if isawaitable(result):
                await asyncio.wait_for(result, timeout=self.probe_timeout_seconds)
            del result
        except asyncio.TimeoutError:
            return self._unavailable_probe("timeout", started=started)
        except Exception:
            return self._unavailable_probe("probe_failed", started=started)
        return {
            "available": True,
            "status": HealthStatus.HEALTHY.value,
            "latency_ms": round((monotonic() - started) * 1000, 2),
        }

    @staticmethod
    def _unavailable_probe(
        reason: str,
        *,
        started: float | None = None,
    ) -> _ProbeResult:
        result: _ProbeResult = {
            "available": False,
            "status": HealthStatus.FAILED.value,
            "reason": reason,
        }
        if started is not None:
            result["latency_ms"] = round((monotonic() - started) * 1000, 2)
        return result

    def _component_snapshot(
        self,
        component: Any,
        method_name: str,
        warnings: list[str],
        **kwargs: Any,
    ) -> dict[str, Any]:
        method = (
            getattr(component, method_name, None) if component is not None else None
        )
        if not callable(method):
            warnings.append(f"{method_name.replace('_', ' ')} is unavailable")
            return {}
        try:
            snapshot = method(**kwargs)
        except Exception:
            warnings.append(f"{method_name.replace('_', ' ')} failed")
            return {}
        return dict(snapshot) if isinstance(snapshot, Mapping) else {}

    @staticmethod
    def _model_capacity(snapshot: Mapping[str, Any]) -> dict[str, Any]:
        queued = snapshot.get("queued_by_priority", {})
        active = snapshot.get("in_flight_by_priority", {})
        by_name = snapshot.get("work_by_name", {})
        return {
            "foreground": {
                "capacity": RuntimeHealthService._nonnegative_int(
                    snapshot.get("foreground_concurrency")
                ),
                "active": RuntimeHealthService._nonnegative_int(
                    queued_or_active(active, "foreground")
                ),
                "queued": RuntimeHealthService._nonnegative_int(
                    queued_or_active(queued, "foreground")
                ),
            },
            "background": {
                "capacity": RuntimeHealthService._nonnegative_int(
                    snapshot.get("background_concurrency")
                ),
                "active": RuntimeHealthService._nonnegative_int(
                    queued_or_active(active, "background")
                ),
                "queued": RuntimeHealthService._nonnegative_int(
                    queued_or_active(queued, "background")
                ),
            },
            "operation_categories": (
                [name[:100] for name in islice(by_name, 20) if isinstance(name, str)]
                if isinstance(by_name, Mapping)
                else []
            ),
        }

    @staticmethod
    def _background_capacity(snapshot: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "capacity": RuntimeHealthService._nonnegative_int(
                snapshot.get("max_concurrency")
            ),
            "queued_for_project": RuntimeHealthService._nonnegative_int(
                snapshot.get("queued_for_project")
            ),
            "global_queued": RuntimeHealthService._nonnegative_int(
                snapshot.get("queued")
            ),
            "global_queue_limit": RuntimeHealthService._nonnegative_int(
                snapshot.get("max_queued_global")
            ),
        }

    @staticmethod
    def _database_capacity(snapshot: Mapping[str, Any]) -> dict[str, Any]:
        pool_size = RuntimeHealthService._nonnegative_int(snapshot.get("pool_size"))
        available = RuntimeHealthService._nonnegative_int(
            snapshot.get("pool_available")
        )
        return {
            "connected": snapshot.get("connected") is True,
            "capacity": RuntimeHealthService._nonnegative_int(snapshot.get("pool_max")),
            "active": max(pool_size - available, 0),
            "available": available,
            "requests_waiting": RuntimeHealthService._nonnegative_int(
                snapshot.get("requests_waiting")
            ),
            "stats_available": snapshot.get("stats_available") is True,
        }

    def _active_project_count(self) -> int:
        projects = getattr(self.projects, "active_projects", {})
        try:
            return max(len(projects), 0)
        except TypeError:
            return 0

    def _active_session_count(self) -> int:
        try:
            reader = getattr(self.sessions, "active_runtime_count", None)
            return max(reader(), 0) if callable(reader) else 0
        except (AttributeError, TypeError):
            return 0

    def _uptime_seconds(self) -> float:
        return max((get_now() - self.started_at).total_seconds(), 0.0)

    @staticmethod
    def _nonnegative_int(value: Any) -> int:
        return (
            value
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0
            else 0
        )

    @staticmethod
    def _nonnegative_int_or_none(value: Any) -> int | None:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None


def queued_or_active(value: Any, key: str) -> Any:
    """Read one priority count without trusting arbitrary component state."""

    return value.get(key, 0) if isinstance(value, Mapping) else 0
