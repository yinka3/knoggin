"""Read-only agent tools for diagnosing the current Knoggin runtime."""

from __future__ import annotations

from typing import Dict

from common.schema.health import HealthActivity, HealthSnapshot, HealthStatus


def _health_service_unavailable(summary: str) -> Dict:
    return HealthSnapshot(
        status=HealthStatus.DEGRADED,
        activity=HealthActivity.IDLE,
        summary=summary,
        warnings=["runtime health service is unavailable"],
    ).model_dump(mode="json")


class HealthTools:
    """Agent-facing, non-mutating runtime diagnostics."""

    async def get_engine_health(self) -> Dict:
        """Report dependency and lifecycle health for the application engine."""

        service = getattr(self, "health_service", None)
        if service is None:
            return _health_service_unavailable("Engine health is unavailable")
        try:
            snapshot = await service.get_engine_health()
        except Exception:
            return _health_service_unavailable("Engine health could not be read")
        return _dump_health_snapshot(snapshot)

    async def get_resource_health(self) -> Dict:
        """Report bounded resource capacity for the current project scope."""

        service = getattr(self, "health_service", None)
        if service is None:
            return _health_service_unavailable("Resource health is unavailable")
        try:
            snapshot = await service.get_resource_health(
                project_id=str(getattr(self, "project_id", "")),
            )
        except Exception:
            return _health_service_unavailable("Resource health could not be read")
        return _dump_health_snapshot(snapshot)

    async def get_ingestion_health(self) -> Dict:
        """Report bounded ingestion worker, queue, checkpoint, and DLQ state."""

        service = getattr(self, "health_service", None)
        if service is None:
            return _health_service_unavailable("Ingestion health is unavailable")
        try:
            snapshot = await service.get_ingestion_health(
                user_name=str(getattr(self, "user_name", "")),
                project_id=str(getattr(self, "project_id", "")),
                session_id=str(getattr(self, "session_id", "")),
            )
        except Exception:
            return _health_service_unavailable(
                "Ingestion health could not be read"
            )
        return _dump_health_snapshot(snapshot)

    async def get_background_health(self) -> Dict:
        """Report bounded scheduler and project background-work health."""

        service = getattr(self, "health_service", None)
        if service is None:
            return _health_service_unavailable("Background health is unavailable")
        try:
            snapshot = await service.get_background_health(
                project_id=str(getattr(self, "project_id", "")),
            )
        except Exception:
            return _health_service_unavailable(
                "Background health could not be read"
            )
        return _dump_health_snapshot(snapshot)


def _dump_health_snapshot(snapshot) -> Dict:
    if isinstance(snapshot, HealthSnapshot):
        return snapshot.model_dump(mode="json")
    if isinstance(snapshot, dict):
        try:
            return HealthSnapshot.model_validate(snapshot).model_dump(mode="json")
        except Exception:
            return _health_service_unavailable(
                "Runtime health returned an invalid snapshot"
            )
    return _health_service_unavailable("Runtime health returned no snapshot")
