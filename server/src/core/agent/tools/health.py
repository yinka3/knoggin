"""Read-only agent tools for diagnosing the current Knoggin runtime."""

from __future__ import annotations

from typing import Dict

from loguru import logger

from common.schema.health import HealthActivity, HealthSnapshot, HealthStatus


def _health_service_unavailable(summary: str) -> Dict:
    return HealthSnapshot(
        status=HealthStatus.DEGRADED,
        activity=HealthActivity.IDLE,
        summary=summary,
        warnings=["runtime health service is unavailable"],
    ).model_dump(mode="json")


def _record_health_adapter_failure(operation: str, error: Exception) -> None:
    """Record a safe category without logging exception text or user scope."""

    logger.error(
        "Health adapter {} failed with {}",
        operation,
        type(error).__name__,
    )


class HealthTools:
    """Agent-facing, non-mutating runtime diagnostics."""

    async def get_engine_health(self) -> Dict:
        """Report dependency and lifecycle health for the application engine."""

        service = getattr(self, "health_service", None)
        if service is None:
            return _health_service_unavailable("Engine health is unavailable")
        try:
            snapshot = await service.get_engine_health()
        except Exception as exc:
            _record_health_adapter_failure("get_engine_health", exc)
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
        except Exception as exc:
            _record_health_adapter_failure("get_resource_health", exc)
            return _health_service_unavailable("Resource health could not be read")
        return _dump_health_snapshot(snapshot)

    async def get_ingestion_health(self) -> Dict:
        """Report bounded ingestion worker and canonical queue state."""

        service = getattr(self, "health_service", None)
        if service is None:
            return _health_service_unavailable("Ingestion health is unavailable")
        try:
            snapshot = await service.get_ingestion_health(
                user_name=str(getattr(self, "user_name", "")),
                project_id=str(getattr(self, "project_id", "")),
            )
        except Exception as exc:
            _record_health_adapter_failure("get_ingestion_health", exc)
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
        except Exception as exc:
            _record_health_adapter_failure("get_background_health", exc)
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
        except Exception as exc:
            _record_health_adapter_failure("validate_health_snapshot", exc)
            return _health_service_unavailable(
                "Runtime health returned an invalid snapshot"
            )
    return _health_service_unavailable("Runtime health returned no snapshot")
