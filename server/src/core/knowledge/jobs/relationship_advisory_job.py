"""Periodic review of repeated, unrecognized relationship wording."""

from __future__ import annotations

from core.project.maintenance_service import ProjectMaintenanceService
from infrastructure.job.base import BaseJob, JobContext, JobResult


class RelationshipAdvisoryJob(BaseJob):
    run_immediately_on_first_check = True
    cadence_seconds = 3600.0

    def __init__(self, maintenance_service: ProjectMaintenanceService) -> None:
        self.maintenance_service = maintenance_service

    @property
    def name(self) -> str:
        return "relationship_advisories"

    async def should_run(self, ctx: JobContext) -> bool:
        return False

    async def execute(self, ctx: JobContext) -> JobResult:
        advisories = await self.maintenance_service.refresh_relationship_advisories(
            ctx.project_id
        )
        return JobResult(
            success=True,
            summary=f"Reviewed {len(advisories)} relationship advisory patterns",
        )
