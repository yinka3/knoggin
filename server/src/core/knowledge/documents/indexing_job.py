"""Scheduled recovery for durable document-indexing work."""

from common.schema.settings import DocumentIndexingSettings
from core.knowledge.documents.service import DocumentService
from infrastructure.job.base import BaseJob, JobContext, JobResult


class DocumentIndexingRecoveryJob(BaseJob):
    """Periodically admit queued document indexes into background work."""

    def __init__(self, document_service: DocumentService, settings: DocumentIndexingSettings):
        self.document_service = document_service
        self.update_settings(settings)

    @property
    def name(self) -> str:
        return "document_index_recovery"

    @property
    def cadence_seconds(self) -> float:
        return self.interval_seconds

    @property
    def run_immediately_on_first_check(self) -> bool:
        return True

    def update_settings(self, settings: DocumentIndexingSettings) -> None:
        self.interval_seconds = settings.recovery_interval_seconds
        self.batch_size = settings.recovery_batch_size

    async def should_run(self, ctx: JobContext) -> bool:
        return await self.document_service.pending_index_count() > 0

    async def execute(self, ctx: JobContext) -> JobResult:
        submitted = await self.document_service.recover_pending_indexes(self.batch_size)
        return JobResult(success=True, summary=f"Submitted {submitted} document indexes")
