"""Erase completed forget requests after their configured retention period."""

from datetime import datetime, timedelta, timezone

from common.schema.settings import ForgetSettings
from infrastructure.job.base import BaseJob, JobContext, JobResult


class ForgetEraseJob(BaseJob):
    run_immediately_on_first_check = True

    def __init__(self, knowledge_store, settings: ForgetSettings):
        self.knowledge_store = knowledge_store
        self.update_settings(settings)

    def update_settings(self, settings: ForgetSettings) -> None:
        self._settings = ForgetSettings.model_validate(settings.model_dump())
        self.cadence_seconds = self._settings.erase_interval_seconds

    @property
    def name(self) -> str:
        return 'forget_erase'

    async def should_run(self, ctx: JobContext) -> bool:
        return False  # The scheduler owns cadence and immediate startup execution.

    async def execute(self, ctx: JobContext) -> JobResult:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._settings.erase_after_days)
        scope = {'user_name': ctx.user_name, 'project_id': ctx.project_id, 'cutoff': cutoff}
        records = await self.knowledge_store.get_due_forget_records(**scope)
        erased = 0
        for record in records:
            erased += await self.knowledge_store.erase_forget_record(record['forget_id'], **scope)
        return JobResult(summary=f'Erased {erased} forget requests; deferred {len(records) - erased}')
