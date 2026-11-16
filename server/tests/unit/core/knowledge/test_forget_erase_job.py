from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from common.schema.settings import ForgetSettings
from core.knowledge.jobs.forget_erase_job import ForgetEraseJob
from infrastructure.job.base import JobContext


@pytest.mark.no_network
async def test_erase_job_applies_live_settings_and_reports_deferred_records():
    store = SimpleNamespace(get_due_forget_records=AsyncMock(return_value=[{'forget_id': 'one'}, {'forget_id': 'two'}]),
                            erase_forget_record=AsyncMock(side_effect=[True, False, True, True]))
    job = ForgetEraseJob(store, ForgetSettings())
    ctx = JobContext(user_name='ada', project_id='project-1')
    before = datetime.now(timezone.utc)
    result = await job.execute(ctx)
    scope = store.get_due_forget_records.call_args.kwargs
    assert scope['user_name'] == 'ada' and scope['project_id'] == 'project-1'
    assert before - timedelta(days=3) <= scope['cutoff'] <= datetime.now(timezone.utc) - timedelta(days=3)
    assert result.success and result.summary == 'Erased 1 forget requests; deferred 1'
    job.update_settings(ForgetSettings(erase_after_days=0, erase_interval_seconds=60))
    assert job.cadence_seconds == 60
    await job.execute(ctx)
    assert store.get_due_forget_records.call_args.kwargs['cutoff'] >= before


def test_forget_settings_reject_negative_retention_and_nonpositive_cadence():
    with pytest.raises(ValidationError):
        ForgetSettings(erase_after_days=-1)
    with pytest.raises(ValidationError):
        ForgetSettings(erase_interval_seconds=0)
