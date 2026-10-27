import asyncio
from types import SimpleNamespace

import pytest

from common.schema.settings import IngestionNoticeSettings
from core.session.document_batch_notices import DocumentBatchNotices
from tests.unit.core.session.test_research_jobs import (
    _answer,
    _finish,
    _jobs,
    _Sessions,
)

DOC = "00000000-0000-0000-0000-0000000000d1"


async def _seed(pg):
    await pg.execute(
        "INSERT INTO public.projects (project_id, user_name, name, domain_config) "
        "VALUES ('p1', 'ada', 'P1', '{}'::jsonb)"
    )
    await pg.execute(
        "INSERT INTO public.sessions (session_id, user_name, project_id, status) "
        "VALUES ('parent', 'ada', 'p1', 'open')"
    )


async def _report_turn_ran(pg, key, value):
    """What a report turn leaves behind once it runs: its saved message."""

    await pg.execute(
        """INSERT INTO public.messages
               (user_name, session_id, message_id, project_id, role, content, metadata)
           VALUES ('ada', 'parent', 1, 'p1', 'user', 'report', %s::jsonb)""",
        (f'{{"{key}": "{value}"}}',),
    )


def _with_pg(sessions, pg):
    sessions.pg = pg
    sessions.user_name = "ada"
    return sessions


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_job_cut_off_by_shutdown_waits_for_the_user_after_restart(real_postgres_client):
    pg = real_postgres_client
    await _seed(pg)
    gate = asyncio.Event()  # never set: the first job is still researching at shutdown
    first = _jobs(_with_pg(_Sessions([_answer("never")], gate=gate), pg), max_running_per_project=1)
    cut = await first.start(project_id="p1", parent_session_id="parent", question="Compare them")
    waiting = await first.start(project_id="p1", parent_session_id="parent", question="List them")
    await asyncio.sleep(0.05)
    await first.shutdown()

    # The job that never started costs nothing to run, so it runs; the cut-off
    # one would pay again from the start, so it waits.
    sessions = _with_pg(_Sessions([_answer("Three of them.")], [_answer("Postgres wins.")]), pg)
    restarted = _jobs(sessions)
    await restarted.resume()
    await _finish(restarted)
    assert restarted.get(waiting["job_id"])["state"] == "completed"
    assert restarted.get(cut["job_id"])["state"] == "interrupted"
    assert [job_id for job_id, _ in sessions.parent.reports] == [waiting["job_id"]]

    # A second restart leaves it waiting too.
    again = _jobs(_with_pg(_Sessions(), pg))
    await again.resume()
    assert again.get(cut["job_id"])["state"] == "interrupted"
    assert cut["job_id"] not in again._tasks

    resumed = await restarted.resume_job(cut["job_id"])
    assert resumed["state"] == "queued"
    await _finish(restarted)
    assert restarted.get(cut["job_id"])["state"] == "completed"
    assert "Postgres wins." in sessions.parent.reports[-1][1]
    with pytest.raises(ValueError, match="interrupted"):
        await restarted.resume_job(cut["job_id"])


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_resume_after_restart_reruns_a_cut_off_job(real_postgres_client):
    await _seed(real_postgres_client)
    gate = asyncio.Event()
    first = _jobs(_with_pg(_Sessions([_answer("never")], gate=gate), real_postgres_client))
    job = await first.start(project_id="p1", parent_session_id="parent", question="Compare them")
    await asyncio.sleep(0.05)
    await first.shutdown()

    sessions = _with_pg(_Sessions([_answer("Postgres wins on joins.")]), real_postgres_client)
    restarted = _jobs(sessions, resume_after_restart=True)
    await restarted.resume()
    await _finish(restarted)
    assert restarted.get(job["job_id"])["state"] == "completed"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_report_lost_to_a_restart_is_sent_again(real_postgres_client):
    await _seed(real_postgres_client)
    sessions = _with_pg(_Sessions([_answer("Postgres wins on joins.")]), real_postgres_client)
    jobs = _jobs(sessions)
    job = await jobs.start(project_id="p1", parent_session_id="parent", question="Compare them")
    await _finish(jobs)
    assert len(sessions.parent.reports) == 1

    # Queued is not delivered: a restart before the report turn ran sends it again.
    again = _with_pg(_Sessions(), real_postgres_client)
    second = _jobs(again)
    await second.resume()
    await _finish(second)
    assert [job_id for job_id, _ in again.parent.reports] == [job["job_id"]]

    # Once the turn ran, the next start finds nothing left to do.
    await _report_turn_ran(real_postgres_client, "job_id", job["job_id"])
    last = _with_pg(_Sessions(), real_postgres_client)
    third = _jobs(last)
    await third.resume()
    assert third._tasks == {} and last.parent.reports == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_folder_notice_survives_a_restart(real_postgres_client, monkeypatch):
    pg = real_postgres_client
    await _seed(pg)
    await pg.execute(
        """INSERT INTO public.project_documents
               (document_id, project_id, original_name, relative_path, size_bytes, content_hash, status)
           VALUES (%s, 'p1', 'a.md', 'notes/a.md', 1, 'h', 'indexing')""",
        (DOC,),
    )
    real_sleep = asyncio.sleep
    monkeypatch.setattr("core.session.document_batch_notices.asyncio.sleep", lambda _s: real_sleep(0.02))
    config = SimpleNamespace(config=SimpleNamespace(developer_settings=SimpleNamespace(
        ingestion_notices=IngestionNoticeSettings(poll_seconds=1, max_wait_minutes=1),
    )))
    reports = []

    class Chat:
        async def queue_report(self, content, *, origin, metadata):
            reports.append((content, metadata))

    async def chat(_session_id):
        return Chat()

    sessions = SimpleNamespace(pg=pg, user_name="ada", get_or_resume_session=chat)
    before = DocumentBatchNotices(sessions, config)
    await before.watch(project_id="p1", session_id="parent", label="notes", document_ids=[DOC])
    await real_sleep(0.05)
    await before.shutdown()

    await pg.execute("UPDATE public.project_documents SET status = 'indexed' WHERE document_id = %s", (DOC,))
    after = DocumentBatchNotices(sessions, config)
    await after.resume()
    await asyncio.wait_for(asyncio.gather(*after._tasks), timeout=5)

    assert len(reports) == 1 and "1 of 1 files are ready" in reports[0][0]

    # Queued is not delivered: a restart before the notice turn ran sends it again.
    again = DocumentBatchNotices(sessions, config)
    await again.resume()
    await asyncio.wait_for(asyncio.gather(*again._tasks), timeout=5)
    assert len(reports) == 2

    # Once the turn ran, the next start just clears the record.
    await _report_turn_ran(pg, "work_id", reports[-1][1]["work_id"])
    last = DocumentBatchNotices(sessions, config)
    await last.resume()
    assert not last._tasks and len(reports) == 2
    assert await pg.fetch_all("SELECT 1 FROM public.background_work WHERE kind = 'folder_notice'") == []
