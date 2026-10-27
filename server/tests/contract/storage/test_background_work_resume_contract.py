import asyncio
from types import SimpleNamespace

import pytest

from common.schema.settings import IngestionNoticeSettings, RootConfig
from core.session.document_batch_notices import DocumentBatchNotices
from core.session.research_jobs import ResearchJobs
from tests.unit.core.session.test_research_jobs import _answer, _Sessions

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


def _with_pg(sessions, pg):
    sessions.pg = pg
    sessions.user_name = "ada"
    return sessions


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_job_cut_off_by_shutdown_runs_again_after_restart(real_postgres_client):
    await _seed(real_postgres_client)
    gate = asyncio.Event()  # never set: the first run is still researching at shutdown
    first = ResearchJobs(_with_pg(_Sessions([_answer("never")], gate=gate), real_postgres_client), SimpleNamespace(config=RootConfig()))
    job = await first.start(project_id="p1", parent_session_id="parent", question="Compare them")
    await asyncio.sleep(0.05)
    await first.shutdown()

    restarted_sessions = _with_pg(_Sessions([_answer("Postgres wins on joins.")]), real_postgres_client)
    restarted = ResearchJobs(restarted_sessions, SimpleNamespace(config=RootConfig()))
    await restarted.resume()
    await asyncio.gather(*list(restarted._tasks.values()))

    assert restarted.get(job["job_id"])["state"] == "completed"
    [(job_id, report)] = restarted_sessions.parent.reports
    assert job_id == job["job_id"] and "Postgres wins on joins." in report

    # A third start finds nothing left to do.
    again = _with_pg(_Sessions(), real_postgres_client)
    third = ResearchJobs(again, SimpleNamespace(config=RootConfig()))
    await third.resume()
    assert third._tasks == {} and again.parent.reports == []
    assert third.get(job["job_id"])["report_delivered"] is True


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
            reports.append(content)

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

    assert reports and "1 of 1 files are ready" in reports[0]
    assert await pg.fetch_all("SELECT 1 FROM public.background_work WHERE kind = 'folder_notice'") == []
