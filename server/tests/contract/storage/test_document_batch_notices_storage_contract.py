import asyncio
from types import SimpleNamespace

import pytest

from common.schema.settings import IngestionNoticeSettings
from core.session.document_batch_notices import DocumentBatchNotices

A = "00000000-0000-0000-0000-00000000000a"
B = "00000000-0000-0000-0000-00000000000b"


class Chat:
    def __init__(self):
        self.reports = []

    async def queue_report(self, content, *, origin, metadata):
        self.reports.append(content)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_notice_waits_on_real_document_statuses(real_postgres_client, monkeypatch):
    pg = real_postgres_client
    await pg.execute(
        "INSERT INTO public.projects (project_id, user_name, name, domain_config) VALUES "
        "('p1', 'ada', 'P1', '{}'::jsonb), ('p2', 'ada', 'P2', '{}'::jsonb)"
    )
    await pg.execute(
        "INSERT INTO public.sessions (session_id, user_name, project_id, status) "
        "VALUES ('s1', 'ada', 'p1', 'open')"
    )
    await pg.execute(
        """INSERT INTO public.project_documents
               (document_id, project_id, original_name, relative_path, size_bytes, content_hash, status)
           VALUES (%s, 'p1', 'a.md', 'notes/a.md', 1, 'h1', 'indexing'),
                  (%s, 'p1', 'b.md', 'notes/b.md', 1, 'h2', 'queued')""",
        (A, B),
    )
    chat = Chat()

    async def get_or_resume_session(_session_id):
        return chat

    notices = DocumentBatchNotices(
        SimpleNamespace(pg=pg, user_name="ada", get_or_resume_session=get_or_resume_session),
        SimpleNamespace(config=SimpleNamespace(developer_settings=SimpleNamespace(
            ingestion_notices=IngestionNoticeSettings(poll_seconds=1, max_wait_minutes=1),
        ))),
    )
    real_sleep = asyncio.sleep
    monkeypatch.setattr(
        "core.session.document_batch_notices.asyncio.sleep", lambda _s: real_sleep(0.05)
    )

    with pytest.raises(ValueError):
        await notices.check_chat(project_id="p2", session_id="s1")
    await notices.watch(project_id="p1", session_id="s1", label="notes", document_ids=[A, B])
    await real_sleep(0.2)
    assert chat.reports == []  # still indexing

    await pg.execute(
        "UPDATE public.project_documents SET status = 'indexed' WHERE document_id = %s", (A,)
    )
    await pg.execute(
        "UPDATE public.project_documents SET status = 'failed', error_message = 'bad PDF' "
        "WHERE document_id = %s", (B,)
    )
    await asyncio.wait_for(asyncio.gather(*notices._tasks), timeout=5)

    [report] = chat.reports
    assert "1 of 2 files are ready" in report and "notes/b.md: bad PDF" in report
