import asyncio
from types import SimpleNamespace

import pytest

from common.schema.settings import IngestionNoticeSettings
from core.session.document_batch_notices import DocumentBatchNotices


class FakePg:
    def __init__(self, polls):
        self.polls = list(polls)
        self.chats = {("s1", "p1")}

    async def fetch_one(self, _query, params):
        _user, session_id, project_id = params
        return {"ok": 1} if (session_id, project_id) in self.chats else None

    async def execute(self, _query, _params):
        pass

    async def fetch_all(self, _query, _params):
        return self.polls.pop(0) if len(self.polls) > 1 else self.polls[0]


class FakeChat:
    def __init__(self):
        self.reports = []

    async def queue_report(self, content, *, origin, metadata):
        self.reports.append((origin, metadata, content))
        return {"queue_id": "q"}


def _doc(document_id, status, *, retry=None, error=None):
    return {"document_id": document_id, "relative_path": f"notes/{document_id}.md",
            "status": status, "next_index_retry_at": retry, "error_message": error}


def _notices(pg, chat):
    async def get_or_resume_session(_session_id):
        return chat

    sessions = SimpleNamespace(pg=pg, user_name="ada", get_or_resume_session=get_or_resume_session)
    config = SimpleNamespace(config=SimpleNamespace(developer_settings=SimpleNamespace(
        ingestion_notices=IngestionNoticeSettings(poll_seconds=1, max_wait_minutes=1),
    )))
    return DocumentBatchNotices(sessions, config)


@pytest.mark.unit
@pytest.mark.no_network
async def test_the_chat_hears_once_every_file_is_indexed_or_out_of_retries(monkeypatch):
    real_sleep = asyncio.sleep
    monkeypatch.setattr("core.session.document_batch_notices.asyncio.sleep", lambda _s: real_sleep(0))
    pg = FakePg([
        [_doc("a", "indexing"), _doc("b", "queued")],
        # A failure with a retry scheduled is not finished yet.
        [_doc("a", "indexed"), _doc("b", "failed", retry="2026-10-27T12:00:00Z")],
        [_doc("a", "indexed"), _doc("b", "failed", error="PDF is encrypted")],
    ])
    chat = FakeChat()
    notices = _notices(pg, chat)

    await notices.watch(project_id="p1", session_id="s1", label="notes", document_ids=["a", "b"])
    await asyncio.gather(*notices._tasks)

    [(origin, metadata, text)] = chat.reports
    assert origin == "folder_ingestion_report" and metadata == {"label": "notes"}
    assert "1 of 2 files are ready" in text
    assert "notes/b.md: PDF is encrypted" in text


@pytest.mark.unit
@pytest.mark.no_network
async def test_a_chat_from_another_project_is_refused():
    notices = _notices(FakePg([[]]), FakeChat())

    with pytest.raises(ValueError):
        await notices.watch(project_id="p2", session_id="s1", label="x", document_ids=["a"])
