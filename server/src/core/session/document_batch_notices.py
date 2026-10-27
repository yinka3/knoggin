"""Tell a chat when a batch of documents (a folder import) finishes indexing."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

from loguru import logger

from common.conf.manager import ConfigManager
from core.session.background_work_store import BackgroundWorkStore

if TYPE_CHECKING:
    from core.session.session_manager import SessionManager

_MAX_LISTED_FAILURES = 20


class DocumentBatchNotices:
    """Watch documents until each is indexed, deleted, or failed with no retry
    left, then queue one report turn in the chat that asked. Each watch is
    recorded until its notice is queued, so a restart resumes it."""

    def __init__(self, sessions: SessionManager, config_manager=None) -> None:
        self._sessions = sessions
        self._store = BackgroundWorkStore(sessions.pg, sessions.user_name)
        self._config_manager = config_manager
        self._tasks: set[asyncio.Task] = set()
        self._closed = False

    def _settings(self):
        config = (self._config_manager or ConfigManager.get()).config
        return config.developer_settings.ingestion_notices

    async def watch(
        self, *, project_id: str, session_id: str, label: str, document_ids: list[str],
    ) -> None:
        """Start watching; raises if the chat is not in this project."""

        if self._closed:
            raise RuntimeError("Document notices are shutting down")
        if not document_ids:
            return
        await self.check_chat(project_id=project_id, session_id=session_id)
        work_id = str(uuid4())
        document_ids = sorted(set(document_ids))
        await self._store.save(
            "folder_notice", work_id, project_id=project_id, session_id=session_id,
            state="watching", payload={"label": label, "document_ids": document_ids},
        )
        self._launch(work_id, project_id, session_id, label, document_ids)

    async def resume(self) -> None:
        """After a restart, keep watching every batch not yet reported."""

        for row in await self._store.load("folder_notice"):
            payload = row["payload"]
            self._launch(
                row["work_id"], row["project_id"], row["session_id"],
                payload["label"], payload["document_ids"],
            )

    def _launch(self, work_id, project_id, session_id, label, document_ids) -> None:
        task = asyncio.create_task(
            self._watch(work_id, project_id, session_id, label, document_ids),
            name=f"document-notice:{session_id}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def check_chat(self, *, project_id: str, session_id: str) -> None:
        """Raise ValueError unless the chat is open and in this project, so a
        notice never lands in another project's chat."""

        row = await self._sessions.pg.fetch_one(
            """SELECT 1 FROM public.sessions
               WHERE user_name = %s AND session_id = %s AND project_id = %s
                 AND status = 'open'""",
            (self._sessions.user_name, session_id, project_id),
        )
        if row is None:
            raise ValueError("Session is not an open chat in this project")

    async def shutdown(self) -> None:
        self._closed = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _watch(
        self, work_id: str, project_id: str, session_id: str, label: str,
        document_ids: list[str],
    ) -> None:
        settings = self._settings()
        waited = 0.0
        try:
            while True:
                rows = await self._statuses(project_id, document_ids)
                if all(_finished(row) for row in rows):
                    break
                if waited >= settings.max_wait_minutes * 60:
                    logger.info("Stopped waiting on document batch for {}", session_id)
                    rows = None
                    break
                await asyncio.sleep(settings.poll_seconds)
                waited += settings.poll_seconds
            await self._report(session_id, label, _report_text(label, document_ids, rows))
            await self._store.delete(work_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Document batch notice failed for {}", session_id)

    async def _statuses(self, project_id: str, document_ids: list[str]) -> list[dict]:
        return await self._sessions.pg.fetch_all(
            """SELECT document_id::text AS document_id, relative_path, status,
                      next_index_retry_at, error_message
               FROM public.project_documents
               WHERE project_id = %s AND document_id = ANY(%s::uuid[])""",
            (project_id, document_ids),
        )

    async def _report(self, session_id: str, label: str, text: str) -> None:
        session = await self._sessions.get_or_resume_session(session_id)
        if session is None:
            return  # The chat was deleted; the documents are still in the project.
        await session.queue_report(
            text, origin="folder_ingestion_report", metadata={"label": label[:200]},
        )


def _finished(row: dict) -> bool:
    return row["status"] in {"indexed", "deleted"} or (
        row["status"] == "failed" and row["next_index_retry_at"] is None
    )


def _report_text(label: str, document_ids: list[str], rows: list[dict] | None) -> str:
    if rows is None:
        return (
            f"Folder import \"{label}\" is still indexing after the notice stopped "
            f"waiting ({len(document_ids)} files). Tell the user briefly that it is "
            "still running and that the documents list shows each file's status."
        )
    indexed = [row for row in rows if row["status"] == "indexed"]
    failed = [row for row in rows if row["status"] == "failed"]
    missing = len(document_ids) - len(rows) + sum(row["status"] == "deleted" for row in rows)
    lines = [
        f"Folder import \"{label}\" finished indexing: {len(indexed)} of "
        f"{len(document_ids)} files are ready to search."
    ]
    if failed:
        lines.append(f"{len(failed)} failed:")
        lines += [
            f"- {row['relative_path']}: {(row['error_message'] or 'unknown error')[:200]}"
            for row in failed[:_MAX_LISTED_FAILURES]
        ]
        if len(failed) > _MAX_LISTED_FAILURES:
            lines.append(f"- and {len(failed) - _MAX_LISTED_FAILURES} more")
    if missing:
        lines.append(f"{missing} were deleted before indexing finished.")
    lines.append(
        "Tell the user briefly. If any failed, name them and say they can be "
        "reindexed from the documents list."
    )
    return "\n".join(lines)
