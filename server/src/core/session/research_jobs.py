"""Background research: research runs that report back to the chat that
started them, so the user can keep talking while they work."""

from __future__ import annotations

import asyncio
from collections import Counter, OrderedDict
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit
from uuid import uuid4

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent.tool_names import research_job_tool_names
from common.schema.primitives import Message
from common.utils.time_utils import get_now_iso
from core.session.background_work_store import BackgroundWorkStore

if TYPE_CHECKING:
    from core.session.session_manager import SessionManager

ResearchDepth = Literal["research", "deep_research"]
_FINISHED = frozenset({"completed", "needs_input", "failed", "cancelled"})
_UNFINISHED = frozenset({"queued", "running", "interrupted"})
_KEPT_FINISHED_JOBS = 100


class ResearchJobs:
    """Run each job as a research turn in its own new chat.

    The job's chat holds the full research and sources and becomes memory like
    any chat. When it ends, the starting chat gets a report turn. Jobs are
    recorded at each change: after a restart, a job that was cut off runs
    again from the start, and a report that was not handed to its chat is.
    """

    def __init__(self, sessions: SessionManager, config_manager) -> None:
        self._sessions = sessions
        postgres = getattr(sessions, "pg", None)
        self._store = (
            BackgroundWorkStore(postgres, sessions.user_name) if postgres is not None else None
        )
        self._config_manager = config_manager
        self._jobs: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._tasks: dict[str, asyncio.Task] = {}
        self._running: Counter[str] = Counter()
        self._slots = asyncio.Condition()
        self._closed = False

    def _settings(self):
        config = (self._config_manager or ConfigManager.get()).config
        return config.developer_settings.research_jobs

    async def start(
        self, *, project_id: str, parent_session_id: str, question: str,
        links: list[str] | None = None, depth: ResearchDepth = "research",
    ) -> dict[str, Any]:
        if self._closed:
            raise RuntimeError("Research jobs are shutting down")
        question = question.strip()
        if not question or len(question) > 4_000:
            raise ValueError("Research question must contain 1 to 4000 characters")
        if depth not in ("research", "deep_research"):
            raise ValueError("depth must be research or deep_research")
        links = list(dict.fromkeys(link.strip() for link in links or () if link.strip()))
        max_links = self._settings().max_links
        if len(links) > max_links:
            raise ValueError(
                f"A research job can read at most {max_links} links "
                "(developer_settings.research_jobs.max_links)"
            )
        for link in links:
            if urlsplit(link).scheme not in ("http", "https"):
                raise ValueError(f"Not an http(s) link: {link}")
        job = {
            "job_id": str(uuid4()), "project_id": project_id,
            "parent_session_id": parent_session_id, "session_id": None,
            "question": question, "links": links, "depth": depth,
            "state": "queued", "error_code": None, "report_delivered": False,
            "created_at": get_now_iso(), "finished_at": None,
        }
        self._remember(job)
        await self._persist(job)
        self._launch(job)
        return self._public(job)

    def _launch(self, job: dict[str, Any]) -> None:
        self._tasks[job["job_id"]] = asyncio.create_task(
            self._run(job), name=f"research-job:{job['job_id']}"
        )

    async def resume(self) -> None:
        """After a restart: run cut-off jobs again and hand undelivered
        reports to their chats. Finished jobs are listed again."""

        if self._store is None:
            return
        for row in await self._store.load("research_job", limit=_KEPT_FINISHED_JOBS):
            job = dict(row["payload"])
            report = job.pop("report", None)
            if job["state"] in _UNFINISHED:
                job.update(state="queued", session_id=None, error_code=None)
                self._remember(job)
                await self._persist(job)
                self._launch(job)
            else:
                self._remember(job)
                if not job["report_delivered"] and job["state"] != "cancelled":
                    self._tasks[job["job_id"]] = asyncio.create_task(
                        self._deliver(job, report), name=f"research-report:{job['job_id']}"
                    )

    async def _deliver(self, job: dict[str, Any], report: str | None) -> None:
        await self._report(job, report)
        self._tasks.pop(job["job_id"], None)

    async def _persist(self, job: dict[str, Any], report: str | None = None) -> None:
        """Record the job; a failed write must not stop the job itself."""

        if self._store is None:
            return
        try:
            await self._store.save(
                "research_job", job["job_id"], project_id=job["project_id"],
                session_id=job["parent_session_id"], state=job["state"],
                payload={**job, "report": report},
            )
        except Exception:
            logger.exception("Research job {} was not recorded", job["job_id"])

    def list(self, *, parent_session_id: str | None = None) -> list[dict[str, Any]]:
        return [
            self._public(job) for job in self._jobs.values()
            if parent_session_id is None or job["parent_session_id"] == parent_session_id
        ]

    def get(self, job_id: str) -> dict[str, Any] | None:
        job = self._jobs.get(job_id)
        return self._public(job) if job is not None else None

    async def cancel(self, job_id: str) -> dict[str, Any] | None:
        job = self._jobs.get(job_id)
        if job is None:
            return None
        task = self._tasks.get(job_id)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return self._public(job)

    async def shutdown(self) -> None:
        """Stop every job before the sessions they use are unloaded."""

        self._closed = True
        tasks = [task for task in self._tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self, job: dict[str, Any]) -> None:
        try:
            async with self._slot(job["project_id"]):
                job["state"] = "running"
                await self._persist(job)
                content = await self._research(job)
        except asyncio.CancelledError:
            # A shutdown is not the user's choice: the job runs again on start.
            job["state"] = "interrupted" if self._closed else "cancelled"
            job["finished_at"] = None if self._closed else get_now_iso()
            self._tasks.pop(job["job_id"], None)
            await asyncio.shield(self._persist(job))
            raise
        except Exception:
            logger.exception("Research job {} failed", job["job_id"])
            job["state"] = "failed"
            job["error_code"] = job["error_code"] or "run_failed"
            content = None
        job["finished_at"] = get_now_iso()
        self._tasks.pop(job["job_id"], None)
        await self._persist(job, content)
        await self._report(job, content)

    async def _research(self, job: dict[str, Any]) -> str | None:
        parent = self._sessions.get_runtime_session(job["parent_session_id"])
        session = await self._sessions.create_session(
            job["project_id"],
            model=getattr(parent, "model", None),
            agent_id=getattr(parent, "agent_id", None),
        )
        job["session_id"] = session.session_id
        content = None
        try:
            stream = await session.open_agent_run_stream(
                Message(content=self._prompt(job)),
                research_mode=job["depth"],
                enabled_tools=list(research_job_tool_names()),
                extra_web_reads=len(job["links"]),
            )
            try:
                async for event in stream:
                    data = event.get("data", {})
                    if event["event"] == "response":
                        job["state"] = "completed"
                        content = data.get("content")
                    elif event["event"] == "clarification":
                        job["state"] = "needs_input"
                        content = data.get("question")
                    elif event["event"] == "error":
                        job["state"] = "failed"
                        job["error_code"] = data.get("code") or "run_failed"
            finally:
                await stream.aclose()
            if job["state"] == "running":
                job["state"] = "failed"
                job["error_code"] = "run_failed"
        finally:
            await self._sessions.deactivate_runtime_session(session.session_id)
        return content

    @staticmethod
    def _prompt(job: dict[str, Any]) -> str:
        if not job["links"]:
            return job["question"]
        links = "\n".join(f"- {link}" for link in job["links"])
        return f"{job['question']}\n\nRead each of these links:\n{links}"

    async def _report(self, job: dict[str, Any], content: str | None) -> None:
        try:
            parent = await self._sessions.get_or_resume_session(job["parent_session_id"])
        except Exception:
            logger.exception("Research job {} could not reopen its chat", job["job_id"])
            return
        if parent is None:
            return  # The chat was deleted; the job's own chat has the result.
        header = f"Background research {job['job_id']} finished.\nQuestion: {job['question']}"
        if job["state"] == "completed":
            text = (
                f"{header}\n\nReport:\n{content or ''}\n\n"
                "Tell the user the main findings briefly, and say the full "
                "report with its sources is in the research chat."
            )
        elif job["state"] == "needs_input":
            text = (
                f"{header}\n\nIt stopped because it needs more direction:\n"
                f"{content or ''}\n\nAsk the user this question."
            )
        else:
            text = (
                f"{header}\n\nIt failed ({job['error_code']}). Tell the user, "
                "and offer to start it again."
            )
        try:
            queued = await parent.queue_report(
                text, origin="research_job_report", metadata={"job_id": job["job_id"]}
            )
        except Exception:
            logger.exception("Research job {} report was not queued", job["job_id"])
            return
        job["report_delivered"] = queued is not None
        await self._persist(job, content)

    @asynccontextmanager
    async def _slot(self, project_id: str):
        async with self._slots:
            await self._slots.wait_for(
                lambda: self._running[project_id]
                < self._settings().max_running_per_project
            )
            self._running[project_id] += 1
        try:
            yield
        finally:
            async with self._slots:
                self._running[project_id] -= 1
                self._slots.notify_all()

    def _remember(self, job: dict[str, Any]) -> None:
        self._jobs[job["job_id"]] = job
        finished = [key for key, old in self._jobs.items() if old["state"] in _FINISHED]
        for key in finished[: max(0, len(self._jobs) - _KEPT_FINISHED_JOBS)]:
            del self._jobs[key]

    @staticmethod
    def _public(job: dict[str, Any]) -> dict[str, Any]:
        return {**job, "links": list(job["links"])}
