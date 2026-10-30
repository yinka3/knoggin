"""Agent tools for background tasks started from the current chat."""

from __future__ import annotations

from typing import Dict

from common.exceptions import ToolExecutionError

_PUBLIC_FIELDS = ("job_id", "state", "question", "mode", "session_id", "error_code")


class BackgroundTaskTools:
    def _jobs(self, tool_name: str):
        jobs = getattr(self, "background_tasks", None)
        if jobs is None:
            raise ToolExecutionError(tool_name, "Background tasks are unavailable")
        return jobs

    def _own_job(self, tool_name: str, job_id: str) -> Dict:
        job = self._jobs(tool_name).get(job_id)
        if job is None or job["parent_session_id"] != self.session_id:
            raise ToolExecutionError(tool_name, "No such background task in this chat")
        return job

    async def start_background_task(
        self, question: str, links: list[str] | None = None, mode: str = "normal",
    ) -> Dict:
        # The task's question becomes the user message of its own run, where
        # links count as the user's. Only pass on links this run already trusts.
        from core.agent.tools.search import _URL_IN_TEXT

        for link in [*(links or []), *_URL_IN_TEXT.findall(question or "")]:
            if not await self.web_url_is_known(link):
                raise ToolExecutionError(
                    "start_background_task",
                    f"{link} did not come from a search result, a page read in this run, "
                    "the user's messages, or the project's saved links. Ask the user to "
                    "send it in a message first.",
                )
        try:
            job = await self._jobs("start_background_task").start(
                project_id=self.project_id, parent_session_id=self.session_id,
                question=question, links=links, mode=mode,
            )
        except ValueError as exc:
            raise ToolExecutionError("start_background_task", str(exc)) from exc
        return {
            **{key: job[key] for key in _PUBLIC_FIELDS},
            "links": len(job["links"]),
            "note": "Running in the background; the report will arrive in this chat.",
        }

    async def list_background_tasks(self) -> Dict:
        jobs = self._jobs("list_background_tasks").list(parent_session_id=self.session_id)
        return {"jobs": [{key: job[key] for key in _PUBLIC_FIELDS} for job in jobs]}

    async def cancel_background_task(self, job_id: str) -> Dict:
        self._own_job("cancel_background_task", job_id)
        job = await self._jobs("cancel_background_task").cancel(job_id)
        return {key: job[key] for key in _PUBLIC_FIELDS}
