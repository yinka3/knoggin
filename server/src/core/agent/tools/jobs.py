"""Agent tools for background research jobs started from the current chat."""

from __future__ import annotations

from typing import Dict

from common.exceptions import ToolExecutionError

_PUBLIC_FIELDS = ("job_id", "state", "question", "depth", "session_id", "error_code")


class ResearchJobTools:
    def _jobs(self, tool_name: str):
        jobs = getattr(self, "research_jobs", None)
        if jobs is None:
            raise ToolExecutionError(tool_name, "Background research is unavailable")
        return jobs

    def _own_job(self, tool_name: str, job_id: str) -> Dict:
        job = self._jobs(tool_name).get(job_id)
        if job is None or job["parent_session_id"] != self.session_id:
            raise ToolExecutionError(tool_name, "No such research job in this chat")
        return job

    async def start_research_job(
        self, question: str, links: list[str] | None = None, depth: str = "research",
    ) -> Dict:
        try:
            job = await self._jobs("start_research_job").start(
                project_id=self.project_id, parent_session_id=self.session_id,
                question=question, links=links, depth=depth,
            )
        except ValueError as exc:
            raise ToolExecutionError("start_research_job", str(exc)) from exc
        return {
            **{key: job[key] for key in _PUBLIC_FIELDS},
            "links": len(job["links"]),
            "note": "Running in the background; the report will arrive in this chat.",
        }

    async def list_research_jobs(self) -> Dict:
        jobs = self._jobs("list_research_jobs").list(parent_session_id=self.session_id)
        return {"jobs": [{key: job[key] for key in _PUBLIC_FIELDS} for job in jobs]}

    async def cancel_research_job(self, job_id: str) -> Dict:
        self._own_job("cancel_research_job", job_id)
        job = await self._jobs("cancel_research_job").cancel(job_id)
        return {key: job[key] for key in _PUBLIC_FIELDS}
