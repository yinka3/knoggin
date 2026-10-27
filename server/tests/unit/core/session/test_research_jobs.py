import asyncio
from types import SimpleNamespace

import pytest

from common.schema.agent.tool_names import research_job_tool_names
from common.schema.settings import RootConfig
from core.session.research_jobs import ResearchJobs


class _Stream:
    def __init__(self, events, gate=None):
        self._events = events
        self._gate = gate
        self.closed = False

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        if self._gate is not None:
            await self._gate.wait()
        for event in self._events:
            yield event

    async def aclose(self):
        self.closed = True


class _Chat:
    def __init__(self, session_id, events=(), gate=None):
        self.session_id = session_id
        self.model = "model-a"
        self.agent_id = "agent-a"
        self.runs = []
        self.reports = []
        self._events = list(events)
        self._gate = gate

    async def open_agent_run_stream(self, message, **kwargs):
        self.runs.append({"content": message.content, **kwargs})
        return _Stream(self._events, self._gate)

    async def queue_report(self, content, *, origin, metadata):
        assert origin == "research_job_report"
        self.reports.append((metadata["job_id"], content))
        return {"queue_id": "q"}


class _Sessions:
    """Each created chat answers with the next scripted events."""

    def __init__(self, *scripts, gate=None):
        self.parent = _Chat("parent")
        self._scripts = list(scripts)
        self._gate = gate
        self.created = []
        self.unloaded = []

    def get_runtime_session(self, session_id):
        return self.parent if session_id == "parent" else None

    async def get_or_resume_session(self, session_id):
        return self.get_runtime_session(session_id)

    async def create_session(self, project_id, **kwargs):
        chat = _Chat(f"job-chat-{len(self.created) + 1}", self._scripts.pop(0), self._gate)
        self.created.append((project_id, kwargs, chat))
        return chat

    async def deactivate_runtime_session(self, session_id):
        self.unloaded.append(session_id)


def _jobs(sessions, **settings):
    config = RootConfig()
    config.developer_settings.research_jobs = (
        config.developer_settings.research_jobs.model_copy(update=settings)
    )
    return ResearchJobs(sessions, SimpleNamespace(config=config))


def _answer(content):
    return {"event": "response", "data": {"content": content}}


async def _finish(jobs):
    await asyncio.gather(*list(jobs._tasks.values()), return_exceptions=True)


@pytest.mark.unit
@pytest.mark.no_network
async def test_job_researches_in_its_own_chat_and_reports_to_the_starting_chat():
    sessions = _Sessions([_answer("Postgres wins on joins.")])
    jobs = _jobs(sessions)

    job = await jobs.start(
        project_id="project-1", parent_session_id="parent",
        question="Compare Postgres and SQLite", links=["https://a.example", "https://b.example"],
    )
    await _finish(jobs)

    project_id, created_with, chat = sessions.created[0]
    assert project_id == "project-1"
    assert created_with == {"model": "model-a", "agent_id": "agent-a"}
    run = chat.runs[0]
    assert run["research_mode"] == "research"
    assert run["extra_web_reads"] == 2
    assert "- https://a.example" in run["content"]
    assert set(run["enabled_tools"]) == set(research_job_tool_names())
    assert sessions.unloaded == ["job-chat-1"]

    finished = jobs.get(job["job_id"])
    assert finished["state"] == "completed"
    assert finished["session_id"] == "job-chat-1"
    assert finished["report_delivered"] is True
    [(job_id, report)] = sessions.parent.reports
    assert job_id == job["job_id"]
    assert "Postgres wins on joins." in report


@pytest.mark.unit
@pytest.mark.no_network
def test_job_tools_are_reads_plus_saving_findings():
    names = set(research_job_tool_names())

    assert {"search_web", "read_web_page", "submit_answer"} <= names
    assert {"create_project_file", "append_project_file", "create_project_folder"} <= names
    assert not names & {
        "start_research_job", "list_research_jobs", "cancel_research_job",
        "get_engine_health", "propose_entity_merge", "update_project_file",
        "delete_project_file", "move_project_file", "edit_agent_brain",
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_jobs_past_the_project_limit_wait_their_turn():
    gate = asyncio.Event()
    sessions = _Sessions([_answer("one")], [_answer("two")], gate=gate)
    jobs = _jobs(sessions, max_running_per_project=1)

    first = await jobs.start(project_id="project-1", parent_session_id="parent", question="one")
    second = await jobs.start(project_id="project-1", parent_session_id="parent", question="two")
    await asyncio.sleep(0.05)

    assert jobs.get(first["job_id"])["state"] == "running"
    assert jobs.get(second["job_id"])["state"] == "queued"
    assert len(sessions.created) == 1

    gate.set()
    await _finish(jobs)
    await _finish(jobs)
    assert [jobs.get(job["job_id"])["state"] for job in (first, second)] == [
        "completed", "completed",
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_cancelled_job_unloads_its_chat_and_sends_no_report():
    gate = asyncio.Event()
    sessions = _Sessions([_answer("never")], gate=gate)
    jobs = _jobs(sessions)

    job = await jobs.start(project_id="project-1", parent_session_id="parent", question="q")
    await asyncio.sleep(0.05)
    cancelled = await jobs.cancel(job["job_id"])

    assert cancelled["state"] == "cancelled"
    assert sessions.unloaded == ["job-chat-1"]
    assert sessions.parent.reports == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_failed_job_still_reports_so_the_user_hears_about_it():
    sessions = _Sessions([{"event": "error", "data": {"code": "llm_budget_exhausted"}}])
    jobs = _jobs(sessions)

    job = await jobs.start(project_id="project-1", parent_session_id="parent", question="q")
    await _finish(jobs)

    assert jobs.get(job["job_id"])["state"] == "failed"
    assert jobs.get(job["job_id"])["error_code"] == "llm_budget_exhausted"
    assert "It failed (llm_budget_exhausted)" in sessions.parent.reports[0][1]


@pytest.mark.unit
@pytest.mark.no_network
async def test_link_count_and_scheme_are_checked_before_starting():
    sessions = _Sessions()
    jobs = _jobs(sessions, max_links=2)

    with pytest.raises(ValueError, match="at most 2 links"):
        await jobs.start(
            project_id="project-1", parent_session_id="parent", question="q",
            links=["https://a.example", "https://b.example", "https://c.example"],
        )
    with pytest.raises(ValueError, match="Not an http"):
        await jobs.start(
            project_id="project-1", parent_session_id="parent", question="q",
            links=["file:///etc/passwd"],
        )
    assert sessions.created == []
