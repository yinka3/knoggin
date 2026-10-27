import asyncio
from types import SimpleNamespace

import httpx

from app import create_app


def test_resume_reruns_only_an_interrupted_job_of_this_chat(monkeypatch, tmp_path):
    jobs = {
        "cut": {"job_id": "cut", "parent_session_id": "s1", "project_id": "p1", "state": "interrupted"},
        "done": {"job_id": "done", "parent_session_id": "s1", "project_id": "p1", "state": "completed"},
    }

    class Jobs:
        def get(self, job_id):
            return jobs.get(job_id)

        async def resume_job(self, job_id):
            job = jobs[job_id]
            if job["state"] != "interrupted":
                raise ValueError("Only an interrupted job can be resumed")
            job["state"] = "queued"
            return job

    class Projects:
        async def get_project(self, project_id):
            return {"status": "active"}

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(
            projects=Projects(), sessions=SimpleNamespace(user_name="ada", research_jobs=Jobs()),
            shutdown=shutdown,
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                other_chat = await client.post("/v1/sessions/s2/research-jobs/cut/resume")
                finished = await client.post("/v1/sessions/s1/research-jobs/done/resume")
                resumed = await client.post("/v1/sessions/s1/research-jobs/cut/resume")
        return other_chat, finished, resumed

    other_chat, finished, resumed = asyncio.run(exercise())
    assert other_chat.status_code == 404
    assert finished.status_code == 409
    assert finished.json()["error"]["code"] == "research_job_not_interrupted"
    assert resumed.status_code == 202 and resumed.json()["state"] == "queued"
