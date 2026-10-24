import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
from pydantic import BaseModel

from app import create_app


def test_review_routes_enforce_scope_and_dispatch_decisions(monkeypatch, tmp_path):
    calls = []

    class Plan(BaseModel):
        kind: str = "advisory"

    review = SimpleNamespace(
        review_id="review-1", scope="project", project_id="project-1",
        kind="relationship_advisory", reasoning="Check relationship",
        proposed_plan=Plan(),
        expected_state={"revision": 3}, status="open",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), resolved_at=None,
    )

    class Maintenance:
        async def list_maintenance_reviews(self, project_id):
            calls.append(("list", project_id))
            return [review]

        async def transition_maintenance_review(self, project_id, review_id, **kwargs):
            calls.append(("decide", project_id, review_id, kwargs))
            return review

    class Projects:
        maintenance_service = Maintenance()

        async def get_project(self, project_id):
            if project_id != "project-1":
                return None
            return {"status": self.status}

        status = "active"

    projects = Projects()

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(projects=projects, shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                base = "/v1/projects/project-1/maintenance/reviews"
                assert (await client.get(base.replace("project-1", "missing"))).status_code == 404
                listed = await client.get(base)
                assert listed.status_code == 200
                assert listed.json()["reviews"][0]["created_at"] == "2026-01-01T00:00:00+00:00"
                url = base + "/review-1/decision"
                assert (await client.post(url, json={"action": "invalid"})).status_code == 422
                projects.status = "archived"
                assert (await client.post(url, json={"action": "dismiss"})).status_code == 409
                projects.status = "active"
                decided = await client.post(url, json={
                    "action": "dismiss", "expected_state": {"revision": 3},
                })
                assert decided.status_code == 200
                assert calls[-1] == (
                    "decide", "project-1", "review-1",
                    {"status": "dismissed", "expected_state": {"revision": 3}, "reason": None},
                )

    asyncio.run(exercise())
