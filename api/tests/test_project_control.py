import asyncio
from types import SimpleNamespace
from uuid import uuid4

import httpx

from app import create_app


def test_project_control_scopes_reads_and_future_only_selection(monkeypatch, tmp_path):
    project = {"project_id": "project-1", "status": "archived"}
    session = {"session_id": "session-1", "semantic_participation_enabled": False,
               "semantic_participation_after_message_id": 42}
    calls = []

    class Maintenance:
        async def inspect_semantic_window_blockages(self, project_id, *, limit):
            calls.append(("inspect", project_id, limit))
            return (SimpleNamespace(model_dump=lambda **_: {"window_id": "window-1"}),)

        async def flush_semantic_memory(self, project_id):
            calls.append(("flush", project_id))
            return project["status"] == "active"

        async def list_maintenance_reviews(self, project_id):
            return [SimpleNamespace(status="open")]

        async def retry_semantic_window(self, project_id, window_id):
            calls.append(("retry", project_id, window_id))
            return SimpleNamespace(window_id=window_id, stage="claimed", attempt_count=0,
                                   policy_snapshot={"api_key": "must-not-appear"})

    class Projects:
        maintenance_service = Maintenance()

        async def get_project(self, project_id):
            return project if project_id == "project-1" else None

        async def reactivate_project(self, project_id):
            project["status"] = "active"
            return {**project, "id": project_id, "name": "Test", "description": "", "allowed_projects": []}

        async def list_session_semantic_participation(self, project_id):
            return [dict(session)]

        async def set_session_semantic_participation(self, project_id, session_ids):
            if session_ids != ["session-1"]:
                raise ValueError("unknown session")
            session["semantic_participation_enabled"] = True
            session["semantic_participation_after_message_id"] = 42
            return [dict(session)]

    async def start(**_kwargs):
        async def shutdown():
            pass
        async def ingestion_health(**_kwargs):
            return SimpleNamespace(status="healthy", summary="Ingestion is healthy", warnings=[])
        return SimpleNamespace(
            projects=Projects(),
            sessions=SimpleNamespace(user_name="ada"),
            health_service=SimpleNamespace(get_ingestion_health=ingestion_health),
            shutdown=shutdown,
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                base = "/v1/projects/project-1"
                assert (await client.get("/v1/projects/other/control")).status_code == 404
                overview = (await client.get(base + "/control")).json()
                assert overview["open_review_count"] == 1
                assert overview["ingestion_health"]["status"] == "healthy"
                assert (await client.get(base + "/semantic-participation")).json()["sessions"][0]["semantic_participation_enabled"] is False
                assert (await client.put(base + "/semantic-participation", json={"session_ids": ["session-1"]})).status_code == 409
                assert (await client.post(base + "/memory/save")).status_code == 409
                activated = await client.post(base + "/reactivate")
                assert activated.status_code == 200
                invalid = await client.put(base + "/semantic-participation", json={"session_ids": ["session-1", "session-1"]})
                assert invalid.status_code == 422
                selected = await client.put(base + "/semantic-participation", json={"session_ids": ["session-1"]})
                assert selected.json()["effect"] == "future_messages_only"
                assert selected.json()["sessions"][0]["semantic_participation_after_message_id"] == 42
                assert (await client.get(base + "/maintenance/semantic-windows/blocked")).json()["windows"] == [{"window_id": "window-1"}]
                retry = await client.post(base + "/maintenance/semantic-windows/" + str(uuid4()) + "/retry")
                assert retry.status_code == 200
                assert retry.json()["retry_accepted"] is True
                assert "must-not-appear" not in retry.text
                assert (await client.post(base + "/maintenance/semantic-windows/bad/retry")).status_code == 422
                saved = await client.post(base + "/memory/save")
                assert saved.status_code == 202
                assert saved.json() == {"flush_requested": True}

    asyncio.run(exercise())
    assert ("inspect", "project-1", 21) in calls
    # The archived project is refused before the service is asked.
    assert calls.count(("flush", "project-1")) == 1
