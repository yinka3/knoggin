import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx

from app import create_app


def test_episode_edit_holds_lease_and_rejects_stale_revision(monkeypatch, tmp_path):
    updated_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    episode = SimpleNamespace(
        episode_id="episode-1", project_id="project-1", summary="Before",
        new_developments=[], updates=[], unresolved=[], user_modified=False,
        created_at=updated_at, updated_at=updated_at,
    )
    active_leases = set()
    writes = []

    class Projects:
        async def get_project(self, project_id):
            return {"status": "active"} if project_id == "project-1" else None

        async def acquire_project_for_session(self, project_id, lease_id):
            active_leases.add(lease_id)
            return SimpleNamespace()

        async def release_project_for_session(self, project_id, lease_id):
            active_leases.remove(lease_id)

    class Store:
        async def get_project_episode(self, episode_id, **kwargs):
            return episode if episode_id == "episode-1" else None

        async def edit_episode(self, **kwargs):
            assert active_leases
            writes.append(kwargs)
            return datetime(2026, 1, 2, tzinfo=timezone.utc)

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(
            projects=Projects(), resources=SimpleNamespace(knowledge_store=Store()),
            sessions=SimpleNamespace(user_name="ada"),
            config_manager=SimpleNamespace(config=SimpleNamespace(
                developer_settings=SimpleNamespace(jobs=SimpleNamespace(
                    episode=SimpleNamespace(max_narrative_chars=20000)
                ))
            )), shutdown=shutdown,
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                url = "/v1/projects/project-1/episodes/episode-1"
                read = await client.get(url)
                assert read.status_code == 200
                assert "embedding" not in read.json()
                body = {"summary": "After", "new_developments": [], "updates": [],
                        "unresolved": [], "expected_updated_at": "2025-01-01T00:00:00Z"}
                assert (await client.put(url, json=body)).status_code == 409
                assert not writes
                body["expected_updated_at"] = updated_at.isoformat()
                changed = await client.put(url, json=body)
                assert changed.status_code == 200
                assert changed.json()["user_modified"] is True
                assert len(writes) == 1
                assert not active_leases

    asyncio.run(exercise())
