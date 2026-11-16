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


def test_episode_browse_and_source_expansion_are_project_scoped(monkeypatch, tmp_path):
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    card = SimpleNamespace(
        episode_id="e1", project_id="p1", summary="Recovery decision",
        new_developments=[], updates=[], unresolved=[], user_modified=False,
        created_at=stamp, updated_at=stamp,
    )
    calls = []

    class Projects:
        async def get_project(self, project_id):
            return {"status": "archived"} if project_id == "p1" else None

    class Store:
        async def get_recent_project_episodes(self, **scope):
            calls.append(("recent", scope))
            return [card]

        async def search_project_episodes(self, query, **scope):
            calls.append(("search", query, scope))
            return [card]

        async def get_project_episode(self, episode_id, **scope):
            calls.append(("get", episode_id, scope))
            return card if episode_id == "e1" else None

        async def get_project_episode_source_messages(self, episode_id, **scope):
            calls.append(("sources", episode_id, scope))
            rows = [{"message_id": 8, "session_id": "s1", "role": "user",
                     "content": "Recovery", "timestamp_ms": 123,
                     "message_position": 0, "attached_at": stamp,
                     "internal": "hidden"},
                    {"message_id": 9, "session_id": "s1", "role": "assistant",
                     "content": "Plan", "timestamp_ms": 124,
                     "message_position": 1, "attached_at": stamp}]
            return rows[scope["offset"]:scope["offset"] + scope["limit"]]

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(
            projects=Projects(), resources=SimpleNamespace(knowledge_store=Store()),
            sessions=SimpleNamespace(user_name="ada"), shutdown=shutdown,
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                base = "/v1/projects/p1/episodes"
                assert (await client.get(base)).json()["items"][0]["summary"] == "Recovery decision"
                assert (await client.get(base, params={"query": "recovery"})).status_code == 200
                assert (await client.get(base, params={"query": " "})).status_code == 422
                sources = await client.get(base + "/e1/sources")
                assert sources.status_code == 200
                assert sources.json()["messages"][0]["content"] == "Recovery"
                assert "internal" not in sources.text
                first = await client.get(base + "/e1/sources", params={"limit": 1})
                assert first.json()["next_offset"] == 1
                second = await client.get(base + "/e1/sources", params={
                    "limit": 1, "offset": 1,
                })
                assert second.json()["messages"][0]["content"] == "Plan"
                assert second.json()["has_more"] is False
                assert (await client.get(base + "/e2/sources")).status_code == 404
                assert (await client.get("/v1/projects/p2/episodes")).status_code == 404

    asyncio.run(exercise())
    assert calls[0][1] == {
        "user_name": "ada", "project_id": "p1",
        "visible_project_ids": ["p1"], "limit": 20,
    }
