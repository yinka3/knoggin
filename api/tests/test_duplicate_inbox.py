import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx

from app import create_app


def test_duplicate_inbox_lists_blocked_pairs_and_dismisses_one(monkeypatch, tmp_path):
    dismissed = []

    class Service:
        async def list_blocked_duplicates(self):
            return [{"survivor_entity_id": 3, "retired_entity_id": 2, "reasoning": "Same person.",
                     "checked_at": datetime(2026, 10, 27, tzinfo=timezone.utc)}]

        async def dismiss_duplicate(self, a, b):
            dismissed.append((a, b))
            return (a, b) == (3, 2)

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(
            projects=SimpleNamespace(entity_maintenance_service=Service()),
            sessions=SimpleNamespace(user_name="ada"), shutdown=shutdown,
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                return (
                    await client.get("/v1/maintenance/duplicates"),
                    await client.post("/v1/maintenance/duplicates/3/2/dismiss"),
                    await client.post("/v1/maintenance/duplicates/7/8/dismiss"),
                    await client.post("/v1/maintenance/duplicates/x/2/dismiss"),
                )

    listed, ok, missing, bad = asyncio.run(exercise())
    assert listed.json()["duplicates"][0]["checked_at"] == "2026-10-27T00:00:00+00:00"
    assert ok.json() == {"dismissed": True}
    assert missing.status_code == 404 and bad.status_code == 404
    assert dismissed == [(3, 2), (7, 8)]
