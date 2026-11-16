import asyncio
from types import SimpleNamespace

import httpx

from app import create_app


def test_background_items_can_be_listed_and_stopped(monkeypatch, tmp_path):
    item = {"item_id": "h1", "session_id": "chat-1", "tool": "run_command",
            "label": "pytest", "state": "running"}

    class Handoffs:
        def list(self, *, session_id):
            return [item] if session_id == "chat-1" else []

        async def stop(self, item_id, *, session_id):
            if item_id != "h1" or session_id != "chat-1":
                return None
            return {**item, "state": "stopped"}

    async def start(**_kwargs):
        async def shutdown():
            pass

        async def list_sessions():
            return {"chat-1": {"project_id": "p1"}}

        return SimpleNamespace(shutdown=shutdown, sessions=SimpleNamespace(
            tool_handoffs=Handoffs(), list_sessions=list_sessions,
        ))

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                return (
                    await client.get("/v1/sessions/chat-1/handoffs"),
                    await client.get("/v1/sessions/missing/handoffs"),
                    await client.delete("/v1/sessions/chat-1/handoffs/h1"),
                    await client.delete("/v1/sessions/chat-1/handoffs/h9"),
                )

    listed, missing, stopped, unknown = asyncio.run(exercise())
    assert listed.json() == {"items": [item]}
    assert missing.status_code == 404
    assert stopped.json()["state"] == "stopped"
    assert unknown.json()["error"]["code"] == "handoff_not_found"
