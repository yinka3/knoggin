import asyncio
from types import SimpleNamespace

import httpx

from app import create_app


def test_chats_in_an_archived_project_are_readable_but_not_changeable(monkeypatch, tmp_path):
    state = SimpleNamespace(status="archived", changed=[])

    class Sessions:
        async def list_sessions(self):
            return {"s1": {"session_id": "s1", "project_id": "p1", "title": "Chat"}}

        async def update_session(self, session_id, changes):
            state.changed.append("update")

        async def delete_session(self, session_id):
            state.changed.append("delete")

        async def set_document_focus(self, session_id, **kwargs):
            state.changed.append("focus")
            return {}

        async def clear_document_focus(self, session_id):
            state.changed.append("clear")

        async def get_document_focus(self, session_id):
            return {"focus": None}

    class Projects:
        async def get_project(self, project_id):
            return {"status": state.status}

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(sessions=Sessions(), projects=Projects(), shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                focus = {"target_type": "subtree", "path_prefix": "docs", "behavior": "prefer"}
                changes = [
                    client.patch("/v1/sessions/s1", json={"title": "Renamed"}),
                    client.put("/v1/sessions/s1/document-focus", json=focus),
                    client.delete("/v1/sessions/s1/document-focus"),
                    client.delete("/v1/sessions/s1"),
                ]
                for change in changes:
                    response = await change
                    assert response.status_code == 409
                    assert response.json()["error"]["code"] == "project_archived"
                assert state.changed == []
                assert (await client.get("/v1/sessions/s1/document-focus")).status_code == 200
                missing = await client.delete("/v1/sessions/nope")
                assert missing.status_code == 404

                state.status = "active"
                assert (await client.delete("/v1/sessions/s1")).status_code == 200
                assert state.changed == ["delete"]

    asyncio.run(exercise())
