import asyncio
from types import SimpleNamespace

import httpx

from app import create_app


def test_knowledge_explorer_scopes_reads_and_removes_private_fields(monkeypatch, tmp_path):
    calls = []

    class Projects:
        async def get_project(self, project_id):
            return {"status": "archived"} if project_id == "p1" else None

    class Store:
        async def get_visible_session_ids(self, **scope):
            calls.append(("sessions", scope))
            return ["s1"]

        async def search_messages_fts(self, query, **scope):
            calls.append(("search_messages", query, scope))
            return [(8, 0.7, "s1")]

        async def get_messages_by_ids(self, ids, **scope):
            calls.append(("messages", ids, scope))
            return [{"id": 8, "session_id": "s1", "role": "assistant",
                     "content": "Answer", "timestamp": 123,
                     "user_name": "ada", "internal": "hidden"}]

        async def search_entity(self, query, **scope):
            calls.append(("search_entities", query, scope))
            return [{"entity_id": 3, "canonical_name": "Ada", "aliases": [],
                     "contexts": [{"project_id": "p1", "entity_type": "Person",
                                   "topic": "People", "internal": "hidden"}],
                     "user_name": "ada"}]

        async def get_entity_by_id(self, entity_id, **scope):
            calls.append(("entity", entity_id, scope))
            return {"id": 3, "canonical_name": "Ada", "aliases": [],
                    "contexts": [{"project_id": "p1", "entity_type": "Person"}]}

        async def get_related_entities(self, ids, **scope):
            calls.append(("connections", ids, scope))
            return [{"project_id": "p1", "source_entity_id": 3,
                     "target_entity_id": 4, "relationship_id": "r1",
                     "evidence_refs": [{"project_id": "p1", "session_id": "s1",
                                        "message_id": 8, "user_name": "private"}],
                     "observation_refs": [{"observation_id": 5,
                                           "context": "private"}]}]

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
                base = "/v1/projects/p1/knowledge"
                found = await client.get(base + "/messages", params={"query": "answer"})
                assert found.status_code == 200
                assert found.json()["items"][0]["score"] == 0.7
                assert "internal" not in found.text
                entities = await client.get(base + "/entities", params={"query": "Ada"})
                assert entities.status_code == 200
                assert "internal" not in entities.text
                connections = await client.get(base + "/entities/3/connections")
                assert connections.status_code == 200
                assert connections.json()["items"][0]["evidence_refs"] == [{
                    "project_id": "p1", "session_id": "s1", "message_id": 8,
                }]
                assert "private" not in connections.text
                assert (await client.get("/v1/projects/p2/knowledge/entities", params={
                    "query": "Ada"
                })).status_code == 404
                assert (await client.get(base + "/messages", params={
                    "query": " "
                })).status_code == 422

    asyncio.run(exercise())
    assert ("search_entities", "Ada", {"visible_project_ids": ["p1"], "limit": 20}) in calls
    assert ("entity", 3, {"visible_project_ids": ["p1"]}) in calls
