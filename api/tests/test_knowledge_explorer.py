import asyncio
from types import SimpleNamespace
from uuid import uuid4

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


def test_context_block_jev_explanation_is_scoped_and_allowlisted(monkeypatch, tmp_path):
    block_id, window_id = str(uuid4()), str(uuid4())

    class Projects:
        async def get_project(self, project_id):
            return {"status": "active"} if project_id == "p1" else None

    class Store:
        async def get_entity_by_id(self, entity_id, **scope):
            assert (entity_id, scope) == (3, {"visible_project_ids": ["p1"]})
            return {"id": 3, "canonical_name": "Ada", "contexts": []}

        async def list_jev_decisions_for_entity(self, entity_id, **scope):
            assert (entity_id, scope) == (3, {
                "user_name": "ada", "project_id": "p1", "limit": 50,
            })
            return [{"capability": "classification", "window_id": window_id,
                     "pass_number": 1, "occurrence_key": "one", "decision": {
                         "baseline_topic": "General", "suggested_topic": "Research",
                         "operational_topic": "Research", "record": {
                             "baseline_outcome": "default_topic",
                             "acceptance_status": "accepted",
                             "evidence_block_ids": [block_id],
                         },
                     }}]

        async def list_jev_decisions_for_block(self, requested_id, **scope):
            assert requested_id == block_id
            assert scope == {"user_name": "ada", "project_id": "p1", "limit": 50}
            return {"block_id": block_id, "excerpt": "Evidence text", "decisions": [{
                "capability": "classification", "window_id": window_id,
                "pass_number": 1, "occurrence_key": "one", "decision": {
                    "baseline_topic": "General", "suggested_topic": "Research",
                    "operational_topic": "Research", "internal": "private",
                    "record": {
                        "baseline_outcome": "default_topic", "acceptance_status": "accepted",
                        "evidence_block_ids": [block_id], "pinned_model": "jev-model",
                        "question_version": "v1", "acceptance_policy_version": "v1",
                        "option_mapping": {"topic_1": "Research"},
                        "result": {"outcome": "available", "response": {"answers": {
                            "topic_choice": {"type": "choice", "choice": "topic_1",
                                             "confidence": 0.9,
                                             "probabilities": {"topic_1": 0.9},
                                             "private": "hidden"},
                        }}},
                    },
                },
            }, {
                "capability": "identity", "window_id": window_id,
                "pass_number": 1, "occurrence_key": "two", "decision": {
                    "baseline_entity_id": 3, "suggested_entity_id": 4,
                    "accepted_entity_id": 4,
                    "record": {
                        "baseline_outcome": "deterministic_reuse",
                        "acceptance_status": "accepted", "evidence_block_ids": [block_id],
                        "pinned_model": "jev-model", "question_version": "v1",
                        "acceptance_policy_version": "v1", "option_mapping": {},
                        "result": {"outcome": "available", "response": {"answers": {}}},
                    },
                },
            }]}

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
                path = f"/v1/projects/p1/knowledge/context-blocks/{block_id}/jev-decisions"
                normal = await client.get(path)
                assert normal.status_code == 200
                assert normal.json()["items"][0]["operational_result"] == "Research"
                assert normal.json()["items"][1]["operational_result"] is None
                assert normal.json()["items"][1]["operational_result_recorded"] is False
                entity = await client.get("/v1/projects/p1/knowledge/entities/3/jev-decisions")
                assert entity.status_code == 200
                assert entity.json()["items"][0]["operational_result"] == "Research"
                assert "advanced" not in normal.text and "private" not in normal.text
                advanced = await client.get(path, params={"advanced": "true"})
                assert advanced.status_code == 200
                assert advanced.json()["items"][0]["advanced"]["window_id"] == window_id
                assert "private" not in advanced.text
                assert (await client.get(path, params={"advanced": "yes"})).status_code == 422
                assert (await client.get(path.replace("/p1/", "/p2/"))).status_code == 404

    asyncio.run(exercise())
