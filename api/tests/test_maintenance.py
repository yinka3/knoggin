import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

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

        async def flag_jev_entity_context_decision(self, project_id, **kwargs):
            calls.append(("flag_jev", project_id, kwargs))
            return review

        async def reopen_jev_entity_context_correction(self, project_id, entity_id, **kwargs):
            calls.append(("reopen_jev", project_id, entity_id, kwargs))
            return {"entity_id": entity_id, "human_correction_active": False}

        async def flag_jev_identity_decision(self, project_id, **kwargs):
            calls.append(("flag_identity", project_id, kwargs))
            return review

        async def apply_jev_identity_review(self, project_id, review_id, **kwargs):
            calls.append(("apply_identity", project_id, review_id, kwargs))
            return {"review": review, "target_entity_id": 3}

        async def reopen_jev_identity_correction(self, project_id, review_id, **kwargs):
            calls.append(("reopen_identity", project_id, review_id, kwargs))
            return {"review_id": review_id, "target_entity_id": 3,
                    "human_correction_active": False}

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
                flag_url = "/v1/projects/project-1/maintenance/jev-context-corrections"
                assert (await client.post(flag_url, json={
                    "capability": "classification", "window_id": str(uuid4()),
                    "pass_number": 1, "occurrence_key": "0:topic", "entity_id": 2,
                    "reasoning": "Check this",
                })).status_code == 422
                flagged = await client.post(flag_url, json={
                    "capability": "classification", "window_id": str(uuid4()),
                    "pass_number": 1, "occurrence_key": "0:topic", "entity_id": 2,
                    "topic": "Identity", "reasoning": "Check this",
                })
                assert flagged.status_code == 201
                assert calls[-1][0] == "flag_jev"
                reopened = await client.post(
                    flag_url + "/2/reopen", json={"expected_review_id": "review-1"},
                )
                assert reopened.status_code == 200
                assert calls[-1] == (
                    "reopen_jev", "project-1", 2,
                    {"expected_review_id": "review-1"},
                )
                identity_url = "/v1/projects/project-1/maintenance/jev-identity-corrections"
                assert (await client.post(identity_url, json={
                    "window_id": str(uuid4()), "pass_number": 1,
                    "occurrence_key": "0:mention", "source_entity_id": 2,
                    "reasoning": "Wrong identity",
                })).status_code == 422
                flagged_identity = await client.post(identity_url, json={
                    "window_id": str(uuid4()), "pass_number": 1,
                    "occurrence_key": "0:mention", "source_entity_id": 2,
                    "target_entity_id": 3, "reasoning": "Wrong identity",
                })
                assert flagged_identity.status_code == 201
                assert calls[-1][0] == "flag_identity"
                applied_identity = await client.post(
                    identity_url + "/review-1/apply",
                    json={"expected_state": {"revision": 3}},
                )
                assert applied_identity.status_code == 200
                assert applied_identity.json()["target_entity_id"] == 3
                assert calls[-1] == (
                    "apply_identity", "project-1", "review-1",
                    {"expected_state": {"revision": 3}},
                )
                reopened_identity = await client.post(
                    identity_url + "/review-1/reopen",
                    json={"expected_target_entity_id": 3},
                )
                assert reopened_identity.status_code == 200
                assert reopened_identity.json()["human_correction_active"] is False
                assert calls[-1] == (
                    "reopen_identity", "project-1", "review-1",
                    {"expected_target_entity_id": 3},
                )

    asyncio.run(exercise())


def test_global_merge_preview_and_proposal_use_review_owner(monkeypatch, tmp_path):
    calls = []
    state_hash = "a" * 64
    review = SimpleNamespace(
        review_id="merge-review", scope="user-global", project_id=None,
        kind="entity_merge", reasoning="Same identity", proposed_plan={"kind": "entity_merge"},
        expected_state={"state_hash": state_hash}, status="open",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), resolved_at=None,
    )

    class EntityMaintenance:
        async def preview_merge(self, **kwargs):
            calls.append(("preview", kwargs))
            return {
                "plan": {"kind": "entity_merge"}, "affected_project_ids": ["project-1"],
                "context_conflicts": [], "ready": True, "state_hash": state_hash,
                "snapshot": {"private": "must not cross HTTP"},
            }

        async def propose(self, **kwargs):
            calls.append(("propose", kwargs))
            return review

    projects = SimpleNamespace(entity_maintenance_service=EntityMaintenance())

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
                candidates = {"survivor_entity_id": 2, "retired_entity_id": 3}
                preview = await client.post("/v1/maintenance/merges/preview", json=candidates)
                assert preview.status_code == 200
                assert preview.json()["ready"] is True
                assert "snapshot" not in preview.json()
                assert calls[-1][0] == "preview"
                proposal = await client.post("/v1/maintenance/merges", json={
                    **candidates, "reasoning": "Same identity", "expected_state_hash": state_hash,
                    "evidence_refs": [{"kind": "message", "identifier": "7"}],
                })
                assert proposal.status_code == 201
                assert proposal.json()["review"]["review_id"] == "merge-review"
                assert calls[-1][0] == "propose"
                assert (await client.post("/v1/maintenance/merges", json={
                    **candidates, "reasoning": "Same identity", "expected_state_hash": "bad",
                    "evidence_refs": [{"kind": "message", "identifier": "7"}],
                })).status_code == 422

    asyncio.run(exercise())
