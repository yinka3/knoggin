import asyncio
from types import SimpleNamespace
from uuid import UUID

import httpx

from app import create_app
from core.knowledge.context.models import ContextRevisionConflictError


def test_context_notes_read_archived_and_write_with_expected_revision(monkeypatch, tmp_path):
    revision_id = UUID(int=1)
    note_id = UUID(int=2)
    calls = []
    leases = set()

    class Store:
        async def get_current_project_context_revision(self, **scope):
            assert scope == {"user_name": "ada", "project_id": "p1"}
            return SimpleNamespace(revision_id=revision_id)

        async def get_project_context_snapshot(self, requested_id, **scope):
            assert requested_id == str(revision_id)
            return SimpleNamespace(blocks=[SimpleNamespace(
                block_id=note_id, section_key="preferences",
                markdown="Keep sources", assertion_kind="human_asserted",
            ), SimpleNamespace(
                block_id=UUID(int=3), section_key="current_state",
                markdown="Internal", assertion_kind="agent_derived",
            )])

    class Processor:
        async def change_user_note(self, **change):
            assert leases
            calls.append(change)
            if change["expected_revision_id"] != revision_id:
                raise ContextRevisionConflictError("stale")

    class Projects:
        status = "archived"

        async def get_project(self, project_id):
            return {"status": self.status} if project_id == "p1" else None

        async def acquire_project_for_session(self, project_id, lease_id):
            leases.add(lease_id)
            return SimpleNamespace(
                project_semantic_processor=Processor(),
                signal_semantic_work=lambda: True,
            )

        async def release_project_for_session(self, project_id, lease_id):
            leases.remove(lease_id)

    projects = Projects()

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(
            projects=projects, resources=SimpleNamespace(knowledge_store=Store()),
            sessions=SimpleNamespace(user_name="ada"), shutdown=shutdown,
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                url = "/v1/projects/p1/context/user-notes"
                read = await client.get(url)
                assert read.status_code == 200
                assert read.json() == {"revision_id": str(revision_id), "notes": [{
                    "block_id": str(note_id), "section_key": "preferences",
                    "markdown": "Keep sources",
                }]}
                blocked = await client.post(url, json={
                    "expected_revision_id": str(revision_id),
                    "section_key": "preferences", "markdown": "New note",
                })
                assert blocked.status_code == 409
                projects.status = "active"
                stale = await client.post(url, json={
                    "expected_revision_id": str(UUID(int=9)),
                    "section_key": "preferences", "markdown": "Stale note",
                })
                assert stale.status_code == 409
                created = await client.post(url, json={
                    "expected_revision_id": str(revision_id),
                    "section_key": "preferences", "markdown": "New note",
                })
                assert created.status_code == 201
                assert not leases

    asyncio.run(exercise())
    assert calls[-1]["operation"] == "add"
    assert calls[-1]["expected_revision_id"] == revision_id


def test_context_local_edits_can_be_reviewed_and_discarded(monkeypatch, tmp_path):
    from core.knowledge.context.models import ContextProjectionConflictError

    local_hash = "a" * 64
    discards = []

    class Processor:
        def read_context_file(self):
            return "my stale edit", local_hash

        async def discard_context_edits(self, **request):
            discards.append(request)
            if request["expected_file_hash"] != local_hash:
                raise ContextProjectionConflictError("changed")
            return "my stale edit"

    class Projects:
        async def get_project(self, project_id):
            return {"status": "active"} if project_id == "p1" else None

        async def acquire_project_for_session(self, project_id, lease_id):
            return SimpleNamespace(
                project_semantic_processor=Processor(),
                signal_semantic_work=lambda: True,
            )

        async def release_project_for_session(self, project_id, lease_id):
            pass

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(
            projects=Projects(), resources=SimpleNamespace(knowledge_store=None),
            sessions=SimpleNamespace(user_name="ada"), shutdown=shutdown,
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                local = await client.get("/v1/projects/p1/context/local-file")
                assert local.json() == {"content": "my stale edit", "content_hash": local_hash}
                url = "/v1/projects/p1/context/discard-local-edits"
                stale = await client.post(url, json={"expected_file_hash": "b" * 64})
                assert stale.status_code == 409
                discarded = await client.post(url, json={"expected_file_hash": local_hash})
                assert discarded.status_code == 200
                assert discarded.json() == {"discarded_content": "my stale edit"}

    asyncio.run(exercise())
    assert discards[-1] == {
        "user_name": "ada", "project_id": "p1", "expected_file_hash": local_hash,
    }
