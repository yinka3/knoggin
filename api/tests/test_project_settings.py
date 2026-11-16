import asyncio
from types import SimpleNamespace

import httpx

from app import create_app
from common.conf.domain_config import DomainConfig
from common.schema.document import FolderScanSettings
from core.project.domain_config_operations import (
    preview_domain_config,
    validate_domain_config,
)
from core.project.domain_config_store import DomainConfigConflict


def test_project_domain_routes_enforce_version_and_project_status(
    monkeypatch, tmp_path
):
    original = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {}},
            "entity_types": {"Project": {"topic": "Work", "labels": ["project"]}},
        }
    )
    candidate = original.to_dict()
    candidate["topics"]["New"] = {}
    state = SimpleNamespace(status="active", domain=original)

    class Projects:
        async def get_project(self, project_id):
            if project_id != "project-1":
                return None
            return {"status": state.status}

        async def get_domain_config(self, _project_id):
            return state.domain

        def validate_domain_config(self, value):
            return validate_domain_config(value)

        async def preview_domain_config(self, _project_id, value):
            return preview_domain_config(state.domain, value)

        async def activate_domain_config(self, _project_id, value, *, expected_version):
            if expected_version != state.domain.version:
                raise DomainConfigConflict(expected_version, state.domain.version)
            previous = state.domain.version
            state.domain = DomainConfig.from_mapping({**value, "version": previous + 1})
            return SimpleNamespace(previous_version=previous, config=state.domain)

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(projects=Projects(), shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                base = "/v1/projects/project-1/domain"
                assert (await client.get(base)).json()["version"] == 1
                invalid = await client.post(base + "/validate", json={})
                assert invalid.status_code == 200
                assert invalid.json()["valid"] is False
                preview = await client.post(base + "/preview", json=candidate)
                assert preview.json()["topics_added"] == ["New"]
                assert preview.json()["next_version"] == 2

                stale = await client.post(
                    base + "/activate",
                    json={"expected_version": 0, "config": candidate},
                )
                assert stale.status_code == 409
                assert stale.json()["error"]["actual_version"] == 1
                activated = await client.post(
                    base + "/activate",
                    json={"expected_version": 1, "config": candidate},
                )
                assert activated.status_code == 200
                assert activated.json()["config"]["version"] == 2

                state.status = "archived"
                assert (await client.get(base)).status_code == 200
                assert (
                    await client.post(
                        base + "/activate",
                        json={"expected_version": 2, "config": candidate},
                    )
                ).status_code == 409
                state.status = "deleted"
                assert (await client.get(base)).status_code == 404

    asyncio.run(exercise())


def test_scan_settings_routes_use_project_owner_without_runtime_lease(
    monkeypatch, tmp_path
):
    state = SimpleNamespace(status="active", settings=FolderScanSettings())

    class Projects:
        async def get_project(self, project_id):
            return {"status": state.status} if project_id == "project-1" else None

        async def get_document_scan_settings(self, _project_id):
            return state.settings

        async def save_document_scan_settings(self, _project_id, settings):
            state.settings = settings
            return settings

        async def reset_document_scan_settings(self, _project_id):
            state.settings = FolderScanSettings()
            return state.settings

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(projects=Projects(), shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                url = "/v1/projects/project-1/scan-settings"
                changed = await client.put(url, json={"max_file_count": 12})
                assert changed.status_code == 200
                assert changed.json()["max_file_count"] == 12
                assert (await client.get(url)).json()["max_file_count"] == 12
                assert (await client.put(url, json={"unknown": 1})).status_code == 422
                assert (await client.delete(url)).json()["max_file_count"] == 1000

                state.status = "archived"
                assert (await client.get(url)).status_code == 409

    asyncio.run(exercise())
