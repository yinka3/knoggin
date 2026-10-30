import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx

from app import create_app
from common.exceptions import SessionBusyError
from common.schema.primitives import ImportedExchange
from common.schema.settings import IngestionSettings

_URL = "/v1/sessions/session-1/imports"
_EXCHANGE = {
    "user_content": "I moved to Lisbon.",
    "user_timestamp": "2023-05-20T02:21:00",
    "assistant_content": "Congrats!",
    "assistant_timestamp": "2023-05-20T02:21:30Z",
}


def test_import_passes_validated_exchanges_and_returns_their_ids(monkeypatch, tmp_path):
    imported = []

    class Session:
        busy = False

        async def import_exchanges(self, exchanges):
            if self.busy:
                raise SessionBusyError()
            imported.append(exchanges)
            return [{"user_message_id": 1, "assistant_message_id": 2, "created": True}]

    session = Session()

    class Sessions:
        async def list_sessions(self):
            return {"session-1": {"session_id": "session-1", "project_id": "project-1"}}

        async def get_or_resume_session(self, session_id):
            return session

    class Projects:
        status = "active"

        async def get_project(self, project_id):
            return {"status": self.status}

    projects = Projects()
    settings = SimpleNamespace(ingestion=IngestionSettings())

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(
            sessions=Sessions(), projects=projects, shutdown=shutdown,
            config_manager=SimpleNamespace(
                config=SimpleNamespace(developer_settings=settings)
            ),
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                result = await client.post(_URL, json={"exchanges": [_EXCHANGE]})
                assert result.status_code == 200
                assert result.json() == {"exchanges": [
                    {"user_message_id": 1, "assistant_message_id": 2, "created": True}
                ]}
                [[exchange]] = imported
                assert isinstance(exchange, ImportedExchange)
                # A time without a zone is read as UTC.
                assert exchange.user_timestamp == datetime(
                    2023, 5, 20, 2, 21, tzinfo=timezone.utc
                )

                backwards = {**_EXCHANGE, "assistant_timestamp": "2023-05-20T02:00:00Z"}
                for body in (
                    {"exchanges": []},
                    {"exchanges": [backwards]},
                    {"exchanges": [{**_EXCHANGE, "role": "user"}]},
                    {"exchanges": [{**_EXCHANGE, "user_content": "  "}]},
                ):
                    rejected = await client.post(_URL, json=body)
                    assert rejected.status_code == 422
                    assert rejected.json()["error"]["code"] == "invalid_import"

                settings.ingestion = IngestionSettings(max_import_exchanges=1)
                oversized = await client.post(
                    _URL, json={"exchanges": [_EXCHANGE, _EXCHANGE]}
                )
                assert oversized.status_code == 413
                assert oversized.json()["error"]["code"] == "batch_too_large"

                session.busy = True
                busy = await client.post(_URL, json={"exchanges": [_EXCHANGE]})
                assert busy.json()["error"]["code"] == "session_busy"

                projects.status = "archived"
                archived = await client.post(_URL, json={"exchanges": [_EXCHANGE]})
                assert archived.json()["error"]["code"] == "project_unavailable"

                missing = await client.post(
                    "/v1/sessions/other/imports", json={"exchanges": [_EXCHANGE]}
                )
                assert missing.status_code == 404
                assert len(imported) == 1

    asyncio.run(exercise())
