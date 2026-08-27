from datetime import datetime, timezone

import pytest

from core.session.session_manager import SessionManager


class RecordingPostgres:
    def __init__(self, fetch_results=None):
        self.calls = []
        self.fetch_results = list(fetch_results or [])

    async def fetch_all(self, query, params=None):
        self.calls.append((query, params))
        return self.fetch_results.pop(0) if self.fetch_results else []

    async def execute(self, query, params=None):
        self.calls.append((query, params))
        return 1


class Resources:
    def __init__(self):
        self.postgres = RecordingPostgres()


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_metadata_update_allows_only_configuration_columns():
    resources = Resources()
    manager = SessionManager(
        resources=resources,
        user_name="ada",
        project_manager=None,
    )

    updated = await manager.update_session(
        "session-1",
        {"model": "gpt-5", "enabled_tools": ["search_documents"]},
    )

    assert updated == {"model": "gpt-5", "enabled_tools": ["search_documents"]}
    assert len(resources.postgres.calls) == 1


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize("payload", [{"status": "deleted"}, {"project_id": "other"}])
async def test_session_metadata_update_rejects_lifecycle_and_ownership_columns(payload):
    resources = Resources()
    manager = SessionManager(
        resources=resources,
        user_name="ada",
        project_manager=None,
    )

    with pytest.raises(ValueError, match="does not allow"):
        await manager.update_session("session-1", payload)

    assert resources.postgres.calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_list_sessions_preserves_native_timestamps_and_normalizes_json_fields():
    created_at = datetime(2026, 8, 14, 12, tzinfo=timezone.utc)
    last_active_at = datetime(2026, 8, 14, 13, tzinfo=timezone.utc)
    resources = Resources()
    resources.postgres = RecordingPostgres(
        [
            [
                {
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "model": "gpt-5",
                    "agent_id": None,
                    "enabled_tools": '["search_documents"]',
                    "document_focus": '{"document_ids": ["document-1"]}',
                    "status": "active",
                    "created_at": created_at,
                    "last_active_at": last_active_at,
                }
            ]
        ]
    )
    manager = SessionManager(
        resources=resources,
        user_name="ada",
        project_manager=None,
    )

    sessions = await manager.list_sessions()

    assert sessions["session-1"]["created_at"] is created_at
    assert sessions["session-1"]["last_active_at"] is last_active_at
    assert sessions["session-1"]["enabled_tools"] == ["search_documents"]
    assert sessions["session-1"]["document_focus"] == {"document_ids": ["document-1"]}
