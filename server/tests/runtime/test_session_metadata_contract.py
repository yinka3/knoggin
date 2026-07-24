import pytest

from core.session.session_manager import SessionManager


class RecordingPostgres:
    def __init__(self):
        self.calls = []

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
        active_sessions={},
        project_manager=None,
    )

    updated = await manager.update_session_metadata(
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
        active_sessions={},
        project_manager=None,
    )

    with pytest.raises(ValueError, match="does not allow"):
        await manager.update_session_metadata("session-1", payload)

    assert resources.postgres.calls == []
