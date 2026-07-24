import pytest

from core.session.session_manager import SessionManager


class Resources:
    def __init__(self, postgres):
        self.postgres = postgres


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_session_metadata_update_persists_an_allowed_configuration_change(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('metadata-session', 'ada', 'project-1')
        """
    )
    manager = SessionManager(
        resources=Resources(real_postgres_client),
        user_name="ada",
        active_sessions={},
        project_manager=None,
    )

    await manager.update_session_metadata(
        "metadata-session",
        {"model": "gpt-5", "enabled_tools": ["search_documents"]},
    )

    assert await real_postgres_client.fetch_one(
        """
        SELECT model, enabled_tools
        FROM sessions
        WHERE session_id = 'metadata-session'
        """
    ) == {"model": "gpt-5", "enabled_tools": ["search_documents"]}
