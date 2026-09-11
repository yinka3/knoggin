from types import SimpleNamespace

import pytest

from core.project.project_manager import ProjectManager


def _manager(client) -> ProjectManager:
    return ProjectManager(SimpleNamespace(postgres=client), user_name="ada")


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_session_semantic_participation_tracks_boundaries_and_scope(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (
            session_id, user_name, project_id, status
        ) VALUES
            ('session-a', 'ada', 'project-1', 'open'),
            ('session-b', 'ada', 'project-1', 'open'),
            ('session-other-project', 'ada', 'project-2', 'open'),
            ('session-deleted', 'ada', 'project-1', 'deleted')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content
        ) VALUES
            ('ada', 'session-a', 101, 'project-1', 'user', 'Excluded.'),
            ('ada', 'session-a', 102, 'project-1', 'assistant', 'Acknowledged.')
        """
    )
    manager = _manager(real_postgres_client)

    participation = await manager.set_session_semantic_participation(
        "project-1", ["session-b"]
    )

    assert {row["session_id"] for row in participation} == {
        "session-a",
        "session-b",
    }
    assert await real_postgres_client.fetch_all(
        """
        SELECT session_id, semantic_participation_enabled,
               semantic_participation_after_message_id
        FROM public.sessions
        WHERE session_id IN ('session-a', 'session-b')
        ORDER BY session_id
        """
    ) == [
        {
            "session_id": "session-a",
            "semantic_participation_enabled": False,
            "semantic_participation_after_message_id": 102,
        },
        {
            "session_id": "session-b",
            "semantic_participation_enabled": True,
            "semantic_participation_after_message_id": 0,
        },
    ]

    await manager.set_session_semantic_participation(
        "project-1", ["session-a", "session-b"]
    )
    assert await real_postgres_client.fetch_one(
        """
        SELECT semantic_participation_enabled,
               semantic_participation_after_message_id
        FROM public.sessions
        WHERE session_id = 'session-a'
        """
    ) == {
        "semantic_participation_enabled": True,
        "semantic_participation_after_message_id": 102,
    }

    await manager.set_session_semantic_participation(
        "project-1", ["session-a", "session-b"]
    )
    assert await real_postgres_client.fetch_one(
        """
        SELECT semantic_participation_after_message_id
        FROM public.sessions
        WHERE session_id = 'session-a'
        """
    ) == {"semantic_participation_after_message_id": 102}

    for unavailable_session_id in (
        "missing-session",
        "session-other-project",
        "session-deleted",
    ):
        with pytest.raises(ValueError, match="Semantic participation"):
            await manager.set_session_semantic_participation(
                "project-1", [unavailable_session_id]
            )
