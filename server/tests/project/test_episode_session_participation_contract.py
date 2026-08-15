from contextlib import asynccontextmanager

from core.project.project_manager import ProjectManager


class _Cursor:
    def __init__(self):
        self.calls = []

    async def execute(self, query, params):
        self.calls.append((query, params))

    async def fetchall(self):
        return [
            {"session_id": "session-a", "episode_participation_enabled": True},
            {"session_id": "session-b", "episode_participation_enabled": False},
        ]


class _Postgres:
    def __init__(self):
        self.cursor = _Cursor()

    @asynccontextmanager
    async def transaction(self):
        yield self.cursor

    async def fetch_all(self, query, params):
        return [
            {
                "session_id": "session-a",
                "episode_participation_enabled": False,
                "episode_participation_after_message_id": 12,
            },
            {
                "session_id": "session-b",
                "episode_participation_enabled": True,
                "episode_participation_after_message_id": 18,
            },
        ]


async def test_participation_changes_set_a_future_message_boundary():
    manager = object.__new__(ProjectManager)
    manager.user_name = "user"
    manager.pg = _Postgres()
    manager.active_projects = {}

    participation = await manager.set_episode_participating_sessions(
        "project", ["session-b"]
    )

    updates = [call for call in manager.pg.cursor.calls if "UPDATE public.sessions" in call[0]]
    assert len(updates) == 2
    assert updates[0][1][0] is False
    assert updates[1][1][0] is True
    assert participation == [
        {"session_id": "session-a", "enabled": False, "after_message_id": 12},
        {"session_id": "session-b", "enabled": True, "after_message_id": 18},
    ]
