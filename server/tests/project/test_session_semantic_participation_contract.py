from contextlib import asynccontextmanager

import pytest

from core.project.project_manager import ProjectManager


class _Cursor:
    def __init__(self, rows, project_status="active"):
        self.calls = []
        self.rows = rows
        self.project_status = project_status

    async def execute(self, query, params):
        self.calls.append((query, params))

    async def fetchall(self):
        return self.rows

    async def fetchone(self):
        return None if self.project_status is None else {"status": self.project_status}


class _Postgres:
    def __init__(self, *, locked_rows, listed_rows, project_status="active"):
        self.cursor = _Cursor(locked_rows, project_status)
        self.listed_rows = listed_rows

    @asynccontextmanager
    async def transaction(self):
        yield self.cursor

    async def fetch_all(self, query, params):
        return self.listed_rows


def _manager(*, locked_rows, listed_rows, project_status="active"):
    manager = object.__new__(ProjectManager)
    manager.user_name = "user"
    manager.pg = _Postgres(locked_rows=locked_rows, listed_rows=listed_rows, project_status=project_status)
    manager.active_projects = {}
    return manager


async def test_participation_changes_set_a_future_message_boundary():
    manager = _manager(
        locked_rows=[
            {"session_id": "session-a", "semantic_participation_enabled": True},
            {"session_id": "session-b", "semantic_participation_enabled": False},
        ],
        listed_rows=[
            {
                "session_id": "session-a",
                "semantic_participation_enabled": False,
                "semantic_participation_after_message_id": 12,
            },
            {
                "session_id": "session-b",
                "semantic_participation_enabled": True,
                "semantic_participation_after_message_id": 18,
            },
        ],
    )

    participation = await manager.set_session_semantic_participation(
        "project", ["session-b"]
    )

    updates = [call for call in manager.pg.cursor.calls if "UPDATE public.sessions" in call[0]]
    assert len(updates) == 2
    assert updates[0][1][0] is False
    assert updates[1][1][0] is True
    assert participation == [
        {
            "session_id": "session-a",
            "semantic_participation_enabled": False,
            "semantic_participation_after_message_id": 12,
        },
        {
            "session_id": "session-b",
            "semantic_participation_enabled": True,
            "semantic_participation_after_message_id": 18,
        },
    ]


async def test_unchanged_participation_does_not_move_the_message_boundary():
    rows = [
        {"session_id": "session-a", "semantic_participation_enabled": False},
        {"session_id": "session-b", "semantic_participation_enabled": True},
    ]
    manager = _manager(locked_rows=rows, listed_rows=[])

    await manager.set_session_semantic_participation("project", ["session-b"])

    assert not any(
        "UPDATE public.sessions" in query for query, _ in manager.pg.cursor.calls
    )


@pytest.mark.parametrize("unavailable_session_id", ["missing", "other-project", "deleted"])
async def test_participation_rejects_sessions_unavailable_to_the_project(
    unavailable_session_id,
):
    manager = _manager(
        locked_rows=[
            {"session_id": "session-a", "semantic_participation_enabled": True}
        ],
        listed_rows=[],
    )

    with pytest.raises(ValueError, match="Semantic participation"):
        await manager.set_session_semantic_participation(
            "project", [unavailable_session_id]
        )

    queries = [query for query, _ in manager.pg.cursor.calls]
    assert "project_id = %s" in queries[0]
    assert "status <> 'deleted'" in queries[1]
    assert not any("UPDATE public.sessions" in query for query in queries)


@pytest.mark.parametrize("status", ["archived", "deleted", None])
async def test_participation_refuses_inactive_project_before_session_changes(status):
    manager = _manager(locked_rows=[], listed_rows=[], project_status=status)
    with pytest.raises(ValueError, match="active project"):
        await manager.set_session_semantic_participation("project", [])
    assert len(manager.pg.cursor.calls) == 1
