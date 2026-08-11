import asyncio

import pytest

from common.utils.diagnostic_context import (
    diagnostic_scope,
    format_diagnostic_scope,
    get_diagnostic_scope,
    inject_diagnostic_scope,
)


def test_diagnostic_scope_is_nested_and_restored_after_exit():
    assert get_diagnostic_scope() == {}

    with diagnostic_scope(user_name="ada", project_id="project-1"):
        assert get_diagnostic_scope() == {"user": "ada", "project": "project-1"}

        with diagnostic_scope(session_id="session-1", work_id="work-1"):
            assert get_diagnostic_scope() == {
                "user": "ada",
                "project": "project-1",
                "session": "session-1",
                "work": "work-1",
            }

        assert get_diagnostic_scope() == {"user": "ada", "project": "project-1"}

    assert get_diagnostic_scope() == {}


@pytest.mark.no_network
async def test_diagnostic_scope_is_isolated_between_async_tasks():
    async def read_scope():
        return get_diagnostic_scope()

    with diagnostic_scope(project_id="project-1"):
        task = asyncio.create_task(read_scope())
        with diagnostic_scope(project_id="project-2"):
            assert get_diagnostic_scope()["project"] == "project-2"

        assert await task == {"project": "project-1"}


def test_loguru_patcher_adds_scope_without_overwriting_bound_value():
    record = {"extra": {}}
    with diagnostic_scope(project_id="project-1", session_id="session-1"):
        inject_diagnostic_scope(record)

    assert record["extra"]["diagnostic_scope"] == "project=project-1 session=session-1"
    assert format_diagnostic_scope({}) == "-"

    record = {"extra": {"diagnostic_scope": "explicit"}}
    inject_diagnostic_scope(record)
    assert record["extra"]["diagnostic_scope"] == "explicit"
