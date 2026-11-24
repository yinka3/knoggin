import pytest

from common.scoping import IDENTITY_ENTITY_ID
from core.knowledge.db.readers.entity_reader import EntityReader


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def execute(self, query, params):
        self.calls.append((query, params))

    async def fetchall(self):
        return self.rows


@pytest.mark.unit
@pytest.mark.no_network
async def test_checks_every_id_in_one_query_on_the_callers_cursor():
    cur = FakeCursor([{"entity_id": 3}, {"entity_id": 9}])

    found = await EntityReader.validate_existing_ids(
        cur, {9: ["a"], 3: ["b"], 12: ["c"]}, user_name="ada", visible_project_ids=["p1"],
    )

    assert found == {3, 9}
    [(query, params)] = cur.calls
    assert params == ([3, 9, 12], "ada", IDENTITY_ENTITY_ID, "ada", ["p1"])
    assert "entity.user_name = %s" in query and "status = 'active'" in query


@pytest.mark.unit
@pytest.mark.no_network
async def test_no_ids_needs_no_query():
    cur = FakeCursor([])

    assert await EntityReader.validate_existing_ids(
        cur, [], user_name="ada", visible_project_ids=["p1"],
    ) == set()
    assert cur.calls == []
