from contextlib import asynccontextmanager

import pytest

from core.knowledge.db.writers.graph_writer import GraphWriter


class Cursor:
    def __init__(self, user_names):
        self.user_names = user_names
        self.statements = []
        self._rows = []

    async def execute(self, query, params=None):
        query = " ".join(query.split())
        self.statements.append((query, params))
        if query.startswith("SELECT entity_id, user_name, canonical_name"):
            self._rows = [{"entity_id": 1, "user_name": "ada", "canonical_name": "ada"}]
        elif query.startswith("SELECT DISTINCT name FROM public.entity_name_supports"):
            self._rows = [{"name": name} for name in self.user_names]
        else:
            self._rows = []

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return self._rows


class Client:
    def __init__(self, cursor):
        self.cursor = cursor

    @asynccontextmanager
    async def transaction(self):
        yield self.cursor


@pytest.mark.unit
@pytest.mark.no_network
async def test_startup_removes_only_names_settings_dropped():
    cursor = Cursor(user_names=["ada", "Ada Lovelace", "A. Lovelace"])

    await GraphWriter(Client(cursor)).ensure_identity_entity("ada", ["Ada Lovelace"])

    deletes = [(query, params) for query, params in cursor.statements if query.startswith("DELETE")]
    assert ("DELETE FROM entity_aliases WHERE entity_id = %s AND alias = ANY(%s)",
            (1, ["A. Lovelace"])) in deletes
    # Nothing wipes every alias, or every name outside settings, any more.
    assert not any(query == "DELETE FROM entity_aliases WHERE entity_id = %s" for query, _ in deletes)
    assert not any("name <> ALL" in query for query, _ in deletes)


@pytest.mark.unit
@pytest.mark.no_network
async def test_unchanged_settings_delete_no_aliases():
    cursor = Cursor(user_names=["ada", "Ada Lovelace"])

    await GraphWriter(Client(cursor)).ensure_identity_entity("ada", ["Ada Lovelace"])

    assert not any(
        query.startswith("DELETE FROM entity_aliases") for query, _ in cursor.statements
    )
