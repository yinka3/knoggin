import json
from datetime import datetime, timezone

import pytest

from core.knowledge.db.readers.fact_audit_reader import FactAuditReader
from tests.fixtures.fakes import RecordingPostgresClient


def audit_row(**overrides):
    row = {
        "fact_change_id": "change-1",
        "user_name": "ada",
        "project_id": "project-1",
        "entity_id": 2,
        "source_msg_ids": "[11]",
        "invalidated_fact_ids": '["fact-old"]',
        "invalidated_fact_snapshots": "[]",
        "created_fact_ids": '["fact-new"]',
        "metadata": json.dumps({"source": "test"}),
        "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_audit_reader_gets_scoped_audit_and_parses_json():
    client = RecordingPostgresClient(fetch_all_results=[[audit_row()]])
    reader = FactAuditReader(client)

    audit = await reader.get_fact_change_audit(
        "change-1",
        user_name="ada",
        project_id="project-1",
    )

    assert audit["source_msg_ids"] == [11]
    assert audit["invalidated_fact_ids"] == ["fact-old"]
    assert audit["created_fact_ids"] == ["fact-new"]
    assert audit["metadata"] == {"source": "test"}
    assert client.calls[0][2] == ("change-1", "ada", "project-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_audit_reader_lists_entity_and_project_history_newest_first():
    client = RecordingPostgresClient(
        fetch_all_results=[[audit_row()], [audit_row(fact_change_id="change-2")]]
    )
    reader = FactAuditReader(client)

    entity_history = await reader.list_fact_change_audits_for_entity(
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        limit=10,
        offset=5,
    )
    project_history = await reader.list_fact_change_audits_for_project(
        user_name="ada",
        project_id="project-1",
    )

    assert entity_history[0]["fact_change_id"] == "change-1"
    assert project_history[0]["fact_change_id"] == "change-2"
    assert "ORDER BY created_at DESC" in client.calls[0][1]
    assert client.calls[0][2] == ("ada", "project-1", 2, 10, 5)
    assert "ORDER BY created_at DESC" in client.calls[1][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_audit_reader_rejects_invalid_scope_and_pagination():
    reader = FactAuditReader(RecordingPostgresClient())

    with pytest.raises(ValueError, match="requires user_name scope"):
        await reader.list_fact_change_audits_for_project(
            user_name="",
            project_id="project-1",
        )
    with pytest.raises(ValueError, match="positive limit"):
        await reader.list_fact_change_audits_for_entity(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            limit=0,
        )
