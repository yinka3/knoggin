import json
from datetime import datetime, timezone

import pytest

from common.schema.primitives import FactRecord
from core.knowledge.db.writers.fact_writer import FactWriter
from tests.fixtures.fakes import RecordingPostgresClient


def fact_row(fact_id="fact-1"):
    return {
        "fact_id": fact_id,
        "entity_id": 2,
        "user_name": "ada",
        "project_id": "project-1",
        "content": "Ada uses Notion.",
        "valid_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "invalid_at": None,
        "confidence": 1.0,
        "source_msg_id": 11,
        "source_user_name": "ada",
        "source_session_id": "session-1",
        "source": "user",
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_remove_fact_with_audit_updates_all_fact_surfaces_transactionally():
    client = RecordingPostgresClient(fetch_one_results=[fact_row()])
    writer = FactWriter(client)

    result = await writer.remove_fact_with_audit(
        fact_change_id="change-1",
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        fact_id="fact-1",
        actor="ada",
        change_type="manual_remove",
        reason="not_true",
        session_id="session-1",
    )

    assert result == {
        "fact_change_id": "change-1",
        "entity_id": 2,
        "invalidated_fact_ids": ["fact-1"],
        "created_fact_ids": [],
    }
    assert client.transaction_enters == 1
    executed_sql = "\n".join(call[1] for call in client.calls)
    assert "FOR UPDATE" in executed_sql
    assert "INSERT INTO fact_change_audits" in executed_sql
    assert "UPDATE facts" in executed_sql
    assert "UPDATE fact_search" in executed_sql
    assert "SET f.invalid_at = $invalid_at" in executed_sql
    assert "SET status = 'applied'" in executed_sql

    audit_call = next(
        call for call in client.calls if "INSERT INTO fact_change_audits" in call[1]
    )
    assert audit_call[2][0] == "change-1"
    assert audit_call[2][8] == "[11]"
    assert audit_call[2][9] == '["fact-1"]'
    snapshot = json.loads(audit_call[2][10])[0]
    assert snapshot["fact_id"] == "fact-1"
    assert snapshot["content"] == "Ada uses Notion."


@pytest.mark.storage
@pytest.mark.no_network
async def test_remove_fact_with_audit_rejects_missing_scoped_fact():
    client = RecordingPostgresClient(fetch_one_results=[None])
    writer = FactWriter(client)

    with pytest.raises(ValueError, match="No active scoped fact"):
        await writer.remove_fact_with_audit(
            fact_change_id="change-1",
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_id="missing",
            actor="ada",
            change_type="manual_remove",
            reason="not_true",
        )

    assert client.transaction_enters == 1
    assert not any("INSERT INTO fact_change_audits" in call[1] for call in client.calls)


@pytest.mark.storage
@pytest.mark.no_network
async def test_replace_facts_with_audit_rejects_duplicate_fact_ids_before_transaction():
    client = RecordingPostgresClient()
    writer = FactWriter(client)

    with pytest.raises(ValueError, match="duplicate fact_ids"):
        await writer.replace_facts_with_audit(
            fact_change_id="change-1",
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_ids=["fact-1", "fact-1"],
            actor="ada",
            change_type="manual_correction",
            reason="wrong_tool",
        )

    assert client.transaction_enters == 0


@pytest.mark.storage
@pytest.mark.no_network
async def test_replace_facts_with_audit_rejects_missing_selected_facts():
    client = RecordingPostgresClient(fetch_all_results=[[fact_row("fact-1")]])
    writer = FactWriter(client)

    with pytest.raises(ValueError, match="Missing active scoped facts"):
        await writer.replace_facts_with_audit(
            fact_change_id="change-1",
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_ids=["fact-1", "fact-2"],
            actor="ada",
            change_type="manual_correction",
            reason="wrong_tool",
        )

    assert client.transaction_enters == 1
    assert not any("INSERT INTO fact_change_audits" in call[1] for call in client.calls)


@pytest.mark.storage
@pytest.mark.no_network
async def test_replace_facts_with_audit_creates_replacement_and_invalidates_old_facts():
    replacement = FactRecord(
        id="fact-new",
        content="Ada uses Linear.",
        valid_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        source_entity_id=2,
        source="user",
        embedding=[0.1, 0.2],
    )
    client = RecordingPostgresClient(
        fetch_all_results=[[fact_row("fact-1"), fact_row("fact-2")]],
        fetch_one_results=[{"fact_id": "fact-new"}, {"projected_count": "1"}],
    )
    writer = FactWriter(client)

    result = await writer.replace_facts_with_audit(
        fact_change_id="change-1",
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        fact_ids=["fact-1", "fact-2"],
        actor="ada",
        change_type="manual_correction",
        reason="wrong_tool",
        replacement_fact=replacement,
        replacement_content="Ada uses Linear.",
    )

    assert result == {
        "fact_change_id": "change-1",
        "entity_id": 2,
        "invalidated_fact_ids": ["fact-1", "fact-2"],
        "created_fact_ids": ["fact-new"],
    }
    executed_sql = "\n".join(call[1] for call in client.calls)
    assert "INSERT INTO fact_change_audits" in executed_sql
    assert "INSERT INTO facts" in executed_sql
    assert "INSERT INTO fact_search" in executed_sql
    assert "UPDATE facts" in executed_sql
    assert "UPDATE fact_search" in executed_sql
    assert "SET f.invalid_at = $invalid_at" in executed_sql

    audit_call = next(
        call for call in client.calls if "INSERT INTO fact_change_audits" in call[1]
    )
    assert audit_call[2][9] == '["fact-1", "fact-2"]'
    assert audit_call[2][11] == '["fact-new"]'
    assert audit_call[2][12] == "Ada uses Linear."
    snapshots = json.loads(audit_call[2][10])
    assert [snapshot["fact_id"] for snapshot in snapshots] == ["fact-1", "fact-2"]
