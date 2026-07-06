from pathlib import Path

import pytest

from core.knowledge.db.writers.fact_audit_writer import FactAuditWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_audit_writer_creates_applying_audit():
    client = RecordingPostgresClient()
    writer = FactAuditWriter(client)
    snapshot = {
        "fact_id": "fact-1",
        "entity_id": 2,
        "content": "Ada uses Notion.",
    }

    await writer.create_applying_audit(
        fact_change_id="change-1",
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        actor="ada",
        change_type="manual_correction",
        reason="wrong_tool",
        session_id="session-1",
        source_msg_ids=[11, 12],
        invalidated_fact_ids=["fact-1"],
        invalidated_fact_snapshots=[snapshot],
        created_fact_ids=["fact-2"],
        replacement_content="Ada uses Linear.",
    )

    call = client.calls[0]
    assert call[0] == "execute_command"
    assert "INSERT INTO fact_change_audits" in call[1]
    assert "'applying'" in call[1]
    assert call[2][0] == "change-1"
    assert call[2][1] == "ada"
    assert call[2][2] == "project-1"
    assert call[2][3] == 2
    assert call[2][7] == "wrong_tool"
    assert call[2][8] == "[11, 12]"
    assert call[2][9] == '["fact-1"]'
    assert call[2][10] == (
        '[{"fact_id": "fact-1", "entity_id": 2, '
        '"content": "Ada uses Notion."}]'
    )
    assert call[2][11] == '["fact-2"]'
    assert call[2][12] == "Ada uses Linear."
    assert call[2][13] == "{}"


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_audit_writer_mark_applied_updates_final_ids():
    client = RecordingPostgresClient()
    writer = FactAuditWriter(client)

    await writer.mark_applied(
        "change-1",
        invalidated_fact_ids=["fact-1"],
        created_fact_ids=["fact-2"],
    )

    call = client.calls[0]
    assert call[0] == "execute_command"
    assert "UPDATE fact_change_audits" in call[1]
    assert "SET status = 'applied'" in call[1]
    assert "failure_reason = NULL" in call[1]
    assert "invalidated_fact_ids = COALESCE" in call[1]
    assert "created_fact_ids = COALESCE" in call[1]
    assert call[2] == ('["fact-1"]', '["fact-2"]', "change-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_audit_writer_create_applied_audit_inserts_then_marks_applied():
    client = RecordingPostgresClient()
    writer = FactAuditWriter(client)

    await writer.create_applied_audit(
        fact_change_id="change-1",
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        actor="profile_refinement",
        change_type="profile_extraction",
        reason="profile_extraction",
        source_msg_ids=[7],
        invalidated_fact_ids=["fact-1"],
        created_fact_ids=["fact-2"],
        metadata={"skipped": [{"reason": "duplicate"}]},
    )

    insert_call, applied_call = client.calls
    assert insert_call[0] == "execute_command"
    assert "INSERT INTO fact_change_audits" in insert_call[1]
    assert insert_call[2][13] == '{"skipped": [{"reason": "duplicate"}]}'
    assert applied_call[0] == "execute_command"
    assert "SET status = 'applied'" in applied_call[1]
    assert applied_call[2] == ('["fact-1"]', '["fact-2"]', "change-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_audit_writer_mark_applied_keeps_existing_ids_when_omitted():
    client = RecordingPostgresClient()
    writer = FactAuditWriter(client)

    await writer.mark_applied("change-1")

    call = client.calls[0]
    assert call[0] == "execute_command"
    assert "COALESCE" in call[1]
    assert call[2] == (None, None, "change-1")


@pytest.mark.storage
@pytest.mark.no_network
async def test_fact_audit_writer_mark_failed_records_reason():
    client = RecordingPostgresClient()
    writer = FactAuditWriter(client)

    await writer.mark_failed("change-1", "manual repair required")

    call = client.calls[0]
    assert call[0] == "execute_command"
    assert "UPDATE fact_change_audits" in call[1]
    assert "SET status = 'failed'" in call[1]
    assert "failure_reason = %s" in call[1]
    assert call[2] == ("manual repair required", "change-1")


def test_fact_change_audit_schema_contract():
    schema_path = (
        Path(__file__).resolve().parents[2] / "src" / "infrastructure" / "schema.sql"
    )
    schema = schema_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS public.fact_change_audits" in schema
    assert "fact_change_id TEXT PRIMARY KEY" in schema
    assert "invalidated_fact_snapshots JSONB NOT NULL DEFAULT '[]'::jsonb" in schema
    assert "change_type IN (" in schema
    assert "metadata JSONB NOT NULL DEFAULT '{}'::jsonb" in schema
    assert "'manual_remove'" in schema
    assert "'manual_correction'" in schema
    assert "'user_remove'" not in schema
    assert "'user_correction'" not in schema
    assert "'profile_extraction'" in schema
    assert "status IN ('applying', 'applied', 'failed')" in schema
    assert "fact_change_audits_entity_idx" in schema
    assert "fact_change_audits_project_idx" in schema
