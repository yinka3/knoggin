import json
from datetime import datetime, timezone

import pytest

from common.schema.primitives import FactRecord
from core.knowledge.db.writers.entity_writer import EntityWriter
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


@pytest.mark.storage
@pytest.mark.no_network
async def test_apply_profile_fact_changes_writes_facts_and_audit_in_one_transaction():
    created = FactRecord(
        id="fact-new",
        content="Ada uses Linear.",
        valid_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        source_entity_id=2,
        source_msg_id=12,
        source_user_name="ada",
        source_session_id="session-1",
        embedding=[0.1, 0.2],
    )
    client = RecordingPostgresClient(
        fetch_all_results=[[fact_row("fact-old")]],
        fetch_one_results=[{"fact_id": "fact-new"}, {"projected_count": "1"}],
    )
    writer = FactWriter(client)

    result = await writer.apply_fact_changes_with_audit(
        fact_change_id="change-1",
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        facts_to_create=[created],
        fact_ids_to_invalidate=["fact-old"],
        actor="profile_refinement",
        change_type="profile_extraction",
        reason="profile_extraction",
        session_id="session-1",
        metadata={"skipped": []},
    )

    assert result == {
        "fact_change_id": "change-1",
        "entity_id": 2,
        "invalidated_fact_ids": ["fact-old"],
        "created_fact_ids": ["fact-new"],
    }
    assert client.transaction_enters == 1
    executed_sql = "\n".join(call[1] for call in client.calls)
    assert "FOR UPDATE" in executed_sql
    assert "INSERT INTO fact_change_audits" in executed_sql
    assert "INSERT INTO facts" in executed_sql
    assert "UPDATE facts" in executed_sql
    assert "UPDATE fact_search" in executed_sql
    assert "SET status = 'applied'" in executed_sql

    audit_call = next(
        call for call in client.calls if "INSERT INTO fact_change_audits" in call[1]
    )
    assert audit_call[2][8] == "[11, 12]"
    assert audit_call[2][9] == '["fact-old"]'
    assert audit_call[2][11] == '["fact-new"]'
    assert json.loads(audit_call[2][10])[0]["fact_id"] == "fact-old"


@pytest.mark.storage
@pytest.mark.no_network
async def test_invalidate_fact_is_a_noop_when_already_invalidated():
    client = RecordingPostgresClient(fetch_one_results=[None])
    writer = FactWriter(client)

    changed = await writer.invalidate_fact(
        "fact-old",
        datetime(2026, 1, 2, tzinfo=timezone.utc),
        project_id="project-1",
    )

    assert changed is False
    executed_sql = "\n".join(call[1] for call in client.calls)
    assert "AND invalid_at IS NULL" in executed_sql


@pytest.mark.storage
@pytest.mark.no_network
async def test_profile_fact_change_can_invalidate_a_fact_created_in_the_same_batch():
    created = FactRecord(
        id="fact-new",
        content="Ada no longer uses Linear.",
        valid_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        source_entity_id=2,
        source_msg_id=12,
        source_user_name="ada",
        source_session_id="session-1",
        embedding=[0.1, 0.2],
    )
    client = RecordingPostgresClient(
        fetch_one_results=[{"fact_id": "fact-new"}, {"projected_count": "1"}],
    )
    writer = FactWriter(client)

    await writer.apply_fact_changes_with_audit(
        fact_change_id="change-1",
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        facts_to_create=[created],
        fact_ids_to_invalidate=["fact-new"],
        actor="profile_refinement",
        change_type="profile_extraction",
    )

    executed_sql = "\n".join(call[1] for call in client.calls)
    assert "SELECT *\n                    FROM facts" not in executed_sql
    assert "INSERT INTO facts" in executed_sql
    assert "UPDATE facts" in executed_sql


@pytest.mark.storage
@pytest.mark.no_network
async def test_profile_fact_change_aborts_before_mutation_when_audit_write_fails():
    created = FactRecord(
        id="fact-new",
        content="Ada uses Linear.",
        valid_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        source_entity_id=2,
        source_msg_id=12,
        source_user_name="ada",
        source_session_id="session-1",
        embedding=[0.1, 0.2],
    )
    client = RecordingPostgresClient()
    writer = FactWriter(client)

    async def fail_audit(*args, **kwargs):
        raise RuntimeError("audit write failed")

    writer.audit_writer.create_applying_audit_with_cursor = fail_audit

    with pytest.raises(RuntimeError, match="audit write failed"):
        await writer.apply_fact_changes_with_audit(
            fact_change_id="change-1",
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            facts_to_create=[created],
            fact_ids_to_invalidate=[],
            actor="profile_refinement",
            change_type="profile_extraction",
        )

    assert client.transaction_enters == 1
    executed_sql = "\n".join(call[1] for call in client.calls)
    assert "INSERT INTO facts" not in executed_sql
    assert "UPDATE facts" not in executed_sql


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_profile_fact_change_rolls_back_facts_and_audit_on_final_audit_failure(
    real_postgres_client,
):
    entity_writer = EntityWriter(real_postgres_client)
    fact_writer = FactWriter(real_postgres_client)
    await entity_writer.write_batch(
        [
            {
                "id": 2,
                "is_new": True,
                "canonical_name": "Ada Lovelace",
                "aliases": ["Ada"],
                "type": "person",
                "topic": "Identity",
                "confidence": 0.9,
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
                "embedding": [0.1] * 1024,
            }
        ],
        [],
    )
    await fact_writer.create_facts_batch(
        2,
        [
            FactRecord(
                id="fact-old",
                content="Ada uses Notion.",
                valid_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                source_entity_id=2,
                embedding=[0.1] * 1024,
            )
        ],
        user_name="ada",
        project_id="project-1",
    )

    async def fail_final_audit_update(*args, **kwargs):
        raise RuntimeError("forced audit completion failure")

    fact_writer.audit_writer.mark_applied_with_cursor = fail_final_audit_update

    with pytest.raises(RuntimeError, match="forced audit completion failure"):
        await fact_writer.apply_fact_changes_with_audit(
            fact_change_id="change-1",
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            facts_to_create=[
                FactRecord(
                    id="fact-new",
                    content="Ada uses Linear.",
                    valid_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                    source_entity_id=2,
                    embedding=[0.1] * 1024,
                )
            ],
            fact_ids_to_invalidate=["fact-old"],
            actor="profile_refinement",
            change_type="profile_extraction",
        )

    facts = await real_postgres_client.fetch_all(
        """
        SELECT fact_id, invalid_at
        FROM facts
        WHERE project_id = %s
        ORDER BY fact_id
        """,
        ("project-1",),
    )
    audits = await real_postgres_client.fetch_all(
        """
        SELECT fact_change_id
        FROM fact_change_audits
        WHERE project_id = %s
        """,
        ("project-1",),
    )
    searches = await real_postgres_client.fetch_all(
        """
        SELECT fact_id
        FROM fact_search
        WHERE project_id = %s
        ORDER BY fact_id
        """,
        ("project-1",),
    )

    assert facts == [{"fact_id": "fact-old", "invalid_at": None}]
    assert audits == []
    assert searches == [{"fact_id": "fact-old"}]
