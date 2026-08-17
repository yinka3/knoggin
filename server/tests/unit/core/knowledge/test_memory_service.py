import pytest

from core.agent.tools.memory import MemoryTools


class RecordingPostgres:
    def __init__(self, rows=None, execute_result=1):
        self.rows = list(rows or [])
        self.execute_result = execute_result
        self.calls = []

    async def fetch_all(self, query, params=None):
        self.calls.append(("fetch_all", query, params))
        return self.rows.pop(0) if self.rows else []

    async def execute(self, query, params=None):
        self.calls.append(("execute", query, params))
        return self.execute_result


class StatefulBrainPostgres:
    def __init__(self):
        self.row = brain_row(revision=1)
        self.calls = []

    async def fetch_all(self, query, params=None):
        self.calls.append(("fetch_all", query, params))
        return [dict(self.row)]

    async def execute(self, query, params=None):
        self.calls.append(("execute", query, params))
        self.row["brain"] = params["content"]
        self.row["brain_revision"] += 1
        return 1


class BrainHarness(MemoryTools):
    def __init__(self, postgres, agent_id="agent-1"):
        self.postgres = postgres
        self.agent_id = agent_id
        self.user_name = "ada"


def brain_row(*, revision=3):
    return {
        "brain": (
            "# Self-Conception\nEvidence-focused agent.\n\n"
            "# Behavioral Directives\nStay grounded.\n\n"
            "# Project Context\nOld context\n\n"
            "# User Preferences & Lessons Learned\nOld lesson\n"
        ),
        "persona": "Evidence-focused",
        "brain_revision": revision,
    }


@pytest.mark.no_network
async def test_read_brain_returns_durable_content_and_revision():
    postgres = RecordingPostgres([[brain_row()]])

    result = await BrainHarness(postgres).read_brain()

    assert result["revision"] == 3
    assert "Old context" in result["content"]
    assert "Project Context" in result["editable_sections"]
    assert "every 5 revisions" in result["snapshot_policy"]
    assert postgres.calls[0][2] == {
        "user_name": "ada",
        "agent_id": "agent-1",
    }


@pytest.mark.no_network
async def test_read_edit_read_brain_round_trip_uses_durable_agent_row():
    postgres = StatefulBrainPostgres()
    tools = BrainHarness(postgres)

    before = await tools.read_brain()
    edited = await tools.edit_brain(
        "Project Context",
        "New durable context",
        expected_revision=before["revision"],
    )
    after = await tools.read_brain()

    assert before["revision"] == 1
    assert edited["revision"] == 2
    assert after["revision"] == 2
    assert "New durable context" in after["content"]
    assert "Old context" not in after["content"]
    assert "Old lesson" in after["content"]


@pytest.mark.no_network
async def test_brain_tools_require_active_durable_agent_identity():
    postgres = RecordingPostgres()
    tools = BrainHarness(postgres, agent_id=None)

    assert await tools.read_brain() == {
        "error": "No durable agent identity is active"
    }
    assert await tools.list_brain_snapshots() == {
        "error": "No durable agent identity is active"
    }
    assert await tools.read_brain_snapshot(1) == {
        "error": "No durable agent identity is active"
    }
    assert (
        await tools.edit_brain("Project Context", "new", expected_revision=1)
    ) == {"error": "No durable agent identity is active"}
    assert (
        await tools.restore_brain_section(
            "Project Context",
            from_snapshot_revision=1,
            expected_current_revision=1,
        )
    ) == {"error": "No durable agent identity is active"}
    assert postgres.calls == []


@pytest.mark.no_network
async def test_edit_brain_rejects_stale_revision_without_writing():
    postgres = RecordingPostgres([[brain_row(revision=4)]])

    result = await BrainHarness(postgres).edit_brain(
        "Project Context",
        "New context",
        expected_revision=3,
    )

    assert result == {
        "error": "Brain changed since it was read",
        "current_revision": 4,
    }
    assert [call[0] for call in postgres.calls] == ["fetch_all"]


@pytest.mark.no_network
async def test_edit_brain_updates_one_section_without_snapshot_before_boundary():
    postgres = RecordingPostgres([[brain_row(revision=3)]])

    result = await BrainHarness(postgres).edit_brain(
        "Project Context",
        "Investigate graph drift.",
        expected_revision=3,
    )

    assert result == {
        "success": True,
        "section": "Project Context",
        "revision": 4,
        "message": "Brain section updated.",
        "snapshot_created": False,
    }
    write = postgres.calls[1]
    assert write[0] == "execute"
    assert "UPDATE public.agents" in write[1]
    assert "INSERT INTO public.agent_brain_snapshots" not in write[1]
    assert "Investigate graph drift." in write[2]["content"]
    assert "Old lesson" in write[2]["content"]
    assert write[2]["expected_revision"] == 3


@pytest.mark.no_network
async def test_edit_brain_records_snapshot_at_boundary_with_summary():
    postgres = RecordingPostgres([[brain_row(revision=4)]])

    result = await BrainHarness(postgres).edit_brain(
        "Project Context",
        "Investigate graph drift.",
        expected_revision=4,
        change_note="Removed stale queue note",
    )

    assert result == {
        "success": True,
        "section": "Project Context",
        "revision": 5,
        "message": "Brain section updated.",
        "snapshot_created": True,
    }
    write = postgres.calls[1]
    assert "INSERT INTO public.agent_brain_snapshots" in write[1]
    assert write[2]["changed_section"] == "Project Context"
    assert write[2]["change_summary"] == (
        "Edited Project Context: Removed stale queue note"
    )


@pytest.mark.no_network
async def test_edit_brain_rejects_noneditable_section():
    postgres = RecordingPostgres([[brain_row()]])

    result = await BrainHarness(postgres).edit_brain(
        "Birth Persona",
        "Rewrite identity",
        expected_revision=3,
    )

    assert "error" in result
    assert not any(call[0] == "execute" for call in postgres.calls)


@pytest.mark.no_network
async def test_list_brain_snapshots_returns_metadata_without_content():
    postgres = RecordingPostgres(
        [
            [{"brain_revision": 7}],
            [
                {
                    "revision": 5,
                    "edited_by": "agent",
                    "change_type": "section_edit",
                    "changed_section": "Project Context",
                    "change_summary": "Edited Project Context",
                    "restored_from_revision": None,
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ],
        ]
    )

    result = await BrainHarness(postgres).list_brain_snapshots()

    assert result["current_revision"] == 7
    assert result["snapshot_interval"] == 5
    assert "every 5 revisions" in result["snapshot_policy"]
    assert result["snapshots"][0]["revision"] == 5
    assert "content" not in result["snapshots"][0]


@pytest.mark.no_network
async def test_read_brain_snapshot_returns_owned_snapshot_content():
    postgres = RecordingPostgres(
        [
            [
                {
                    "revision": 5,
                    "content": brain_row(revision=5)["brain"],
                    "edited_by": "agent",
                    "change_type": "section_edit",
                    "changed_section": "Project Context",
                    "change_summary": "Edited Project Context",
                    "restored_from_revision": None,
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        ]
    )

    result = await BrainHarness(postgres).read_brain_snapshot(5)

    assert result["revision"] == 5
    assert "Old context" in result["content"]
    assert postgres.calls[0][2]["revision"] == 5


@pytest.mark.no_network
async def test_restore_brain_section_restores_one_section_and_snapshots():
    current = brain_row(revision=7)
    snapshot = brain_row(revision=5)
    snapshot["brain"] = snapshot["brain"].replace("Old context", "Snapshot context")
    postgres = RecordingPostgres(
        [
            [current],
            [{"content": snapshot["brain"]}],
        ]
    )

    result = await BrainHarness(postgres).restore_brain_section(
        "Project Context",
        from_snapshot_revision=5,
        expected_current_revision=7,
        change_note="Undo bad context compaction",
    )

    assert result == {
        "success": True,
        "section": "Project Context",
        "revision": 8,
        "restored_from_revision": 5,
        "snapshot_created": True,
        "message": "Brain section restored from snapshot.",
    }
    write = postgres.calls[2]
    assert "INSERT INTO public.agent_brain_snapshots" in write[1]
    assert "Snapshot context" in write[2]["content"]
    assert "Old lesson" in write[2]["content"]
    assert write[2]["changed_section"] == "Project Context"
    assert write[2]["restored_from_revision"] == 5
    assert write[2]["change_summary"] == (
        "Restored Project Context from snapshot 5: Undo bad context compaction"
    )


@pytest.mark.no_network
async def test_restore_brain_section_rejects_stale_current_revision():
    postgres = RecordingPostgres([[brain_row(revision=8)]])

    result = await BrainHarness(postgres).restore_brain_section(
        "Project Context",
        from_snapshot_revision=5,
        expected_current_revision=7,
    )

    assert result == {
        "error": "Brain changed since it was read",
        "current_revision": 8,
    }
    assert [call[0] for call in postgres.calls] == ["fetch_all"]


@pytest.mark.no_network
async def test_restore_brain_section_rejects_noneditable_section():
    postgres = RecordingPostgres(
        [
            [brain_row(revision=7)],
            [{"content": brain_row(revision=5)["brain"]}],
        ]
    )

    result = await BrainHarness(postgres).restore_brain_section(
        "Self-Conception",
        from_snapshot_revision=5,
        expected_current_revision=7,
    )

    assert "error" in result
    assert not any(call[0] == "execute" for call in postgres.calls)
