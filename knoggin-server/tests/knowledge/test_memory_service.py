import pytest

from knoggin_server.agent.tools.memory import MemoryTools


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


class BrainHarness(MemoryTools):
    def __init__(self, postgres, agent_id="agent-1"):
        self.postgres = postgres
        self.agent_id = agent_id
        self.user_name = "ada"


def brain_row(*, revision=3):
    return {
        "instructions": (
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
    assert postgres.calls[0][2] == {
        "user_name": "ada",
        "agent_id": "agent-1",
    }


@pytest.mark.no_network
async def test_brain_tools_require_active_durable_agent_identity():
    postgres = RecordingPostgres()
    tools = BrainHarness(postgres, agent_id=None)

    assert await tools.read_brain() == {
        "error": "No durable agent identity is active"
    }
    assert (
        await tools.edit_brain("Project Context", "new", expected_revision=1)
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
async def test_edit_brain_updates_one_section_and_records_revision():
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
    }
    write = postgres.calls[1]
    assert write[0] == "execute"
    assert "UPDATE public.agents" in write[1]
    assert "INSERT INTO public.agent_brain_revisions" in write[1]
    assert "Investigate graph drift." in write[2]["content"]
    assert "Old lesson" in write[2]["content"]
    assert write[2]["expected_revision"] == 3


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
