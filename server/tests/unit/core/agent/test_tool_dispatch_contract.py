import pytest

from common.exceptions import ToolExecutionError
from common.schema.agent.tool_contracts import TOOL_SCHEMAS_BY_NAME
from core.agent.tool_runtime import execute_tool
from core.agent.tools.memory import MemoryTools


class DispatchTools:
    def __init__(self):
        self.calls = []

    async def search_messages(self, query, limit=None):
        self.calls.append(("search_messages", query, limit))
        return [{"id": "msg_1"}]

    async def search_entity(self, query, limit=None):
        self.calls.append(("search_entity", query, limit))
        return [{"id": 1, "query": query}]

    async def get_recent_activity(self, entity_name, hours=None):
        self.calls.append(("get_recent_activity", entity_name, hours))
        return [{"entity": entity_name}]

    async def episode_check(self, query, entity_name=None):
        self.calls.append(("episode_check", query, entity_name))
        return {"resolution": "exact"}

    async def read_episode(self, episode_id):
        self.calls.append(("read_episode", episode_id))
        return [{"id": episode_id}]

    async def read_document(
        self,
        document_id=None,
        relative_path=None,
        start_line=1,
        end_line=None,
    ):
        self.calls.append(
            ("read_document", document_id, relative_path, start_line, end_line)
        )
        return [{"document_id": document_id, "content": "lines"}]

    async def list_folder_uploads(self, visibility_scope=None, limit=25):
        self.calls.append(
            ("list_folder_uploads", visibility_scope, limit)
        )
        return [{"folder_root_id": "folder-1"}]

    async def get_folder_upload_summary(self, folder_root_id):
        self.calls.append(("get_folder_upload_summary", folder_root_id))
        return {"folder_root_id": folder_root_id}

    async def list_folder_tree(
        self,
        folder_root_id,
        path_prefix=None,
        max_depth=3,
        use_focus=True,
    ):
        self.calls.append(
            (
                "list_folder_tree",
                folder_root_id,
                path_prefix,
                max_depth,
                use_focus,
            )
        )
        return [{"name": "src", "type": "directory"}]

    async def broken(self):
        raise RuntimeError("method exploded")


@pytest.mark.no_network
async def test_execute_tool_dispatches_known_tools_and_coerces_schema_types():
    tools = DispatchTools()

    result = await execute_tool(
        tools,
        "search_messages",
        {"query": 1234, "limit": "5"},
    )
    activity = await execute_tool(
        tools,
        "get_recent_activity",
        {"entity_name": "Knoggin", "hours": "48"},
    )
    entity = await execute_tool(
        tools,
        "search_entity",
        {"query": 99, "limit": "2"},
    )
    file_content = await execute_tool(
        tools,
        "read_document",
        {"document_id": "file-1", "start_line": "2", "end_line": "4"},
    )
    uploads = await execute_tool(
        tools,
        "list_folder_uploads",
        {"visibility_scope": "project", "limit": "7"},
    )
    summary = await execute_tool(
        tools,
        "get_folder_upload_summary",
        {"folder_root_id": "folder-1"},
    )
    tree = await execute_tool(
        tools,
        "list_folder_tree",
        {
            "folder_root_id": "folder-1",
            "path_prefix": "src",
            "max_depth": "4",
            "use_focus": "false",
        },
    )
    episode = await execute_tool(
        tools,
        "episode_check",
        {"query": "What changed?", "entity_name": 7},
    )
    expanded_episode = await execute_tool(
        tools,
        "read_episode",
        {"episode_id": 42},
    )

    assert result == {"data": [{"id": "msg_1"}]}
    assert activity == {"data": [{"entity": "Knoggin"}]}
    assert entity == {"data": [{"id": 1, "query": "99"}]}
    assert file_content == {
        "data": [{"document_id": "file-1", "content": "lines"}]
    }
    assert uploads == {"data": [{"folder_root_id": "folder-1"}]}
    assert summary == {"data": {"folder_root_id": "folder-1"}}
    assert tree == {"data": [{"name": "src", "type": "directory"}]}
    assert episode == {"data": {"resolution": "exact"}}
    assert expanded_episode == {"data": [{"id": "42"}]}
    assert tools.calls == [
        ("search_messages", "1234", 5),
        ("get_recent_activity", "Knoggin", 48),
        ("search_entity", "99", 2),
        ("read_document", "file-1", None, 2, 4),
        ("list_folder_uploads", "project", 7),
        ("get_folder_upload_summary", "folder-1"),
        ("list_folder_tree", "folder-1", "src", 4, False),
        ("episode_check", "What changed?", "7"),
        ("read_episode", "42"),
    ]


@pytest.mark.no_network
async def test_execute_tool_rejects_executor_control_operations():
    tools = DispatchTools()

    with pytest.raises(ToolExecutionError, match="Unknown tool"):
        await execute_tool(
            tools,
            "request_clarification",
            {"question": "Which project?"},
        )

    assert tools.calls == []


@pytest.mark.no_network
def test_request_replanning_is_not_an_agent_tool():
    assert "request_replanning" not in TOOL_SCHEMAS_BY_NAME


@pytest.mark.no_network
async def test_execute_tool_raises_for_unknown_or_missing_methods():
    tools = DispatchTools()

    with pytest.raises(ToolExecutionError) as unknown:
        await execute_tool(tools, "not_a_tool", {})

    assert unknown.value.details["tool"] == "not_a_tool"
    assert "Unknown tool" in unknown.value.message

    with pytest.raises(ToolExecutionError) as missing:
        await execute_tool(tools, "edit_brain", {
            "section": "Role",
            "content": "updated",
            "expected_revision": 1,
        })

    assert missing.value.details["tool"] == "edit_brain"
    assert "Tool method not found" in missing.value.message


@pytest.mark.no_network
async def test_execute_tool_rejects_direct_entity_merge_bypass():
    tools = DispatchTools()

    with pytest.raises(ToolExecutionError) as direct_merge:
        await execute_tool(
            tools,
            "merge_entities",
            {"primary_id": 2, "duplicate_id": 3},
        )

    assert direct_merge.value.details["tool"] == "merge_entities"
    assert "Unknown tool" in direct_merge.value.message


@pytest.mark.no_network
async def test_execute_tool_wraps_tool_method_exceptions(monkeypatch):
    tools = DispatchTools()

    monkeypatch.setattr(
        "core.agent.tools.registry.TOOL_DISPATCH",
        {"broken_tool": ("broken", [])},
    )
    monkeypatch.setattr(
        "core.agent.tool_runtime.TOOL_DISPATCH",
        {"broken_tool": ("broken", [])},
    )

    with pytest.raises(ToolExecutionError) as exc:
        await execute_tool(tools, "broken_tool", {})

    assert exc.value.details["tool"] == "broken_tool"
    assert "Tool execution failed" in exc.value.message


class RecordingPostgres:
    def __init__(self):
        self.rows = [
            {
                "brain": "## Role\nOld role\n",
                "persona": "Careful assistant",
                "brain_revision": 1,
            }
        ]
        self.fetch_calls = []
        self.execute_calls = []

    async def fetch_all(self, query, params):
        self.fetch_calls.append((query, params))
        return self.rows

    async def execute(self, query, params):
        self.execute_calls.append((query, params))
        self.rows[0]["brain"] = params["content"]
        self.rows[0]["brain_revision"] += 1
        return 1


class MemoryToolHarness(MemoryTools):
    def __init__(self, postgres=None, agent_id="agent-1"):
        self.postgres = postgres
        self.agent_id = agent_id
        self.user_name = "ada"


@pytest.mark.no_network
async def test_memory_tools_read_and_edit_brain_with_configured_postgres():
    postgres = RecordingPostgres()
    tools = MemoryToolHarness(postgres)

    brain = await tools.read_brain()
    edited = await tools.edit_brain(
        "Behavioral Directives",
        "Ada prefers scoped tests",
        expected_revision=1,
    )

    assert brain["revision"] == 1
    assert "Old role" in brain["content"]
    assert edited == {
        "success": True,
        "section": "Behavioral Directives",
        "revision": 2,
        "message": "Brain section updated.",
        "snapshot_created": False,
    }
    assert postgres.fetch_calls
    assert postgres.execute_calls[0][1]["agent_id"] == "agent-1"
    assert "Ada prefers scoped tests" in postgres.execute_calls[0][1]["content"]


@pytest.mark.no_network
async def test_memory_tools_return_clean_defaults_without_active_agent():
    tools = MemoryToolHarness(postgres=None, agent_id=None)

    assert await tools.read_brain() == {"error": "No durable agent identity is active"}
    assert await tools.list_brain_snapshots() == {
        "error": "No durable agent identity is active"
    }
    assert await tools.read_brain_snapshot(1) == {
        "error": "No durable agent identity is active"
    }
    assert await tools.edit_brain("Behavioral Directives", "note", 1) == {
        "error": "No durable agent identity is active"
    }
    assert await tools.restore_brain_section(
        "Behavioral Directives",
        1,
        1,
    ) == {"error": "No durable agent identity is active"}


@pytest.mark.no_network
async def test_normal_memory_tools_keep_community_only_tools_unavailable():
    tools = MemoryToolHarness(postgres=RecordingPostgres())

    assert await tools.save_insight("community insight") == {
        "error": "save_insight is only available in community discussions."
    }
    assert await tools.spawn_specialist("Expert", "Persona") == {
        "error": "spawn_specialist is only available in community discussions."
    }
