import hashlib

import pytest

from common.exceptions import ToolExecutionError
from common.schema.tool_schema import (
    REVERSIBLE_WRITE_CAPABILITY,
    TOOL_SCHEMAS_BY_NAME,
    get_schema_capability,
    validate_tool_arguments,
)
from core.agent.internals import execute_tool
from core.agent.tools.registry import ToolPermissions, get_default_tool_limits
from core.agent.tools.workspace import WorkspaceTools


class FakeWorkspaceService:
    def __init__(self):
        self.calls = []

    async def list_files(self, *, path_prefix=None, limit=100):
        self.calls.append(("list", path_prefix, limit))
        return [{"relative_path": "notes.md", "content_hash": "a" * 64}]

    async def read_file(self, path, **kwargs):
        self.calls.append(("read", path, kwargs))
        return {"relative_path": path, "content": "hello\n"}

    async def create_file(self, path, content):
        self.calls.append(("create", path, content))
        return {"relative_path": path, "content_hash": hashlib.sha256(content.encode()).hexdigest()}

    async def update_file(self, path, content, *, expected_content_hash):
        self.calls.append(("update", path, content, expected_content_hash))
        return {"relative_path": path, "content_hash": hashlib.sha256(content.encode()).hexdigest()}

    async def append_file(self, path, content, *, expected_content_hash):
        self.calls.append(("append", path, content, expected_content_hash))
        return {"relative_path": path, "content_hash": expected_content_hash}


class WorkspaceHarness(WorkspaceTools):
    def __init__(self, service=None):
        self.workspace_service = service


@pytest.mark.no_network
async def test_workspace_tools_forward_bounded_project_scoped_operations():
    service = FakeWorkspaceService()
    tools = WorkspaceHarness(service)
    expected_hash = "b" * 64

    assert await tools.list_workspace_files(path_prefix="docs", limit=3)
    await tools.read_workspace_file(
        "docs/notes.md",
        start_line=2,
        end_line=4,
        max_characters=100,
    )
    await tools.create_workspace_file("docs/new.md", "new")
    await tools.update_workspace_file("docs/new.md", "replacement", expected_hash)
    await tools.append_workspace_file("docs/new.md", "more", expected_hash)

    assert service.calls == [
        ("list", "docs", 3),
        (
            "read",
            "docs/notes.md",
            {
                "start_line": 2,
                "end_line": 4,
                "max_lines": 200,
                "max_characters": 100,
            },
        ),
        ("create", "docs/new.md", "new"),
        ("update", "docs/new.md", "replacement", expected_hash),
        ("append", "docs/new.md", "more", expected_hash),
    ]


@pytest.mark.no_network
async def test_project_markdown_is_readable_but_protected_from_ordinary_writes():
    service = FakeWorkspaceService()
    tools = WorkspaceHarness(service)

    await tools.read_workspace_file("PROJECT.md")
    assert service.calls[0][0] == "read"

    for method in (
        tools.create_workspace_file,
        tools.update_workspace_file,
        tools.append_workspace_file,
    ):
        with pytest.raises(PermissionError, match="PROJECT.md"):
            if method.__name__ == "create_workspace_file":
                await method("project.md", "content")
            else:
                await method(".\\PROJECT.md", "content", "a" * 64)

    assert len(service.calls) == 1

    with pytest.raises(ValueError, match="must not escape"):
        await tools.create_workspace_file("../outside.md", "content")
    with pytest.raises(ValueError, match="must not escape"):
        await tools.list_workspace_files(path_prefix="../outside")


@pytest.mark.no_network
async def test_workspace_writes_require_authorization_and_are_audited():
    service = FakeWorkspaceService()

    with pytest.raises(ToolExecutionError, match="authorization context"):
        await execute_tool(
            WorkspaceHarness(service),
            "create_workspace_file",
            {"path": "notes.md", "content": "hello"},
        )

    class FakePostgres:
        def __init__(self):
            self.calls = []

        async def execute(self, query, params):
            self.calls.append((query, params))

    postgres = FakePostgres()
    tools = WorkspaceHarness(service)
    tools.postgres = postgres
    tools.active_tool_schemas = {
        "create_workspace_file": TOOL_SCHEMAS_BY_NAME["create_workspace_file"]
    }
    tools.tool_authorization = ToolPermissions(
        user_name="user",
        agent_id="agent",
        project_id="project",
        session_id="session",
        run_id="run",
        allowed_tools=frozenset({"create_workspace_file"}),
        allowed_capabilities=frozenset({REVERSIBLE_WRITE_CAPABILITY}),
    )

    result = await execute_tool(
        tools,
        "create_workspace_file",
        {"path": "notes.md", "content": "hello"},
    )

    assert result["data"]["relative_path"] == "notes.md"
    assert len(postgres.calls) == 2
    assert "INSERT INTO public.agent_tool_audits" in postgres.calls[0][0]
    assert postgres.calls[1][1]["status"] == "succeeded"


@pytest.mark.no_network
async def test_workspace_schema_registry_limits_and_bounds():
    expected = {
        "list_workspace_files",
        "read_workspace_file",
        "create_workspace_file",
        "update_workspace_file",
        "append_workspace_file",
    }
    assert expected <= set(TOOL_SCHEMAS_BY_NAME)
    for name in expected - {"list_workspace_files", "read_workspace_file"}:
        assert get_schema_capability(TOOL_SCHEMAS_BY_NAME[name]) == REVERSIBLE_WRITE_CAPABILITY

    limits = get_default_tool_limits()
    assert {name: limits[name] for name in expected} == {
        "list_workspace_files": 4,
        "read_workspace_file": 4,
        "create_workspace_file": 2,
        "update_workspace_file": 2,
        "append_workspace_file": 2,
    }

    schema = TOOL_SCHEMAS_BY_NAME["create_workspace_file"]
    assert validate_tool_arguments(
        schema,
        {"path": "notes.md", "content": "ok"},
    ) == []
    assert validate_tool_arguments(
        schema,
        {"path": "notes.md", "content": ""},
    )
