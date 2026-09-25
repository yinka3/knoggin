import hashlib

import pytest

from common.exceptions import ToolExecutionError
from common.schema.agent.tool_contracts import (
    REVERSIBLE_WRITE_CAPABILITY,
    TOOL_SCHEMAS_BY_NAME,
    get_schema_capability,
    validate_tool_arguments,
)
from core.agent.tool_runtime import execute_tool
from core.agent.tools.registry import ToolPermissions, get_default_tool_limits
from core.agent.tools.workspace import ProjectFileTools


class FakeDocumentService:
    def __init__(self):
        self.calls = []
        self.project_id = "project-1"
        self.registered_documents = {}

    async def list_project_files(self, *, path_prefix=None, limit=100):
        self.calls.append(("list", path_prefix, limit))
        return [{"relative_path": "notes.md", "content_hash": "a" * 64}]

    async def read_project_file(self, path, **kwargs):
        self.calls.append(("read", path, kwargs))
        return {"relative_path": path, "content": "hello\n"}

    async def get_document_info(self, *, relative_path=None, document_id=None):
        self.calls.append(("document_info", relative_path, document_id))
        if relative_path in self.registered_documents:
            return self.registered_documents[relative_path]
        raise FileNotFoundError("Document not found")

    async def create_project_file(self, path, content):
        self.calls.append(("create", path, content))
        return {
            "relative_path": path,
            "content_hash": hashlib.sha256(content.encode()).hexdigest(),
        }

    async def update_project_file(self, path, content, *, expected_content_hash):
        self.calls.append(("update", path, content, expected_content_hash))
        return {
            "relative_path": path,
            "content_hash": hashlib.sha256(content.encode()).hexdigest(),
        }

    async def append_project_file(self, path, content, *, expected_content_hash):
        self.calls.append(("append", path, content, expected_content_hash))
        return {"relative_path": path, "content_hash": expected_content_hash}


class ProjectFileHarness(ProjectFileTools):
    def __init__(self, service=None):
        self.document_service = service


@pytest.mark.no_network
async def test_project_file_tools_forward_bounded_project_scoped_operations():
    service = FakeDocumentService()
    tools = ProjectFileHarness(service)
    expected_hash = "b" * 64

    assert await tools.list_project_files(path_prefix="docs", limit=3)
    await tools.read_project_file(
        "docs/notes.md",
        start_line=2,
        end_line=4,
        max_characters=100,
    )
    await tools.create_project_file("docs/new.md", "new")
    await tools.update_project_file("docs/new.md", "replacement", expected_hash)
    await tools.append_project_file("docs/new.md", "more", expected_hash)

    assert service.calls == [
        ("list", "docs", 3),
        ("document_info", "docs/notes.md", None),
        (
            "read",
            "docs/notes.md",
            {
                "start_line": 2,
                "end_line": 4,
                "max_characters": 100,
            },
        ),
        ("create", "docs/new.md", "new"),
        ("update", "docs/new.md", "replacement", expected_hash),
        ("append", "docs/new.md", "more", expected_hash),
    ]


@pytest.mark.no_network
async def test_registered_evidence_document_cannot_bypass_provenance_aware_read():
    service = FakeDocumentService()
    service.registered_documents["docs/evidence.md"] = {
        "document_id": "document-1",
        "project_id": "project-1",
    }
    tools = ProjectFileHarness(service)

    with pytest.raises(ToolExecutionError, match="read_project_document"):
        await tools.read_project_file("docs/evidence.md")

    assert service.calls == [("document_info", "docs/evidence.md", None)]


@pytest.mark.no_network
async def test_project_markdown_is_readable_but_protected_from_ordinary_writes():
    service = FakeDocumentService()
    tools = ProjectFileHarness(service)

    await tools.read_project_file("PROJECT.md")
    assert service.calls[0][0] == "read"

    for method in (
        tools.create_project_file,
        tools.update_project_file,
        tools.append_project_file,
    ):
        with pytest.raises(PermissionError, match="PROJECT.md"):
            if method.__name__ == "create_project_file":
                await method("project.md", "content")
            else:
                await method(".\\PROJECT.md", "content", "a" * 64)

    assert len(service.calls) == 1

    with pytest.raises(ValueError, match="must not escape"):
        await tools.create_project_file("../outside.md", "content")
    with pytest.raises(ValueError, match="must not escape"):
        await tools.list_project_files(path_prefix="../outside")


@pytest.mark.no_network
async def test_project_file_writes_require_authorization_and_are_audited():
    service = FakeDocumentService()

    with pytest.raises(ToolExecutionError, match="authorization context"):
        await execute_tool(
            ProjectFileHarness(service),
            "create_project_file",
            {"path": "notes.md", "content": "hello"},
        )

    class FakePostgres:
        def __init__(self):
            self.calls = []

        async def execute(self, query, params):
            self.calls.append((query, params))

    postgres = FakePostgres()
    tools = ProjectFileHarness(service)
    tools.postgres = postgres
    tools.active_tool_schemas = {
        "create_project_file": TOOL_SCHEMAS_BY_NAME["create_project_file"]
    }
    tools.tool_authorization = ToolPermissions(
        user_name="user",
        agent_id="agent",
        project_id="project",
        audit_project_id="project",
        session_id="session",
        run_id="run",
        allowed_tools=frozenset({"create_project_file"}),
        allowed_capabilities=frozenset({REVERSIBLE_WRITE_CAPABILITY}),
    )

    result = await execute_tool(
        tools,
        "create_project_file",
        {"path": "notes.md", "content": "hello"},
    )

    assert result["data"]["relative_path"] == "notes.md"
    assert len(postgres.calls) == 2
    assert "INSERT INTO public.agent_tool_audits" in postgres.calls[0][0]
    assert postgres.calls[1][1]["status"] == "succeeded"


@pytest.mark.no_network
async def test_project_file_schema_registry_limits_and_bounds():
    expected = {
        "list_project_files",
        "read_project_file",
        "create_project_file",
        "update_project_file",
        "append_project_file",
        "move_project_file",
        "delete_project_file",
        "create_project_folder",
    }
    assert expected <= set(TOOL_SCHEMAS_BY_NAME)
    for name in expected - {"list_project_files", "read_project_file"}:
        assert (
            get_schema_capability(TOOL_SCHEMAS_BY_NAME[name])
            == REVERSIBLE_WRITE_CAPABILITY
        )

    limits = get_default_tool_limits()
    assert {name: limits[name] for name in expected} == {
        "list_project_files": 4,
        "read_project_file": 4,
        "create_project_file": 2,
        "update_project_file": 2,
        "append_project_file": 2,
        "move_project_file": 2,
        "delete_project_file": 2,
        "create_project_folder": 2,
    }

    schema = TOOL_SCHEMAS_BY_NAME["create_project_file"]
    assert (
        validate_tool_arguments(
            schema,
            {"path": "notes.md", "content": "ok"},
        )
        == []
    )
    assert validate_tool_arguments(
        schema,
        {"path": "notes.md", "content": ""},
    )
