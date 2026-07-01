import pytest

from common.schema.aac_schema import AAC_SPECIFIC_SCHEMAS
from common.schema.tool_schema import (
    DESTRUCTIVE_WRITE_CAPABILITY,
    READ_CAPABILITY,
    SAFE_DEFAULT_CAPABILITIES,
    TOOL_SCHEMAS,
    get_schema_capability,
)
from knoggin_server.agent.tools.registry import TOOL_DISPATCH, ToolAuthorizationContext
from knoggin_server.agent.types import AgentContext


def _schema_names():
    return {
        schema["function"]["name"]
        for schema in [*TOOL_SCHEMAS, *AAC_SPECIFIC_SCHEMAS]
    }


@pytest.mark.no_network
def test_obsolete_memory_tools_are_not_agent_facing():
    exposed_names = _schema_names() | set(TOOL_DISPATCH)

    assert "save_memory" not in exposed_names
    assert "forget_memory" not in exposed_names
    assert {"read_brain", "edit_brain", "restore_brain_section"} <= exposed_names


@pytest.mark.no_network
def test_direct_destructive_merge_execution_is_not_agent_facing():
    exposed_names = _schema_names() | set(TOOL_DISPATCH)

    assert "merge_entities" not in exposed_names
    assert "propose_entity_merge" in exposed_names
    assert get_schema_capability(
        next(
            schema
            for schema in TOOL_SCHEMAS
            if schema["function"]["name"] == "propose_entity_merge"
        )
    ) != DESTRUCTIVE_WRITE_CAPABILITY
    assert DESTRUCTIVE_WRITE_CAPABILITY not in SAFE_DEFAULT_CAPABILITIES


@pytest.mark.no_network
def test_destructive_capability_requires_runtime_confirmation():
    context = ToolAuthorizationContext(
        user_name="ada",
        agent_id="agent-1",
        project_id="project-1",
        session_id="session-1",
        run_id="run-1",
        allowed_tools=frozenset({"dangerous_tool", "read_tool"}),
        allowed_capabilities=frozenset(
            {DESTRUCTIVE_WRITE_CAPABILITY, READ_CAPABILITY}
        ),
    )

    assert (
        context.authorize("dangerous_tool", DESTRUCTIVE_WRITE_CAPABILITY)
        == "Destructive tool execution requires explicit confirmation"
    )
    assert context.authorize("read_tool", READ_CAPABILITY) is None

    confirmed = ToolAuthorizationContext(
        user_name="ada",
        agent_id="agent-1",
        project_id="project-1",
        session_id="session-1",
        run_id="run-1",
        allowed_tools=frozenset({"dangerous_tool"}),
        allowed_capabilities=frozenset({DESTRUCTIVE_WRITE_CAPABILITY}),
        confirmation_state="confirmed",
    )

    assert confirmed.authorize("dangerous_tool", DESTRUCTIVE_WRITE_CAPABILITY) is None


@pytest.mark.no_network
def test_agent_context_uses_candidate_list_not_legacy_maintenance_booleans():
    fields = AgentContext.__dataclass_fields__

    assert "maintenance_candidates" in fields
    assert "maintenance_needed" not in fields
    assert "topic_evaluation_needed" not in fields
