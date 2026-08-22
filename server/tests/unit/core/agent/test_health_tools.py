import pytest

from common.schema.health import HealthSnapshot
from core.agent.tools.health import HealthTools


class HealthyService:
    async def get_engine_health(self):
        return HealthSnapshot(summary="Engine is healthy")

    async def get_resource_health(self, *, project_id: str):
        assert project_id == "project-a"
        return HealthSnapshot(summary="Resources are healthy")

    async def get_ingestion_health(
        self, *, user_name: str, project_id: str, session_id: str
    ):
        assert (user_name, project_id, session_id) == (
            "ada",
            "project-a",
            "session-a",
        )
        return HealthSnapshot(summary="Ingestion is healthy")

    async def get_background_health(self, *, project_id: str):
        assert project_id == "project-a"
        return HealthSnapshot(summary="Background work is healthy")


class ToolHarness(HealthTools):
    health_service = HealthyService()
    user_name = "ada"
    project_id = "project-a"
    session_id = "session-a"


@pytest.mark.unit
@pytest.mark.no_network
async def test_health_tools_return_the_common_json_envelope():
    tools = ToolHarness()

    engine = await tools.get_engine_health()
    resources = await tools.get_resource_health()

    assert engine["status"] == "healthy"
    assert engine["activity"] == "idle"
    assert resources["summary"] == "Resources are healthy"
    ingestion = await tools.get_ingestion_health()
    assert ingestion["summary"] == "Ingestion is healthy"
    background = await tools.get_background_health()
    assert background["summary"] == "Background work is healthy"
    assert resources["checked_at"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_health_tools_fail_closed_when_service_is_missing():
    tools = ToolHarness()
    tools.health_service = None

    result = await tools.get_engine_health()

    assert result["status"] == "degraded"
    assert result["warnings"] == ["runtime health service is unavailable"]


@pytest.mark.unit
@pytest.mark.no_network
def test_health_tools_are_registered_with_low_limits_and_runtime_guidance():
    pytest.importorskip(
        "torch",
        reason="registry import follows the optional embedding runtime stack",
    )
    pytest.importorskip(
        "transformers.utils",
        reason="registry import follows the optional embedding runtime stack",
    )
    from core.agent.tools.registry import (
        get_default_tool_limits,
        get_runtime_instructions,
        get_tool_schemas,
    )

    schemas = get_tool_schemas(
        enabled_tools=["get_engine_health", "get_resource_health"],
    )
    names = {schema["function"]["name"] for schema in schemas}
    instructions = get_runtime_instructions(schemas)
    limits = get_default_tool_limits()

    assert {"get_engine_health", "get_resource_health"} <= names
    assert limits["get_engine_health"] == 1
    assert limits["get_resource_health"] == 1
    assert "read-only diagnostics" in instructions
    assert "ordinary project questions" in instructions
