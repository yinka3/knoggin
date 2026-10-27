"""Portable agent tool-name contracts used by configuration validation."""

from common.schema.agent.community_tools import AAC_SPECIFIC_SCHEMAS
from common.schema.agent.tool_contracts import TOOL_SCHEMAS


def get_configurable_tool_names() -> frozenset[str]:
    """Return every tool name that may be referenced by config overrides.

    The runtime registry owns dispatch and default limits.  Configuration only
    needs the portable schema-level name contract, which keeps the common
    configuration layer independent from runtime tool implementations.
    """

    schemas = (*TOOL_SCHEMAS, *AAC_SPECIFIC_SCHEMAS)
    return frozenset(
        schema["function"]["name"]
        for schema in schemas
        if isinstance(schema.get("function"), dict)
        and isinstance(schema["function"].get("name"), str)
    )


JOB_TOOL_NAMES = frozenset(
    {"start_research_job", "list_research_jobs", "cancel_research_job"}
)
_NOT_FOR_RESEARCH_JOBS = frozenset({"runtime:health", "maintenance", "jobs"})


def research_job_tool_names() -> tuple[str, ...]:
    """Tools a background research run may use: reads only, no health or
    maintenance tools, and no starting further jobs."""

    return tuple(
        schema["function"]["name"]
        for schema in TOOL_SCHEMAS
        if schema["function"]["capability"] == "read"
        and not _NOT_FOR_RESEARCH_JOBS & set(schema["function"].get("tags", ()))
    )
