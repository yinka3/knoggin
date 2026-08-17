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
