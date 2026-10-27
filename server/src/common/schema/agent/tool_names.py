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
WEB_TOOL_NAMES = frozenset({"search_web", "search_news", "read_web_page"}) | JOB_TOOL_NAMES
MAINTENANCE_TOOL_NAMES = frozenset({
    "get_engine_health", "get_resource_health", "get_ingestion_health",
    "get_background_health", "inspect_duplicate_entities", "advise_entity_merge",
    "propose_entity_merge", "report_relationship_conflict",
})


def tool_groups() -> dict[str, frozenset[str]]:
    """Named tool sets that agents, chats and runs may enable by name. Core is
    everything that is neither web access nor memory maintenance."""

    every = frozenset(schema["function"]["name"] for schema in TOOL_SCHEMAS)
    return {
        "core": every - WEB_TOOL_NAMES - MAINTENANCE_TOOL_NAMES,
        "web": WEB_TOOL_NAMES,
        "maintenance": MAINTENANCE_TOOL_NAMES,
    }


def expand_tool_groups(names) -> tuple[str, ...]:
    """Replace group names with their tools, keeping first-seen order."""

    groups = tool_groups()
    expanded: dict[str, None] = {}
    for name in names:
        for tool in sorted(groups[name]) if name in groups else (name,):
            expanded.setdefault(tool)
    return tuple(expanded)


# The only writes research may make: saving its findings to project files.
RESEARCH_WRITE_TOOL_NAMES = frozenset(
    {"create_project_file", "append_project_file", "create_project_folder", "save_findings"}
)
# Tools that only make sense inside a research run.
RESEARCH_ONLY_TOOL_NAMES = frozenset({"record_finding", "save_findings"})
_NOT_FOR_RESEARCH_JOBS = frozenset({"runtime:health", "maintenance", "jobs"})


def research_run_blocked_tool_names() -> frozenset[str]:
    """Tools no research run gets: every write except saving findings (so no
    Brain edits, maintenance, or overwriting, moving, or deleting files), and
    no starting jobs, which measured runs did to themselves when replanning."""

    return JOB_TOOL_NAMES | frozenset(
        schema["function"]["name"]
        for schema in TOOL_SCHEMAS
        if schema["function"]["capability"] != "read"
        and schema["function"]["name"] not in RESEARCH_WRITE_TOOL_NAMES
    )


def research_job_tool_names() -> tuple[str, ...]:
    """Tools a background research run may use: reads (without health or
    maintenance tools) plus saving findings to project files."""

    return tuple(
        schema["function"]["name"]
        for schema in TOOL_SCHEMAS
        if (
            schema["function"]["capability"] == "read"
            and not _NOT_FOR_RESEARCH_JOBS & set(schema["function"].get("tags", ()))
        )
        or schema["function"]["name"] in RESEARCH_WRITE_TOOL_NAMES
    )
