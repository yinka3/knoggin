from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional

import httpx

from common.conf.topics_config import TopicConfig
from common.schema.aac_schema import AAC_SPECIFIC_SCHEMAS
from common.schema.settings import EpisodeSettings
from common.schema.tool_schema import (
    CAPABILITY_CLASSES,
    DESTRUCTIVE_WRITE_CAPABILITY,
    SAFE_DEFAULT_CAPABILITIES,
    TOOL_SCHEMAS,
    get_schema_capability,
)
from core.agent.tools.graph import GraphTools
from core.agent.tools.health import HealthTools
from core.agent.tools.maintenance import MaintenanceTools
from core.agent.tools.memory import MemoryTools
from core.agent.tools.search import SearchTools
from core.agent.tools.topic_tools import TopicTools
from core.agent.tools.workspace import WorkspaceTools
from core.knowledge.documents import DocumentService
from core.knowledge.entity.resolver import EntityResolver

SPECIAL_TOOL_NAMES = frozenset(
    {"request_clarification", "request_replanning", "submit_answer"}
)

CORE_TOOL_LAYERS = frozenset({"core_memory", "core_brain"})
FEATURE_TOOL_LAYERS = frozenset(
    {
        "feature_external",
        "feature_project_admin",
        "feature_project_workspace",
        "feature_maintenance",
        "feature_community",
    }
)
RUNTIME_TOOL_LAYERS = frozenset({"runtime_special", "runtime_health"})
VALID_TOOL_LAYERS = CORE_TOOL_LAYERS | FEATURE_TOOL_LAYERS | RUNTIME_TOOL_LAYERS

BASE_TOOL_SCHEMA_BY_NAME = {
    schema["function"]["name"]: schema for schema in TOOL_SCHEMAS
}

ToolResultHook = Callable[[Any, Any, str, Dict], Awaitable[None]]
ToolErrorHook = Callable[[Any, Any, str], Awaitable[bool]]
RuntimeInstructionHook = Callable[[Any, frozenset[str]], str]


@dataclass(frozen=True)
class ToolModule:
    name: str
    layer: str
    tools: frozenset[str]
    default_limits: tuple[tuple[str, int], ...] = ()
    after_tool_result: Optional[ToolResultHook] = None
    after_tool_error: Optional[ToolErrorHook] = None
    runtime_instructions: Optional[RuntimeInstructionHook] = None

    @property
    def schema_names(self) -> frozenset[str]:
        """Every owned tool has exactly one schema in this catalog."""

        return self.tools


@dataclass(frozen=True)
class ToolDefinition:
    """Canonical runtime description of one callable agent tool.

    The public maps below are derived indexes retained for callers that need a
    lookup by name, layer, or module. New runtime behavior should be added to
    this definition rather than consulting parallel maps.
    """

    name: str
    schema: dict
    dispatch: tuple[str, tuple[str, ...]] | None
    layer: str
    module: ToolModule
    capability: str
    default_limit: Optional[int] = None


async def _project_admin_after_tool_result(
    ctx,
    tools,
    tool_name: str,
    result: Dict,
) -> None:
    del tools
    if tool_name != "update_topics":
        return
    data = result.get("data")
    if isinstance(data, dict) and data.get("success"):
        ctx.active_topics = list(data.get("active_topics", ctx.active_topics))


async def _maintenance_after_tool_result(
    ctx,
    tools,
    tool_name: str,
    result: Dict,
) -> None:
    candidate = _maintenance_candidate_for_tool(ctx, tool_name)
    if not candidate:
        return

    data = result.get("data")
    if isinstance(data, dict) and data.get("error"):
        await _record_maintenance_failure(ctx, tools, candidate)
        return

    if tool_name == "update_topics" and isinstance(data, dict):
        if data.get("success"):
            await _mark_maintenance_handled(ctx, tools, candidate)
        return

    if tool_name == "check_graph_health" and isinstance(data, dict):
        if not data.get("suggestions"):
            await _mark_maintenance_handled(ctx, tools, candidate)


async def _maintenance_after_tool_error(ctx, tools, tool_name: str) -> bool:
    candidate = _maintenance_candidate_for_tool(ctx, tool_name)
    if not candidate:
        return False
    await _record_maintenance_failure(ctx, tools, candidate)
    return True


def _maintenance_runtime_instructions(ctx, active_tool_names: frozenset[str]) -> str:
    candidates = [
        candidate
        for candidate in ctx.maintenance_candidates
        if candidate.suggested_tool in active_tool_names
    ]
    if not candidates:
        return ""

    lines = [
        "[SYSTEM NOTICE: Optional maintenance is available. "
        "The system has already checked eligibility and tool availability. "
        "You may handle one candidate if it is relevant, but do not block "
        "the user's response if maintenance is not useful right now.",
        "Maintenance candidates:",
    ]
    for candidate in candidates:
        lines.append(
            "- "
            f"{candidate.kind} via `{candidate.suggested_tool}` "
            f"({candidate.priority} priority): {candidate.reason}"
        )
    lines.append("]")
    return "\n".join(lines)


def _health_runtime_instructions(ctx, active_tool_names: frozenset[str]) -> str:
    del ctx
    health_tools = sorted(
        active_tool_names
        & {
            "get_engine_health",
            "get_resource_health",
            "get_ingestion_health",
            "get_background_health",
        }
    )
    if not health_tools:
        return ""
    return (
        "[SYSTEM NOTICE: Runtime health tools are read-only diagnostics. "
        "Use them only when the user asks about Knoggin health, availability, "
        "capacity, or delays; do not use them for ordinary project questions. "
        "Each health tool is intentionally limited to one call per run. "
        "Health results may describe runtime state but do not authorize any "
        "administrative or mutating action.]"
    )


def _maintenance_candidate_for_tool(ctx, tool_name: str):
    from core.agent.maintenance import find_candidate_for_tool

    return find_candidate_for_tool(ctx.maintenance_candidates, tool_name)


async def _mark_maintenance_handled(ctx, tools, candidate) -> None:
    from loguru import logger

    from core.agent.maintenance import mark_maintenance_handled

    redis = getattr(tools, "redis", None)
    project_id = ctx.scope.project_id or str(getattr(tools, "project_id", ""))
    if redis is not None and project_id:
        try:
            await mark_maintenance_handled(
                redis,
                candidate,
                user_name=ctx.scope.user_name,
                project_id=project_id,
            )
        except Exception as exc:
            logger.warning(
                "Failed to clear maintenance state for "
                f"{candidate.id}: {exc}"
            )
    _remove_maintenance_candidate(ctx, candidate)


async def _record_maintenance_failure(ctx, tools, candidate) -> None:
    from loguru import logger

    from core.agent.maintenance import record_maintenance_failure

    redis = getattr(tools, "redis", None)
    project_id = ctx.scope.project_id or str(getattr(tools, "project_id", ""))
    if redis is not None and project_id:
        try:
            await record_maintenance_failure(
                redis,
                candidate,
                user_name=ctx.scope.user_name,
                project_id=project_id,
            )
        except Exception as exc:
            logger.warning(
                "Failed to record maintenance failure for "
                f"{candidate.id}: {exc}"
            )
    _remove_maintenance_candidate(ctx, candidate)


def _remove_maintenance_candidate(ctx, candidate) -> None:
    ctx.maintenance_candidates = [
        item for item in ctx.maintenance_candidates if item.id != candidate.id
    ]


TOOL_MODULES = {
    "core_memory": ToolModule(
        name="core_memory",
        layer="core_memory",
        tools=frozenset(
            {
                "search_messages",
                "search_entity",
                "get_connections",
                "get_recent_activity",
                "episode_check",
                "read_episode",
                "read_recent_episodes",
                "find_path",
                "get_hierarchy",
                "list_documents",
                "list_folder_uploads",
                "get_folder_upload_summary",
                "list_folder_tree",
                "get_document_info",
                "read_document",
                "search_documents",
            }
        ),
        default_limits=(
            ("search_messages", 6),
            ("get_connections", 8),
            ("search_entity", 8),
            ("episode_check", 6),
            ("read_episode", 4),
            ("read_recent_episodes", 4),
            ("get_recent_activity", 8),
            ("find_path", 8),
            ("get_hierarchy", 8),
            ("list_documents", 4),
            ("list_folder_uploads", 4),
            ("get_folder_upload_summary", 6),
            ("list_folder_tree", 6),
            ("get_document_info", 6),
            ("read_document", 6),
            ("search_documents", 8),
        ),
    ),
    "feature_external": ToolModule(
        name="feature_external",
        layer="feature_external",
        tools=frozenset({"web_search", "news_search"}),
        default_limits=(("web_search", 8), ("news_search", 8)),
    ),
    "feature_project_workspace": ToolModule(
        name="feature_project_workspace",
        layer="feature_project_workspace",
        tools=frozenset(
            {
                "list_workspace_files",
                "read_workspace_file",
                "create_workspace_file",
                "update_workspace_file",
                "append_workspace_file",
            }
        ),
        default_limits=(
            ("list_workspace_files", 4),
            ("read_workspace_file", 4),
            ("create_workspace_file", 2),
            ("update_workspace_file", 2),
            ("append_workspace_file", 2),
        ),
    ),
    "feature_project_admin": ToolModule(
        name="feature_project_admin",
        layer="feature_project_admin",
        tools=frozenset({"update_topics"}),
        default_limits=(("update_topics", 1),),
        after_tool_result=_project_admin_after_tool_result,
    ),
    "core_brain": ToolModule(
        name="core_brain",
        layer="core_brain",
        tools=frozenset(
            {
                "read_brain",
                "list_brain_snapshots",
                "read_brain_snapshot",
                "edit_brain",
                "restore_brain_section",
            }
        ),
        default_limits=(
            ("read_brain", 4),
            ("list_brain_snapshots", 4),
            ("read_brain_snapshot", 4),
            ("edit_brain", 2),
            ("restore_brain_section", 2),
        ),
    ),
    "feature_community": ToolModule(
        name="feature_community",
        layer="feature_community",
        tools=frozenset({"save_insight", "spawn_specialist"}),
        default_limits=(("save_insight", 4), ("spawn_specialist", 2)),
    ),
    "feature_maintenance": ToolModule(
        name="feature_maintenance",
        layer="feature_maintenance",
        tools=frozenset({"check_graph_health", "propose_entity_merge"}),
        after_tool_result=_maintenance_after_tool_result,
        after_tool_error=_maintenance_after_tool_error,
        runtime_instructions=_maintenance_runtime_instructions,
    ),
    "runtime_special": ToolModule(
        name="runtime_special",
        layer="runtime_special",
        tools=SPECIAL_TOOL_NAMES,
        default_limits=(("request_replanning", 2),),
    ),
    "runtime_health": ToolModule(
        name="runtime_health",
        layer="runtime_health",
        tools=frozenset(
            {
                "get_engine_health",
                "get_resource_health",
                "get_ingestion_health",
                "get_background_health",
            }
        ),
        default_limits=(
            ("get_engine_health", 1),
            ("get_resource_health", 1),
            ("get_ingestion_health", 1),
            ("get_background_health", 1),
        ),
        runtime_instructions=_health_runtime_instructions,
    ),
}

_SCHEMA_BY_NAME = {
    schema["function"]["name"]: schema
    for schema in [*TOOL_SCHEMAS, *AAC_SPECIFIC_SCHEMAS]
}


def _dispatch_from_schema(
    name: str,
    schema: dict,
) -> tuple[str, tuple[str, ...]] | None:
    """Derive direct method dispatch from the public tool schema.

    Runtime-special tools are interpreted by the executor rather than a Tools
    method. Every other callable tool deliberately uses the schema name as its
    method name, so parameter keys cannot drift from the schema contract.
    """

    if name in SPECIAL_TOOL_NAMES:
        return None
    parameters = schema["function"].get("parameters", {}).get("properties", {})
    return name, tuple(parameters)


_DECLARED_MODULE_BY_TOOL = {
    tool_name: module
    for module in TOOL_MODULES.values()
    for tool_name in module.tools
}
TOOL_DEFINITIONS = {
    name: ToolDefinition(
        name=name,
        schema=schema,
        dispatch=_dispatch_from_schema(name, schema),
        layer=_DECLARED_MODULE_BY_TOOL[name].layer,
        module=_DECLARED_MODULE_BY_TOOL[name],
        capability=get_schema_capability(schema),
        default_limit=dict(_DECLARED_MODULE_BY_TOOL[name].default_limits).get(name),
    )
    for name, schema in _SCHEMA_BY_NAME.items()
    if name in _DECLARED_MODULE_BY_TOOL
}

# Derived compatibility indexes. ToolDefinition is the runtime authority.
TOOL_DISPATCH = {
    name: definition.dispatch
    for name, definition in TOOL_DEFINITIONS.items()
    if definition.dispatch is not None
}
TOOL_LAYERS = {
    layer: frozenset(
        definition.name
        for definition in TOOL_DEFINITIONS.values()
        if definition.layer == layer
    )
    for layer in VALID_TOOL_LAYERS
}
TOOL_LAYER_BY_NAME = {
    definition.name: definition.layer for definition in TOOL_DEFINITIONS.values()
}
TOOL_MODULE_BY_NAME = {
    definition.name: definition.module for definition in TOOL_DEFINITIONS.values()
}
TOOL_SCHEMA_MODULE_BY_NAME = dict(TOOL_MODULE_BY_NAME)
DEFAULT_TOOL_LIMITS = {
    definition.name: definition.default_limit
    for definition in TOOL_DEFINITIONS.values()
    if definition.default_limit is not None
}


def get_tool_layer(tool_name: str) -> Optional[str]:
    return TOOL_LAYER_BY_NAME.get(tool_name)


def get_tools_by_layer(layer: str) -> frozenset[str]:
    return TOOL_LAYERS.get(layer, frozenset())


def is_core_tool(tool_name: str) -> bool:
    return get_tool_layer(tool_name) in CORE_TOOL_LAYERS


def is_feature_tool(tool_name: str) -> bool:
    return get_tool_layer(tool_name) in FEATURE_TOOL_LAYERS


def get_default_tool_limits() -> Dict[str, int]:
    return dict(DEFAULT_TOOL_LIMITS)


def get_registered_tool_names() -> frozenset[str]:
    """Return every callable tool name owned by the runtime registry."""

    return frozenset(TOOL_DEFINITIONS)


def get_tool_module(tool_name: str) -> Optional[ToolModule]:
    return TOOL_MODULE_BY_NAME.get(tool_name)


def get_tool_schemas(
    enabled_tools: list[str] | None = None,
    tags: list[str] | None = None,
    capabilities: list[str] | set[str] | frozenset[str] | None = None,
) -> list[dict]:
    enabled_set = set(enabled_tools) if enabled_tools is not None else None
    tags_set = set(tags) if tags else None
    capability_set = (
        set(capabilities)
        if capabilities is not None
        else set(SAFE_DEFAULT_CAPABILITIES)
    )
    if capabilities is None and enabled_set is not None:
        capability_set.update(
            get_schema_capability(BASE_TOOL_SCHEMA_BY_NAME[name])
            for name in enabled_set
            if name in BASE_TOOL_SCHEMA_BY_NAME
        )
    invalid_capabilities = capability_set - CAPABILITY_CLASSES
    if invalid_capabilities:
        raise ValueError(
            "Unknown tool capabilities: "
            + ", ".join(sorted(invalid_capabilities))
        )

    filtered = []
    for schema in TOOL_SCHEMAS:
        name = schema["function"]["name"]
        if name not in TOOL_SCHEMA_MODULE_BY_NAME:
            continue
        if name in SPECIAL_TOOL_NAMES:
            filtered.append(schema)
            continue

        is_enabled = enabled_set is None or name in enabled_set
        has_capability = get_schema_capability(schema) in capability_set
        has_tag = True
        if tags_set is not None:
            tool_tags = set(schema["function"].get("tags", []))
            has_tag = bool(tool_tags & tags_set)

        if is_enabled and has_tag and has_capability:
            filtered.append(schema)

    return filtered


def get_active_tool_names(schemas: Iterable[dict]) -> frozenset[str]:
    return frozenset(schema["function"]["name"] for schema in schemas)


def get_runtime_instructions(ctx, active_tool_names: frozenset[str]) -> str:
    instructions = [
        instruction
        for module in TOOL_MODULES.values()
        if module.runtime_instructions is not None
        for instruction in [module.runtime_instructions(ctx, active_tool_names)]
        if instruction
    ]
    return "\n\n".join(instructions)


async def apply_tool_result_hooks(
    ctx,
    tools,
    tool_name: str,
    result: Dict,
) -> None:
    for module in TOOL_MODULES.values():
        if module.after_tool_result is not None:
            await module.after_tool_result(ctx, tools, tool_name, result)


async def apply_tool_error_hooks(ctx, tools, tool_name: str) -> bool:
    handled = False
    for module in TOOL_MODULES.values():
        if module.after_tool_error is not None:
            handled = await module.after_tool_error(ctx, tools, tool_name) or handled
    return handled


@dataclass(frozen=True)
class ToolPermissions:
    user_name: str
    agent_id: str
    project_id: str
    session_id: str
    run_id: str
    allowed_tools: frozenset[str]
    allowed_capabilities: frozenset[str]
    confirmation_state: str = "not_confirmed"

    def authorize(self, tool_name: str, capability: str) -> Optional[str]:
        if tool_name not in self.allowed_tools:
            return f"Tool '{tool_name}' is not enabled for this run"
        if capability not in self.allowed_capabilities:
            return (
                f"Capability '{capability}' is not enabled for this run"
            )
        if (
            capability == DESTRUCTIVE_WRITE_CAPABILITY
            and self.confirmation_state != "confirmed"
        ):
            return "Destructive tool execution requires explicit confirmation"
        return None


def configure_tool_authorization(
    tools,
    schemas: Iterable[dict],
    *,
    user_name: str,
    agent_id: str,
    project_id: str,
    session_id: str,
    run_id: str,
    confirmation_state: str = "not_confirmed",
) -> ToolPermissions:
    schema_map = {
        schema["function"]["name"]: schema
        for schema in schemas
    }
    capabilities = frozenset(
        get_schema_capability(schema) for schema in schema_map.values()
    )
    context = ToolPermissions(
        user_name=user_name,
        agent_id=agent_id,
        project_id=project_id,
        session_id=session_id,
        run_id=run_id,
        allowed_tools=frozenset(schema_map),
        allowed_capabilities=capabilities,
        confirmation_state=confirmation_state,
    )
    tools.tool_authorization = context
    tools.active_tool_schemas = schema_map
    return context


def validate_registry_contract() -> None:
    schemas = [*TOOL_SCHEMAS, *AAC_SPECIFIC_SCHEMAS]
    schema_names = {
        schema["function"]["name"] for schema in schemas
    }
    duplicate_names = {
        name for name in schema_names
        if sum(
            schema["function"]["name"] == name
            for schema in schemas
        ) > 1
    }
    # edit_brain has an AAC-specific schema variant, but it remains the same
    # core Brain tool with the same executable method and capability.
    duplicate_names.discard("edit_brain")
    if duplicate_names:
        raise RuntimeError(
            f"Duplicate tool schemas: {sorted(duplicate_names)}"
        )

    definition_names = set(TOOL_DEFINITIONS)
    missing_definitions = schema_names - definition_names
    extra_definitions = definition_names - schema_names
    if missing_definitions or extra_definitions:
        raise RuntimeError(
            "Tool definition contract mismatch: "
            f"missing={sorted(missing_definitions)}, "
            f"extra={sorted(extra_definitions)}"
        )
    for name, definition in TOOL_DEFINITIONS.items():
        if definition.name != name:
            raise RuntimeError(f"Tool definition key/name mismatch: {name}")
        if definition.schema["function"]["name"] != name:
            raise RuntimeError(f"Tool definition '{name}' schema mismatch")
        if definition.capability != get_schema_capability(definition.schema):
            raise RuntimeError(f"Tool definition '{name}' capability mismatch")

    dispatch_names = set(TOOL_DISPATCH)
    missing_dispatch = schema_names - SPECIAL_TOOL_NAMES - dispatch_names
    missing_schema = dispatch_names - schema_names
    if missing_dispatch or missing_schema:
        raise RuntimeError(
            "Tool registry contract mismatch: "
            f"missing_dispatch={sorted(missing_dispatch)}, "
            f"missing_schema={sorted(missing_schema)}"
        )

    layer_names = set(TOOL_LAYERS)
    invalid_layers = layer_names - VALID_TOOL_LAYERS
    if invalid_layers:
        raise RuntimeError(f"Invalid tool layers: {sorted(invalid_layers)}")

    layered_names = [
        tool_name
        for tool_names in TOOL_LAYERS.values()
        for tool_name in tool_names
    ]
    duplicate_layer_names = {
        tool_name
        for tool_name in layered_names
        if layered_names.count(tool_name) > 1
    }
    exposed_tool_names = schema_names | dispatch_names
    missing_layers = exposed_tool_names - set(TOOL_LAYER_BY_NAME)
    extra_layers = set(TOOL_LAYER_BY_NAME) - exposed_tool_names
    if duplicate_layer_names or missing_layers or extra_layers:
        raise RuntimeError(
            "Tool layer contract mismatch: "
            f"duplicates={sorted(duplicate_layer_names)}, "
            f"missing={sorted(missing_layers)}, "
            f"extra={sorted(extra_layers)}"
        )

    module_names = set(TOOL_MODULES)
    missing_modules = layer_names - module_names
    extra_modules = module_names - layer_names
    if missing_modules or extra_modules:
        raise RuntimeError(
            "Tool module contract mismatch: "
            f"missing={sorted(missing_modules)}, "
            f"extra={sorted(extra_modules)}"
        )

    module_tool_names = [
        tool_name
        for module in TOOL_MODULES.values()
        for tool_name in module.tools
    ]
    duplicate_module_tools = {
        tool_name
        for tool_name in module_tool_names
        if module_tool_names.count(tool_name) > 1
    }
    missing_module_tools = set(TOOL_LAYER_BY_NAME) - set(TOOL_MODULE_BY_NAME)
    extra_module_tools = set(TOOL_MODULE_BY_NAME) - set(TOOL_LAYER_BY_NAME)
    if duplicate_module_tools or missing_module_tools or extra_module_tools:
        raise RuntimeError(
            "Tool module assignment mismatch: "
            f"duplicates={sorted(duplicate_module_tools)}, "
            f"missing={sorted(missing_module_tools)}, "
            f"extra={sorted(extra_module_tools)}"
        )

    for module_name, module in TOOL_MODULES.items():
        if module.name != module_name:
            raise RuntimeError(
                f"Tool module key/name mismatch: {module_name} != {module.name}"
            )
        if module.layer != module_name:
            raise RuntimeError(
                f"Tool module '{module_name}' layer mismatch: {module.layer}"
            )
        if module.tools != TOOL_LAYERS[module.layer]:
            raise RuntimeError(
                f"Tool module '{module_name}' tools do not match its layer"
            )
        schema_names_outside_module = module.schema_names - module.tools
        if schema_names_outside_module:
            raise RuntimeError(
                f"Tool module '{module_name}' owns schemas outside its tools: "
                f"{sorted(schema_names_outside_module)}"
            )

    default_limit_names = [
        tool_name
        for module in TOOL_MODULES.values()
        for tool_name, _ in module.default_limits
    ]
    duplicate_default_limits = {
        tool_name
        for tool_name in default_limit_names
        if default_limit_names.count(tool_name) > 1
    }
    unowned_default_limits = set(DEFAULT_TOOL_LIMITS) - set(TOOL_MODULE_BY_NAME)
    if duplicate_default_limits or unowned_default_limits:
        raise RuntimeError(
            "Default tool limit contract mismatch: "
            f"duplicates={sorted(duplicate_default_limits)}, "
            f"unowned={sorted(unowned_default_limits)}"
        )

    schema_module_names = [
        schema_name
        for module in TOOL_MODULES.values()
        for schema_name in module.schema_names
    ]
    duplicate_schema_module_names = {
        schema_name
        for schema_name in schema_module_names
        if schema_module_names.count(schema_name) > 1
    }
    schema_owner_names = set(TOOL_SCHEMA_MODULE_BY_NAME)
    missing_schema_owners = schema_names - schema_owner_names
    extra_schema_owners = schema_owner_names - exposed_tool_names
    if duplicate_schema_module_names or missing_schema_owners or extra_schema_owners:
        raise RuntimeError(
            "Tool schema ownership mismatch: "
            f"duplicates={sorted(duplicate_schema_module_names)}, "
            f"missing={sorted(missing_schema_owners)}, "
            f"extra={sorted(extra_schema_owners)}"
        )

    for schema in schemas:
        get_schema_capability(schema)

    invalid_capabilities = {
        get_schema_capability(schema)
        for schema in schemas
    } - CAPABILITY_CLASSES
    if invalid_capabilities:
        raise RuntimeError(
            f"Invalid tool capabilities: {sorted(invalid_capabilities)}"
        )

    schema_by_name = {
        schema["function"]["name"]: schema
        for schema in schemas
    }
    for tool_name, dispatch_entry in TOOL_DISPATCH.items():
        if dispatch_entry is None:
            continue
        method_name, parameter_names = dispatch_entry
        if not callable(getattr(Tools, method_name, None)):
            raise RuntimeError(
                f"Tool '{tool_name}' has no concrete method '{method_name}'"
            )
        schema_parameters = set(
            schema_by_name[tool_name]["function"]
            .get("parameters", {})
            .get("properties", {})
        )
        if set(parameter_names) != schema_parameters:
            raise RuntimeError(
                f"Tool '{tool_name}' dispatch/schema parameters differ: "
                f"dispatch={sorted(parameter_names)}, "
                f"schema={sorted(schema_parameters)}"
            )


class Tools(
    SearchTools,
    GraphTools,
    MemoryTools,
    TopicTools,
    MaintenanceTools,
    HealthTools,
    WorkspaceTools,
):
    def __init__(
        self,
        user_name: str,
        entities: EntityResolver,
        session_id: str,
        topic_config: Optional[TopicConfig] = None,
        search_config: Optional[dict] = None,
        document_service: Optional[DocumentService] = None,
        document_focus: Optional[dict] = None,
        knowledge_store=None,
        postgres=None,
        redis=None,
        agent_id: Optional[str] = None,
        topic_refresh_callback=None,
        episode_settings: Optional[EpisodeSettings] = None,
        health_service=None,
        workspace_service=None,
    ):
        if knowledge_store is None or postgres is None or redis is None:
            raise ValueError(
                "Tools requires explicit knowledge_store, postgres, and redis"
            )

        self.session_id = session_id
        self.knowledge_store = knowledge_store
        self.postgres = postgres
        self.entities = entities
        self.user_name = user_name
        self.redis = redis
        self.embedding_service = entities.embedding_service
        self.project_id = entities.project_id
        self.readable_project_ids = entities.readable_project_ids
        self.topic_config = topic_config
        self.document_service = document_service
        self.workspace_service = workspace_service
        self.document_focus = document_focus
        self.active_topics = topic_config.active_topics if topic_config else None
        self.search_cfg = search_config or {}
        episode_settings = episode_settings or EpisodeSettings()
        self.episode_retrieval_limit = episode_settings.retrieval_episode_limit
        self.agent_id = agent_id or "AGENT_IDENTITY"
        self.topic_refresh_callback = topic_refresh_callback
        self.tool_authorization: Optional[ToolPermissions] = None
        self.active_tool_schemas: Dict[str, dict] = {}
        self.health_service = health_service

        self._http_client = httpx.AsyncClient(timeout=10.0)

    async def get_document_manifest(self):
        """Get indexed documents for prompt context."""
        if not self.document_service:
            return []
        documents = await self.document_service.list_documents(
            session_id=self.session_id
        )
        return [
            document
            for document in documents
            if document.get("status") == "indexed"
        ]

    async def close(self):
        await self._http_client.aclose()


validate_registry_contract()
