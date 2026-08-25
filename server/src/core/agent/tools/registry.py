from dataclasses import dataclass
from typing import Dict, Iterable, Optional

import httpx

from common.conf.domain_config import CompiledDomain
from common.schema.agent.community_tools import AAC_SPECIFIC_SCHEMAS
from common.schema.agent.tool_contracts import (
    CAPABILITY_CLASSES,
    SAFE_DEFAULT_CAPABILITIES,
    TOOL_SCHEMAS,
    get_schema_capability,
)
from core.agent.tools.graph import GraphTools
from core.agent.tools.health import HealthTools
from core.agent.tools.maintenance import MaintenanceTools
from core.agent.tools.memory import MemoryTools
from core.agent.tools.search import SearchTools
from core.agent.tools.workspace import WorkspaceTools
from core.knowledge.documents import DocumentService
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.retrieval import KnowledgeRetrieval


@dataclass(frozen=True)
class ToolDefinition:
    """The canonical runtime description of one agent tool."""

    name: str
    schema: dict
    dispatch: tuple[str, tuple[str, ...]] | None
    capability: str
    default_limit: Optional[int] = None
    runtime_instruction: Optional[str] = None
    executor_protocol: bool = False


_HEALTH_RUNTIME_INSTRUCTION = (
    "[SYSTEM NOTICE: Runtime health tools are read-only diagnostics. "
    "Use them only when the user asks about Knoggin health, availability, "
    "capacity, or delays; do not use them for ordinary project questions. "
    "Each health tool is intentionally limited to one call per run. "
    "Health results may describe runtime state but do not authorize any "
    "administrative or mutating action.]"
)


def _canonical_schema(name: str) -> dict:
    for schema in TOOL_SCHEMAS:
        if schema["function"]["name"] == name:
            return schema
    for schema in AAC_SPECIFIC_SCHEMAS:
        if schema["function"]["name"] == name:
            return schema
    raise RuntimeError(f"Missing tool schema for {name}")


def _definition(
    name: str,
    *,
    default_limit: Optional[int] = None,
    runtime_instruction: Optional[str] = None,
    executor_protocol: bool = False,
) -> ToolDefinition:
    schema = _canonical_schema(name)
    parameters = schema["function"].get("parameters", {}).get("properties", {})
    return ToolDefinition(
        name=name,
        schema=schema,
        dispatch=None if executor_protocol else (name, tuple(parameters)),
        capability=get_schema_capability(schema),
        default_limit=default_limit,
        runtime_instruction=runtime_instruction,
        executor_protocol=executor_protocol,
    )


TOOL_DEFINITIONS = {
    "get_engine_health": _definition(
        "get_engine_health",
        default_limit=1,
        runtime_instruction=_HEALTH_RUNTIME_INSTRUCTION,
    ),
    "get_resource_health": _definition(
        "get_resource_health",
        default_limit=1,
        runtime_instruction=_HEALTH_RUNTIME_INSTRUCTION,
    ),
    "get_ingestion_health": _definition(
        "get_ingestion_health",
        default_limit=1,
        runtime_instruction=_HEALTH_RUNTIME_INSTRUCTION,
    ),
    "get_background_health": _definition(
        "get_background_health",
        default_limit=1,
        runtime_instruction=_HEALTH_RUNTIME_INSTRUCTION,
    ),
    "search_entity": _definition("search_entity", default_limit=8),
    "get_connections": _definition("get_connections", default_limit=8),
    "find_path": _definition("find_path", default_limit=8),
    "search_messages": _definition("search_messages", default_limit=6),
    "get_recent_activity": _definition("get_recent_activity", default_limit=8),
    "request_clarification": _definition(
        "request_clarification",
        executor_protocol=True,
    ),
    "episode_check": _definition("episode_check", default_limit=6),
    "read_episode": _definition("read_episode", default_limit=4),
    "read_recent_episodes": _definition("read_recent_episodes", default_limit=4),
    "read_brain": _definition("read_brain", default_limit=4),
    "list_brain_snapshots": _definition("list_brain_snapshots", default_limit=4),
    "read_brain_snapshot": _definition("read_brain_snapshot", default_limit=4),
    "edit_brain": _definition("edit_brain", default_limit=2),
    "restore_brain_section": _definition("restore_brain_section", default_limit=2),
    "list_documents": _definition("list_documents", default_limit=4),
    "list_folder_uploads": _definition("list_folder_uploads", default_limit=4),
    "get_folder_upload_summary": _definition(
        "get_folder_upload_summary",
        default_limit=6,
    ),
    "list_folder_tree": _definition("list_folder_tree", default_limit=6),
    "get_document_info": _definition("get_document_info", default_limit=6),
    "read_document": _definition("read_document", default_limit=6),
    "search_documents": _definition("search_documents", default_limit=8),
    "web_search": _definition("web_search", default_limit=8),
    "news_search": _definition("news_search", default_limit=8),
    "submit_answer": _definition("submit_answer", executor_protocol=True),
    "check_graph_health": _definition("check_graph_health"),
    "propose_entity_merge": _definition("propose_entity_merge"),
    "report_relationship_conflict": _definition("report_relationship_conflict"),
    "list_workspace_files": _definition("list_workspace_files", default_limit=4),
    "read_workspace_file": _definition("read_workspace_file", default_limit=4),
    "create_workspace_file": _definition(
        "create_workspace_file",
        default_limit=2,
    ),
    "update_workspace_file": _definition(
        "update_workspace_file",
        default_limit=2,
    ),
    "append_workspace_file": _definition(
        "append_workspace_file",
        default_limit=2,
    ),
    "save_insight": _definition("save_insight", default_limit=4),
    "spawn_specialist": _definition("spawn_specialist", default_limit=2),
    "search_insights": _definition("search_insights", default_limit=4),
    "vote_insight": _definition("vote_insight", default_limit=4),
    "remove_insight_vote": _definition("remove_insight_vote", default_limit=4),
    "consult_specialist": _definition("consult_specialist", default_limit=2),
}


def get_tool_definition(tool_name: str) -> Optional[ToolDefinition]:
    return TOOL_DEFINITIONS.get(tool_name)


def get_default_tool_limits() -> Dict[str, int]:
    return {
        name: definition.default_limit
        for name, definition in TOOL_DEFINITIONS.items()
        if definition.default_limit is not None
    }


def get_registered_tool_names() -> frozenset[str]:
    """Return every callable tool name owned by the runtime registry."""

    return frozenset(TOOL_DEFINITIONS)


def get_tool_schemas(
    enabled_tools: list[str] | tuple[str, ...] | None = None,
    tags: list[str] | None = None,
    capabilities: list[str] | set[str] | frozenset[str] | None = None,
    additional_schemas: Iterable[dict] = (),
) -> list[dict]:
    """Resolve model-visible schemas from canonical definitions for one run."""

    enabled_set = set(enabled_tools) if enabled_tools is not None else None
    tags_set = set(tags) if tags else None
    capability_set = (
        set(capabilities)
        if capabilities is not None
        else set(SAFE_DEFAULT_CAPABILITIES)
    )
    if capabilities is None and enabled_set is not None:
        capability_set.update(
            TOOL_DEFINITIONS[name].capability
            for name in enabled_set
            if name in TOOL_DEFINITIONS
        )
    invalid_capabilities = capability_set - CAPABILITY_CLASSES
    if invalid_capabilities:
        raise ValueError(
            "Unknown tool capabilities: "
            + ", ".join(sorted(invalid_capabilities))
        )

    additional = tuple(additional_schemas)
    overrides = {
        schema["function"]["name"]: schema
        for schema in additional
        if schema.get("function", {}).get("name") in TOOL_DEFINITIONS
    }
    for name, schema in overrides.items():
        if get_schema_capability(schema) != TOOL_DEFINITIONS[name].capability:
            raise ValueError(
                f"Tool schema override for '{name}' changes its capability"
            )
    filtered: list[dict] = []
    selected_names: set[str] = set()
    for base_schema in TOOL_SCHEMAS:
        name = base_schema["function"]["name"]
        definition = TOOL_DEFINITIONS[name]
        schema = overrides.get(name, definition.schema)
        if definition.executor_protocol:
            filtered.append(schema)
            selected_names.add(name)
            continue

        is_enabled = enabled_set is None or name in enabled_set
        has_capability = definition.capability in capability_set
        has_tag = True
        if tags_set is not None:
            has_tag = bool(set(schema["function"].get("tags", [])) & tags_set)
        if is_enabled and has_capability and has_tag:
            filtered.append(schema)
            selected_names.add(name)

    for schema in additional:
        name = schema.get("function", {}).get("name")
        if name and name not in selected_names:
            filtered.append(schema)
            selected_names.add(name)
    return filtered


def get_runtime_instructions(schemas: Iterable[dict]) -> str:
    instructions = []
    for schema in schemas:
        name = schema.get("function", {}).get("name")
        definition = get_tool_definition(name) if name else None
        instruction = definition.runtime_instruction if definition else None
        if instruction and instruction not in instructions:
            instructions.append(instruction)
    return "\n\n".join(instructions)


@dataclass(frozen=True)
class ToolPermissions:
    user_name: str
    agent_id: str
    project_id: str
    audit_project_id: str | None
    session_id: str
    run_id: str
    allowed_tools: frozenset[str]
    allowed_capabilities: frozenset[str]

    def authorize(self, tool_name: str, capability: str) -> Optional[str]:
        if tool_name not in self.allowed_tools:
            return f"Tool '{tool_name}' is not enabled for this run"
        if capability not in self.allowed_capabilities:
            return f"Capability '{capability}' is not enabled for this run"
        return None


@dataclass(frozen=True)
class ToolRuntime:
    """The immutable tool policy snapshot used by one AgentRun."""

    schemas: tuple[dict, ...]
    permissions: ToolPermissions
    runtime_instructions: str


def build_tool_runtime(
    *,
    enabled_tools: list[str] | tuple[str, ...] | None,
    additional_schemas: Iterable[dict],
    user_name: str,
    agent_id: str,
    project_id: str,
    audit_project_id: str | None,
    session_id: str,
    run_id: str,
) -> ToolRuntime:
    schemas = tuple(
        get_tool_schemas(
            enabled_tools,
            additional_schemas=additional_schemas,
        )
    )
    schema_map = {schema["function"]["name"]: schema for schema in schemas}
    permissions = ToolPermissions(
        user_name=user_name,
        agent_id=agent_id,
        project_id=project_id,
        audit_project_id=audit_project_id,
        session_id=session_id,
        run_id=run_id,
        allowed_tools=frozenset(schema_map),
        allowed_capabilities=frozenset(
            get_schema_capability(schema) for schema in schema_map.values()
        ),
    )
    return ToolRuntime(
        schemas=schemas,
        permissions=permissions,
        runtime_instructions=get_runtime_instructions(schemas),
    )


def install_tool_runtime(
    tools,
    runtime: ToolRuntime,
    references: Dict[str, str],
) -> None:
    """Bind a run's fixed tool policy and local references to its Tools object."""

    tools.active_tool_schemas = {
        schema["function"]["name"]: schema for schema in runtime.schemas
    }
    tools.tool_authorization = runtime.permissions
    tools.short_uuid_references = references


def validate_registry_contract() -> None:
    schemas = [*TOOL_SCHEMAS, *AAC_SPECIFIC_SCHEMAS]
    schema_names = {schema["function"]["name"] for schema in schemas}
    duplicate_names = {
        name
        for name in schema_names
        if sum(schema["function"]["name"] == name for schema in schemas) > 1
    }
    # Community discussion supplies a narrower presentation of the same tool.
    duplicate_names.discard("edit_brain")
    if duplicate_names:
        raise RuntimeError(f"Duplicate tool schemas: {sorted(duplicate_names)}")

    definition_names = set(TOOL_DEFINITIONS)
    if definition_names != schema_names:
        raise RuntimeError(
            "Tool definition contract mismatch: "
            f"missing={sorted(schema_names - definition_names)}, "
            f"extra={sorted(definition_names - schema_names)}"
        )

    for name, definition in TOOL_DEFINITIONS.items():
        if definition.name != name:
            raise RuntimeError(f"Tool definition key/name mismatch: {name}")
        if definition.schema["function"]["name"] != name:
            raise RuntimeError(f"Tool definition '{name}' schema mismatch")
        if definition.capability != get_schema_capability(definition.schema):
            raise RuntimeError(f"Tool definition '{name}' capability mismatch")
        if definition.executor_protocol != (definition.dispatch is None):
            raise RuntimeError(f"Tool definition '{name}' dispatch mismatch")
        if definition.dispatch is None:
            continue
        method_name, parameter_names = definition.dispatch
        if not callable(getattr(Tools, method_name, None)):
            raise RuntimeError(
                f"Tool '{name}' has no concrete method '{method_name}'"
            )
        schema_parameters = set(
            definition.schema["function"].get("parameters", {}).get("properties", {})
        )
        if set(parameter_names) != schema_parameters:
            raise RuntimeError(
                f"Tool '{name}' dispatch/schema parameters differ: "
                f"dispatch={sorted(parameter_names)}, "
                f"schema={sorted(schema_parameters)}"
            )


class Tools(
    SearchTools,
    GraphTools,
    MemoryTools,
    MaintenanceTools,
    HealthTools,
    WorkspaceTools,
):
    def __init__(
        self,
        user_name: str,
        entities: EntityResolver,
        session_id: str,
        compiled_domain: Optional[CompiledDomain] = None,
        search_config: Optional[dict] = None,
        document_service: Optional[DocumentService] = None,
        document_focus: Optional[dict] = None,
        knowledge_retrieval: Optional[KnowledgeRetrieval] = None,
        knowledge_store=None,
        postgres=None,
        agent_id: Optional[str] = None,
        health_service=None,
        workspace_service=None,
    ):
        if knowledge_store is None or postgres is None:
            raise ValueError("Tools requires explicit knowledge_store and postgres")
        if knowledge_retrieval is None:
            raise ValueError("Tools requires a project-scoped KnowledgeRetrieval")

        self.session_id = session_id
        self.knowledge_store = knowledge_store
        self.knowledge_retrieval = knowledge_retrieval
        self.postgres = postgres
        self.entities = entities
        self.user_name = user_name
        self.embedding_service = entities.embedding_service
        self.project_id = entities.project_id
        self.readable_project_ids = entities.readable_project_ids
        self.compiled_domain = compiled_domain
        self.document_service = document_service
        self.workspace_service = workspace_service
        self.document_focus = document_focus
        self.active_topics = (
            list(compiled_domain.active_topics) if compiled_domain else None
        )
        self.search_cfg = search_config or {}
        self.agent_id = agent_id or "AGENT_IDENTITY"
        self.tool_authorization: Optional[ToolPermissions] = None
        self.active_tool_schemas: Dict[str, dict] = {}
        self.short_uuid_references: Dict[str, str] = {}
        self.health_service = health_service

        self._http_client = httpx.AsyncClient(timeout=10.0)

    # Internal-memory tools are formatting/argument adapters only. Retrieval
    # policy, ranking, and evidence expansion live in the
    # project-scoped KnowledgeRetrieval service.
    async def search_messages(self, query: str, limit: int = None):
        return await self.knowledge_retrieval.search_messages(
            query, session_id=self.session_id, limit=limit
        )

    async def search_entity(self, query: str, limit: int = None):
        return await self.knowledge_retrieval.search_entities(
            query, session_id=self.session_id, limit=limit
        )

    async def get_connections(self, entity_name: str):
        return await self.knowledge_retrieval.get_connections(
            entity_name, session_id=self.session_id
        )

    async def get_recent_activity(self, entity_name: str, hours: int = 24):
        return await self.knowledge_retrieval.get_recent_activity(
            entity_name, session_id=self.session_id, hours=hours
        )

    async def episode_check(self, query: str, entity_name: Optional[str] = None):
        return await self.knowledge_retrieval.episode_check(
            query, session_id=self.session_id, entity_name=entity_name
        )

    async def read_episode(self, episode_id: str):
        return await self.knowledge_retrieval.read_episode(
            episode_id, session_id=self.session_id
        )

    async def read_recent_episodes(self, limit: int = 2):
        return await self.knowledge_retrieval.read_recent_episodes(
            session_id=self.session_id, limit=limit
        )

    async def find_path(self, entity_a: str, entity_b: str):
        return await self.knowledge_retrieval.find_path(
            entity_a, entity_b, session_id=self.session_id
        )

    async def get_hot_topic_context(self, hot_topics, *, slim: bool = False):
        return await self.knowledge_retrieval.get_hot_topic_context(
            hot_topics, session_id=self.session_id, slim=slim
        )

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
