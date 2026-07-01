from dataclasses import dataclass
from typing import Dict, Iterable, Optional

import httpx

from common.conf.topics_config import TopicConfig
from common.schema.aac_schema import AAC_SPECIFIC_SCHEMAS
from common.schema.tool_schema import (
    CAPABILITY_CLASSES,
    DESTRUCTIVE_WRITE_CAPABILITY,
    TOOL_SCHEMAS,
    get_schema_capability,
)
from knoggin_server.agent.tools.graph import GraphTools
from knoggin_server.agent.tools.maintenance import MaintenanceTools
from knoggin_server.agent.tools.memory import MemoryTools
from knoggin_server.agent.tools.search import SearchTools
from knoggin_server.agent.tools.topic_tools import TopicTools
from knoggin_server.knowledge.services.document_service import DocumentService
from knoggin_server.knowledge.services.entity_service import EntityManager

TOOL_DISPATCH = {
    "search_messages": ("search_messages", ["query", "limit"]),
    "search_entity": ("search_entity", ["query", "limit"]),
    "get_connections": ("get_connections", ["entity_name"]),
    "get_recent_activity": ("get_recent_activity", ["entity_name", "hours"]),
    "fact_check": ("fact_check", ["entity_name", "query"]),
    "find_path": ("find_path", ["entity_a", "entity_b"]),
    "get_hierarchy": ("get_hierarchy", ["entity_name", "direction"]),
    "read_brain": ("read_brain", []),
    "list_brain_snapshots": ("list_brain_snapshots", []),
    "read_brain_snapshot": ("read_brain_snapshot", ["revision"]),
    "edit_brain": (
        "edit_brain",
        ["section", "content", "expected_revision", "change_note"],
    ),
    "restore_brain_section": (
        "restore_brain_section",
        [
            "section",
            "from_snapshot_revision",
            "expected_current_revision",
            "change_note",
        ],
    ),
    "list_documents": (
        "list_documents",
        [
            "folder_root_id",
            "path_prefix",
            "visibility_scope",
            "limit",
            "use_focus",
        ],
    ),
    "list_folder_uploads": (
        "list_folder_uploads",
        ["visibility_scope", "limit"],
    ),
    "get_folder_upload_summary": (
        "get_folder_upload_summary",
        ["folder_root_id", "use_focus"],
    ),
    "list_folder_tree": (
        "list_folder_tree",
        ["folder_root_id", "path_prefix", "max_depth", "use_focus"],
    ),
    "get_document_info": (
        "get_document_info",
        ["document_id", "relative_path", "use_focus"],
    ),
    "read_document": (
        "read_document",
        [
            "document_id",
            "relative_path",
            "start_line",
            "end_line",
            "use_focus",
        ],
    ),
    "search_documents": (
        "search_documents",
        [
            "query",
            "document_name",
            "relative_path",
            "path_prefix",
            "folder_root_id",
            "limit",
            "use_focus",
        ],
    ),
    "web_search": ("web_search", ["query", "limit", "freshness"]),
    "news_search": ("news_search", ["query", "limit", "freshness"]),
    "request_clarification": None,  # handled specially
    "request_replanning": None,  # handled specially
    "save_insight": ("save_insight", ["content"]),
    "spawn_specialist": (
        "spawn_specialist",
        ["name", "persona", "initial_directives"],
    ),
    "update_topics": ("update_topics", ["add_topics", "deactivate_topics", "reasoning"]),
    "check_graph_health": ("check_graph_health", []),
    "propose_entity_merge": (
        "propose_entity_merge",
        [
            "primary_id",
            "duplicate_id",
            "evidence_fact_ids",
            "reasoning",
            "confidence",
        ],
    ),
}

SPECIAL_TOOL_NAMES = frozenset(
    {"request_clarification", "request_replanning", "submit_answer"}
)


@dataclass(frozen=True)
class ToolAuthorizationContext:
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
) -> ToolAuthorizationContext:
    schema_map = {
        schema["function"]["name"]: schema
        for schema in schemas
    }
    capabilities = frozenset(
        get_schema_capability(schema) for schema in schema_map.values()
    )
    context = ToolAuthorizationContext(
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
    # edit_brain is intentionally specialized for AAC but must retain the
    # same executable method and capability.
    duplicate_names.discard("edit_brain")
    if duplicate_names:
        raise RuntimeError(
            f"Duplicate tool schemas: {sorted(duplicate_names)}"
        )

    dispatch_names = set(TOOL_DISPATCH)
    missing_dispatch = schema_names - SPECIAL_TOOL_NAMES - dispatch_names
    missing_schema = dispatch_names - schema_names
    if missing_dispatch or missing_schema:
        raise RuntimeError(
            "Tool registry contract mismatch: "
            f"missing_dispatch={sorted(missing_dispatch)}, "
            f"missing_schema={sorted(missing_schema)}"
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


class Tools(SearchTools, GraphTools, MemoryTools, TopicTools, MaintenanceTools):
    def __init__(
        self,
        user_name: str,
        entities: EntityManager,
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
        self.document_focus = document_focus
        self.active_topics = topic_config.active_topics if topic_config else None
        self.search_cfg = search_config or {}
        self.agent_id = agent_id or "AGENT_IDENTITY"
        self.topic_refresh_callback = topic_refresh_callback
        self.tool_authorization: Optional[ToolAuthorizationContext] = None
        self.active_tool_schemas: Dict[str, dict] = {}

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
