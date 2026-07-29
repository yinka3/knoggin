import inspect
from types import SimpleNamespace

import pytest

from common.conf.model_catalog import CURATED_MODEL_CATALOG, ModelCatalogEntry
from common.schema.aac_schema import AAC_SPECIFIC_SCHEMAS
from common.schema.tool_schema import (
    DESTRUCTIVE_WRITE_CAPABILITY,
    READ_CAPABILITY,
    SAFE_DEFAULT_CAPABILITIES,
    TOOL_SCHEMAS,
    get_filtered_schemas,
    get_schema_capability,
)
from common.schema import primitives
from common.schema.contracts import (
    BatchResult,
    EngineScope,
    GraphMutationPlan,
    RelationshipObservation,
)
from common.schema.events import InternalEvent
from common.schema.settings import RootConfig
from common.utils.event_persistence_policy import normalize_coordination_event
from common.utils.local_references import build_local_id_maps
from core.knowledge.db.write_graph_db import build_graph_mutation_plan
from core.agent.tools.registry import (
    DEFAULT_TOOL_LIMITS,
    TOOL_DISPATCH,
    TOOL_DEFINITIONS,
    TOOL_LAYER_BY_NAME,
    TOOL_LAYERS,
    TOOL_MODULES,
    TOOL_SCHEMA_MODULE_BY_NAME,
    ToolPermissions,
    get_active_tool_names,
    get_default_tool_limits,
    get_runtime_instructions,
    get_tool_layer,
    get_tool_module,
    get_tool_schemas,
    get_tools_by_layer,
    is_core_tool,
    is_feature_tool,
)
from core.agent.types import (
    AgentContext,
    AgentRunIdentity,
    AgentRunConfig,
    AgentState,
    MaintenanceCandidate,
    RetrievedEvidence,
)


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
    context = ToolPermissions(
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

    confirmed = ToolPermissions(
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


@pytest.mark.no_network
def test_agent_tools_have_exactly_one_layer_assignment():
    exposed_names = _schema_names() | set(TOOL_DISPATCH)
    flattened = [
        tool_name
        for tool_names in TOOL_LAYERS.values()
        for tool_name in tool_names
    ]

    assert set(TOOL_LAYER_BY_NAME) == exposed_names
    assert sorted(flattened) == sorted(set(flattened))


@pytest.mark.no_network
def test_agent_tool_layers_mark_brain_as_core_and_features_as_optional():
    assert get_tool_layer("read_brain") == "core_brain"
    assert get_tool_layer("edit_brain") == "core_brain"
    assert get_tool_layer("restore_brain_section") == "core_brain"
    assert is_core_tool("edit_brain") is True

    assert get_tools_by_layer("feature_external") == frozenset(
        {"web_search", "news_search"}
    )
    assert is_feature_tool("web_search") is True
    assert get_tool_layer("save_insight") == "feature_community"
    assert get_tool_layer("spawn_specialist") == "feature_community"
    assert get_tool_layer("check_graph_health") == "feature_maintenance"
    assert get_tool_layer("update_topics") == "feature_project_admin"


@pytest.mark.no_network
def test_agent_runtime_special_tools_are_layered_but_not_feature_tools():
    assert get_tools_by_layer("runtime_special") == frozenset(
        {"request_clarification", "request_replanning", "submit_answer"}
    )
    assert is_core_tool("request_clarification") is False
    assert is_feature_tool("request_clarification") is False
    assert get_tool_layer("unknown_tool") is None


@pytest.mark.no_network
def test_agent_tool_modules_match_layer_ownership():
    assert set(TOOL_MODULES) == set(TOOL_LAYERS)

    flattened = [
        tool_name
        for module in TOOL_MODULES.values()
        for tool_name in module.tools
    ]
    assert sorted(flattened) == sorted(set(flattened))
    assert set(flattened) == set(TOOL_LAYER_BY_NAME)

    for module_name, module in TOOL_MODULES.items():
        assert module.name == module_name
        assert module.layer == module_name
        assert module.tools == TOOL_LAYERS[module.layer]
        for tool_name in module.tools:
            assert get_tool_module(tool_name) == module


@pytest.mark.no_network
def test_tool_definitions_are_the_complete_runtime_catalog():
    assert set(TOOL_DEFINITIONS) == _schema_names()
    for name, definition in TOOL_DEFINITIONS.items():
        assert definition.name == name
        assert definition.schema["function"]["name"] == name
        assert definition.layer == get_tool_layer(name)
        assert definition.module == get_tool_module(name)


@pytest.mark.no_network
def test_removed_compatibility_inputs_cannot_reenter_runtime_contracts():
    batch_parameters = inspect.signature(BatchResult.__init__).parameters
    assert not {
        "entity_ids",
        "entity_message_map",
        "new_entity_ids",
        "alias_updated_ids",
        "alias_updates",
        "candidate_suggestions",
    } & set(batch_parameters)

    local_reference_parameters = inspect.signature(build_local_id_maps).parameters
    assert "use_local_references" not in local_reference_parameters


@pytest.mark.no_network
def test_relationship_observations_are_flat_and_old_record_types_are_absent():
    fields = RelationshipObservation.model_fields

    assert set(fields) == {
        "message_id",
        "entity_a_name",
        "entity_b_name",
        "relationship_type",
        "confidence",
        "context",
        "identity_rooted",
    }
    assert not any(
        hasattr(primitives, name)
        for name in ("ConnectionRecord", "MessageConnections", "MessageUserConnections")
    )

    plan_fields = GraphMutationPlan.__dataclass_fields__
    assert "relationship_writes" in plan_fields
    assert "user_relationship_writes" not in plan_fields
    assert not hasattr(GraphMutationPlan, "to_graph_payloads")

    graph_plan_source = inspect.getsource(build_graph_mutation_plan)
    assert ".entity_pairs" not in graph_plan_source
    assert ".user_connections" not in graph_plan_source


@pytest.mark.no_network
def test_static_model_catalog_is_not_runtime_configuration():
    assert "curated_models" not in RootConfig.model_fields
    assert len(CURATED_MODEL_CATALOG) >= 1
    assert all(isinstance(item, ModelCatalogEntry) for item in CURATED_MODEL_CATALOG)
    assert all(item.id and item.name for item in CURATED_MODEL_CATALOG)


@pytest.mark.no_network
def test_episode_and_event_contracts_have_explicit_owners():
    assert not any(
        hasattr(primitives, name)
        for name in (
            "Episode",
            "EpisodeVersion",
            "MessageEpisode",
            "EntityEpisode",
            "RelationshipEpisode",
            "EpisodeCheckpoint",
        )
    )

    event_parameters = inspect.signature(normalize_coordination_event).parameters
    assert tuple(event_parameters) == ("internal_event",)
    event = InternalEvent(
        ts="2026-01-01T00:00:00Z",
        scope_id="project-1",
        component="job",
        event="timeout",
        data={"job": "episode"},
    )
    fields = normalize_coordination_event(event)
    assert isinstance(fields, dict)
    assert fields["event"] == "job.timeout"


@pytest.mark.no_network
def test_agent_tool_modules_own_known_schema_names():
    schema_names = _schema_names()

    assert set(TOOL_SCHEMA_MODULE_BY_NAME) == schema_names
    for schema_name, module in TOOL_SCHEMA_MODULE_BY_NAME.items():
        assert schema_name in module.schema_names
        assert schema_name in module.tools
        assert get_tool_module(schema_name) == module

    assert TOOL_SCHEMA_MODULE_BY_NAME["web_search"].name == "feature_external"
    assert TOOL_SCHEMA_MODULE_BY_NAME["news_search"].name == "feature_external"
    assert TOOL_SCHEMA_MODULE_BY_NAME["update_topics"].name == (
        "feature_project_admin"
    )
    assert TOOL_SCHEMA_MODULE_BY_NAME["save_insight"].name == "feature_community"
    assert TOOL_SCHEMA_MODULE_BY_NAME["spawn_specialist"].name == (
        "feature_community"
    )


@pytest.mark.no_network
def test_agent_tool_module_defaults_preserve_current_limits():
    expected = {
        "search_messages": 6,
        "get_connections": 8,
        "search_entity": 8,
        "episode_check": 6,
        "read_episode": 4,
        "read_recent_episodes": 4,
        "get_recent_activity": 8,
        "find_path": 8,
        "get_hierarchy": 8,
        "list_documents": 4,
        "list_folder_uploads": 4,
        "get_folder_upload_summary": 6,
        "list_folder_tree": 6,
        "get_document_info": 6,
        "read_document": 6,
        "search_documents": 8,
        "web_search": 8,
        "news_search": 8,
        "update_topics": 1,
        "read_brain": 4,
        "list_brain_snapshots": 4,
        "read_brain_snapshot": 4,
        "edit_brain": 2,
        "restore_brain_section": 2,
        "save_insight": 4,
        "spawn_specialist": 2,
        "request_replanning": 2,
    }

    assert DEFAULT_TOOL_LIMITS == expected
    assert get_default_tool_limits() == expected

    config = AgentRunConfig()
    assert config.get_tool_limit("web_search", config.max_calls) == 8
    assert config.get_tool_limit("edit_brain", config.max_calls) == 2
    assert config.get_tool_limit("check_graph_health", config.max_calls) == (
        config.max_calls
    )


@pytest.mark.no_network
def test_registry_schema_helper_matches_low_level_filtering():
    scenarios = [
        {},
        {"enabled_tools": ["search_messages", "update_topics"]},
        {"tags": ["documents:read"]},
        {"capabilities": [READ_CAPABILITY]},
    ]

    for kwargs in scenarios:
        assert get_active_tool_names(get_tool_schemas(**kwargs)) == (
            get_active_tool_names(get_filtered_schemas(**kwargs))
        )


@pytest.mark.no_network
def test_runtime_instruction_hook_is_owned_by_maintenance_module():
    hooked_modules = [
        module.name
        for module in TOOL_MODULES.values()
        if module.runtime_instructions is not None
    ]

    assert hooked_modules == ["feature_maintenance"]


@pytest.mark.no_network
def test_runtime_instructions_are_empty_without_active_maintenance_candidate():
    ctx = AgentContext(
        config=AgentRunConfig(),
        state=AgentState(),
        evidence=RetrievedEvidence(),
        scope=EngineScope(user_name="ada", session_id="session-1"),
        agent=AgentRunIdentity(
            config=SimpleNamespace(id="agent-1"), name="STELLA", persona=""
        ),
    )
    ctx.maintenance_candidates = [
        MaintenanceCandidate(
            id="topic_evaluation:project-1",
            kind="topic_evaluation",
            reason="Project heartbeat reached 40 messages.",
            suggested_tool="update_topics",
        )
    ]

    assert get_runtime_instructions(ctx, frozenset({"search_messages"})) == ""

    instruction = get_runtime_instructions(ctx, frozenset({"update_topics"}))
    assert "Optional maintenance is available" in instruction
    assert "`update_topics`" in instruction
