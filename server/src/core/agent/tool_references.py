from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Dict, List

from common.utils.local_references import (
    register_short_uuid_references,
    resolve_local_id,
)

if TYPE_CHECKING:
    from core.agent.run import AgentRun
    from core.agent.tools.registry import Tools

# UUID-backed IDs that can be selected again in a later agent tool call.  The
# model sees compact typed handles (for example ``ep_a3f91c``), not UUIDs.
# Numeric entity and message IDs stay numeric; they are already concise and
# should not be forced through this UUID lookup.
_RESULT_UUID_FIELDS: Dict[str, Dict[str, str]] = {
    "episode_check": {"episode_id": "ep"},
    "read_recent_episodes": {"episode_id": "ep"},
    "list_documents": {
        "document_id": "doc",
        "folder_root_id": "folder",
    },
    "list_folder_uploads": {"folder_root_id": "folder"},
    "get_folder_upload_summary": {
        "document_id": "doc",
        "folder_root_id": "folder",
    },
    "list_folder_tree": {
        "document_id": "doc",
        "folder_root_id": "folder",
    },
    "get_document_info": {
        "document_id": "doc",
        "folder_root_id": "folder",
    },
    "read_document": {
        "document_id": "doc",
        "folder_root_id": "folder",
    },
    "search_documents": {
        "document_id": "doc",
        "folder_root_id": "folder",
    },
    "check_graph_health": {
        "episode_id": "ep",
        "evidence_episode_ids": "ep",
    },
    "propose_entity_merge": {
        "episode_id": "ep",
        "evidence_episode_ids": "ep",
    },
}

_TOOL_ARGUMENT_UUID_FIELDS: Dict[str, Dict[str, str]] = {
    "read_episode": {"episode_id": "ep"},
    "list_documents": {"folder_root_id": "folder"},
    "get_folder_upload_summary": {"folder_root_id": "folder"},
    "list_folder_tree": {"folder_root_id": "folder"},
    "get_document_info": {"document_id": "doc"},
    "read_document": {"document_id": "doc"},
    "search_documents": {"folder_root_id": "folder"},
    "propose_entity_merge": {
        "evidence_episode_ids": "ep",
    },
}

# These scope and graph-internal IDs are not useful to the model and can be
# UUIDs themselves. Keep concise numeric entity/message evidence intact.
_HIDDEN_MODEL_ID_FIELDS = {
    "project_id",
    "session_id",
    "user_id",
    "agent_id",
    "run_id",
    "relationship_id",
}

def _iter_uuid_reference_values(value, field_prefixes: Dict[str, str]):
    if isinstance(value, dict):
        for key, item in value.items():
            prefix = field_prefixes.get(key)
            if prefix is not None:
                if isinstance(item, list):
                    yield prefix, [
                        identifier
                        for identifier in item
                        if isinstance(identifier, str)
                    ]
                elif isinstance(item, str):
                    yield prefix, [item]
            yield from _iter_uuid_reference_values(item, field_prefixes)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_uuid_reference_values(item, field_prefixes)


def _register_result_uuid_references(
    ctx: AgentRun,
    tool_name: str,
    data,
) -> Dict[str, Dict[str, str]]:
    """Return real-ID-to-handle maps and retain handles for this execution."""

    field_prefixes = _RESULT_UUID_FIELDS.get(tool_name, {})
    identifiers_by_prefix: Dict[str, List[str]] = {}
    for prefix, identifiers in _iter_uuid_reference_values(data, field_prefixes):
        identifiers_by_prefix.setdefault(prefix, []).extend(identifiers)

    return {
        prefix: register_short_uuid_references(
            identifiers,
            prefix,
            ctx.short_uuid_references,
        )
        for prefix, identifiers in identifiers_by_prefix.items()
    }


def _localize_uuid_value(
    actual_to_short: Dict[str, str],
    value,
):
    if isinstance(value, list):
        return [_localize_uuid_value(actual_to_short, item) for item in value]
    if not isinstance(value, str):
        return value
    try:
        return actual_to_short[value]
    except KeyError as exc:
        raise ValueError(
            "Missing compact UUID handle for model-facing tool result."
        ) from exc


def _localize_tool_data(
    value,
    field_prefixes: Dict[str, str],
    actual_to_short_by_prefix: Dict[str, Dict[str, str]],
):
    if isinstance(value, list):
        return [
            _localize_tool_data(item, field_prefixes, actual_to_short_by_prefix)
            for item in value
        ]
    if not isinstance(value, dict):
        return value

    localized = {}
    for key, item in value.items():
        prefix = field_prefixes.get(key)
        if prefix is not None:
            # Folder-owned document fields are intentionally nullable for
            # ordinary uploads.  No compact handle is registered for an
            # absent ID, and the model should see that absence unchanged.
            if item is None:
                localized[key] = None
                continue
            localized[key] = _localize_uuid_value(
                actual_to_short_by_prefix[prefix], item
            )
        elif key in _HIDDEN_MODEL_ID_FIELDS:
            continue
        else:
            localized[key] = _localize_tool_data(
                item,
                field_prefixes,
                actual_to_short_by_prefix,
            )
    return localized


def model_safe_tool_result(result: Dict) -> Dict:
    """Remove executor-only source payloads before a result enters prompt state."""
    model_result = deepcopy(result)
    if isinstance(model_result, dict) and "data" in model_result:
        model_result["data"] = _remove_source_context(model_result["data"])
    return model_result


def _remove_source_context(value):
    if isinstance(value, list):
        return [_remove_source_context(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {
        key: _remove_source_context(item)
        for key, item in value.items()
        if key != "source_context"
    }


def localize_agent_tool_result(
    ctx: AgentRun,
    tool_name: str,
    result: Dict,
) -> Dict:
    """Return the model-only version of a backend tool result.

    The backend result remains untouched for hooks and operational handling.
    The returned copy is what enters the agent's evidence and the next LLM turn.
    """

    localized_result = model_safe_tool_result(result)
    if not isinstance(localized_result, dict) or "data" not in localized_result:
        return localized_result

    actual_to_short_by_prefix = _register_result_uuid_references(
        ctx,
        tool_name,
        localized_result["data"],
    )
    localized_result["data"] = _localize_tool_data(
        localized_result["data"],
        _RESULT_UUID_FIELDS.get(tool_name, {}),
        actual_to_short_by_prefix,
    )
    return localized_result


def resolve_agent_tool_arguments(tools: Tools, tool_name: str, args: Dict) -> Dict:
    """Resolve compact UUID handles through the active execution's lookup."""

    local_to_actual = getattr(tools, "short_uuid_references", None)
    if local_to_actual is None:
        return args

    resolved = dict(args)
    for field_name, prefix in _TOOL_ARGUMENT_UUID_FIELDS.get(
        tool_name, {}
    ).items():
        value = resolved.get(field_name)
        if value is None:
            continue

        if isinstance(value, list):
            local_values = [str(item) for item in value]
            if len(local_values) != len(set(local_values)):
                raise ValueError(
                    f"Duplicate local {prefix} references are not allowed."
                )
            resolved[field_name] = [
                _resolve_short_uuid_reference(item, prefix, local_to_actual)
                for item in value
            ]
        else:
            resolved[field_name] = _resolve_short_uuid_reference(
                value,
                prefix,
                local_to_actual,
            )

    return resolved


def _resolve_short_uuid_reference(
    value,
    prefix: str,
    local_to_actual: Dict[str, str],
) -> str:
    """Resolve one correctly typed compact UUID handle."""

    if not isinstance(value, str) or not value.startswith(f"{prefix}_"):
        raise ValueError(f"Expected a {prefix}_ UUID handle for this argument.")
    return str(resolve_local_id(value, local_to_actual))
