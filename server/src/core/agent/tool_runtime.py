from __future__ import annotations

import json
import uuid
from typing import Dict, Optional, Tuple

from loguru import logger

from common.exceptions import ToolExecutionError
from common.schema.agent.tool_contracts import (
    READ_CAPABILITY,
    TOOL_SCHEMAS,
    validate_tool_arguments,
)
from core.agent.tool_references import resolve_agent_tool_arguments
from core.agent.tools.registry import Tools, get_tool_definition

_TOOL_PARAM_TYPES: Dict[str, Dict[str, str]] = {}
for _schema in TOOL_SCHEMAS:
    _fn = _schema.get("function", {})
    _name = _fn.get("name", "")
    _props = _fn.get("parameters", {}).get("properties", {})
    _TOOL_PARAM_TYPES[_name] = {k: v.get("type", "string") for k, v in _props.items()}
def _coerce_arg(value, expected_type: str):
    """Best-effort coercion of LLM-provided arg values to declared schema types."""
    if value is None:
        return value
    if expected_type == "integer":
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (ValueError, TypeError):
            return value
    if expected_type == "string":
        if isinstance(value, str):
            return value
        return str(value)
    if expected_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes"}:
                return True
            if normalized in {"false", "0", "no"}:
                return False
    return value
def summarize_result(tool_name: str, result: Dict) -> Tuple[str, int]:
    """Summarize tool result for trace."""
    if "error" in result:
        return f"Error: {result['error']}", 0

    data = result.get("data")
    if data is None:
        return "No results", 0

    if tool_name in (
        "get_connections",
        "get_recent_activity",
        "search_messages",
        "search_entity",
    ):
        count = len(data) if isinstance(data, list) else 0
        return f"Found {count} results", count

    if tool_name == "find_path":
        if data:
            return f"Path found: {len(data)} hops", len(data)
        return "No path", 0

    if tool_name in ("episode_check", "read_recent_episodes"):
        if isinstance(data, dict):
            res_type = data.get("resolution", "unknown")
            results = data.get("results", [])
            count = len(results)
            return f"Resolved via {res_type} ({count} matches)", count
        return "No results", 0

    if tool_name in ("edit_brain", "restore_brain_section"):
        if "error" in result:
            return f"Error: {result['error']}", 0
        return "Brain updated", 1

    if tool_name in ("read_brain", "list_brain_snapshots", "read_brain_snapshot"):
        return "Brain loaded", 1

    if tool_name in ("search_documents", "read_document", "read_web_page"):
        count = len(data) if isinstance(data, list) else 0
        if count > 0 and "error" not in (data[0] if data else {}):
            if tool_name == "read_document":
                return "Read document content", 1
            if tool_name == "read_web_page":
                return "Read web content", 1
            return f"Found {count} relevant chunks", count
        return "No results", 0

    if tool_name in ("list_documents", "list_folder_uploads", "list_folder_tree"):
        count = len(data) if isinstance(data, list) else 0
        return f"Found {count} items", count

    if tool_name == "get_folder_upload_summary":
        if isinstance(data, dict):
            return "Loaded folder upload summary", 1
        return "No results", 0

    return "Completed", 1


async def execute_tool(tools: Tools, name: str, args: Dict) -> Dict:
    definition = get_tool_definition(name)
    if definition is None or definition.dispatch is None:
        raise ToolExecutionError(name, f"Unknown tool: {name}")

    method_name, param_keys = definition.dispatch
    method = getattr(tools, method_name, None)
    if method is None:
        raise ToolExecutionError(name, f"Tool method not found: {method_name}")

    active_schemas = getattr(tools, "active_tool_schemas", {})
    schema = active_schemas.get(name) or definition.schema
    authorization = getattr(tools, "tool_authorization", None)
    capability = definition.capability if schema else READ_CAPABILITY

    logger.info(f"[TOOL CALL] {name}: {json.dumps(args, default=str)}")
    audit_id = None
    try:
        if authorization is not None:
            rejection = authorization.authorize(name, capability)
            if rejection:
                if capability != READ_CAPABILITY:
                    audit_id = await _start_tool_audit(
                        tools,
                        name,
                        capability,
                        args,
                        authorization,
                    )
                raise ToolExecutionError(name, rejection)
        elif capability != READ_CAPABILITY:
            raise ToolExecutionError(
                name,
                "Write tool execution requires an authorization context",
            )

        if capability != READ_CAPABILITY:
            audit_id = await _start_tool_audit(
                tools,
                name,
                capability,
                args,
                authorization,
            )

        kwargs = dict(args)

        param_types = (
            {
                key: value.get("type", "string")
                for key, value in schema["function"]
                .get("parameters", {})
                .get("properties", {})
                .items()
            }
            if schema
            else _TOOL_PARAM_TYPES.get(name, {})
        )
        for k, v in kwargs.items():
            if k in param_types:
                kwargs[k] = _coerce_arg(v, param_types[k])

        if schema:
            validation_errors = validate_tool_arguments(schema, kwargs)
            if validation_errors:
                raise ToolExecutionError(
                    name,
                    "Invalid arguments: " + "; ".join(validation_errors),
                )

            parameter_names = set(
                schema["function"]
                .get("parameters", {})
                .get("properties", {})
            )
            kwargs = {
                key: value
                for key, value in kwargs.items()
                if key in parameter_names
            }
        else:
            kwargs = {k: args.get(k) for k in param_keys if k in args}

        # Tool schemas validate the model-facing local values first. Only then
        # resolve them for the scoped backend reader/writer call.
        kwargs = resolve_agent_tool_arguments(tools, name, kwargs)
        result = await method(**kwargs)
        if audit_id:
            audit_status = (
                "rejected"
                if isinstance(result, dict) and result.get("error")
                else "succeeded"
            )
            await _safe_finish_tool_audit(
                tools,
                audit_id,
                status=audit_status,
                result=result,
            )
        return {"data": result}
    except ToolExecutionError:
        if audit_id:
            await _safe_finish_tool_audit(
                tools,
                audit_id,
                status="rejected",
                error="Tool execution was rejected.",
            )
        raise
    except Exception:
        if audit_id:
            await _safe_finish_tool_audit(
                tools,
                audit_id,
                status="failed",
                error="Tool execution failed.",
            )
        logger.exception("Tool {} failed", name)
        raise ToolExecutionError(name, "Tool execution failed")


def _redact_audit_value(value):
    if isinstance(value, dict):
        return {
            key: (
                "[REDACTED]"
                if any(
                    marker in key.lower()
                    for marker in ("token", "secret", "password", "api_key")
                )
                else _redact_audit_value(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_audit_value(item) for item in value]
    return value


async def _start_tool_audit(
    tools,
    tool_name: str,
    capability: str,
    arguments: Dict,
    authorization,
) -> str:
    postgres = getattr(tools, "postgres", None)
    if postgres is None or authorization is None:
        raise ToolExecutionError(
            tool_name,
            "Write audit context is unavailable",
        )

    audit_id = str(uuid.uuid4())
    await postgres.execute(
        """
        INSERT INTO public.agent_tool_audits (
            audit_id,
            user_name,
            agent_id,
            project_id,
            session_id,
            run_id,
            tool_name,
            capability,
            arguments,
            status
        ) VALUES (
            %(audit_id)s,
            %(user_name)s,
            %(agent_id)s,
            %(project_id)s,
            %(session_id)s,
            %(run_id)s,
            %(tool_name)s,
            %(capability)s,
            %(arguments)s::jsonb,
            'started'
        )
        """,
        {
            "audit_id": audit_id,
            "user_name": authorization.user_name,
            "agent_id": authorization.agent_id,
            "project_id": authorization.audit_project_id,
            "session_id": authorization.session_id,
            "run_id": authorization.run_id,
            "tool_name": tool_name,
            "capability": capability,
            "arguments": json.dumps(
                _redact_audit_value(arguments),
                default=str,
            ),
        },
    )
    return audit_id


async def _finish_tool_audit(
    tools,
    audit_id: str,
    *,
    status: str,
    result=None,
    error: Optional[str] = None,
) -> None:
    await tools.postgres.execute(
        """
        UPDATE public.agent_tool_audits
        SET status = %(status)s,
            result = %(result)s::jsonb,
            error = %(error)s,
            completed_at = NOW()
        WHERE audit_id = %(audit_id)s
        """,
        {
            "audit_id": audit_id,
            "status": status,
            "result": (
                json.dumps(_redact_audit_value(result), default=str)
                if result is not None
                else None
            ),
            "error": error,
        },
    )


async def _safe_finish_tool_audit(tools, audit_id: str, **values) -> None:
    try:
        await _finish_tool_audit(tools, audit_id, **values)
    except Exception:
        # The durable "started" row still proves the write was authorized and
        # attempted. Do not turn a completed domain write into a false failure
        # solely because its audit completion update had an infrastructure fault.
        logger.exception(
            f"Failed to finish agent tool audit {audit_id}"
        )
