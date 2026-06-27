import json
import uuid
from typing import Dict, List, Optional, Tuple, Union

from loguru import logger

from common.exceptions import ToolExecutionError
from common.schema.tool_schema import (
    READ_CAPABILITY,
    TOOL_SCHEMAS,
    TOOL_SCHEMAS_BY_NAME,
    get_schema_capability,
    validate_tool_arguments,
)
from common.utils.time_utils import parse_iso_time_or_now
from knoggin_server.agent.formatters import (
    format_entity_results,
    format_fact_results,
    format_graph_results,
    format_hierarchy_results,
    format_hot_topic_context,
    format_path_results,
    format_retrieved_messages,
)
from knoggin_server.agent.tools.registry import TOOL_DISPATCH, Tools
from knoggin_server.agent.types import AgentContext, RetrievedEvidence

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


def build_user_message(
    ctx: AgentContext, last_result: Optional[Union[Dict, List[Dict]]] = None
) -> str:
    msg = ""

    if ctx.history:
        recent = ctx.history[-ctx.config.max_history_turns :]
        msg += "**Recent conversation:**\n"
        for turn in recent:
            role = "USER" if turn["role"] == "user" else "AGENT"
            ts = turn.get("timestamp")
            if ts:
                try:
                    dt = parse_iso_time_or_now(ts)
                    msg += f"[{dt.strftime('%H:%M')}] {role}: {turn['content']}\n"
                except Exception:
                    msg += f"{role}: {turn['content']}\n"
            else:
                msg += f"{role}: {turn['content']}\n"
        msg += "\n"

    if ctx.is_community and ctx.current_participants:
        msg += f"**Participants:** {', '.join(ctx.current_participants)}\n\n"

    msg += f"**Query:** {ctx.user_query}\n"
    msg += f"**Calls remaining:** {ctx.config.max_calls - ctx.state.call_count}\n"

    if ctx.state.last_error:
        msg += f"\n**Last action rejected:** {ctx.state.last_error}\n"

    # Latest tool results — full detail
    if last_result:
        msg += "\n**Last tool result(s):**\n"
        results = last_result if isinstance(last_result, list) else [last_result]
        for r in results:
            tool = r.get("tool", "unknown")
            data = r.get("result", {}).get("data")

            if "error" in r:
                msg += f"- `{tool}`: Error - {r['error']}\n"
            elif tool in (
                "search_messages",
                "search_entity",
                "get_connections",
                "get_recent_activity",
                "find_path",
                "fact_check",
                "get_hierarchy",
                "search_documents",
                "read_document",
                "web_search",
                "news_search",
            ):
                data_val = data if isinstance(data, list) else []
                count = len(data_val)
                if count > 0:
                    msg += (
                        f"- `{tool}`: Found {count} items. "
                        "(See 'Retrieved Context' below)\n"
                    )
                else:
                    msg += f"- `{tool}`: No results found.\n"
            else:
                if not data:
                    msg += f"- `{tool}`: No results found\n"
                else:
                    msg += f"- `{tool}`: {json.dumps(data, indent=2, default=str)}\n"

    if ctx.hot_topic_context:
        msg += (
            "\n**Hot topic context (pre-fetched):**\n"
            f"{format_hot_topic_context(ctx.hot_topic_context)}\n"
        )

    if ctx.evidence.has_any():
        msg += "\n**Accumulated context:**\n"
        msg += _format_evidence(ctx.evidence, last_result)

    return msg


def _format_evidence(
    evidence: RetrievedEvidence, last_result: Optional[Union[Dict, List[Dict]]] = None
) -> str:
    """
    Format evidence with full detail for new results,
    compact summary for previously seen data.
    """
    msg = ""

    new_profile_ids = set()
    new_message_keys = set()
    new_graph_keys = set()

    if last_result:
        results = last_result if isinstance(last_result, list) else [last_result]
        for r in results:
            tool = r.get("tool")
            data = r.get("result", {}).get("data")
            if not data or not isinstance(data, list):
                continue
            if tool == "search_entity":
                new_profile_ids = {d.get("id") for d in data if d.get("id")}
            elif tool == "search_messages":
                new_message_keys = {
                    _message_evidence_key(d) for d in data if _message_evidence_key(d)
                }
            elif tool in ("search_documents", "read_document"):
                new_message_keys = {_document_evidence_key(d) for d in data}
            elif tool in ("get_connections", "get_recent_activity"):
                new_graph_keys = {
                    (d.get("source"), d.get("target"))
                    for d in data
                    if d.get("source") and d.get("target")
                }
            elif tool == "fact_check":
                # For fact_check, we'll treat all results in the latest call as 'new'
                pass

    if evidence.summary:
        msg += f"**Core Evidence Summary:**\n{evidence.summary}\n\n"

    if evidence.profiles:
        new_profiles = [p for p in evidence.profiles if p.get("id") in new_profile_ids]
        old_profiles = [
            p for p in evidence.profiles if p.get("id") not in new_profile_ids
        ]

        if old_profiles:
            names = [p.get("canonical_name", "?") for p in old_profiles]
            msg += f"Previously retrieved entities: {', '.join(names)}\n"
        if new_profiles:
            msg += f"\n**New entity results:**\n{format_entity_results(new_profiles)}\n"

    if evidence.graph:
        new_graph = [
            g
            for g in evidence.graph
            if (g.get("source"), g.get("target")) in new_graph_keys
        ]
        old_graph = [
            g
            for g in evidence.graph
            if (g.get("source"), g.get("target")) not in new_graph_keys
        ]

        if old_graph:
            msg += f"Previously retrieved connections: {len(old_graph)} edges\n"
        if new_graph:
            msg += f"\n**New connection results:**\n{format_graph_results(new_graph)}\n"

    if evidence.paths:
        msg += f"\n**Path results:**\n{format_path_results(evidence.paths)}\n"

    if evidence.messages:
        new_msgs = [
            m for m in evidence.messages if _message_evidence_key(m) in new_message_keys
        ]
        old_msgs = [
            m
            for m in evidence.messages
            if _message_evidence_key(m) not in new_message_keys
        ]

        if old_msgs:
            msg += f"Previously retrieved messages: {len(old_msgs)} results\n"
        if new_msgs:
            msg += (
                f"\n**New message results:**\n{format_retrieved_messages(new_msgs)}\n"
            )

    if evidence.hierarchy:
        msg += (
            "\n**Hierarchy results:**\n"
            f"{format_hierarchy_results(evidence.hierarchy)}\n"
        )

    if evidence.facts:
        msg += f"\n**Fact check results:**\n{format_fact_results(evidence.facts)}\n"

    return msg


def build_evidence_context(evidence: RetrievedEvidence) -> str:
    """Serialize all evidence to a string for token counting."""
    return _format_evidence(evidence, last_result=None)


def _merge_unique(target_list: List, new_items, key_func) -> None:
    existing_keys = {key_func(item) for item in target_list}
    for item in new_items:
        k = key_func(item)
        if k not in existing_keys:
            target_list.append(item)
            existing_keys.add(k)


def _message_evidence_key(item: Dict) -> Optional[Tuple]:
    if not isinstance(item, dict):
        return None

    if item.get("source_type") == "document":
        return _document_evidence_key(item)

    item_id = item.get("id")
    if item_id is None:
        return None

    user_name = item.get("user_name")
    session_id = item.get("session_id")
    if user_name or session_id:
        return ("message", user_name, session_id, item_id)
    return ("message", item_id)


def _document_evidence_key(item: Dict) -> Optional[Tuple]:
    if not isinstance(item, dict):
        return None

    document_id = item.get("document_id")
    chunk_index = item.get("chunk_index")
    item_id = item.get("id")

    if document_id is not None or chunk_index is not None:
        return ("document", document_id or "document", chunk_index or 0)
    if isinstance(item_id, str) and item_id.startswith("document:"):
        return ("document", item_id)
    return None


def _normalize_document_chunks(data: List[Dict]) -> List[Dict]:
    """Normalize document retrieval into the standard message shape."""
    return [
        {
            "id": (
                f"document:{chunk.get('document_id', 'document')}:"
                f"{chunk.get('chunk_index', 0)}"
            ),
            "document_id": chunk.get("document_id", "document"),
            "chunk_index": chunk.get("chunk_index", 0),
            "content": chunk.get("content", ""),
            "message": chunk.get("content", ""),
            "role": "document",
            "score": chunk.get("score", 0.5),
            "source": chunk.get("document_name", "uploaded document"),
            "source_type": "document",
            "context": [
                {
                    "role": "document",
                    "timestamp": chunk.get(
                        "document_name", "uploaded document"
                    ),
                    "content": chunk.get("content", ""),
                    "is_hit": True,
                }
            ],
        }
        for chunk in data
        if "error" not in chunk
    ]


def update_accumulators(ctx: AgentContext, tool_name: str, result: Dict):
    """
    Merge newly retrieved tool results into accumulated evidence context.
    Prevents duplicate entries and applies ranking or limits where required.
    """
    if not result or "error" in result:
        return

    data = result.get("data")
    if not data:
        return

    def _acc_messages(ev, data, cfg):
        _merge_unique(
            ev.messages,
            data if isinstance(data, list) else [],
            _message_evidence_key,
        )
        if len(ev.messages) > cfg.max_accumulated_messages:
            ev.messages.sort(
                key=lambda x: x.get("score") if x.get("score") is not None else 0.5,
                reverse=True,
            )
            ev.messages = ev.messages[: cfg.max_accumulated_messages]

    def _acc_extend_or_append(target, data):
        if isinstance(data, dict):
            target.append(data)
        elif isinstance(data, list):
            target.extend(data)

    strategies = {
        "search_messages": lambda ev, d, cfg: _acc_messages(ev, d, cfg),
        "search_entity": lambda ev, d, cfg: _merge_unique(
            ev.profiles, d if isinstance(d, list) else [], lambda x: x["id"]
        ),
        "get_connections": lambda ev, d, cfg: _merge_unique(
            ev.graph,
            d if isinstance(d, list) else [],
            lambda x: (x.get("source"), x.get("target")),
        ),
        "get_recent_activity": lambda ev, d, cfg: _merge_unique(
            ev.graph,
            d if isinstance(d, list) else [],
            lambda x: (x.get("source"), x.get("target")),
        ),
        "find_path": lambda ev, d, cfg: ev.paths.extend(
            d if isinstance(d, list) else []
        ),
        "get_hierarchy": lambda ev, d, cfg: _acc_extend_or_append(ev.hierarchy, d),
        "fact_check": lambda ev, d, cfg: _acc_extend_or_append(ev.facts, d),
        "search_documents": lambda ev, d, cfg: _merge_unique(
            ev.messages,
            _normalize_document_chunks(d) if isinstance(d, list) else [],
            _message_evidence_key,
        ),
        "read_document": lambda ev, d, cfg: _merge_unique(
            ev.messages,
            _normalize_document_chunks(d) if isinstance(d, list) else [],
            _message_evidence_key,
        ),
        "web_search": lambda ev, d, cfg: _merge_unique(
            ev.sources, d if isinstance(d, list) else [], lambda x: x.get("url")
        ),
        "news_search": lambda ev, d, cfg: _merge_unique(
            ev.sources, d if isinstance(d, list) else [], lambda x: x.get("url")
        ),
    }

    strategy = strategies.get(tool_name)
    if strategy:
        strategy(ctx.evidence, data, ctx.config)


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

    if tool_name == "fact_check":
        if isinstance(data, dict):
            res_type = data.get("resolution", "unknown")
            results = data.get("results", [])
            count = len(results)
            return f"Resolved via {res_type} ({count} matches)", count
        return "No results", 0

    if tool_name == "edit_brain":
        if "error" in result:
            return f"Error: {result['error']}", 0
        return "Brain updated", 1

    if tool_name == "read_brain":
        return "Brain loaded", 1

    if tool_name in ("search_documents", "read_document"):
        count = len(data) if isinstance(data, list) else 0
        if count > 0 and "error" not in (data[0] if data else {}):
            if tool_name == "read_document":
                return "Read document content", 1
            return f"Found {count} relevant chunks", count
        return "No results", 0

    if tool_name in ("list_documents", "list_folder_uploads", "list_folder_tree"):
        count = len(data) if isinstance(data, list) else 0
        return f"Found {count} items", count

    if tool_name == "get_folder_upload_summary":
        if isinstance(data, dict):
            return "Loaded folder upload summary", 1
        return "No results", 0

    if tool_name == "request_replanning":
        return "Requested a new plan", 1

    return "Completed", 1


async def execute_tool(tools: Tools, name: str, args: Dict) -> Dict:

    if name == "request_clarification":
        return {"clarification": args.get("question", "Could you clarify?")}
    if name == "request_replanning":
        return {"replanning": args.get("reason", "No reason provided")}

    dispatch_entry = TOOL_DISPATCH.get(name)
    if dispatch_entry is None:
        raise ToolExecutionError(name, f"Unknown tool: {name}")

    method_name, param_keys = dispatch_entry
    method = getattr(tools, method_name, None)
    if method is None:
        raise ToolExecutionError(name, f"Tool method not found: {method_name}")

    active_schemas = getattr(tools, "active_tool_schemas", {})
    schema = active_schemas.get(name) or TOOL_SCHEMAS_BY_NAME.get(name)
    authorization = getattr(tools, "tool_authorization", None)
    capability = get_schema_capability(schema) if schema else READ_CAPABILITY

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
    except Exception as e:
        if audit_id:
            await _safe_finish_tool_audit(
                tools,
                audit_id,
                status="failed",
                error=str(e),
            )
        logger.error(f"Tool {name} failed: {e}")
        raise ToolExecutionError(name, str(e))


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
            confirmation_state,
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
            %(confirmation_state)s,
            %(arguments)s::jsonb,
            'started'
        )
        """,
        {
            "audit_id": audit_id,
            "user_name": authorization.user_name,
            "agent_id": authorization.agent_id,
            "project_id": authorization.project_id,
            "session_id": authorization.session_id,
            "run_id": authorization.run_id,
            "tool_name": tool_name,
            "capability": capability,
            "confirmation_state": authorization.confirmation_state,
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
