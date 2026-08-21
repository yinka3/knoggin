from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple, Union

from common.utils.time_utils import parse_iso_time_or_now
from core.agent.formatters import (
    format_entity_results,
    format_episode_results,
    format_graph_results,
    format_hot_topic_context,
    format_path_results,
    format_retrieved_messages,
)
from core.agent.run import AgentRun


def build_user_message(
    ctx: AgentRun, last_result: Optional[Union[Dict, List[Dict]]] = None
) -> str:
    msg = ""

    if ctx.history:
        recent = ctx.history[-ctx.limits.max_history_turns :]
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
    msg += f"**Calls remaining:** {ctx.limits.max_calls - ctx.call_count}\n"

    if ctx.last_error:
        msg += f"\n**Last action rejected:** {ctx.last_error}\n"

    # Latest tool results — full detail
    if last_result:
        msg += "\n**Last tool result(s):**\n"
        results = last_result if isinstance(last_result, list) else [last_result]
        for r in results:
            tool = r.get("tool", "unknown")
            data = r.get("result", {}).get("data")

            if "error" in r:
                msg += f"- `{tool}`: Error - {r['error']}\n"
            elif tool in ("episode_check", "read_recent_episodes"):
                result_groups = (
                    data.get("results", []) if isinstance(data, dict) else []
                )
                count = sum(
                    len(group.get("episodes", []))
                    for group in result_groups
                    if isinstance(group, dict)
                )
                if count > 0:
                    msg += (
                        f"- `{tool}`: Found {count} episode(s). "
                        "(See 'Retrieved Context' below)\n"
                    )
                else:
                    msg += f"- `{tool}`: No results found.\n"
            elif tool in (
                "search_messages",
                "search_entity",
                "get_connections",
                "get_recent_activity",
                "find_path",
                "read_episode",
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

    if ctx.has_any():
        msg += "\n**Accumulated context:**\n"
        msg += _format_evidence(ctx, last_result)

    return msg


def _format_evidence(
    evidence: AgentRun, last_result: Optional[Union[Dict, List[Dict]]] = None
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
            elif tool in ("search_messages", "read_episode"):
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
            elif tool in ("episode_check", "read_recent_episodes"):
                # Episode checks are structured context rather than list evidence.
                pass

    if evidence.evidence_summary:
        msg += f"**Core Evidence Summary:**\n{evidence.evidence_summary}\n\n"

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

    if evidence.episodes:
        msg += (
            "\n**Episode check results:**\n"
            f"{format_episode_results(evidence.episodes)}\n"
        )

    return msg


def build_evidence_context(evidence: AgentRun) -> str:
    """Serialize all evidence to a string for token counting."""
    return _format_evidence(evidence, last_result=None)


def _merge_unique(target_list: List, new_items, key_func) -> None:
    existing_keys = {key_func(item) for item in target_list}
    for item in new_items:
        k = key_func(item)
        if k not in existing_keys:
            target_list.append(item)
            existing_keys.add(k)


def _trim_oldest(target_list: List, limit: int) -> None:
    if len(target_list) > limit:
        del target_list[:-limit]


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


def _stable_evidence_key(item) -> Tuple:
    return ("json", json.dumps(item, sort_keys=True, default=str))


def _path_evidence_key(item: Dict) -> Tuple:
    if isinstance(item, dict):
        entity_a = item.get("entity_a")
        entity_b = item.get("entity_b")
        if entity_a is not None and entity_b is not None:
            return ("path", entity_a, entity_b)
    return _stable_evidence_key(item)


def _episode_evidence_key(item: Dict) -> Tuple:
    if isinstance(item, dict):
        episode_id = item.get("episode_id", item.get("id"))
        if episode_id is not None:
            return ("episode", episode_id)
    return _stable_evidence_key(item)


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


def update_accumulators(ctx: AgentRun, tool_name: str, result: Dict):
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

    def _acc_unique(target, data, key_func, limit: int):
        items = data if isinstance(data, list) else []
        _merge_unique(target, items, key_func)
        _trim_oldest(target, limit)

    def _acc_unique_extend_or_append(target, data, key_func, limit: int):
        if isinstance(data, dict):
            items = [data]
        elif isinstance(data, list):
            items = data
        else:
            items = []
        _merge_unique(target, items, key_func)
        _trim_oldest(target, limit)

    def _acc_documents(ev, data, cfg):
        _merge_unique(
            ev.messages,
            _normalize_document_chunks(data) if isinstance(data, list) else [],
            _message_evidence_key,
        )
        _trim_oldest(ev.messages, cfg.max_accumulated_messages)

    strategies = {
        "search_messages": lambda ev, d, cfg: _acc_messages(ev, d, cfg),
        "search_entity": lambda ev, d, cfg: _acc_unique(
            ev.profiles,
            d,
            lambda x: x["id"],
            cfg.max_accumulated_profiles,
        ),
        "get_connections": lambda ev, d, cfg: _acc_unique(
            ev.graph,
            d,
            lambda x: (x.get("source"), x.get("target")),
            cfg.max_accumulated_graph,
        ),
        "get_recent_activity": lambda ev, d, cfg: _acc_unique(
            ev.graph,
            d,
            lambda x: (x.get("source"), x.get("target")),
            cfg.max_accumulated_graph,
        ),
        "find_path": lambda ev, d, cfg: _acc_unique(
            ev.paths,
            d,
            _path_evidence_key,
            cfg.max_accumulated_paths,
        ),
        "episode_check": lambda ev, d, cfg: _acc_unique_extend_or_append(
            ev.episodes,
            d,
            _episode_evidence_key,
            cfg.max_accumulated_episodes,
        ),
        "read_recent_episodes": lambda ev, d, cfg: _acc_unique_extend_or_append(
            ev.episodes,
            d,
            _episode_evidence_key,
            cfg.max_accumulated_episodes,
        ),
        "read_episode": lambda ev, d, cfg: _acc_messages(ev, d, cfg),
        "search_documents": lambda ev, d, cfg: _acc_documents(ev, d, cfg),
        "read_document": lambda ev, d, cfg: _acc_documents(ev, d, cfg),
        "web_search": lambda ev, d, cfg: _acc_unique(
            ev.sources,
            d,
            lambda x: x.get("url"),
            cfg.max_accumulated_sources,
        ),
        "news_search": lambda ev, d, cfg: _acc_unique(
            ev.sources,
            d,
            lambda x: x.get("url"),
            cfg.max_accumulated_sources,
        ),
    }

    strategy = strategies.get(tool_name)
    if strategy:
        strategy(ctx, data, ctx.limits)
