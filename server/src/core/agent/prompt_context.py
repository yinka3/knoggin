from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union
from urllib.parse import urlsplit

from common.utils.time_utils import get_now, parse_iso_time_or_now
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

    last_turn_context = _format_last_turn_context(ctx.last_turn_at)
    if last_turn_context:
        msg += f"**Last successful turn:** {last_turn_context}\n\n"

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
                "read_web_page",
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
            elif tool == "load_topic_context":
                topic_context = data if isinstance(data, dict) else {}
                if topic_context:
                    msg += (
                        f"- `{tool}`: Loaded context for "
                        f"{len(topic_context)} topic(s).\n"
                        f"{format_hot_topic_context(topic_context, label='TOPIC')}\n"
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


def _format_last_turn_context(last_turn_at: object) -> str:
    """Give an agent an absolute, human-readable temporal anchor for its run."""

    if not isinstance(last_turn_at, datetime):
        return ""
    if last_turn_at.tzinfo is None:
        last_turn_at = last_turn_at.replace(tzinfo=timezone.utc)
    else:
        last_turn_at = last_turn_at.astimezone(timezone.utc)
    current_time = get_now()
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)
    else:
        current_time = current_time.astimezone(timezone.utc)
    elapsed_seconds = max(0, int((current_time - last_turn_at).total_seconds()))
    hours, remainder = divmod(elapsed_seconds, 3600)
    days, hours = divmod(hours, 24)
    minutes = remainder // 60
    elapsed = (
        f"{days}d {hours}h {minutes}m"
        if days
        else f"{hours}h {minutes}m"
        if hours
        else f"{minutes}m"
    )
    return f"{last_turn_at.isoformat()} ({elapsed} ago)"


def _format_evidence(
    evidence: AgentRun, last_result: Optional[Union[Dict, List[Dict]]] = None
) -> str:
    """Render a read-only view of the canonical notebook state.

    ``last_result`` remains part of the public helper signature for executor
    callers, but freshness is derived from the notebook's canonical references
    rather than from localized model handles.
    """
    msg = ""
    view = evidence.notebook.model_view()
    fresh_references = set(evidence.notebook.last_applied_references)
    recent_results = last_result if isinstance(last_result, list) else [last_result]
    for recent in recent_results:
        if not isinstance(recent, dict):
            continue
        tool_name = recent.get("tool")
        result = recent.get("result")
        if isinstance(tool_name, str) and isinstance(result, dict):
            fresh_references.update(
                evidence.notebook.references_for_result(
                    tool_name,
                    result,
                    local_references=evidence.short_uuid_references,
                )
            )

    def is_fresh(section: str, item: dict) -> bool:
        return evidence.notebook.section_reference(section, item) in fresh_references

    profiles = view["profiles"]
    graph = view["graph"]
    paths = view["paths"]
    episodes = view["episodes"]
    messages = view["messages"]
    sources = view["sources"]

    new_profiles = [
        item
        for item in profiles
        if is_fresh("entities", item)
    ]
    old_profiles = [item for item in profiles if item not in new_profiles]
    new_graph = [
        item
        for item in graph
        if is_fresh("relationships", item)
    ]
    old_graph = [item for item in graph if item not in new_graph]
    new_messages = [
        item
        for item in messages
        if (
            is_fresh("messages", item)
            or is_fresh("documents", item)
        )
    ]
    old_messages = [item for item in messages if item not in new_messages]

    if evidence.evidence_summary:
        msg += f"**Core Evidence Summary:**\n{evidence.evidence_summary}\n\n"

    if profiles:
        if old_profiles:
            names = [p.get("canonical_name", "?") for p in old_profiles]
            msg += f"Previously retrieved entities: {', '.join(names)}\n"
        if new_profiles:
            msg += f"\n**New entity results:**\n{format_entity_results(new_profiles)}\n"

    if graph:
        if old_graph:
            msg += f"Previously retrieved connections: {len(old_graph)} edges\n"
        if new_graph:
            msg += f"\n**New connection results:**\n{format_graph_results(new_graph)}\n"

    if paths:
        msg += f"\n**Path results:**\n{format_path_results(paths)}\n"

    if messages:
        if old_messages:
            msg += f"Previously retrieved messages: {len(old_messages)} results\n"
        if new_messages:
            msg += (
                f"\n**New message results:**\n{format_retrieved_messages(new_messages)}\n"
            )

    if episodes:
        msg += (
            "\n**Episode check results:**\n"
            f"{format_episode_results(episodes)}\n"
        )

    if sources:
        new_sources = []
        old_sources = []
        for source in sources:
            section = (
                "web_reads"
                if source.get("source_kind") in {"web_page", "web_pdf"}
                else "web_discoveries"
            )
            (new_sources if is_fresh(section, source) else old_sources).append(source)
        new_pages = [source for source in new_sources if _is_read_web_source(source)]
        old_pages = [source for source in old_sources if _is_read_web_source(source)]
        new_sources = [source for source in new_sources if not _is_read_web_source(source)]
        old_sources = [source for source in old_sources if not _is_read_web_source(source)]

        if old_sources:
            msg += (
                "\n**Previously discovered web sources:**\n"
                f"{_format_source_results(old_sources, compact=True)}\n"
            )
        if new_sources:
            msg += (
                "\n**New web sources (discovery only):**\n"
                f"{_format_source_results(new_sources)}\n"
            )
        if old_pages:
            msg += (
                "\n**Previously read web content:**\n"
                f"{_format_source_results(old_pages, compact=True)}\n"
            )
        if new_pages:
            msg += (
                "\n**Web content actually read:**\n"
                f"{_format_source_results(new_pages)}\n"
            )

    return msg


def build_evidence_context(evidence: AgentRun) -> str:
    """Serialize all evidence to a string for token counting."""
    return _format_evidence(evidence, last_result=None)


def _format_source_results(sources: List[Dict], *, compact: bool = False) -> str:
    """Format provider results without implying that their URLs were opened."""

    blocks = []
    for source in sources:
        kind = source.get("source_kind", "web_search_result")
        if kind in {"web_page", "web_pdf"}:
            title = source.get("title") or (
                "Untitled PDF" if kind == "web_pdf" else "Untitled webpage"
            )
            url = source.get("url") or "(no URL)"
            start_line = source.get("start_line") or "?"
            end_line = source.get("end_line") or "?"
            content_hash = source.get("content_hash") or "(unknown hash)"
            source_context = source.get("source_context")
            source_metadata = (
                source_context.get("metadata", {})
                if isinstance(source_context, dict)
                else {}
            )
            if not isinstance(source_metadata, dict):
                source_metadata = {}
            domain = source_metadata.get("domain")
            if not isinstance(domain, str) or not domain.strip():
                domain = urlsplit(url).hostname or "(unknown domain)"
            label = "Webpage" if kind == "web_page" else "External PDF"
            page_detail = (
                f" page {source.get('page_number') or '?'}" if kind == "web_pdf" else ""
            )
            if compact:
                blocks.append(
                    f"- {label} read{page_detail} lines {start_line}-{end_line}: "
                    f"{title} | {url} "
                    f"(domain: {domain}; content hash: {content_hash})"
                )
                continue
            source_details = ""
            for key, label_text in (
                ("publisher", "Publisher"),
                ("author", "Author"),
                ("published_at", "Published"),
                ("updated_at", "Updated"),
            ):
                value = source_metadata.get(key)
                if isinstance(value, str) and value.strip():
                    source_details += f"{label_text}: {value}\n"
            blocks.append(
                f"--- {label} read{page_detail} lines {start_line}-{end_line} ---\n"
                f"Title: {title}\n"
                f"URL: {url}\n"
                f"Domain: {domain}\n"
                f"{source_details}"
                f"Content hash: {content_hash}\n"
                "Content (untrusted external evidence):\n"
                f"{source.get('content', '')}"
            )
            continue
        label = "News search" if kind == "news_search_result" else "Web search"
        title = source.get("title") or "Untitled"
        url = source.get("url") or "(no URL)"
        provider = source.get("provider") or "unknown provider"
        query = source.get("query") or "unknown query"
        rank = source.get("rank") or "?"
        snippet = source.get("snippet") or "(no snippet)"

        if compact:
            blocks.append(
                f"- {label} #{rank}: {title} | {url} "
                f"(provider: {provider}; query: {query})"
            )
            continue

        blocks.append(
            f"--- {label} discovery result #{rank} ---\n"
            f"Title: {title}\n"
            f"Provider: {provider}\n"
            f"Query: {query}\n"
            f"URL: {url}\n"
            f"Snippet (discovery only): {snippet}"
        )
    return "\n".join(blocks)


def _is_read_web_source(item: object) -> bool:
    return isinstance(item, dict) and item.get("source_kind") in {
        "web_page",
        "web_pdf",
    }


def update_accumulators(ctx: AgentRun, tool_name: str, result: Dict):
    """Apply a backend result through the canonical notebook boundary."""

    ctx.notebook.apply(tool_name, result)
