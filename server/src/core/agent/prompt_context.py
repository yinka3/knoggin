from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union

from common.utils.time_utils import get_now, parse_iso_time_or_now
from core.agent.formatters import format_hot_topic_context
from core.agent.notebook_renderer import render_notebook
from core.agent.run import AgentRun


def build_user_message(
    ctx: AgentRun, last_result: Optional[Union[Dict, List[Dict]]] = None
) -> str:
    """Compose run metadata, immediate feedback, and the canonical notebook."""

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

    if last_result:
        msg += "\n**Last tool result(s):**\n"
        results = last_result if isinstance(last_result, list) else [last_result]
        for result in results:
            tool = result.get("tool", "unknown")
            data = result.get("result", {}).get("data")

            if "error" in result:
                msg += f"- `{tool}`: Error - {result['error']}\n"
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
                        "(See accumulated notebook below)\n"
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
                data_items = data if isinstance(data, list) else []
                if data_items:
                    msg += (
                        f"- `{tool}`: Found {len(data_items)} items. "
                        "(See accumulated notebook below)\n"
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
            elif not data:
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
        msg += build_evidence_context(ctx)

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


def build_evidence_context(evidence: AgentRun) -> str:
    """Render the same bounded notebook projection used in model prompts."""

    return render_notebook(
        evidence.notebook,
        local_uuid_references=evidence.short_uuid_references,
    )
