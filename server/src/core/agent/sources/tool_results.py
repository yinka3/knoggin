"""Source-reference extraction from successful agent tool results."""

from __future__ import annotations

from typing import TYPE_CHECKING, Mapping

from loguru import logger

from common.schema.source.references import SourceReferenceCandidate

if TYPE_CHECKING:
    from core.agent.executor import _ToolCall
    from core.agent.run import AgentRun


_TOOL_SOURCE_ENCOUNTERS = {
    "search_project_documents": "document_search",
    "read_project_document": "document_read",
    "search_web": "web_search",
    "search_news": "news_search",
    "read_web_page": "web_read",
}


def capture_tool_source_candidates(
    ctx: AgentRun,
    call: _ToolCall,
    result: Mapping,
) -> list[SourceReferenceCandidate]:
    """Build candidates from a tool result already admitted to the notebook.

    The raw backend result is intentionally consumed here, before model-facing
    result localization removes ``source_context``. Invalid or incomplete tool
    items are not sources and must not turn an otherwise successful tool call
    into an agent failure. This function has no recording side effect: the
    executor calls it only after the matching notebook admission succeeds.
    """

    encounter_kind = _TOOL_SOURCE_ENCOUNTERS.get(call.name)
    if encounter_kind is None or not call.call_id:
        return []

    data = result.get("data")
    if not isinstance(data, list):
        return []

    candidates = []
    for result_position, item in enumerate(data):
        if not isinstance(item, Mapping):
            continue
        source_context = item.get("source_context")
        if not isinstance(source_context, Mapping):
            continue
        try:
            candidate = SourceReferenceCandidate.model_validate(
                {
                    **source_context,
                    "project_id": ctx.project_id,
                    "session_id": ctx.session_id,
                    "encounter_kind": encounter_kind,
                    "agent_run_id": ctx.run_id,
                    "tool_call_id": call.call_id,
                    "result_position": result_position,
                }
            )
        except ValueError as exc:
            logger.warning(
                "Ignoring invalid source context from {} result {}: {}",
                call.name,
                result_position,
                exc,
            )
            continue
        candidates.append(candidate)
    return candidates
