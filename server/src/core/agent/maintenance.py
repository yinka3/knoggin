from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from common.utils.time_utils import get_now_unix
from core.agent.tools.registry import (
    get_active_tool_names,
    get_tool_schemas,
)
from infrastructure.redis_client import RedisKeys

GRAPH_MERGE_SCAN_CANDIDATE = "graph_merge_scan"
DEFAULT_MAINTENANCE_MAX_ATTEMPTS = 3
DEFAULT_MAINTENANCE_COOLDOWN_SECONDS = 3600


@dataclass(frozen=True)
class MaintenanceCandidate:
    """Python-selected autonomous maintenance work offered to an agent run."""

    id: str
    kind: str
    reason: str
    suggested_tool: str
    priority: str = "normal"
    metadata: Dict = field(default_factory=dict)
    attempts: int = 0
    cooldown_until: Optional[float] = None


def active_tool_names(enabled_tools: list[str] | None) -> frozenset[str]:
    return get_active_tool_names(get_tool_schemas(enabled_tools))


def candidate_id(kind: str, project_id: str) -> str:
    return f"{kind}:{project_id}"


async def build_maintenance_candidates(
    *,
    redis,
    user_name: str,
    project_id: str | None,
    enabled_tools: list[str] | None,
    max_attempts: int = DEFAULT_MAINTENANCE_MAX_ATTEMPTS,
) -> list[MaintenanceCandidate]:
    if not project_id:
        return []

    tools = active_tool_names(enabled_tools)
    candidates: list[MaintenanceCandidate] = []

    if "check_graph_health" in tools:
        merge_count = await redis.scard(RedisKeys.merge_queue(user_name, project_id))
        if int(merge_count or 0) > 0:
            candidate = await _candidate_if_available(
                redis=redis,
                user_name=user_name,
                project_id=project_id,
                kind=GRAPH_MERGE_SCAN_CANDIDATE,
                reason=f"Merge queue has {int(merge_count)} candidate entities.",
                suggested_tool="check_graph_health",
                priority="low",
                metadata={"merge_queue_count": int(merge_count)},
                max_attempts=max_attempts,
            )
            if candidate:
                candidates.append(candidate)

    return candidates


async def mark_maintenance_handled(
    redis,
    candidate: MaintenanceCandidate,
    *,
    user_name: str,
    project_id: str,
) -> None:
    await redis.delete(
        RedisKeys.maintenance_attempts(user_name, project_id, candidate.id),
        RedisKeys.maintenance_cooldown(user_name, project_id, candidate.id),
    )


async def record_maintenance_failure(
    redis,
    candidate: MaintenanceCandidate,
    *,
    user_name: str,
    project_id: str,
    cooldown_seconds: int = DEFAULT_MAINTENANCE_COOLDOWN_SECONDS,
) -> int:
    attempts = await redis.incr(
        RedisKeys.maintenance_attempts(user_name, project_id, candidate.id)
    )
    cooldown_until = get_now_unix() + cooldown_seconds
    await redis.set(
        RedisKeys.maintenance_cooldown(user_name, project_id, candidate.id),
        cooldown_until,
        ex=cooldown_seconds,
    )
    return int(attempts)


def find_candidate_for_tool(
    candidates: Iterable[MaintenanceCandidate],
    tool_name: str,
) -> MaintenanceCandidate | None:
    for candidate in candidates:
        if candidate.suggested_tool == tool_name:
            return candidate
    return None


async def _candidate_if_available(
    *,
    redis,
    user_name: str,
    project_id: str,
    kind: str,
    reason: str,
    suggested_tool: str,
    priority: str,
    metadata: dict[str, Any],
    max_attempts: int,
) -> MaintenanceCandidate | None:
    cid = candidate_id(kind, project_id)
    attempts = _safe_int(
        await redis.get(RedisKeys.maintenance_attempts(user_name, project_id, cid))
    )
    if attempts >= max_attempts:
        return None

    cooldown_raw = await redis.get(
        RedisKeys.maintenance_cooldown(user_name, project_id, cid)
    )
    cooldown_until = _safe_float(cooldown_raw)
    if cooldown_until is not None:
        if cooldown_until > get_now_unix():
            return None
        await redis.delete(RedisKeys.maintenance_cooldown(user_name, project_id, cid))
        cooldown_until = None

    return MaintenanceCandidate(
        id=cid,
        kind=kind,
        reason=reason,
        suggested_tool=suggested_tool,
        priority=priority,
        metadata=metadata,
        attempts=attempts,
        cooldown_until=cooldown_until,
    )


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
