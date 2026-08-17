"""Task-local correlation identifiers for diagnostics only.

The values in this module are intentionally never used for authorization,
database scoping, graph routing, or other business behavior. Those boundaries
continue to receive explicit scope arguments.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token

current_user_name: ContextVar[str | None] = ContextVar(
    "current_user_name",
    default=None,
)
current_project_id: ContextVar[str | None] = ContextVar(
    "current_project_id",
    default=None,
)
current_session_id: ContextVar[str | None] = ContextVar(
    "current_session_id",
    default=None,
)
current_agent_run_id: ContextVar[str | None] = ContextVar(
    "current_agent_run_id",
    default=None,
)
current_ingestion_batch_id: ContextVar[str | None] = ContextVar(
    "current_ingestion_batch_id",
    default=None,
)
current_episode_build_id: ContextVar[str | None] = ContextVar(
    "current_episode_build_id",
    default=None,
)
current_work_id: ContextVar[str | None] = ContextVar(
    "current_work_id",
    default=None,
)

_FIELDS: tuple[tuple[str, ContextVar[str | None]], ...] = (
    ("user", current_user_name),
    ("project", current_project_id),
    ("session", current_session_id),
    ("agent_run", current_agent_run_id),
    ("ingestion_batch", current_ingestion_batch_id),
    ("episode_build", current_episode_build_id),
    ("work", current_work_id),
)


def get_diagnostic_scope() -> dict[str, str]:
    """Return populated correlation identifiers for the current async task."""

    return {
        name: value
        for name, variable in _FIELDS
        if (value := variable.get()) is not None
    }


def format_diagnostic_scope(scope: Mapping[str, str] | None = None) -> str:
    """Format a compact stable scope string suitable for structured log output."""

    values = get_diagnostic_scope() if scope is None else scope
    return " ".join(f"{name}={value}" for name, value in values.items()) or "-"


def inject_diagnostic_scope(record: dict) -> None:
    """Loguru patcher that supplements, but never overwrites, bound log fields."""

    record["extra"].setdefault("diagnostic_scope", format_diagnostic_scope())


def install_diagnostic_log_patcher() -> None:
    """Attach correlation fields to Loguru records without altering its sinks."""

    from loguru import logger

    logger.configure(patcher=inject_diagnostic_scope)


@contextmanager
def diagnostic_scope(
    *,
    user_name: str | None = None,
    project_id: str | None = None,
    session_id: str | None = None,
    agent_run_id: str | None = None,
    ingestion_batch_id: str | None = None,
    episode_build_id: str | None = None,
    work_id: str | None = None,
) -> Iterator[None]:
    """Temporarily attach explicit workflow identifiers to the current task.

    Omitted values inherit any outer scope. Values are restored even when the
    wrapped operation raises or is cancelled.
    """

    values = (
        (current_user_name, user_name),
        (current_project_id, project_id),
        (current_session_id, session_id),
        (current_agent_run_id, agent_run_id),
        (current_ingestion_batch_id, ingestion_batch_id),
        (current_episode_build_id, episode_build_id),
        (current_work_id, work_id),
    )
    tokens: list[tuple[ContextVar[str | None], Token[str | None]]] = []
    try:
        for variable, value in values:
            if value is not None:
                tokens.append((variable, variable.set(value)))
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


install_diagnostic_log_patcher()
