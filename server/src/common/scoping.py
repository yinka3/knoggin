from typing import Iterable, List, Optional

IDENTITY_SCOPE = "__identity__"
IDENTITY_ENTITY_ID = 1


def build_readable_project_ids(
    project_id: str, allowed_projects: Optional[Iterable[str]] = None
) -> List[str]:
    project_id = _normalized_scope_value(project_id)
    if project_id is None:
        raise ValueError("A project_id is required to build readable project scopes")

    readable = [IDENTITY_SCOPE, project_id]
    readable.extend(
        normalized
        for project_id in (allowed_projects or [])
        if (normalized := _normalized_scope_value(project_id)) is not None
    )
    return list(dict.fromkeys(readable))


def require_scope_value(value: str, field_name: str, operation: str) -> str:
    normalized = _normalized_scope_value(value)
    if normalized is None:
        raise ValueError(f"{operation} requires {field_name} scope")
    return normalized


def require_visible_project_ids(
    visible_project_ids: Optional[Iterable[str]], operation: str
) -> List[str]:
    project_ids = list(
        dict.fromkeys(
            normalized
            for project_id in (visible_project_ids or [])
            if (normalized := _normalized_scope_value(project_id)) is not None
        )
    )
    if not project_ids:
        raise ValueError(f"{operation} requires visible_project_ids scope")
    return project_ids


def _normalized_scope_value(value: object) -> str | None:
    """Return one nonblank canonical scope string, or no value."""

    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None
