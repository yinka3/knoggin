from typing import Iterable, List, Optional

IDENTITY_SCOPE = "__identity__"
IDENTITY_ENTITY_ID = 1


def build_readable_project_ids(
    project_id: str, allowed_projects: Optional[Iterable[str]] = None
) -> List[str]:
    if not project_id or not project_id.strip():
        raise ValueError("A project_id is required to build readable project scopes")

    readable = [IDENTITY_SCOPE, project_id]
    readable.extend(pid for pid in (allowed_projects or []) if pid)
    return list(dict.fromkeys(readable))


def require_scope_value(value: str, field_name: str, operation: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{operation} requires {field_name} scope")
    return value


def require_visible_project_ids(
    visible_project_ids: Optional[Iterable[str]], operation: str
) -> List[str]:
    project_ids = list(
        dict.fromkeys(
            project_id
            for project_id in (visible_project_ids or [])
            if project_id and project_id.strip()
        )
    )
    if not project_ids:
        raise ValueError(f"{operation} requires visible_project_ids scope")
    return project_ids
