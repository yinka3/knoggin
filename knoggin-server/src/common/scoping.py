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
