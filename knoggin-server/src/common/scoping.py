from typing import Iterable, List, Optional

GLOBAL_PROJECT_SCOPE = "__global__"
IDENTITY_ENTITY_ID = 1


def build_readable_project_ids(
    project_id: Optional[str], allowed_projects: Optional[Iterable[str]] = None
) -> List[str]:
    readable = [GLOBAL_PROJECT_SCOPE]
    if project_id:
        readable.append(project_id)
    readable.extend(pid for pid in (allowed_projects or []) if pid)
    return list(dict.fromkeys(readable))
