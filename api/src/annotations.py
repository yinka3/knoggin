"""Resolve selected response text before it becomes a user instruction."""

from common.response_annotations import with_annotations  # noqa: F401  (re-exported)
from contracts.sessions import ResponseAnnotation


async def resolve_annotations(runtime, session_id: str, project_id: str,
                              annotations: list[ResponseAnnotation]) -> list[dict]:
    if not annotations:
        return []
    ids = list(dict.fromkeys(item.message_id for item in annotations))
    rows = await runtime.resources.knowledge_store.get_messages_by_ids(
        ids, user_name=runtime.sessions.user_name,
        session_ids=[session_id], visible_project_ids=[project_id],
        discoverable_only=True,
    )
    by_id = {row["id"]: row for row in rows if row["role"] == "assistant"}
    resolved = []
    for item in annotations:
        row = by_id.get(item.message_id)
        if row is None or item.end <= item.start or row["content"][item.start:item.end] != item.quote:
            raise ValueError("Selected response text does not match this session")
        resolved.append(item.model_dump())
    return resolved
