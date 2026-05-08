import asyncio
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_app_state
from api.state import AppState
from infrastructure.database.memgraph_client import MemgraphClient

router = APIRouter()


@router.get("/")
async def list_profiles(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    topic: Optional[str] = None,
    entity_type: Optional[str] = None,
    q: Optional[str] = None,
    state: AppState = Depends(get_app_state),
):
    memgraph: MemgraphClient = state.resources.memgraph
    entities, total = await memgraph.list_entities(
        limit=limit, offset=offset, topic=topic, entity_type=entity_type, search=q
    )

    return {"entities": entities, "total": total, "limit": limit, "offset": offset}


@router.get("/{entity_id}")
async def get_profile(entity_id: int, state: AppState = Depends(get_app_state)):
    memgraph: MemgraphClient = state.resources.memgraph

    entity = await memgraph.get_entity_by_id(entity_id)

    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    facts = await memgraph.get_facts_for_entity(entity_id, active_only=True)

    entity["facts"] = [
        {
            "content": f.content,
            "valid_at": f.valid_at.isoformat() if f.valid_at else None,
        }
        for f in (facts or [])
    ]

    entity["connections"] = await memgraph.get_neighbor_entities(entity_id, limit=20)

    parents, children = await asyncio.gather(
        memgraph.get_parent_entities(entity_id), memgraph.get_child_entities(entity_id)
    )

    entity["hierarchy"] = {
        "parent": {"id": parents[0]["id"], "name": parents[0]["canonical_name"]}
        if parents
        else None,
        "children": [{"id": c["id"], "name": c["canonical_name"]} for c in children],
    }

    return entity


@router.delete("/{entity_id}")
async def delete_entity(entity_id: int, state: AppState = Depends(get_app_state)):
    memgraph: MemgraphClient = state.resources.memgraph

    entity = await memgraph.get_entity_by_id(entity_id)

    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    if entity_id == 1:
        raise HTTPException(status_code=400, detail="Cannot delete user entity")

    deleted = await memgraph.delete_entity(entity_id)

    if not deleted:
        raise HTTPException(status_code=500, detail="Failed to delete entity")

    # Evict from active EntityManager caches
    for context in state.active_sessions.values():
        if hasattr(context, "entities"):
            context.entities.remove_entities([entity_id])

    return {
        "deleted": True,
        "entity_id": entity_id,
        "canonical_name": entity.get("canonical_name"),
    }
