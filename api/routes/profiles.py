import asyncio
from functools import partial
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Query

from api.state import AppState
from db.store import MemGraphStore

router = APIRouter()

def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state

@router.get("/")
async def list_profiles(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    topic: Optional[str] = None,
    entity_type: Optional[str] = None,
    q: Optional[str] = None,
    state: AppState = Depends(get_app_state)
):
    store: MemGraphStore = state.resources.store
    loop = asyncio.get_running_loop()
    entities, total = await loop.run_in_executor(
        None,
        partial(store.list_entities, limit=limit, offset=offset, topic=topic, entity_type=entity_type, search=q)
    )
    
    return {
        "entities": entities,
        "total": total,
        "limit": limit,
        "offset": offset
    }

@router.get("/{entity_id}")
async def get_profile(
    entity_id: int,
    state: AppState = Depends(get_app_state)
):
    store: MemGraphStore = state.resources.store
    loop = asyncio.get_running_loop()
    
    entity = await loop.run_in_executor(
        None,
        lambda: store.get_entity_by_id(entity_id)
    )
    
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")
    
    facts = await loop.run_in_executor(
        None,
        partial(store.get_facts_for_entity, entity_id, active_only=True)
    )

    entity["facts"] = [
        {"content": f.content, "valid_at": f.valid_at.isoformat() if f.valid_at else None}
        for f in (facts or [])
    ]
    
    entity["connections"] = await loop.run_in_executor(
        None,
        partial(store.get_neighbor_entities, entity_id, limit=20)
    )
    
    parents, children = await asyncio.gather(
        loop.run_in_executor(None, partial(store.get_parent_entities, entity_id)),
        loop.run_in_executor(None, partial(store.get_child_entities, entity_id))
    )
    
    entity["hierarchy"] = {
        "parent": {"id": parents[0]["id"], "name": parents[0]["canonical_name"]} if parents else None,
        "children": [{"id": c["id"], "name": c["canonical_name"]} for c in children]
    }
    
    return entity


@router.delete("/{entity_id}")
async def delete_entity(
    entity_id: int,
    state: AppState = Depends(get_app_state)
):
    store: MemGraphStore = state.resources.store
    loop = asyncio.get_running_loop()
    
    entity = await loop.run_in_executor(
        None,
        lambda: store.get_entity_by_id(entity_id)
    )
    
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")
    
    if entity_id == 1:
        raise HTTPException(status_code=400, detail="Cannot delete user entity")
    
    deleted = await loop.run_in_executor(
        None,
        partial(store.delete_entity, entity_id)
    )
    
    if not deleted:
        raise HTTPException(status_code=500, detail="Failed to delete entity")
    
    # Evict from active EntityResolver caches
    for context in state.active_sessions.values():
        if hasattr(context, 'ent_resolver'):
            context.ent_resolver.remove_entities([entity_id])
    
    return {
        "deleted": True,
        "entity_id": entity_id,
        "canonical_name": entity.get("canonical_name")
    }