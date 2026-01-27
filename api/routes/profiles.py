from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request

from api.state import AppState
from db.store import MemGraphStore

router = APIRouter()

def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state

@router.get("/")
async def list_profiles(
    limit: int = 20,
    offset: int = 0,
    topic: Optional[str] = None,
    type: Optional[str] = None,
    q: Optional[str] = None,
    state: AppState = Depends(get_app_state)
):
    store: MemGraphStore = state.resources.store
    entities, total = store.list_entities(
        limit=limit,
        offset=offset,
        topic=topic,
        entity_type=type,
        search=q
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
    
    entity = store.get_entity_by_id(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")
    
    facts = store.get_facts_for_entity(entity_id, active_only=True)
    entity["facts"] = [
        {"content": f.content, "valid_at": f.valid_at.isoformat() if f.valid_at else None}
        for f in facts
    ]
    
    entity["connections"] = store.get_neighbor_entities(entity_id, limit=20)
    
    parents = store.get_parent_entities(entity_id)
    children = store.get_child_entities(entity_id)
    entity["hierarchy"] = {
        "parent": parents[0]["canonical_name"] if parents else None,
        "children": [c["canonical_name"] for c in children]
    }
    
    return entity