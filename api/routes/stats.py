from fastapi import APIRouter, Depends, Request
from api.state import AppState

router = APIRouter()

def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state

@router.get("/")
async def get_stats(state: AppState = Depends(get_app_state)):
    import asyncio
    
    loop = asyncio.get_running_loop()
    graph_stats = await loop.run_in_executor(
        None, 
        state.resources.store.get_graph_stats
    )
    
    sessions = await state.list_sessions()
    agents = await state.list_agents()
    
    return {
        "sessions": len(sessions),
        "agents": len(agents),
        "entities": graph_stats["entities"],
        "facts": graph_stats["facts"],
        "relationships": graph_stats["relationships"]
    }

@router.get("/breakdown")
async def get_stats_breakdown(state: AppState = Depends(get_app_state)):
    """Get entity breakdowns for dashboard charts."""
    import asyncio
    
    loop = asyncio.get_running_loop()
    
    by_type, by_topic, top_connected = await asyncio.gather(
        loop.run_in_executor(None, state.resources.store.get_entity_count_by_type),
        loop.run_in_executor(None, state.resources.store.get_entity_count_by_topic),
        loop.run_in_executor(None, lambda: state.resources.store.get_top_connected_entities(10))
    )
    
    return {
        "by_type": by_type,
        "by_topic": by_topic,
        "top_connected": top_connected
    }