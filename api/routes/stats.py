import asyncio
import time
from fastapi import APIRouter, Depends
from loguru import logger

from api.deps import get_app_state
from api.state import AppState
from shared.redisclient import RedisKeys

_stats_cache = {"data": None, "ts": 0}
_breakdown_cache = {"data": None, "ts": 0}
CACHE_TTL = 30

router = APIRouter()

@router.get("/")
async def get_stats(state: AppState = Depends(get_app_state)):
    now = time.time()
    if _stats_cache["data"] and now - _stats_cache["ts"] < CACHE_TTL:
        return _stats_cache["data"]
    
    try:
        loop = asyncio.get_running_loop()
        graph_stats = await loop.run_in_executor(
            None, 
            state.resources.store.get_graph_stats
        )
    except Exception as e:
        logger.error(f"Failed to get graph stats: {e}")
        graph_stats = {"entities": 0, "facts": 0, "relationships": 0}
    
    sessions = await state.list_sessions()
    agents = await state.list_agents()
    
    global_stats_key = RedisKeys.global_stats()
    global_stats_raw = await state.resources.redis.hgetall(global_stats_key)
    
    result = {
        "sessions": len(sessions),
        "agents": len(agents),
        "entities": graph_stats["entities"],
        "facts": graph_stats["facts"],
        "relationships": graph_stats["relationships"],
        "total_tokens": int(global_stats_raw.get("total_tokens", 0) if global_stats_raw else 0),
        "total_cost": float(global_stats_raw.get("total_cost", 0.0) if global_stats_raw else 0.0)
    }
    
    _stats_cache["data"] = result
    _stats_cache["ts"] = now
    return result

@router.get("/breakdown")
async def get_stats_breakdown(state: AppState = Depends(get_app_state)):
    now = time.time()
    if _breakdown_cache["data"] and now - _breakdown_cache["ts"] < CACHE_TTL:
        return _breakdown_cache["data"]
    
    loop = asyncio.get_running_loop()
    
    try:
        by_type, by_topic, top_connected = await asyncio.gather(
            loop.run_in_executor(None, state.resources.store.get_entity_count_by_type),
            loop.run_in_executor(None, state.resources.store.get_entity_count_by_topic),
            loop.run_in_executor(None, lambda: state.resources.store.get_top_connected_entities(10))
        )
    except Exception as e:
        logger.error(f"Failed to get stats breakdown: {e}")
        by_type, by_topic, top_connected = [], [], []
    
    result = {
        "by_type": by_type,
        "by_topic": by_topic,
        "top_connected": top_connected
    }
    
    _breakdown_cache["data"] = result
    _breakdown_cache["ts"] = now
    return result