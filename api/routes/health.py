import asyncio
from fastapi import APIRouter, Depends, Request
from api.state import AppState

router = APIRouter()

def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state

@router.get("/health")
async def health_check(state: AppState = Depends(get_app_state)):
    checks = {
        "redis": False,
        "memgraph": False
    }
    
    try:
        await state.resources.redis.ping()
        checks["redis"] = True
    except Exception:
        pass
    
    try:
        await asyncio.to_thread(state.resources.store.driver.verify_connectivity)
        checks["memgraph"] = True
    except Exception:
        pass
    
    status = "ok" if all(checks.values()) else "degraded"
    
    return {
        "status": status,
        "checks": checks
    }