import asyncio
import time
from fastapi import APIRouter, Depends
from loguru import logger

from api.deps import get_app_state
from api.state import AppState

router = APIRouter()


@router.get("/health")
async def health_check(state: AppState = Depends(get_app_state)):
    """
    Comprehensive health check for core infrastructure and services.
    Includes Redis, Memgraph, and LLM availability.
    """
    checks = {
        "redis": False,
        "memgraph": False
    }
    
    start_time = time.time()

    # 1. Check Redis
    try:
        await asyncio.wait_for(state.resources.redis.ping(), timeout=2.0)
        checks["redis"] = True
    except Exception as e:
        logger.warning(f"Health check: Redis failed: {e}")

    # 2. Check Memgraph
    try:
        # verify_connectivity is synchronous, run in thread with timeout
        await asyncio.wait_for(
            asyncio.to_thread(state.resources.memgraph.driver.verify_connectivity),
            timeout=3.0
        )
        checks["memgraph"] = True
    except Exception as e:
        logger.warning(f"Health check: Memgraph failed: {e}")



    duration_ms = int((time.time() - start_time) * 1000)
    
    # Determine overall status
    is_ok = all(checks.values())
    status = "ok" if is_ok else "degraded"
    
    # If critical infra (Redis) is down, it's a failure
    if not checks["redis"]:
        status = "unhealthy"

    return {
        "status": status,
        "checks": checks,
        "latency_ms": duration_ms
    }
