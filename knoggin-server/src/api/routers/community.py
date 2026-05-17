import asyncio
import json

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from loguru import logger

from common.conf.base import get_config, update_config_value
from common.utils.events import CommunityEventEmitter
from infrastructure.redis_client import RedisKeys
from knoggin.community.services.community_manager import CommunityManager
from api.deps import get_app_state
from api.state import AppState

router = APIRouter()


def _get_community_store(app_state: AppState):
    """Lazy singleton community memgraph on app_state."""
    if not hasattr(app_state, "_community_store"):
        from knoggin.community.db.community_store import CommunityStore

        app_state._community_store = CommunityStore(app_state.resources.memgraph.driver)
    return app_state._community_store


@router.post("/toggle")
async def toggle_community(request: Request, enabled: bool):
    update_config_value("developer_settings", {"community": {"enabled": enabled}})
    return {
        "enabled": enabled,
        "message": f"Community {'enabled' if enabled else 'disabled'} successfully.",
    }


@router.get("/discussions")
async def list_discussions(app_state: AppState = Depends(get_app_state)):
    memgraph = _get_community_store(app_state)
    discussions = await memgraph.get_discussions()
    return {"discussions": discussions}


@router.get("/discussions/{discussion_id}")
async def get_discussion_history(discussion_id: str, app_state: AppState = Depends(get_app_state)):
    memgraph = _get_community_store(app_state)
    messages = await memgraph.get_discussion_history(discussion_id)
    return {"discussion_id": discussion_id, "messages": messages}


@router.get("/hierarchy")
async def get_agent_hierarchy(app_state: AppState = Depends(get_app_state)):
    memgraph = _get_community_store(app_state)
    hierarchy = await memgraph.get_agent_hierarchy()
    return {"hierarchy": hierarchy}


@router.websocket("/ws")
async def community_stream(websocket: WebSocket):
    user_name = websocket.app.state.app_state.user_name
    await websocket.accept()

    emitter = CommunityEventEmitter.get()
    queue = await emitter.subscribe(user_name)

    try:
        await websocket.send_text(
            json.dumps({"type": "connected", "user_name": user_name})
        )
        while True:
            event = await queue.get()
            await websocket.send_text(json.dumps(event))
    except WebSocketDisconnect:
        pass
    finally:
        await emitter.unsubscribe(user_name, queue)


@router.get("/stats")
async def get_community_status(app_state: AppState = Depends(get_app_state)):
    redis = app_state.resources.redis

    active_id = await redis.get(RedisKeys.community_discussion_active())
    if active_id:
        active_id = (
            active_id.decode("utf-8") if isinstance(active_id, bytes) else active_id
        )
    config = get_config()
    comm_cfg = config.developer_settings.community

    return {
        "active_discussion_id": active_id,
        "enabled": comm_cfg.enabled,
        "interval_minutes": comm_cfg.interval_minutes,
        "max_turns": comm_cfg.max_turns,
    }


@router.get("/agents/{agent_id}/memory")
async def get_community_agent_memory(agent_id: str, app_state: AppState = Depends(get_app_state)):
    redis = app_state.resources.redis
    user_name = app_state.user_name

    key = RedisKeys.community_agent_memory(user_name, agent_id)
    raw = await redis.hgetall(key)

    entries = []
    if raw:
        for mem_id, payload in raw.items():
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue
            entries.append(
                {
                    "id": mem_id,
                    "content": data.get("content", ""),
                    "discussion_id": data.get("discussion_id"),
                    "created_at": data.get("created_at", ""),
                }
            )
        entries.sort(key=lambda x: x["created_at"])

    return {"agent_id": agent_id, "memory": entries, "total": len(entries)}


@router.get("/insights")
async def get_insights(limit: int = 10, app_state: AppState = Depends(get_app_state)):
    memgraph = _get_community_store(app_state)
    insights = await memgraph.get_discussion_insights(limit)
    return {"insights": insights}


@router.post("/trigger")
async def trigger_discussion_manual(app_state: AppState = Depends(get_app_state)):
    """Manually trigger an AAC discussion for testing."""

    config = get_config()
    comm_cfg = config.developer_settings.community
    if not comm_cfg.enabled:
        raise HTTPException(
            status_code=400,
            detail="Community feature is disabled. Enable it first via POST /toggle.",
        )

    manager = CommunityManager(app_state.resources, app_state.user_name)

    try:
        await manager.trigger_discussion()

        active_id = await app_state.resources.redis.get(
            RedisKeys.community_discussion_active()
        )
        if active_id:
            active_id = (
                active_id.decode("utf-8") if isinstance(active_id, bytes) else active_id
            )

        return {
            "status": "triggered",
            "discussion_id": active_id,
            "message": "Discussion started"
            if active_id
            else "Failed to start a new discussion. Either one is already running, or the system couldn't generate a valid seed topic.",
        }
    except Exception as e:
        logger.error(f"Manual trigger failed: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to start manual discussion due to an internal error.",
        )


@router.post("/close")
async def close_discussion_manual(app_state: AppState = Depends(get_app_state)):
    """Manually end the active AAC discussion."""

    try:
        active_id = await app_state.resources.redis.get(
            RedisKeys.community_discussion_active()
        )
        if not active_id:
            return {"status": "success", "message": "No active discussion to close."}

        await app_state.resources.redis.delete(RedisKeys.community_discussion_active())

        return {
            "status": "success",
            "message": f"Discussion {active_id.decode('utf-8') if isinstance(active_id, bytes) else active_id} has been forcefully closed.",
        }
    except Exception as e:
        logger.error(f"Failed to manually close discussion: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to close discussion due to an internal error.",
        )
