
import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from dataclasses import asdict
from common.utils.events import DebugEventEmitter

router = APIRouter()


@router.websocket("/{session_id}/ws")
async def debug_stream(websocket: WebSocket, session_id: str):
    """
    Stream debug events for a session.
    
    Query params:
        verbose: Include detailed dev-mode events (default: false)
    """
    verbose = websocket.query_params.get("verbose", "false").lower() == "true"
    
    await websocket.accept()
    
    emitter = DebugEventEmitter.get()
    queue = await emitter.subscribe(session_id)
    
    try:
        await websocket.send_text(json.dumps({
            "type": "connected",
            "session_id": session_id,
            "verbose": verbose
        }))
        
        while True:
            event = await queue.get()
            
            if event.verbose_only and not verbose:
                continue
            
            payload = asdict(event)
            del payload["verbose_only"]
            await websocket.send_text(json.dumps(payload))
            
    except WebSocketDisconnect:
        pass
    finally:
        await emitter.unsubscribe(session_id, queue)