import json
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.state import AppState
from agent.streaming import run_stream
from schema.dtypes import MessageData

router = APIRouter()


def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state


class ChatRequest(BaseModel):
    message: str
    hot_topics: Optional[List[str]] = None
    timezone: Optional[str] = None


@router.post("/{session_id}")
async def send_message(
    session_id: str,
    body: ChatRequest,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    msg = MessageData(
        message=body.message,
        timestamp=datetime.now(timezone.utc)
    )
    await context.add(msg)
    
    async def event_stream():
        final_response = None
        
        try:
            history = await context.get_conversation_context(num_turns=10)
            
            formatted_history = [
                {"role": turn["role"], "content": turn["content"], "timestamp": turn["timestamp"]}
                for turn in history
            ]
            
            async for event in run_stream(
                user_query=body.message,
                user_name=state.user_name,
                session_id=session_id,
                conversation_history=formatted_history,
                hot_topics=body.hot_topics or [],
                topic_config=context.topic_config,
                llm=context.llm,
                store=context.store,
                ent_resolver=context.ent_resolver,
                redis_client=state.resources.redis
            ):
                if event["event"] == "response":
                    final_response = event["data"]["content"]
                
                yield f"event: {event['event']}\ndata: {json.dumps(event['data'])}\n\n"
            
            if final_response:
                await context.add_assistant_turn(
                    content=final_response,
                    timestamp=datetime.now(timezone.utc)
                )
                
        except Exception as e:
            yield f"event: error\ndata: {json.dumps({'message': str(e)})}\n\n"
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )


@router.get("/{session_id}/history")
async def get_history(
    session_id: str,
    limit: int = 40,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    history = await context.get_conversation_context(num_turns=limit)
    
    return {
        "session_id": session_id,
        "messages": [
            {
                "role": turn["role"],
                "content": turn["content"],
                "timestamp": turn["timestamp"],
                "msg_id": turn.get("user_msg_id")
            }
            for turn in history
        ]
    }