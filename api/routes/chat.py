import json
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel

from api.state import AppState
from agent.streaming import run_stream
from shared.schema.dtypes import MessageData, AgentConfig
from shared.config import get_config_value
from shared.redisclient import RedisKeys

router = APIRouter()


def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state


class ChatRequest(BaseModel):
    message: str
    hot_topics: Optional[List[str]] = None
    model: Optional[str] = None
    timezone: Optional[str] = None


@router.post("/{session_id}")
async def send_message(
    session_id: str,
    body: ChatRequest,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    sessions = await state.list_sessions()
    session_meta = sessions.get(session_id, {})
    agent_id = session_meta.get("agent_id") or await state.get_default_agent_id()
    enabled_tools = session_meta.get("enabled_tools")
    
    agent = await state.get_agent(agent_id)
    if not agent:
        logger.warning(f"Agent {agent_id} not found, falling back to default")
        agent_id = await state.get_default_agent_id()
        agent = await state.get_agent(agent_id)
        
        session_meta["agent_id"] = agent_id
        await state.resources.redis.hset(
            RedisKeys.sessions(state.user_name),
            session_id,
            json.dumps(session_meta)
        )
    
    agent_persona = agent.persona if agent else ""
    agent_name = agent.name if agent else "STELLA"
    
    effective_model = body.model or context.model or (agent.model if agent else None)
    
    msg = MessageData(
        message=body.message,
        timestamp=datetime.now(timezone.utc)
    )
    await context.add(msg)

    dev_settings = get_config_value("developer_settings", {})
    limits = dev_settings.get("limits", {})
    context_turns = limits.get("conversation_context_turns", 10)
    
    async def event_stream():
        final_response = None
        tool_calls_log = []
        final_usage = None
        
        try:
            history = await context.get_conversation_context(num_turns=context_turns)
            
            formatted_history = [
                {"role": turn["role"], "content": turn["content"], "timestamp": turn["timestamp"]}
                for turn in history
            ]

            async for event in run_stream(
                user_query=body.message,
                user_name=state.user_name,
                session_id=session_id,
                agent_id=agent_id,
                agent_name=agent_name,
                agent_persona=agent_persona,
                conversation_history=formatted_history,
                hot_topics=body.hot_topics or [],
                topic_config=context.topic_config,
                llm=context.llm,
                store=context.store,
                ent_resolver=context.ent_resolver,
                redis_client=state.resources.redis,
                model=effective_model,
                enabled_tools=enabled_tools,
                file_rag=context.file_rag,
                user_timezone=body.timezone
            ):
                
                if event["event"] == "tool_start":
                    tool_calls_log.append({
                        "tool": event["data"].get("tool"),
                        "args": event["data"].get("args"),
                        "thinking": event["data"].get("thinking"),
                    })
                
                if event["event"] == "tool_result":
                    if tool_calls_log:
                        tool_calls_log[-1]["summary"] = event["data"].get("summary")
                        tool_calls_log[-1]["count"] = event["data"].get("count")

                if event["event"] == "response":
                    final_response = event["data"]["content"]
                    final_usage = event["data"].get("usage")
                elif event["event"] == "clarification":
                    final_response = event["data"]["question"]
                    final_usage = event["data"].get("usage")
                
                yield f"event: {event['event']}\ndata: {json.dumps(event['data'])}\n\n"
            
            if final_response:
                metadata = {}
                if tool_calls_log:
                    metadata["tool_calls"] = tool_calls_log
                if final_usage:
                    metadata["usage"] = final_usage
                
                await context.add_assistant_turn(
                    content=final_response,
                    timestamp=datetime.now(timezone.utc),
                    metadata=metadata or None
                )
                
        except Exception as e:
            error_payload = {
                "message": str(e),
                "msg_id": msg.id if msg.id != -1 else None,
                "partial_response": final_response,
                "retryable": True
            }
            
            if final_response:
                try:
                    metadata = {}
                    if tool_calls_log:
                        metadata["tool_calls"] = tool_calls_log
                    if final_usage:
                        metadata["usage"] = final_usage
                    
                    await context.add_assistant_turn(
                        content=final_response,
                        timestamp=datetime.now(timezone.utc),
                        metadata=metadata or None
                    )
                    error_payload["response_saved"] = True
                except Exception:
                    error_payload["response_saved"] = False
            
            yield f"event: error\ndata: {json.dumps(error_payload)}\n\n"
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.get("/{session_id}/history")
async def get_history(
    session_id: str,
    limit: int = Query(40, ge=1, le=100),
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
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
                "msg_id": turn.get("user_msg_id"),
                **(turn.get("metadata") or {})
            }
            for turn in history
        ]
    }