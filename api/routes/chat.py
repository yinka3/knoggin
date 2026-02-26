import json
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel

from api.deps import get_app_state
from api.state import AppState
from agent.streaming import run_stream
from shared.schema.dtypes import MessageData, AgentConfig
from shared.config import get_config_value
from shared.redisclient import RedisKeys

router = APIRouter()



class ChatRequest(BaseModel):
    message: str
    hot_topics: Optional[List[str]] = None
    model: Optional[str] = None
    timezone: Optional[str] = None


class ExtractFactsRequest(BaseModel):
    content: str
    user_msg_id: int


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
        final_sources = None
        first_tool_start = None
        
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
                enabled_tools=enabled_tools if enabled_tools is not None else (agent.enabled_tools if agent else None),
                file_rag=context.file_rag,
                user_timezone=body.timezone,
                mcp_manager=context.mcp_manager,
                agent_temperature=agent.temperature if agent else 0.7
            ):
                
                if event["event"] == "tool_start":
                    if first_tool_start is None:
                        first_tool_start = time.time()
                    tool_calls_log.append({
                        "tool": event["data"].get("tool"),
                        "args": event["data"].get("args"),
                        "thinking": event["data"].get("thinking"),
                        "_start": time.time(),
                    })
                
                if event["event"] == "tool_result":
                    if tool_calls_log:
                        tc = tool_calls_log[-1]
                        tc["summary"] = event["data"].get("summary")
                        tc["count"] = event["data"].get("count")
                        if "_start" in tc:
                            _start = float(tc.pop("_start"))
                            tc["duration"] = int(round((time.time() - _start) * 1000))

                if event["event"] == "response":
                    final_response = event["data"]["content"]
                    final_usage = event["data"].get("usage")
                    final_sources = event["data"].get("sources")
                elif event["event"] == "clarification":
                    final_response = event["data"]["question"]
                    final_usage = event["data"].get("usage")
                
                yield f"event: {event['event']}\ndata: {json.dumps(event['data'])}\n\n"
            
            if final_response:
                metadata: Dict[str, Any] = {}
                if tool_calls_log:
                    metadata["tool_calls"] = tool_calls_log
                    if first_tool_start is not None:
                        metadata["total_duration"] = round((time.time() - first_tool_start) * 1000)
                if final_usage:
                    metadata["usage"] = final_usage
                if final_sources:
                    metadata["sources"] = final_sources
                
                await context.add_assistant_turn(
                    content=final_response,
                    timestamp=datetime.now(timezone.utc),
                    metadata=metadata or None,
                    user_msg_id=msg.id
                )
                
                # Yield a final event with the msg_id
                yield f"event: msg_id\ndata: {json.dumps({'msg_id': msg.id})}\n\n"

                
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
                        metadata=metadata or None,
                        user_msg_id=msg.id
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


@router.post("/{session_id}/extract")
async def extract_message_facts(
    session_id: str,
    body: ExtractFactsRequest,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
        
    await context._maybe_extract_assistant(body.content, body.user_msg_id)
    return {"status": "success", "message": "Fact extraction triggered successfully"}