import json
import time
import html
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel

from api.deps import get_app_state
from api.state import AppState
from agent.orchestrator import Orchestrator
from common.schema.dtypes import MessageData
from common.config.base import get_config_value, get_config
from common.infra.redis import RedisKeys

router = APIRouter()
orchestrator = Orchestrator(resources=None)  # Resources will be passed in at runtime via AppState


class ChatRequest(BaseModel):
    message: str
    hot_topics: Optional[List[str]] = None
    model: Optional[str] = None
    timezone: Optional[str] = None
    working_memory: Optional[Dict[str, str]] = None
    client_tools: Optional[List[Dict[str, Any]]] = None





@router.post("/{session_id}")
async def send_message(
    session_id: str,
    body: ChatRequest,
    state: AppState = Depends(get_app_state)
):
    # Ensure orchestrator has access to resources
    if orchestrator._resources is None:
        orchestrator._resources = state.resources

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
    
    # HTML sanitization to prevent prompt injection 
    safe_message = html.escape(body.message)
    
    msg = MessageData(
        message=safe_message,
        timestamp=datetime.now(timezone.utc)
    )
    await context.add(msg)

    config = get_config()
    limits = config.developer_settings.limits
    context_turns = limits.conversation_context_turns or 10
    
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

            async for event in orchestrator.run_stream(
                user_query=safe_message,
                user_name=state.user_name,
                session_id=session_id,
                redis=state.resources.redis,
                model=effective_model,
                agent_id=agent_id,
                enabled_tools=enabled_tools if enabled_tools is not None else (agent.enabled_tools if agent else None),
                user_timezone=body.timezone,
                agent_temperature=agent.temperature if agent else 0.7,
                agent_instructions=agent.instructions if agent else None,
                agent_rules=body.working_memory.get("rules") if body.working_memory else None,
                agent_preferences=body.working_memory.get("preferences") if body.working_memory else None,
                agent_icks=body.working_memory.get("icks") if body.working_memory else None,
                conversation_history=formatted_history,
                client_tools=body.client_tools,
                hot_topics=body.hot_topics,
                agent_persona_override=agent_persona,
                agent_name_override=agent_name
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

                if len(history) <= 1 and not session_meta.get("title"):
                    try:
                        title_prompt = f"User message: {body.message}\nAssistant response: {final_response}\n\nGenerate a short, concise (3-5 words) title for this conversation. Reply with ONLY the title."
                        title = await state.resources.llm_service.call_llm(
                            system="You are an AI that generates concise titles for chat sessions. Respond with only the title, no quotes or prefix.",
                            user=title_prompt
                        )
                        if title:
                            title = title.strip('"').strip()
                            session_meta["title"] = title
                            await state.resources.redis.hset(
                                RedisKeys.sessions(state.user_name),
                                session_id,
                                json.dumps(session_meta)
                            )
                            yield f"event: session_title\ndata: {json.dumps({'title': title})}\n\n"
                    except Exception as e:
                        logger.warning(f"Failed to generate session title: {e}")

                
        except Exception as e:
            logger.error(f"Chat stream generic exception: {e}")
            error_payload = {
                "message": "An unexpected error occurred while processing your message.",
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

