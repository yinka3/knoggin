import asyncio
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from loguru import logger

from routes.main import get_context
from routes.models import ChatRequest
from main.context import Context
from schema.dtypes import MessageData
from agent.loop import run

router = APIRouter(prefix="/chat", tags=["chat"])

TIMEOUT_SECONDS = 60.0


def format_sse(data: dict, event: str = None) -> str:
    lines = []
    if event:
        lines.append(f"event: {event}")
    lines.append(f"data: {json.dumps(data)}")
    lines.append("")
    lines.append("")
    return "\n".join(lines)


@router.post("")
async def chat(request: ChatRequest, context: Context = Depends(get_context)):
    
    async def stream_response():
        yield format_sse({}, "start")
        
        try:
            msg = MessageData(
                message=request.message,
                timestamp=datetime.now(timezone.utc)
            )
            context._fire_and_forget(context.add(msg))
            
            history = [{"role": m.role, "content": m.content} for m in request.history]
            
            topics = context.store.get_topics_by_status()
            active = topics.get("active", []) + topics.get("hot", [])
            hot = topics.get("hot", [])
            
            try:
                result = await asyncio.wait_for(
                    run(
                        user_query=request.message,
                        user_name=context.user_name,
                        conversation_history=history,
                        hot_topics=hot,
                        active_topics=active,
                        llm=context.llm,
                        store=context.store,
                        ent_resolver=context.ent_resolver,
                        redis_client=context.redis_client
                    ),
                    timeout=TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                logger.error(f"STELLA timeout after {TIMEOUT_SECONDS}s")
                yield format_sse({"message": "Request timed out. Please try again."}, "error:timeout")
                return
            
            status = result.get("status")

            if status == "complete":
                turn_id = await context.add_to_conversation_log(
                    role="assistant",
                    content=result["response"],
                    timestamp=datetime.now(timezone.utc)
                )
                context.ent_resolver.add_message(f"turn_{turn_id}", result["response"])
                yield format_sse({"response": result["response"]}, "message")
                yield format_sse({
                    "tools_used": result["tools_used"],
                    "state": result["state"],
                    "profiles": result.get("profiles", []),
                    "messages": result.get("messages", [])
                }, "done")
            
            elif status == "clarification_needed":
                turn_id = await context.add_to_conversation_log(
                    role="assistant",
                    content=result["question"],
                    timestamp=datetime.now(timezone.utc)
                )
                context.ent_resolver.add_message(f"turn_{turn_id}", result["question"])
                yield format_sse({"question": result["question"]}, "clarification")
                yield format_sse({
                    "tools_used": result["tools_used"],
                    "state": result["state"]
                }, "done")
            
            else:
                logger.error(f"Unexpected status: {status}")
                yield format_sse({"message": "Unexpected response format"}, "error:internal")
        
        except Exception as e:
            logger.exception(f"Chat error: {e}")
            yield format_sse({"message": "An unexpected error occurred."}, "error:internal")
    
    return StreamingResponse(
        stream_response(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )