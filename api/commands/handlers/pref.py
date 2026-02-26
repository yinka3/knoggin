import uuid
import json
from datetime import datetime, timezone
from api.commands.registry import command, CommandContext
from shared.redisclient import RedisKeys

async def _save_to_agent_memory(ctx: CommandContext, category: str, content: str) -> dict:
    session = await ctx.state.get_session(ctx.session_id)
    if not session:
        raise ValueError("Session not found")
        
    agent_id = session.agent_id
    if not agent_id:
        raise ValueError("No active agent in this session")
        
    mem_id = f"mem_{uuid.uuid4().hex[:8]}"
    
    key = RedisKeys.agent_working_memory(agent_id, category)
    payload = json.dumps({
        "content": content,
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    await ctx.state.resources.redis.hset(key, mem_id, payload)
    
    return {
        "id": mem_id,
        "content": content,
        "category": category
    }

@command("/pref", description="Add a preference to the active Agent's memory")
async def handle_pref(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Preference text required")
    return await _save_to_agent_memory(ctx, "preferences", ctx.args.strip())

@command("/ick", description="Add an ick to the active Agent's memory")
async def handle_ick(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Ick text required")
    return await _save_to_agent_memory(ctx, "icks", ctx.args.strip())

@command("/rule", description="Add a rule to the active Agent's memory")
async def handle_rule(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Rule text required")
    return await _save_to_agent_memory(ctx, "rules", ctx.args.strip())