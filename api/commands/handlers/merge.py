import json
from datetime import datetime, timezone

from api.commands.registry import command, CommandContext
from shared.redisclient import RedisKeys

UNDO_TTL_SECONDS = 7200


@command("/merge list", description="List pending merge proposals")
async def handle_merge_list(ctx: CommandContext) -> dict:
    redis = ctx.state.resources.redis
    proposals_key = RedisKeys.merge_proposals(ctx.state.user_name, ctx.session_id)
    
    raw_proposals = await redis.lrange(proposals_key, 0, -1)
    
    if not raw_proposals:
        return {"proposals": [], "message": "No pending merge proposals"}
    
    proposals = []
    for idx, raw in enumerate(raw_proposals, start=1):
        p = json.loads(raw)
        proposals.append({
            "index": idx,
            "primary_name": p["primary_name"],
            "secondary_name": p["secondary_name"],
            "score": p.get("llm_score", 0)
        })
    
    return {"proposals": proposals}

@command("/merge approve", description="Approve a merge proposal")
async def handle_merge_approve(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Index required. Use /merge list to see proposals.")
    
    try:
        index = int(ctx.args.strip())
    except ValueError:
        raise ValueError("Index must be a number. Use /merge list to see proposals.")
    
    redis = ctx.state.resources.redis
    store = ctx.state.resources.store
    proposals_key = RedisKeys.merge_proposals(ctx.state.user_name, ctx.session_id)
    
    raw_proposals = await redis.lrange(proposals_key, 0, -1)
    
    if not raw_proposals:
        raise ValueError("No pending merge proposals.")
    
    if index < 1 or index > len(raw_proposals):
        raise ValueError(f"Invalid index. Use /merge list to see {len(raw_proposals)} proposals.")
    
    raw = raw_proposals[index - 1]
    proposal = json.loads(raw)

    primary_id = proposal["primary_id"]
    secondary_id = proposal["secondary_id"]
    
    snapshot = {
        "secondary_id": secondary_id,
        "secondary_name": proposal["secondary_name"],
        "primary_id": primary_id,
        "primary_name": proposal["primary_name"],
        "merged_at": datetime.now(timezone.utc).isoformat()
    }
    
    undo_key = RedisKeys.merge_undo(ctx.session_id, primary_id, secondary_id)
    await redis.setex(undo_key, UNDO_TTL_SECONDS, json.dumps(snapshot))
    
    success = store.merge_entities(primary_id, secondary_id)
    
    if not success:
        await redis.delete(undo_key)
        raise RuntimeError("Merge failed")
    
    await redis.lrem(proposals_key, 1, raw)
    
    if ctx.session_id in ctx.state.active_sessions:
        resolver = ctx.state.active_sessions[ctx.session_id].ent_resolver
        resolver.merge_into(primary_id, secondary_id)
    
    return {
        "merged": True,
        "primary": proposal["primary_name"],
        "secondary": proposal["secondary_name"],
        "undo_available_for": f"{UNDO_TTL_SECONDS // 3600} hours",
        "undo_hint": f"Use '/merge undo {proposal['primary_name']}' to reverse"
    }


@command("/merge reject", description="Reject a merge proposal by index")
async def handle_merge_reject(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Index required. Use /merge list to see proposals.")
    
    try:
        index = int(ctx.args.strip())
    except ValueError:
        raise ValueError("Index must be a number. Use /merge list to see proposals.")
    
    redis = ctx.state.resources.redis
    proposals_key = RedisKeys.merge_proposals(ctx.state.user_name, ctx.session_id)
    
    raw_proposals = await redis.lrange(proposals_key, 0, -1)
    
    if not raw_proposals:
        raise ValueError("No pending merge proposals.")
    
    if index < 1 or index > len(raw_proposals):
        raise ValueError(f"Invalid index. Use /merge list to see {len(raw_proposals)} proposals.")
    
    raw = raw_proposals[index - 1]
    proposal = json.loads(raw)
    
    await redis.lrem(proposals_key, 1, raw)
    
    return {
        "rejected": True,
        "primary": proposal["primary_name"],
        "secondary": proposal["secondary_name"]
    }


@command("/merge undo", description="Undo a recent merge by entity name")
async def handle_merge_undo(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Entity name required")
    
    entity_name = ctx.args.strip().lower()
    redis = ctx.state.resources.redis
    store = ctx.state.resources.store
    
    # Scan for matching undo keys
    matches = []
    async for key in redis.scan_iter(match=f"merge_undo:{ctx.session_id}:*"):
        raw = await redis.get(key)
        if raw:
            snapshot = json.loads(raw)
            if (snapshot["primary_name"].lower() == entity_name or 
                snapshot["secondary_name"].lower() == entity_name):
                matches.append((key, snapshot))
    
    if not matches:
        raise ValueError(f"No undo available for '{ctx.args.strip()}'. Window may have expired.")
    
    if len(matches) > 1:
        options = [f"{s['primary_name']} ← {s['secondary_name']}" for _, s in matches]
        raise ValueError(f"Multiple matches found. Be more specific:\n" + "\n".join(options))
    
    undo_key, snapshot = matches[0]
    
    primary_id = snapshot["primary_id"]
    secondary_id = snapshot["secondary_id"]
    secondary_name = snapshot["secondary_name"]
    
    query = """
    MATCH (p:Entity {id: $primary_id})
    RETURN p.aliases AS aliases
    """
    
    with store.driver.session() as session:
        result = session.run(query, {"primary_id": primary_id}).single()
        if not result:
            raise RuntimeError("Primary entity not found")
        
        current_aliases = result["aliases"] or []
    
    updated_aliases = [a for a in current_aliases if a.lower() != secondary_name.lower()]
    
    recreate_query = """
    CREATE (e:Entity {
        id: $secondary_id,
        canonical_name: $secondary_name,
        aliases: [$secondary_name],
        type: 'unknown',
        last_updated: timestamp(),
        last_mentioned: timestamp()
    })
    
    WITH e
    MATCH (p:Entity {id: $primary_id})
    SET p.aliases = $updated_aliases
    
    RETURN e.id AS id
    """
    
    with store.driver.session() as session:
        result = session.run(recreate_query, {
            "secondary_id": secondary_id,
            "secondary_name": secondary_name,
            "primary_id": primary_id,
            "updated_aliases": updated_aliases
        }).single()
        
        if not result:
            raise RuntimeError("Failed to recreate entity")
    
    # Cleanup undo key
    await redis.delete(undo_key)
    
    if ctx.session_id in ctx.state.active_sessions:
        resolver = ctx.state.active_sessions[ctx.session_id].ent_resolver
        resolver.remove_entities([primary_id, secondary_id])
    
    return {
        "undone": True,
        "restored_entity": secondary_name,
        "primary_entity": snapshot["primary_name"],
        "warning": f"{secondary_name} restored as empty entity. Facts and connections remain on {snapshot['primary_name']}."
    }