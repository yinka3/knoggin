import uuid
from api.commands.registry import command, CommandContext


@command("/pref", description="Add a preference")
async def handle_pref(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Preference text required")
    
    pref_id = str(uuid.uuid4())
    
    success = ctx.state.resources.store.create_preference(
        id=pref_id,
        content=ctx.args.strip(),
        kind="preference",
        session_id=ctx.session_id
    )
    
    if not success:
        raise RuntimeError("Failed to save preference")
    
    return {
        "id": pref_id,
        "content": ctx.args.strip(),
        "kind": "preference"
    }


@command("/ick", description="Add an ick")
async def handle_ick(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Ick text required")
    
    ick_id = str(uuid.uuid4())
    
    success = ctx.state.resources.store.create_preference(
        id=ick_id,
        content=ctx.args.strip(),
        kind="ick",
        session_id=ctx.session_id
    )
    
    if not success:
        raise RuntimeError("Failed to save ick")
    
    return {
        "id": ick_id,
        "content": ctx.args.strip(),
        "kind": "ick"
    }